#!/usr/bin/env python3
"""
Multi-Pipeline Kafka Consumer  —  FINAL STABLE VERSION
Pipelines : Animal Detection (AN) | Head Count (HC) | Entry/Exit (EE)
Server    : 10.1.41.56  |  NVIDIA L40S  |  ee-venv
Dashboard : http://0.0.0.0:8675

FIXES APPLIED
─────────────────────────────────────────────────────────────────────────────
1. COCO class IDs corrected: 16-21 (cat→elephant).
2. EE_MAX_AGE reduced 90→30 to prune ghost boxes faster.
3. SKIP_FRAMES set to 1 for Kalman filter consistency.
4. GPU semaphore (_gpu_semaphore) caps concurrency at 3 to prevent starvation.
5. Dead-track cleanup for memory stability.
6. EE bbox drawing gated on in_roi.
7. [NEW] JS Timestamp Sequencing: Prevents out-of-order frames in the dashboard.
8. [NEW] Traffic Pipeline Alignment: Nanosecond timestamps, unified group ID, and increased poll limits.
"""


# ── Eventlet monkey-patch MUST be first ──────────────────────────────────────
import eventlet
eventlet.monkey_patch()

# ── Standard library ──────────────────────────────────────────────────────────
import base64
import csv
import gc
import json
import logging
import math
import os
import re
import struct
import threading
import time
import warnings
from collections import deque
from datetime import datetime, timezone

# ── Third-party ───────────────────────────────────────────────────────────────
import cv2
import numpy as np
import torch
import pynvml
from flask import Flask, Response
from flask_socketio import SocketIO, join_room
from kafka import KafkaConsumer
from ultralytics import YOLO

# ── DeepSort (Entry/Exit) ─────────────────────────────────────────────────────
import sys
sys.path.insert(0, '/data/Entry_Exit')
sys.path.insert(0, '/data/Entry_Exit/deep_sort')
sys.path.insert(0, '/data/Entry_Exit/tools')
import generate_detections as gdet
from deep_sort import nn_matching
from deep_sort.detection import Detection
from deep_sort.tracker import Tracker

import tensorflow as tf   # TF 2.18 — Re-ID encoder backend

warnings.filterwarnings('ignore')


# ──────────────────────────────────────────────────────────────────────────────
# 1. CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────

# ── Kafka ─────────────────────────────────────────────────────────────────────
BROKERS  = '10.1.40.43:9092,10.1.40.44:9093,10.1.40.45:9094'
GROUP_ID = 'MULTI_PIPELINE_CONSUMER_V1'

# ── Model paths ───────────────────────────────────────────────────────────────
MODEL_ANIMAL_PATH    = '/data/Animal_Detection/models/yolov8m.pt'
MODEL_HEADCOUNT_PATH = '/data/Head_count/yolov8-3b2-100_200.pt'
MODEL_EE_YOLO_PATH   = '/data/Entry_Exit/yolov5m.pt'
REID_MODEL_PATH      = '/data/Entry_Exit/mars-small128.pb'
BOTSORT_CFG          = '/data/Animal_Detection/botsort.yaml'
EE_CAMERA_CFG        = '/data/Entry_Exit/camera_config.json'

# ── Output ────────────────────────────────────────────────────────────────────
BASE_OUTPUT_DIR = '/data/multi_pipeline_consumer/output'

# ── Resolution ────────────────────────────────────────────────────────────────
FRAME_W = 640
FRAME_H = 360

# ── Entry/Exit resolution lock (ROIs calibrated to this) ─────────────────────
EE_WIDTH  = 1024
EE_HEIGHT = 576

# ── Inference settings ────────────────────────────────────────────────────────
SKIP_FRAMES       = 1
AN_CONF_THRESH    = 0.55
HC_CONF_THRESH    = 0.30
EE_CONF_THRESH    = 0.45

AN_ANIMAL_CLASSES = [16, 17, 18, 19, 20, 21]
HC_PERSON_CLASS   = [0]
EE_PERSON_CLASS   = [0]
AN_WARMUP_FRAMES  = 3
HC_WARMUP_FRAMES  = 3

# ── Head Count CSV interval ───────────────────────────────────────────────────
HC_INTERVAL_SEC = 10.0
HC_STATS_EVERY  = 50

# ── Entry/Exit DeepSort params ────────────────────────────────────────────────
EE_MAX_COSINE_DIST = 0.3
EE_NN_BUDGET       = None
EE_MAX_AGE         = 30
EE_N_INIT          = 3
EE_MAX_IOU_DIST    = 0.7
EE_BATCH_SIZE      = 4
EE_LOOKBACK_FRAMES = 8
EE_DOT_THRESH      = 3.0
EE_CSV_INTERVAL_SEC = 300

# ── Web dashboard ─────────────────────────────────────────────────────────────
PORT = 8675

# ── Save annotated video ──────────────────────────────────────────────────────
SAVE_VIDEO = True

# GPU semaphore — limits concurrent CUDA calls
_gpu_semaphore = threading.Semaphore(3)


# ──────────────────────────────────────────────────────────────────────────────
# 2. CAMERA → PIPELINE REGISTRY
# ──────────────────────────────────────────────────────────────────────────────
CAMERA_REGISTRY = [
    {'topic': 'video.raw.g1_ol',   'folder': 'gate_1_outside_left',         'ip': '10.1.34.236', 'AN': True,  'HC': True,  'EE': True },
    {'topic': 'video.raw.g1_me',   'folder': 'gate_1_main_entry',           'ip': '10.1.34.235', 'AN': True,  'HC': True,  'EE': True },
    {'topic': 'video.raw.g2_en',   'folder': 'gate_2_entry_camera',         'ip': '10.1.34.238', 'AN': True,  'HC': True,  'EE': True },
    {'topic': 'video.raw.g2_ex',   'folder': 'gate_2_exit_camera',          'ip': '10.1.34.239', 'AN': True,  'HC': True,  'EE': True },
    {'topic': 'video.raw.a2_ez',   'folder': 'a2_gf_electronic_zone',       'ip': '10.1.34.59',  'AN': False, 'HC': True,  'EE': False},
    {'topic': 'video.raw.a2_mk',   'folder': 'a2_gf_makerspace_worktops',   'ip': '10.1.34.54',  'AN': False, 'HC': True,  'EE': False},
    {'topic': 'video.raw.dr2_da1', 'folder': 'dr2_1f_dining_area_1',        'ip': '10.1.34.224', 'AN': False, 'HC': True,  'EE': False},
    {'topic': 'video.raw.dr2_dc2', 'folder': 'dr2_gf_dining_cam_2',         'ip': '10.1.34.249', 'AN': False, 'HC': True,  'EE': False},
    {'topic': 'video.raw.sc_d58',  'folder': 'd58_summer_court_2',          'ip': '10.1.35.51',  'AN': True,  'HC': True,  'EE': False},
    {'topic': 'video.raw.gh_od1',  'folder': 'gh_gf_outdoor_dining_area_1', 'ip': '10.1.34.105', 'AN': True,  'HC': False, 'EE': False},
]

CAM_REGISTRY_MAP = {c['topic']: c for c in CAMERA_REGISTRY}
ALL_TOPICS       = [c['topic'] for c in CAMERA_REGISTRY]


# ──────────────────────────────────────────────────────────────────────────────
# 3. ENTRY/EXIT CAMERA CONFIG LOADER
# ──────────────────────────────────────────────────────────────────────────────
EE_CAM_CONFIGS = {}

def load_ee_camera_configs():
    global EE_CAM_CONFIGS
    with open(EE_CAMERA_CFG, 'r') as f:
        raw = json.load(f)

    ip_pattern = re.compile(r'\((\d+\.\d+\.\d+\.\d+)\)')
    for key, val in raw.items():
        m = ip_pattern.search(key)
        if not m:
            continue
        ip = m.group(1)
        EE_CAM_CONFIGS[ip] = {
            'roi'  : np.array(val['roi'], dtype=np.int32),
            'vec_x': val['vec_x'],
            'vec_y': val['vec_y'],
        }
    logging.info(f"[EE Config] Loaded ROI configs for {len(EE_CAM_CONFIGS)} cameras.")


# ──────────────────────────────────────────────────────────────────────────────
# 4. FLASK + SOCKETIO
# ──────────────────────────────────────────────────────────────────────────────
app      = Flask(__name__)
socketio = SocketIO(app, async_mode='eventlet',
                    cors_allowed_origins='*',
                    logger=False, engineio_logger=False)

_latest_jpeg = {t: None for t in ALL_TOPICS}
_latest_ts   = {t: 0.0 for t in ALL_TOPICS}
_jpeg_lock   = {t: threading.Lock() for t in ALL_TOPICS}


def publish_frame(topic: str, frame: np.ndarray, ts_sec: float):
    ok, enc = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 78])
    if not ok:
        return
    jpg_bytes = enc.tobytes()
    with _jpeg_lock[topic]:
        _latest_jpeg[topic] = jpg_bytes
        _latest_ts[topic]   = ts_sec
    b64 = base64.b64encode(jpg_bytes).decode('ascii')
    # Sending ts to help dashboard maintain ordering
    socketio.emit('frame', {'cam': topic, 'img': b64, 'ts': ts_sec}, room=topic)


@socketio.on('connect')
def on_connect():
    from flask import request
    logging.info(f"[WS] Client connected: {request.sid}")


@socketio.on('disconnect')
def on_disconnect():
    from flask import request
    logging.info(f"[WS] Client disconnected: {request.sid}")


@socketio.on('subscribe')
def on_subscribe(data):
    cams = data.get('cams', ALL_TOPICS)
    for cam in cams:
        if cam in ALL_TOPICS:
            join_room(cam)
    for cam in cams:
        with _jpeg_lock[cam]:
            buf = _latest_jpeg[cam]
            ts  = _latest_ts[cam]
        if buf:
            b64 = base64.b64encode(buf).decode('ascii')
            socketio.emit('frame', {'cam': cam, 'img': b64, 'ts': ts})


@app.route('/')
def index():
    return _build_dashboard_html()


@app.route('/snapshot/<path:topic>')
def snapshot(topic):
    if topic not in _latest_jpeg:
        return 'Unknown topic', 404
    with _jpeg_lock[topic]:
        buf = _latest_jpeg[topic]
    if buf is None:
        blank = np.zeros((FRAME_H, FRAME_W, 3), dtype=np.uint8)
        _, enc = cv2.imencode('.jpg', blank)
        buf = enc.tobytes()
    return Response(buf, mimetype='image/jpeg',
                    headers={'Cache-Control': 'no-store'})


# ──────────────────────────────────────────────────────────────────────────────
# 5. DASHBOARD HTML
# ──────────────────────────────────────────────────────────────────────────────
def _build_dashboard_html() -> str:
    cam_meta = []
    for c in CAMERA_REGISTRY:
        badges = []
        if c['HC']: badges.append('HC')
        if c['EE']: badges.append('EE')
        if c['AN']: badges.append('AN')
        cam_meta.append({
            'topic' : c['topic'],
            'label' : c['folder'].replace('_', ' ').title(),
            'badges': badges,
        })
    cam_meta_js = json.dumps(cam_meta)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Campus AI Monitoring</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;600&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.min.js"></script>
<style>
  :root {{
    --bg:       #080c10;
    --surface:  #0e1318;
    --border:   #1c2530;
    --accent:   #00e5ff;
    --accent2:  #ff6b35;
    --accent3:  #39ff14;
    --text:     #c8d8e8;
    --muted:    #4a5f72;
    --hc:       #39ff14;
    --ee:       #ff6b35;
    --an:       #ffdd00;
    --font-mono: 'Space Mono', monospace;
    --font-body: 'DM Sans', sans-serif;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    background: var(--bg);
    color: var(--text);
    font-family: var(--font-body);
    min-height: 100vh;
    overflow-x: hidden;
  }}
  header {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 12px 24px;
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    position: sticky; top: 0; z-index: 50;
  }}
  .header-left {{ display: flex; align-items: center; gap: 14px; }}
  .logo-mark {{
    width: 32px; height: 32px;
    border: 2px solid var(--accent);
    border-radius: 6px;
    display: grid; place-items: center;
    font-family: var(--font-mono);
    font-size: 11px; color: var(--accent);
    letter-spacing: -.05em;
  }}
  h1 {{
    font-family: var(--font-mono);
    font-size: .85rem;
    font-weight: 700;
    color: var(--accent);
    letter-spacing: .1em;
    text-transform: uppercase;
  }}
  .header-right {{ display: flex; align-items: center; gap: 20px; }}
  #clock {{
    font-family: var(--font-mono);
    font-size: .78rem;
    color: var(--muted);
    letter-spacing: .05em;
  }}
  #conn-status {{
    font-family: var(--font-mono);
    font-size: .7rem;
    padding: 3px 10px;
    border-radius: 4px;
    border: 1px solid var(--muted);
    color: var(--muted);
    transition: all .3s;
  }}
  #conn-status.live {{
    border-color: var(--accent3);
    color: var(--accent3);
    box-shadow: 0 0 8px rgba(57,255,20,.25);
  }}
  .legend {{
    display: flex; gap: 16px; align-items: center;
    padding: 8px 24px;
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    font-size: .7rem;
    font-family: var(--font-mono);
  }}
  .legend-item {{ display: flex; align-items: center; gap: 6px; }}
  .legend-dot {{ width: 8px; height: 8px; border-radius: 50%; }}
  .grid {{
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 10px;
    padding: 14px;
  }}
  @media (max-width: 1200px) {{ .grid {{ grid-template-columns: repeat(3, 1fr); }} }}
  @media (max-width: 860px)  {{ .grid {{ grid-template-columns: repeat(2, 1fr); }} }}
  @media (max-width: 540px)  {{ .grid {{ grid-template-columns: 1fr; }} }}
  .cam-card {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    overflow: hidden;
    cursor: pointer;
    transition: border-color .2s, transform .15s, box-shadow .2s;
    position: relative;
  }}
  .cam-card:hover {{
    border-color: var(--accent);
    transform: translateY(-2px);
    box-shadow: 0 8px 24px rgba(0,229,255,.1);
  }}
  .cam-header {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 6px 10px;
    background: rgba(0,0,0,.3);
    border-bottom: 1px solid var(--border);
  }}
  .cam-label {{
    font-family: var(--font-mono);
    font-size: .65rem;
    color: var(--accent);
    letter-spacing: .04em;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 140px;
  }}
  .badge-row {{ display: flex; gap: 4px; flex-shrink: 0; }}
  .badge {{
    font-family: var(--font-mono);
    font-size: .55rem;
    padding: 1px 5px;
    border-radius: 3px;
    font-weight: 700;
    letter-spacing: .04em;
  }}
  .badge-HC {{ background: rgba(57,255,20,.15);  color: var(--hc); border: 1px solid rgba(57,255,20,.3);  }}
  .badge-EE {{ background: rgba(255,107,53,.15); color: var(--ee); border: 1px solid rgba(255,107,53,.3); }}
  .badge-AN {{ background: rgba(255,221,0,.15);  color: var(--an); border: 1px solid rgba(255,221,0,.3);  }}
  canvas {{ width: 100%; display: block; background: #000; aspect-ratio: 16/9; }}
  .cam-footer {{
    display: flex; justify-content: space-between; align-items: center;
    padding: 4px 10px;
    font-family: var(--font-mono);
    font-size: .6rem;
    color: var(--muted);
  }}
  .fps-tag {{ color: var(--accent); }}
  #modal {{
    display: none;
    position: fixed; inset: 0;
    background: rgba(0,0,0,.92);
    z-index: 200;
    flex-direction: column;
    align-items: center;
    justify-content: center;
  }}
  #modal.open {{ display: flex; }}
  #modal-header {{
    display: flex; align-items: center; gap: 12px;
    margin-bottom: 10px;
  }}
  #modal-title {{
    font-family: var(--font-mono);
    font-size: .9rem;
    color: var(--accent);
    letter-spacing: .08em;
    text-transform: uppercase;
  }}
  #modal-badges {{ display: flex; gap: 6px; }}
  #modal-canvas {{
    max-width: 95vw; max-height: 84vh;
    border: 1px solid var(--accent);
    border-radius: 6px;
    display: block;
    box-shadow: 0 0 40px rgba(0,229,255,.15);
  }}
  #modal-close {{
    margin-top: 12px;
    padding: 6px 24px;
    background: transparent;
    color: var(--text);
    border: 1px solid var(--border);
    border-radius: 6px;
    cursor: pointer;
    font-family: var(--font-mono);
    font-size: .75rem;
    letter-spacing: .06em;
    transition: border-color .2s, color .2s;
  }}
  #modal-close:hover {{ border-color: var(--accent); color: var(--accent); }}
  footer {{
    text-align: center;
    padding: 10px;
    font-family: var(--font-mono);
    font-size: .6rem;
    color: var(--muted);
    border-top: 1px solid var(--border);
    letter-spacing: .05em;
  }}
</style>
</head>
<body>

<header>
  <div class="header-left">
    <div class="logo-mark">AI</div>
    <h1>Campus Monitoring System</h1>
  </div>
  <div class="header-right">
    <span id="clock">--:--:--</span>
    <span id="conn-status">CONNECTING</span>
  </div>
</header>

<div class="legend">
  <span style="color:var(--muted);margin-right:4px;">PIPELINES:</span>
  <div class="legend-item"><div class="legend-dot" style="background:var(--hc)"></div>Head Count</div>
  <div class="legend-item"><div class="legend-dot" style="background:var(--ee)"></div>Entry / Exit</div>
  <div class="legend-item"><div class="legend-dot" style="background:var(--an)"></div>Animal Detection</div>
</div>

<div class="grid" id="cam-grid"></div>

<div id="modal">
  <div id="modal-header">
    <div id="modal-title"></div>
    <div id="modal-badges"></div>
  </div>
  <canvas id="modal-canvas"></canvas>
  <button id="modal-close" onclick="closeModal()">&#x2715; CLOSE</button>
</div>

<footer>WebSocket push &mdash; server emits on inference completion &mdash; port {PORT}</footer>

<script>
const CAM_META    = {cam_meta_js};
const canvases    = {{}};
const ctxs        = {{}};
const fpsCounters = {{}};
const lastDisplayedTs = {{}}; // [NEW] Track latest timestamp per camera
let   modalCam    = null;
const modalCanvas = document.getElementById('modal-canvas');
const modalCtx    = modalCanvas.getContext('2d');

const grid = document.getElementById('cam-grid');
CAM_META.forEach(cam => {{
  const card = document.createElement('div');
  card.className = 'cam-card';
  card.innerHTML = `
    <div class="cam-header">
      <span class="cam-label" title="${{cam.topic}}">${{cam.label}}</span>
      <div class="badge-row">${{cam.badges.map(b=>`<span class="badge badge-${{b}}">${{b}}</span>`).join('')}}</div>
    </div>
    <canvas id="cv-${{cam.topic}}"></canvas>
    <div class="cam-footer">
      <span style="color:var(--muted)">${{cam.topic}}</span>
      <span class="fps-tag" id="fps-${{cam.topic}}">-- fps</span>
    </div>`;
  card.onclick = () => openModal(cam);
  grid.appendChild(card);

  const cv = document.getElementById('cv-' + cam.topic);
  canvases[cam.topic] = cv;
  ctxs[cam.topic]     = cv.getContext('2d');
  lastDisplayedTs[cam.topic] = 0; // Initialize
  fpsCounters[cam.topic] = {{
    count: 0,
    last:  performance.now(),
    el:    document.getElementById('fps-' + cam.topic)
  }};
}});

function tick() {{
  document.getElementById('clock').textContent = new Date().toLocaleTimeString('en-GB');
}}
tick(); setInterval(tick, 1000);

setInterval(() => {{
  const now = performance.now();
  CAM_META.forEach(cam => {{
    const f = fpsCounters[cam.topic];
    const dt = (now - f.last) / 1000;
    if (dt >= 0.9) {{
      f.el.textContent = (f.count / dt).toFixed(1) + ' fps';
      f.count = 0; f.last = now;
    }}
  }});
}}, 500);

// [FIX] Ensuring frames are drawn in sequential order
function drawFrame(topic, b64, ts) {{
  // Discard frame if an older frame has already been displayed
  if (lastDisplayedTs[topic] > 0 && ts < lastDisplayedTs[topic]) {{
    return;
  }}

  const img = new Image();
  img.onload = () => {{
    // Double check timestamp in callback because onload is async
    if (lastDisplayedTs[topic] > 0 && ts < lastDisplayedTs[topic]) return;
    lastDisplayedTs[topic] = ts;

    const cv = canvases[topic];
    if (cv.width !== img.naturalWidth) {{
      cv.width  = img.naturalWidth;
      cv.height = img.naturalHeight;
    }}
    ctxs[topic].drawImage(img, 0, 0);
    fpsCounters[topic].count++;
    if (modalCam === topic) {{
      if (modalCanvas.width !== img.naturalWidth) {{
        modalCanvas.width  = img.naturalWidth;
        modalCanvas.height = img.naturalHeight;
      }}
      modalCtx.drawImage(img, 0, 0);
    }}
  }};
  img.src = 'data:image/jpeg;base64,' + b64;
}}

function openModal(cam) {{
  modalCam = cam.topic;
  document.getElementById('modal-title').textContent = cam.label;
  document.getElementById('modal-badges').innerHTML =
    cam.badges.map(b => `<span class="badge badge-${{b}}">${{b}}</span>`).join('');
  document.getElementById('modal').classList.add('open');
}}
function closeModal() {{
  modalCam = null;
  document.getElementById('modal').classList.remove('open');
}}
document.getElementById('modal').addEventListener('click', e => {{
  if (e.target === document.getElementById('modal')) closeModal();
}});
document.addEventListener('keydown', e => {{ if (e.key === 'Escape') closeModal(); }});

const socket = io({{transports: ['websocket'], upgrade: false}});

socket.on('connect', () => {{
  const s = document.getElementById('conn-status');
  s.textContent = '● LIVE';
  s.className   = 'live';
  socket.emit('subscribe', {{cams: CAM_META.map(c => c.topic)}});
}});

socket.on('disconnect', () => {{
  const s = document.getElementById('conn-status');
  s.textContent = 'RECONNECTING';
  s.className   = '';
}});

socket.on('frame', data => {{
  drawFrame(data.cam, data.img, data.ts);
}});
</script>
</body>
</html>"""


# ──────────────────────────────────────────────────────────────────────────────
# 6. UTILITY HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def draw_text_bg(img, text, x, y, font_scale=0.45, thickness=1,
                 bg=(0, 0, 0), fg=(255, 255, 255)):
    (tw, th), _ = cv2.getTextSize(text, cv2.FONT_HERSHEY_SIMPLEX,
                                   font_scale, thickness)
    cv2.rectangle(img, (x, y - th - 4), (x + tw + 2, y + 4), bg, -1)
    cv2.putText(img, text, (x, y), cv2.FONT_HERSHEY_SIMPLEX,
                font_scale, fg, thickness, cv2.LINE_AA)


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def init_nvml():
    try:
        pynvml.nvmlInit()
        return pynvml.nvmlDeviceGetHandleByIndex(0)
    except Exception:
        return None


def get_gpu_util(handle) -> int:
    if handle is None:
        return -1
    try:
        return pynvml.nvmlDeviceGetUtilizationRates(handle).gpu
    except Exception:
        return -1


class StreamingCSVWriter:
    """Writes CSV rows directly to disk per frame. Thread-safe."""
    def __init__(self, path: str, headers: list):
        self._path = path
        self._lock = threading.Lock()
        if not os.path.exists(path):
            with open(path, 'w', newline='') as f:
                csv.writer(f).writerow(headers)

    def write(self, row: list):
        with self._lock:
            with open(self._path, 'a', newline='') as f:
                csv.writer(f).writerow(row)


_csv_lock = threading.Lock()


# ──────────────────────────────────────────────────────────────────────────────
# 7. ANIMAL DETECTION PIPELINE
# ──────────────────────────────────────────────────────────────────────────────

AN_CLASS_NAMES = {
    16: 'cat', 17: 'dog',  18: 'horse',
    19: 'sheep', 20: 'cow', 21: 'elephant'
}

AN_COLORS = {
    'cat': (255, 165, 0), 'dog': (0, 200, 255),
    'horse': (0, 255, 128), 'sheep': (255, 255, 0),
    'cow': (200, 0, 255), 'elephant': (255, 80, 80),
}


def run_animal_detection(topic: str, frame: np.ndarray, display: np.ndarray,
                         model: YOLO, frame_index: int,
                         capture_ts: str, wall_ts: str,
                         csv_writer, frame_count_warmup_done: bool):
    with _gpu_semaphore:
        with torch.no_grad():
            results = model.track(
                frame,
                persist  = True,
                tracker  = BOTSORT_CFG,
                classes  = AN_ANIMAL_CLASSES,
                conf     = AN_CONF_THRESH,
                imgsz    = 640,
                half     = True,
                verbose  = False,
            )

    boxes_result = results[0].boxes
    if boxes_result is None or len(boxes_result) == 0:
        return 0

    rows = []
    detection_count = 0

    for box in boxes_result:
        cls_id   = int(box.cls[0])
        cls_name = AN_CLASS_NAMES.get(cls_id, 'unknown')
        conf     = float(box.conf[0])
        track_id = int(box.id[0]) if box.id is not None else -1
        x1, y1, x2, y2 = [int(c) for c in box.xyxy[0]]
        detection_count += 1

        color = AN_COLORS.get(cls_name, (200, 200, 200))
        cv2.rectangle(display, (x1, y1), (x2, y2), color, 2)
        label = f"{cls_name} {conf:.2f} id:{track_id}"
        draw_text_bg(display, label, x1, max(y1 - 5, 12), bg=color, fg=(0, 0, 0))

        rows.append([
            wall_ts, capture_ts, topic, frame_index,
            cls_name, cls_id, f"{conf:.4f}", track_id,
            x1, y1, x2, y2, 'N/A',
        ])

    if rows:
        with _csv_lock:
            with open(csv_writer, 'a', newline='') as f:
                csv.writer(f).writerows(rows)

    cv2.putText(display, f"Animals: {detection_count}",
                (FRAME_W - 150, 55),
                cv2.FONT_HERSHEY_SIMPLEX, 0.55, (255, 165, 0), 2)
    return detection_count


# ──────────────────────────────────────────────────────────────────────────────
# 8. HEAD COUNT PIPELINE
# ──────────────────────────────────────────────────────────────────────────────

def run_head_count(topic: str, frame: np.ndarray, display: np.ndarray,
                   model: YOLO, frame_index: int,
                   video_ts_sec: float, wall_ts: str,
                   hc_bucket: dict,
                   stats_writer: StreamingCSVWriter,
                   bucket_csv_path: str,
                   warmup_fps_tracker: dict,
                   gpu_handle) -> int:

    t_start = time.perf_counter()

    with _gpu_semaphore:
        with torch.no_grad():
            results = model.track(
                frame,
                persist  = True,
                tracker  = BOTSORT_CFG,
                classes  = HC_PERSON_CLASS,
                conf     = HC_CONF_THRESH,
                imgsz    = 640,
                half     = True,
                verbose  = False,
            )

    torch.cuda.synchronize()
    infer_ms = (time.perf_counter() - t_start) * 1000.0

    head_count = 0
    if results[0].boxes is not None:
        for box in results[0].boxes:
            if float(box.conf[0]) >= HC_CONF_THRESH:
                head_count += 1
                hx1, hy1, hx2, hy2 = [int(c) for c in box.xyxy[0]]
                cv2.rectangle(display, (hx1, hy1), (hx2, hy2), (255, 0, 255), 1)

    warmup_done = warmup_fps_tracker.get('warmup_done', False)
    if not warmup_done and frame_index >= HC_WARMUP_FRAMES:
        warmup_fps_tracker['warmup_done'] = True
        warmup_fps_tracker['start_time']  = time.perf_counter()
        warmup_fps_tracker['frame_count'] = 0
    if warmup_fps_tracker.get('warmup_done'):
        warmup_fps_tracker['frame_count'] += 1
        elapsed = time.perf_counter() - warmup_fps_tracker['start_time']
        running_fps = warmup_fps_tracker['frame_count'] / elapsed if elapsed > 0 else 0.0
    else:
        running_fps = 0.0

    cuda_alloc = torch.cuda.memory_allocated() / 1e6
    cuda_resv  = torch.cuda.memory_reserved()  / 1e6
    gpu_util   = get_gpu_util(gpu_handle)

    stats_writer.write([
        wall_ts, frame_index, f"{video_ts_sec:.4f}",
        f"{infer_ms:.3f}", f"{running_fps:.3f}", head_count,
        f"{cuda_alloc:.2f}", f"{cuda_resv:.2f}",
        gpu_util if gpu_util >= 0 else '',
    ])

    hc_bucket['counts'].append(head_count)
    elapsed_bucket = time.time() - hc_bucket['interval_start']

    if elapsed_bucket >= HC_INTERVAL_SEC:
        counts = hc_bucket['counts']
        avg    = sum(counts) / len(counts) if counts else 0.0
        interval_end = hc_bucket['interval_start'] + elapsed_bucket

        with _csv_lock:
            with open(bucket_csv_path, 'a', newline='') as f:
                csv.writer(f).writerow([
                    wall_ts,
                    f"{hc_bucket['interval_start_vid']:.4f}",
                    f"{interval_end:.4f}",
                    f"{avg:.2f}",
                ])

        hc_bucket['interval_start']     = time.time()
        hc_bucket['interval_start_vid'] = video_ts_sec
        hc_bucket['counts']             = []

    cv2.putText(display, f"Heads: {head_count}",
                (FRAME_W - 140, 30),
                cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 0, 255), 2)
    return head_count


# ──────────────────────────────────────────────────────────────────────────────
# 9. ENTRY / EXIT PIPELINE
# ──────────────────────────────────────────────────────────────────────────────

def build_ee_encoder():
    gpus = tf.config.list_physical_devices('GPU')
    for gpu in gpus:
        try:
            tf.config.experimental.set_memory_growth(gpu, True)
        except RuntimeError:
            pass
    encoder = gdet.create_box_encoder(REID_MODEL_PATH, batch_size=EE_BATCH_SIZE)
    return encoder


def init_ee_tracker():
    metric  = nn_matching.NearestNeighborDistanceMetric(
        'cosine', EE_MAX_COSINE_DIST, EE_NN_BUDGET)
    tracker = Tracker(metric,
                      max_iou_distance=EE_MAX_IOU_DIST,
                      max_age=EE_MAX_AGE,
                      n_init=EE_N_INIT)
    return tracker


def run_entry_exit(topic: str, frame: np.ndarray, display: np.ndarray,
                   yolo_model: YOLO, encoder, tracker,
                   ee_config: dict,
                   track_history: dict, track_state: dict,
                   session_totals: dict,
                   ee_bucket: dict,
                   ee_csv_path: str,
                   frame_index: int):

    roi_poly = ee_config.get('roi')
    vec_x    = ee_config.get('vec_x', 0.0)
    vec_y    = ee_config.get('vec_y', 1.0)

    ee_frame = cv2.resize(frame, (EE_WIDTH, EE_HEIGHT))

    with _gpu_semaphore:
        results = yolo_model(ee_frame, conf=EE_CONF_THRESH,
                             classes=EE_PERSON_CLASS, verbose=False)

    detections = []
    if len(results[0].boxes) > 0:
        for box in results[0].boxes:
            x1, y1, x2, y2 = [int(c) for c in box.xyxy[0]]
            conf = float(box.conf[0])
            detections.append(([x1, y1, x2 - x1, y2 - y1], conf, 'person'))

    if detections:
        boxes_tlwh = np.array([d[0] for d in detections], dtype=np.float32)
        confs      = np.array([d[1] for d in detections], dtype=np.float32)
        features   = encoder(ee_frame, boxes_tlwh)
        ds_detections = []
        if features is not None:
            for i in range(min(len(boxes_tlwh), len(features))):
                feat = features[i]
                if feat is not None:
                    ds_detections.append(
                        Detection(boxes_tlwh[i], confs[i], 'person', feat))
    else:
        ds_detections = []

    tracker.predict()
    tracker.update(ds_detections)

    active_ids = {t.track_id for t in tracker.tracks if t.is_confirmed()}
    for dead_id in list(track_history.keys()):
        if dead_id not in active_ids:
            del track_history[dead_id]
            track_state.pop(dead_id, None)

    if roi_poly is not None:
        scale_x    = FRAME_W / EE_WIDTH
        scale_y    = FRAME_H / EE_HEIGHT
        scaled_roi = (roi_poly * np.array([scale_x, scale_y])).astype(np.int32)
        cv2.polylines(display, [scaled_roi], True, (255, 107, 53), 1)

    sx = FRAME_W / EE_WIDTH
    sy = FRAME_H / EE_HEIGHT

    for track in tracker.tracks:
        if not track.is_confirmed():
            continue

        tid  = track.track_id
        tlwh = track.to_tlwh()
        cx   = int(tlwh[0] + tlwh[2] / 2)
        cy   = int(tlwh[1] + tlwh[3] / 2)

        if tid not in track_history:
            track_history[tid] = deque(maxlen=EE_LOOKBACK_FRAMES + 5)
        track_history[tid].append((cx, cy))

        if tid not in track_state:
            track_state[tid] = 0

        in_roi = False
        if roi_poly is not None:
            in_roi = cv2.pointPolygonTest(
                roi_poly, (float(cx), float(cy)), False) >= 0
        else:
            in_roi = True

        if not in_roi:
            continue

        event = None
        if len(track_history[tid]) >= EE_LOOKBACK_FRAMES:
            curr = track_history[tid][-1]
            prev = track_history[tid][-EE_LOOKBACK_FRAMES]
            dx   = curr[0] - prev[0]
            dy   = curr[1] - prev[1]
            dot  = (dx * vec_x) + (dy * vec_y)

            if track_state[tid] == 0:
                if dot > EE_DOT_THRESH:
                    track_state[tid]        = 1
                    session_totals['entry'] += 1
                    event = 'ENTRY'
                    logging.info(
                        f"[{topic}] ENTRY | ID={tid:4d} | dot={dot:+.2f} | "
                        f"Total In={session_totals['entry']} "
                        f"Out={session_totals['exit']}")
                elif dot < -EE_DOT_THRESH:
                    track_state[tid]       = 1
                    session_totals['exit'] += 1
                    event = 'EXIT'
                    logging.info(
                        f"[{topic}] EXIT  | ID={tid:4d} | dot={dot:+.2f} | "
                        f"Total In={session_totals['entry']} "
                        f"Out={session_totals['exit']}")

        dx1 = int(tlwh[0] * sx)
        dy1 = int(tlwh[1] * sy)
        dx2 = int((tlwh[0] + tlwh[2]) * sx)
        dy2 = int((tlwh[1] + tlwh[3]) * sy)

        color = (0, 255, 150) if track_state.get(tid, 0) == 1 else (200, 200, 200)
        if event == 'ENTRY':
            color = (57, 255, 20)
        elif event == 'EXIT':
            color = (0, 100, 255)

        cv2.rectangle(display, (dx1, dy1), (dx2, dy2), color, 1)
        lbl = f"ID:{tid}"
        if event:
            lbl += f" {event}"
        draw_text_bg(display, lbl, dx1, max(dy1 - 4, 10))

    cv2.putText(display,
                f"IN:{session_totals['entry']}  OUT:{session_totals['exit']}",
                (6, FRAME_H - 10),
                cv2.FONT_HERSHEY_SIMPLEX, 0.55, (255, 107, 53), 2)

    now = time.time()
    if now - ee_bucket['last_write'] >= EE_CSV_INTERVAL_SEC:
        ts_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with _csv_lock:
            with open(ee_csv_path, 'a', newline='') as f:
                csv.writer(f).writerow([
                    ts_str, topic,
                    session_totals['entry'],
                    session_totals['exit'],
                ])
        ee_bucket['last_write'] = now
        logging.info(
            f"[{topic}] EE CSV flush — "
            f"IN={session_totals['entry']} OUT={session_totals['exit']}")


# ──────────────────────────────────────────────────────────────────────────────
# 10. MAIN PROCESSING THREAD
# ──────────────────────────────────────────────────────────────────────────────

def process_feed(cam_cfg: dict):
    topic  = cam_cfg['topic']
    folder = cam_cfg['folder']
    run_AN = cam_cfg['AN']
    run_HC = cam_cfg['HC']
    run_EE = cam_cfg['EE']
    cam_ip = cam_cfg['ip']

    log = logging.getLogger(topic)
    log.info(f"Starting — AN={run_AN} HC={run_HC} EE={run_EE}")

    cam_root = os.path.join(BASE_OUTPUT_DIR, folder)
    dirs = {}
    for pipeline in ['animal_detection', 'head_count', 'entry_exit']:
        dirs[pipeline] = {
            'csv'      : os.path.join(cam_root, pipeline, 'csv'),
            'inference': os.path.join(cam_root, pipeline, 'inference'),
        }
        os.makedirs(dirs[pipeline]['csv'],       exist_ok=True)
        os.makedirs(dirs[pipeline]['inference'], exist_ok=True)

    an_csv_path = None
    if run_AN:
        an_csv_path = os.path.join(dirs['animal_detection']['csv'],
                                   f"{folder}_animal_detection.csv")
        if not os.path.exists(an_csv_path):
            with open(an_csv_path, 'w', newline='') as f:
                csv.writer(f).writerow([
                    'wall_time_iso', 'capture_timestamp', 'source_id',
                    'frame_index', 'class_name', 'class_id', 'confidence',
                    'track_id', 'x1', 'y1', 'x2', 'y2', 'image_ref',
                ])

    hc_stats_writer = None
    hc_bucket_path  = None
    hc_bucket       = {}
    hc_fps_tracker  = {}
    if run_HC:
        hc_stats_path  = os.path.join(dirs['head_count']['csv'],
                                      f"{folder}_headcount_stats.csv")
        hc_bucket_path = os.path.join(dirs['head_count']['csv'],
                                      f"{folder}_headcount_bucket.csv")
        hc_stats_writer = StreamingCSVWriter(hc_stats_path, [
            'wall_time_iso', 'frame_index', 'video_timestamp_sec',
            'inference_time_ms', 'running_avg_fps', 'head_count',
            'cuda_mem_allocated_mb', 'cuda_mem_reserved_mb', 'gpu_utilization_pct',
        ])
        if not os.path.exists(hc_bucket_path):
            with open(hc_bucket_path, 'w', newline='') as f:
                csv.writer(f).writerow([
                    'wall_time_iso', 'interval_start_sec',
                    'interval_end_sec', 'avg_head_count',
                ])
        hc_bucket      = {'interval_start': time.time(),
                          'interval_start_vid': 0.0, 'counts': []}
        hc_fps_tracker = {'warmup_done': False}

    ee_csv_path    = None
    ee_config      = None
    ee_tracker     = None
    ee_encoder     = None
    track_history  = {}
    track_state    = {}
    session_totals = {'entry': 0, 'exit': 0}
    ee_bucket      = {'last_write': time.time()}

    if run_EE:
        ee_csv_path = os.path.join(dirs['entry_exit']['csv'],
                                   f"{folder}_entry_exit.csv")
        if not os.path.exists(ee_csv_path):
            with open(ee_csv_path, 'w', newline='') as f:
                csv.writer(f).writerow([
                    'Timestamp', 'CameraIP', 'TotalEntries', 'TotalExits',
                ])
        ee_config = EE_CAM_CONFIGS.get(cam_ip)

    model_animal    = None
    model_headcount = None
    model_ee_yolo   = None

    if run_AN:
        model_animal = YOLO(MODEL_ANIMAL_PATH)
        model_animal.to('cuda')

    if run_HC:
        model_headcount = YOLO(MODEL_HEADCOUNT_PATH)
        model_headcount.to('cuda')

    if run_EE:
        model_ee_yolo = YOLO(MODEL_EE_YOLO_PATH)
        ee_encoder    = build_ee_encoder()
        ee_tracker    = init_ee_tracker()

    gpu_handle = init_nvml()

    # Robust Kafka connection using manual assignment to bypass rebalancing storms
    consumer = None
    while consumer is None:
        try:
            # Note: we still provide group_id for offset tracking, but we assign partitions manually
            consumer = KafkaConsumer(
                bootstrap_servers     = BROKERS.split(','),
                group_id              = GROUP_ID,
                auto_offset_reset     = 'latest',
                max_poll_interval_ms  = 600000,
                session_timeout_ms    = 60000,
                heartbeat_interval_ms = 20000,
                max_poll_records      = 10,
                fetch_max_bytes       = 52428800,
            )
            
            # Manually assign all partitions for this topic to this consumer
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                raise Exception(f"No partitions found for topic {topic}")
                
            from kafka import TopicPartition
            tp_list = [TopicPartition(topic, p) for p in partitions]
            consumer.assign(tp_list)
            
            log.info(f"Kafka consumer assigned — topic: {topic} | partitions: {partitions}")
            
            # Skip lag
            consumer.seek_to_end()
            log.info(f"Skipped lag for {topic}, now at the latest offset.")
                
        except Exception as e:
            log.warning(f"Kafka assignment failed for {topic}: {e}. Retrying in 5s...")
            consumer = None
            time.sleep(5)

    video_writer = None
    vid_pipeline = 'head_count' if run_HC else ('animal_detection' if run_AN else 'entry_exit')
    vid_path     = os.path.join(dirs[vid_pipeline]['inference'],
                                f"{folder}_annotated.avi")

    frame_count = 0

    for msg in consumer:
        frame_count += 1
        if frame_count % SKIP_FRAMES != 0:
            continue

        try:
            # Unpack 8-byte uint64 nanoseconds and convert to seconds
            ts_sec     = struct.unpack('>Q', msg.value[:8])[0] / 1e9
            capture_ts = datetime.fromtimestamp(ts_sec).strftime(
                             '%Y-%m-%d %H:%M:%S.%f')[:-3]
            wall_ts    = now_iso()

            frame = cv2.imdecode(
                np.frombuffer(msg.value[8:], np.uint8), cv2.IMREAD_COLOR)
            if frame is None:
                continue

            frame   = cv2.resize(frame, (FRAME_W, FRAME_H))
            display = frame.copy()

            if SAVE_VIDEO and video_writer is None:
                video_writer = cv2.VideoWriter(
                    vid_path,
                    cv2.VideoWriter_fourcc(*'XVID'),
                    18.0,
                    (FRAME_W, FRAME_H),
                )

            video_ts_sec = frame_count / 18.0

            if run_AN and model_animal is not None:
                run_animal_detection(
                    topic, frame, display,
                    model_animal, frame_count,
                    capture_ts, wall_ts,
                    an_csv_path,
                    frame_count > AN_WARMUP_FRAMES,
                )

            if run_HC and model_headcount is not None:
                run_head_count(
                    topic, frame, display,
                    model_headcount, frame_count,
                    video_ts_sec, wall_ts,
                    hc_bucket,
                    hc_stats_writer,
                    hc_bucket_path,
                    hc_fps_tracker,
                    gpu_handle,
                )

            if run_EE and model_ee_yolo is not None:
                run_entry_exit(
                    topic, frame, display,
                    model_ee_yolo, ee_encoder, ee_tracker,
                    ee_config or {},
                    track_history, track_state,
                    session_totals,
                    ee_bucket,
                    ee_csv_path,
                    frame_count,
                )

            cv2.putText(display, capture_ts,
                        (6, FRAME_H - 28),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.38, (180, 180, 180), 1)

            if video_writer:
                video_writer.write(display)

            publish_frame(topic, display, ts_sec)

            if frame_count % 500 == 0:
                gc.collect()
                torch.cuda.empty_cache()

            if frame_count % 200 == 0:
                log.info(
                    f"Frame {frame_count} | "
                    f"EE IN={session_totals['entry']} OUT={session_totals['exit']}"
                    if run_EE else f"Frame {frame_count}"
                )

        except Exception as e:
            log.error(f"Frame {frame_count} error: {e}", exc_info=False)
            continue


# ──────────────────────────────────────────────────────────────────────────────
# 11. ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    logging.basicConfig(
        level  = logging.INFO,
        format = '%(asctime)s [%(name)s] %(levelname)s %(message)s',
        datefmt= '%Y-%m-%d %H:%M:%S',
    )

    load_ee_camera_configs()
    os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

    threads = []
    for cam_cfg in CAMERA_REGISTRY:
        t = threading.Thread(
            target = process_feed,
            args   = (cam_cfg,),
            name   = cam_cfg['topic'],
            daemon = True,
        )
        threads.append(t)
        t.start()
        time.sleep(3)

    logging.info(
        f"All {len(threads)} camera threads started. "
        f"Dashboard: http://0.0.0.0:{PORT}"
    )

    socketio.run(app, host='0.0.0.0', port=PORT,
                 allow_unsafe_werkzeug=True)

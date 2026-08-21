#!/usr/bin/env python3
"""
Multi-Pipeline Kafka Consumer — PHASE 2 PRODUCTION FIX
=======================================================
Pipelines : Animal Detection (AN) | Head Count (HC) | Entry/Exit (EE)
Server    : 10.1.41.56  |  NVIDIA L40S  |  ee-venv
Dashboard : http://0.0.0.0:8675

PHASE 1 CHANGES (carried forward — do not revert)
───────────────────────────────────────────────────────────────────────────────
FIX-T1 : Removed eventlet + monkey_patch() — eliminated exit-139 segfaults.
FIX-T2 : SocketIO async_mode='threading' — CUDA-safe WebSocket server.
FIX-T3 : Split GPU semaphores: _pytorch_sem=Semaphore(3), _tf_sem=Semaphore(2).
FIX-T4 : torch.cuda.synchronize() in AN pipeline after model.track().

PHASE 2 CHANGES (applied in this file)
───────────────────────────────────────────────────────────────────────────────
FIX-P2-1 : Dynamic dominant partition detection.
FIX-P2-2 : Per-camera consumer group_id.
FIX-P2-3 : Static group membership via group_instance_id.
FIX-P2-4 : Session timeout 60s → 15s, heartbeat 20s → 3s.
FIX-P2-5 : max_poll_records 10 → 1.
FIX-P2-6 : Manual offset commit every 10 processed frames.
FIX-P2-7 : WebSocket transport fix.
FIX-P2-8 : Dominant partition startup logging.

EMERGENCY FIXES APPLIED TO THIS BUILD
───────────────────────────────────────────────────────────────────────────────
FIX-E1 : Delta Check for Dominant Partition.
         Instead of absolute offset size (which selected dead historical 
         partitions), it now snapshots offsets 1.5s apart and selects the 
         partition with actual traffic growth. Fixes the "6 dead cameras" bug.
FIX-E2 : Video Rotation (Container Corruption Fix).
         Automatically releases and creates a new .avi file every 18,000 frames 
         (~16 mins). Fixes the "8.5GB frozen video" corruption caused by OpenCV.
FIX-E3 : Dynamic Lag-Based Frame Skipping.
         Measures end-to-end latency via Kafka timestamps. If lag > 3 seconds, 
         skips GPU inference to immediately catch up to real-time. Fixes the 
         "videos falling behind" symptom.
"""

import sys

# ── PHASE 1 SAFETY GUARD — DO NOT REMOVE ─────────────────────────────────────
# eventlet.monkey_patch() was the root cause of exit-139 segfaults on this
# system. CUDA and TensorFlow use C-level OS threads incompatible with
# eventlet's green thread patching. This assertion fires before any threads
# or GPU calls are made — if eventlet is ever re-introduced, the process
# exits with a clear message rather than a cryptic segfault 8 minutes later.
assert 'eventlet' not in sys.modules, (
    "FATAL: eventlet is loaded in this process. "
    "eventlet.monkey_patch() causes SIGSEGV with CUDA/TF on this system. "
    "Remove any 'import eventlet' and 'eventlet.monkey_patch()' calls. "
    "See Phase 1 documentation for full root cause analysis."
)

# ── Standard library ──────────────────────────────────────────────────────────
import base64
import csv
import gc
import json
import logging
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
from kafka import KafkaConsumer, TopicPartition
from ultralytics import YOLO

# ── DeepSort (Entry/Exit) ─────────────────────────────────────────────────────
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
BROKERS = '10.1.40.43:9092,10.1.40.44:9093,10.1.40.45:9094'

# NOTE: GROUP_ID constant removed in Phase 2.
# Per-camera group IDs are now constructed inside process_feed() as:
#   f"mlp_{topic.replace('.','_')}"
# This ensures complete isolation between camera consumer groups.

# ── Model paths ───────────────────────────────────────────────────────────────
MODEL_ANIMAL_PATH    = '/data/Animal_Detection/runs/animal_v1/weights/best.pt'
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
SKIP_FRAMES       = 1          # Phase 3 will make this dynamic based on lag
AN_CONF_THRESH    = 0.45
HC_CONF_THRESH    = 0.30
EE_CONF_THRESH    = 0.45

AN_ANIMAL_CLASSES = [0]
HC_PERSON_CLASS   = [0]
EE_PERSON_CLASS   = [0]
AN_WARMUP_FRAMES  = 3
HC_WARMUP_FRAMES  = 3

# ── Limiters & Optimization (FIX-E2 & FIX-E3) ─────────────────────────────────
# MAX_FRAMES_PER_AVI = 18000  # Storage overhaul: video storage disabled, AVI rotation unused.
MAX_LAG_FRAMES     = 54     # Drop frames if consumer falls >54 frames (~3s) behind

# ── Head Count CSV interval ───────────────────────────────────────────────────
HC_INTERVAL_SEC = 10.0

# ── Entry/Exit DeepSort params ────────────────────────────────────────────────
EE_MAX_COSINE_DIST  = 0.3
EE_NN_BUDGET        = None
EE_MAX_AGE          = 30
EE_N_INIT           = 3
EE_MAX_IOU_DIST     = 0.7
EE_BATCH_SIZE       = 4
EE_LOOKBACK_FRAMES  = 8
EE_DOT_THRESH       = 3.0
EE_CSV_INTERVAL_SEC = 300

# ── Web dashboard ─────────────────────────────────────────────────────────────
PORT = 8675

# ── Save annotated video ──────────────────────────────────────────────────────
# Storage overhaul: video storage disabled entirely — only CSV metadata is
# persisted now (raw_frames/annotated_frames JPEGs and AVI video removed).
# SAVE_VIDEO = True

# ── Offset commit frequency (FIX-P2-6) ───────────────────────────────────────
# Commit the consumer offset every N successfully processed frames.
# Lower = less data loss on crash but more broker I/O.
# Higher = more potential replay on crash but less I/O.
# 10 is the correct balance for this pipeline.
COMMIT_EVERY_N_FRAMES = 10


# ──────────────────────────────────────────────────────────────────────────────
# GPU CONCURRENCY SEMAPHORES (Phase 1 FIX-T3 — do not change)
# ──────────────────────────────────────────────────────────────────────────────
# _pytorch_sem gates AN (YOLOv8m BotSORT) + HC (YOLOv8 BotSORT).
# _tf_sem      gates EE (YOLOv5 + TF ReID encoder).
# PyTorch and TF run in isolated lanes — no cross-framework CUDA context
# interference. Values chosen for L40S 48GB VRAM headroom.
_pytorch_sem = threading.Semaphore(3)
_tf_sem      = threading.Semaphore(2)


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
    """
    Load per-camera ROI polygons and direction vectors from the JSON config.
    Called once at startup before any threads are spawned.
    Raises on missing or malformed config — fail fast rather than silently
    processing frames without ROI boundaries.
    """
    global EE_CAM_CONFIGS
    try:
        with open(EE_CAMERA_CFG, 'r') as f:
            raw = json.load(f)
    except FileNotFoundError:
        logging.critical(
            "[EE Config] Camera config file not found: %s", EE_CAMERA_CFG
        )
        raise
    except json.JSONDecodeError as exc:
        logging.critical(
            "[EE Config] Malformed JSON in %s: %s", EE_CAMERA_CFG, exc
        )
        raise

    ip_pattern = re.compile(r'\((\d+\.\d+\.\d+\.\d+)\)')
    loaded = 0
    for key, val in raw.items():
        m = ip_pattern.search(key)
        if not m:
            logging.warning(
                "[EE Config] Could not extract IP from key '%s' — skipping.", key
            )
            continue
        ip = m.group(1)
        EE_CAM_CONFIGS[ip] = {
            'roi'  : np.array(val['roi'], dtype=np.int32),
            'vec_x': val['vec_x'],
            'vec_y': val['vec_y'],
        }
        loaded += 1

    logging.info("[EE Config] Loaded ROI configs for %d cameras.", loaded)
    if loaded == 0:
        logging.warning(
            "[EE Config] Zero camera configs loaded — all EE pipelines will "
            "run without ROI gating. Check camera_config.json format."
        )


# ──────────────────────────────────────────────────────────────────────────────
# 4. DOMINANT PARTITION DETECTION (FIX-E1: DELTA CHECK)
# ──────────────────────────────────────────────────────────────────────────────

def detect_dominant_partition(
    topic: str,
    brokers: list,
    log: logging.Logger,
) -> int:
    """
    Phase 2.1: Dynamic dominant partition detection using Delta Check.
    Finds the active partition by measuring growth over a 1.5 second window.
    """
    backoff = 2
    max_attempts = 5

    for attempt in range(1, max_attempts + 1):
        probe = None
        try:
            # Create a temporary lightweight consumer just for offset probing.
            # No group_id needed — this consumer never commits or processes frames.
            probe = KafkaConsumer(bootstrap_servers=brokers)

            # Get all partition IDs for this topic.
            partitions = probe.partitions_for_topic(topic)
            if not partitions:
                raise RuntimeError(
                    f"Topic '{topic}' returned no partitions — "
                    "topic may not exist on this cluster."
                )

            # Build TopicPartition objects for all partitions.
            tp_list = [TopicPartition(topic, p) for p in sorted(partitions)]

            # Snapshot 1
            offsets_t1 = probe.end_offsets(tp_list)
            
            # Wait long enough to observe at least one producer batch flush.
            # Producers batch ~50 frames at 18 FPS → 1 batch takes 2.7s.
            # 3.5s guarantees we catch at least one batch on every topic.
            time.sleep(3.5)
            
            # Snapshot 2
            offsets_t2 = probe.end_offsets(tp_list)

            # Calculate growth delta
            deltas = {tp: offsets_t2[tp] - offsets_t1[tp] for tp in tp_list}
            
            delta_summary = ' | '.join(
                f"P{tp.partition}: +{deltas[tp]}" 
                for tp in sorted(tp_list, key=lambda x: x.partition)
            )
            log.info("Partition delta scan — topic: %s | %s", topic, delta_summary)

            # Select the partition with the highest growth = dominant partition.
            dominant_tp = max(deltas, key=lambda tp: deltas[tp])
            
            # Fallback: If producer is temporarily down, fallback to absolute size
            if deltas[dominant_tp] == 0:
                log.warning("No live traffic detected. Falling back to highest absolute offset.")
                dominant_tp = max(offsets_t2, key=lambda tp: offsets_t2[tp])

            dominant_partition = dominant_tp.partition
            dominant_offset    = offsets_t2[dominant_tp]

            log.info(
                "Dominant partition selected — topic: %s | partition: %d "
                "| messages: %s | This is the live producer stream.",
                topic, dominant_partition, f"{dominant_offset:,}",
            )

            return dominant_partition

        except Exception as exc:
            log.error(
                "Partition detection attempt %d/%d failed for topic %s: %s — "
                "retrying in %ds.",
                attempt, max_attempts, topic, exc, backoff,
                exc_info=True,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

        finally:
            # Always close the probe consumer to release broker connections.
            if probe is not None:
                try:
                    probe.close()
                except Exception:
                    pass

    raise RuntimeError(
        f"Failed to detect dominant partition for topic '{topic}' "
        f"after {max_attempts} attempts. Check broker connectivity."
    )


# ──────────────────────────────────────────────────────────────────────────────
# 5. FLASK + SOCKETIO
#    async_mode='threading' — real OS threads, CUDA-safe (Phase 1 FIX-T2).
#    WebSocket upgrade not supported by Werkzeug in threading mode.
#    JS client configured to use polling transport (FIX-P2-7).
# ──────────────────────────────────────────────────────────────────────────────
app      = Flask(__name__)
socketio = SocketIO(
    app,
    async_mode          = 'threading',  # Phase 1 FIX-T2 — CUDA-safe
    cors_allowed_origins = '*',
    logger              = False,
    engineio_logger     = False,
)

_latest_jpeg = {t: None for t in ALL_TOPICS}
_latest_ts   = {t: 0.0  for t in ALL_TOPICS}
_jpeg_lock   = {t: threading.Lock() for t in ALL_TOPICS}


def publish_frame(topic: str, frame: np.ndarray, ts_sec: float):
    """
    JPEG-encode the annotated frame and push it to the WebSocket room
    for this camera. Also stores the latest JPEG for snapshot requests.

    Args:
        topic  : Kafka topic string (used as room name and cache key)
        frame  : Annotated BGR frame (numpy array)
        ts_sec : Capture timestamp in seconds (float, from producer header)
    """
    ok, enc = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 78])
    if not ok:
        logging.warning(
            "[Dashboard] JPEG encode failed for topic %s — frame skipped.", topic
        )
        return

    jpg_bytes = enc.tobytes()
    with _jpeg_lock[topic]:
        _latest_jpeg[topic] = jpg_bytes
        _latest_ts[topic]   = ts_sec

    b64 = base64.b64encode(jpg_bytes).decode('ascii')
    socketio.emit('frame', {'cam': topic, 'img': b64, 'ts': ts_sec}, room=topic)


@socketio.on('connect')
def on_connect():
    from flask import request
    logging.info("[WS] Client connected: %s", request.sid)


@socketio.on('disconnect')
def on_disconnect():
    from flask import request
    logging.info("[WS] Client disconnected: %s", request.sid)


@socketio.on('subscribe')
def on_subscribe(data):
    """
    Client sends a list of camera topics to subscribe to.
    Adds the client to each room and immediately sends the latest
    buffered frame so the dashboard is not blank on load.
    """
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
    """Serve the latest JPEG frame for a given camera as a still image."""
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
# 6. DASHBOARD HTML
#    FIX-P2-7: JS transport changed from ['websocket'] to ['polling'].
#    Werkzeug dev server does not support WebSocket upgrades in threading mode.
#    The previous configuration caused HTTP 500 errors on every client
#    reconnect. Socket.IO long-polling works correctly and eliminates all 500s.
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
  #modal-header {{ display: flex; align-items: center; gap: 12px; margin-bottom: 10px; }}
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
<footer>Socket.IO push — server emits on inference completion — port {PORT}</footer>
<script>
const CAM_META    = {cam_meta_js};
const canvases    = {{}};
const ctxs        = {{}};
const fpsCounters = {{}};
const lastDisplayedTs = {{}};
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
  lastDisplayedTs[cam.topic] = 0;
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

function drawFrame(topic, b64, ts) {{
  if (lastDisplayedTs[topic] > 0 && ts < lastDisplayedTs[topic]) return;
  const img = new Image();
  img.onload = () => {{
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

// FIX-P2-7: transports changed from ['websocket'] to ['polling'].
// Werkzeug dev server does not support WebSocket protocol upgrade in
// threading mode. The previous ['websocket'] config caused HTTP 500 errors
// on every reconnect and client disconnections. Socket.IO polling transport
// works correctly with threading mode and eliminates all 500 errors.
const socket = io({{transports: ['polling'], upgrade: false}});

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
# 7. UTILITY HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def draw_text_bg(img, text, x, y, font_scale=0.45, thickness=1,
                 bg=(0, 0, 0), fg=(255, 255, 255)):
    """Render text with a filled background rectangle for visibility."""
    (tw, th), _ = cv2.getTextSize(
        text, cv2.FONT_HERSHEY_SIMPLEX, font_scale, thickness
    )
    cv2.rectangle(img, (x, y - th - 4), (x + tw + 2, y + 4), bg, -1)
    cv2.putText(img, text, (x, y), cv2.FONT_HERSHEY_SIMPLEX,
                font_scale, fg, thickness, cv2.LINE_AA)


def now_iso() -> str:
    """Return current UTC time as ISO 8601 string for CSV records."""
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def _get_daily_csv_path(static_path: str, headers: list) -> str:
    """
    Return a date-stamped CSV path for today derived from static_path.
    Creates the file with headers if it does not yet exist for today.
    Midnight rotation is automatic — the date is recomputed on every call.
    """
    directory  = os.path.dirname(static_path)
    today      = datetime.now().strftime('%Y-%m-%d')
    dated_path = os.path.join(directory, f"{today}_{os.path.basename(static_path)}")
    if not os.path.exists(dated_path):
        with open(dated_path, 'w', newline='') as f:
            csv.writer(f).writerow(headers)
    return dated_path


def init_nvml():
    """
    Initialise NVML for GPU utilisation monitoring.
    Returns the device handle on success, None if unavailable.
    Non-fatal — GPU utilisation column will be empty in HC CSV if unavailable.
    """
    try:
        pynvml.nvmlInit()
        handle = pynvml.nvmlDeviceGetHandleByIndex(0)
        logging.info("[NVML] GPU monitoring initialised successfully.")
        return handle
    except Exception as exc:
        logging.warning(
            "[NVML] Could not initialise GPU monitoring: %s — "
            "gpu_utilization column will be empty.", exc
        )
        return None


def get_gpu_util(handle) -> int:
    """Return GPU utilisation percentage (0-100), or -1 if unavailable."""
    if handle is None:
        return -1
    try:
        return pynvml.nvmlDeviceGetUtilizationRates(handle).gpu
    except Exception:
        return -1


class StreamingCSVWriter:
    """
    Thread-safe CSV writer that appends rows directly to disk.
    Each write opens, appends, and closes the file to ensure data is
    flushed even if the process is killed between writes.
    """
    def __init__(self, path: str, headers: list):
        self._base_path = path
        self._headers   = headers
        self._lock      = threading.Lock()

    def write(self, row: list):
        with self._lock:
            dated = _get_daily_csv_path(self._base_path, self._headers)
            with open(dated, 'a', newline='') as f:
                csv.writer(f).writerow(row)


_csv_lock = threading.Lock()


# ──────────────────────────────────────────────────────────────────────────────
# 8. ANIMAL DETECTION PIPELINE
#    Semaphore: _pytorch_sem (Phase 1 FIX-T3)
#    CUDA sync: torch.cuda.synchronize() after model.track() (Phase 1 FIX-T4)
# ──────────────────────────────────────────────────────────────────────────────

AN_CLASS_NAMES = {0: 'animal'}

AN_COLORS = {'animal': (0, 200, 255)}


def run_animal_detection(
    topic: str,
    frame: np.ndarray,
    display: np.ndarray,
    model: YOLO,
    frame_index: int,
    capture_ts: str,
    wall_ts: str,
    csv_path: str,
    warmup_done: bool,
    raw_frames_dir: str,
    ann_frames_dir: str,
    bbox_csv_dir: str,
) -> int:
    """
    Run YOLOv8m BotSORT animal detection on the given frame.

    Semaphore: _pytorch_sem — max 3 concurrent PyTorch inferences.
    CUDA sync: torch.cuda.synchronize() blocks until kernel completes
               before releasing the semaphore. Without this, the next
               thread could submit a new kernel on a still-busy device.
    """
    with _pytorch_sem:
        with torch.no_grad():
            results = model.track(
                frame,
                persist  = True,
                tracker  = BOTSORT_CFG,
                classes  = AN_ANIMAL_CLASSES,
                conf     = AN_CONF_THRESH,
                imgsz    = 1280,
                half     = True,
                verbose  = False,
            )
        # Phase 1 FIX-T4: wait for CUDA kernel completion before releasing
        # semaphore. Ensures semaphore actually gates GPU execution.
        torch.cuda.synchronize()

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
            with open(_get_daily_csv_path(csv_path, [
                'wall_time_iso', 'capture_timestamp', 'source_id',
                'frame_index', 'class_name', 'class_id', 'confidence',
                'track_id', 'x1', 'y1', 'x2', 'y2', 'image_ref',
            ]), 'a', newline='') as f:
                csv.writer(f).writerows(rows)

        # Storage overhaul: raw/annotated JPEG storage disabled — CSV-only output.
        # ts_tag   = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        # stem     = f"frame_{frame_index:08d}_{ts_tag}"
        #
        # cv2.imwrite(
        #     os.path.join(raw_frames_dir, f"{stem}_raw.jpg"),
        #     frame,
        # )
        #
        # cv2.imwrite(
        #     os.path.join(ann_frames_dir, f"{stem}_ann.jpg"),
        #     display,
        # )

        bbox_csv_headers = [
            'frame_index', 'class_name', 'class_id', 'confidence',
            'track_id', 'x1', 'y1', 'x2', 'y2', 'capture_ts',
        ]
        static_bbox_csv = os.path.join(bbox_csv_dir, "bboxes.csv")
        dated_bbox_csv = _get_daily_csv_path(static_bbox_csv, bbox_csv_headers)
        with open(dated_bbox_csv, 'a', newline='') as f:
            writer = csv.writer(f)
            for row in rows:
                writer.writerow([
                    row[3],  # frame_index
                    row[4],  # class_name
                    row[5],  # class_id
                    row[6],  # confidence
                    row[7],  # track_id
                    row[8],  # x1
                    row[9],  # y1
                    row[10], # x2
                    row[11], # y2
                    row[1],  # capture_ts
                ])

    cv2.putText(
        display, f"Animals: {detection_count}",
        (FRAME_W - 150, 55),
        cv2.FONT_HERSHEY_SIMPLEX, 0.55, (255, 165, 0), 2,
    )
    return detection_count


# ──────────────────────────────────────────────────────────────────────────────
# 9. HEAD COUNT PIPELINE
#    Semaphore: _pytorch_sem (Phase 1 FIX-T3)
#    CUDA sync: already present, retained.
# ──────────────────────────────────────────────────────────────────────────────

def run_head_count(
    topic: str,
    frame: np.ndarray,
    display: np.ndarray,
    model: YOLO,
    frame_index: int,
    video_ts_sec: float,
    wall_ts: str,
    hc_bucket: dict,
    stats_writer: StreamingCSVWriter,
    bucket_csv_path: str,
    warmup_fps_tracker: dict,
    gpu_handle,
    raw_frames_dir: str,
    ann_frames_dir: str,
    bbox_csv_dir: str,
) -> int:
    """
    Run custom YOLOv8 BotSORT head count on the given frame.
    Semaphore: _pytorch_sem — shares PyTorch pool with AN pipeline.
    """
    t_start = time.perf_counter()

    with _pytorch_sem:
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

    if head_count > 0:
        # Storage overhaul: raw/annotated JPEG storage disabled — CSV-only output.
        # ts_tag = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        # stem   = f"frame_{frame_index:08d}_{ts_tag}"
        #
        # cv2.imwrite(os.path.join(raw_frames_dir, f"{stem}_raw.jpg"), frame)
        # cv2.imwrite(os.path.join(ann_frames_dir, f"{stem}_ann.jpg"), display)

        bbox_csv_headers = [
            'frame_index', 'confidence', 'x1', 'y1', 'x2', 'y2',
        ]
        static_bbox_csv = os.path.join(bbox_csv_dir, "bboxes.csv")
        dated_bbox_csv = _get_daily_csv_path(static_bbox_csv, bbox_csv_headers)
        with open(dated_bbox_csv, 'a', newline='') as f:
            writer = csv.writer(f)
            if results[0].boxes is not None:
                for box in results[0].boxes:
                    if float(box.conf[0]) >= HC_CONF_THRESH:
                        hx1, hy1, hx2, hy2 = [int(c) for c in box.xyxy[0]]
                        writer.writerow([
                            frame_index,
                            f"{float(box.conf[0]):.4f}",
                            hx1, hy1, hx2, hy2,
                        ])

    # ── FPS tracking (post-warmup) ────────────────────────────────────────────
    if not warmup_fps_tracker.get('warmup_done') and frame_index >= HC_WARMUP_FRAMES:
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

    # ── Interval bucket aggregation ───────────────────────────────────────────
    hc_bucket['counts'].append(head_count)
    elapsed_bucket = time.time() - hc_bucket['interval_start']

    if elapsed_bucket >= HC_INTERVAL_SEC:
        counts = hc_bucket['counts']
        avg    = sum(counts) / len(counts) if counts else 0.0
        interval_end = hc_bucket['interval_start'] + elapsed_bucket

        with _csv_lock:
            with open(_get_daily_csv_path(bucket_csv_path, [
                'wall_time_iso', 'interval_start_sec',
                'interval_end_sec', 'avg_head_count',
            ]), 'a', newline='') as f:
                csv.writer(f).writerow([
                    wall_ts,
                    f"{hc_bucket['interval_start_vid']:.4f}",
                    f"{interval_end:.4f}",
                    f"{avg:.2f}",
                ])

        hc_bucket['interval_start']     = time.time()
        hc_bucket['interval_start_vid'] = video_ts_sec
        hc_bucket['counts']             = []

    cv2.putText(
        display, f"Heads: {head_count}",
        (FRAME_W - 140, 30),
        cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 0, 255), 2,
    )
    return head_count


# ──────────────────────────────────────────────────────────────────────────────
# 10. ENTRY / EXIT PIPELINE
#     Semaphore: _tf_sem (Phase 1 FIX-T3) — isolated from PyTorch AN/HC.
# ──────────────────────────────────────────────────────────────────────────────

def build_ee_encoder():
    """
    Build the TensorFlow ReID feature encoder.
    Sets GPU memory growth to prevent TF from claiming all VRAM at init.
    """
    gpus = tf.config.list_physical_devices('GPU')
    for gpu in gpus:
        try:
            tf.config.experimental.set_memory_growth(gpu, True)
        except RuntimeError as exc:
            logging.warning(
                "[EE Encoder] Could not set memory growth for GPU: %s", exc
            )
    encoder = gdet.create_box_encoder(REID_MODEL_PATH, batch_size=EE_BATCH_SIZE)
    logging.info("[EE Encoder] ReID encoder built successfully.")
    return encoder


def init_ee_tracker():
    """Initialise a new DeepSort tracker instance for one camera thread."""
    metric  = nn_matching.NearestNeighborDistanceMetric(
        'cosine', EE_MAX_COSINE_DIST, EE_NN_BUDGET
    )
    return Tracker(
        metric,
        max_iou_distance = EE_MAX_IOU_DIST,
        max_age          = EE_MAX_AGE,
        n_init           = EE_N_INIT,
    )


def run_entry_exit(
    topic: str,
    frame: np.ndarray,
    display: np.ndarray,
    yolo_model: YOLO,
    encoder,
    tracker,
    ee_config: dict,
    track_history: dict,
    track_state: dict,
    session_totals: dict,
    ee_bucket: dict,
    ee_csv_path: str,
    frame_index: int,
    raw_frames_dir: str,
    ann_frames_dir: str,
    bbox_csv_dir: str,
):
    """
    Run YOLOv5 + DeepSort + TF ReID entry/exit counting on the given frame.
    Semaphore: _tf_sem — max 2 concurrent TF inferences, isolated from PyTorch.
    """
    roi_poly = ee_config.get('roi')
    vec_x    = ee_config.get('vec_x', 0.0)
    vec_y    = ee_config.get('vec_y', 1.0)

    ee_frame = cv2.resize(frame, (EE_WIDTH, EE_HEIGHT))

    with _tf_sem:
        results = yolo_model(
            ee_frame,
            conf    = EE_CONF_THRESH,
            classes = EE_PERSON_CLASS,
            verbose = False,
        )

    # ── Build DeepSort detections ─────────────────────────────────────────────
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
                        Detection(boxes_tlwh[i], confs[i], 'person', feat)
                    )
    else:
        ds_detections = []

    tracker.predict()
    tracker.update(ds_detections)

    # ── Dead-track cleanup — prevents unbounded dict growth ───────────────────
    active_ids = {t.track_id for t in tracker.tracks if t.is_confirmed()}
    for dead_id in list(track_history.keys()):
        if dead_id not in active_ids:
            del track_history[dead_id]
            track_state.pop(dead_id, None)

    # ── Draw ROI boundary ─────────────────────────────────────────────────────
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
                roi_poly, (float(cx), float(cy)), False
            ) >= 0
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
                        "[%s] ENTRY | ID=%4d | dot=%+.2f | "
                        "Total In=%d Out=%d",
                        topic, tid, dot,
                        session_totals['entry'], session_totals['exit'],
                    )
                elif dot < -EE_DOT_THRESH:
                    track_state[tid]       = 1
                    session_totals['exit'] += 1
                    event = 'EXIT'
                    logging.info(
                        "[%s] EXIT  | ID=%4d | dot=%+.2f | "
                        "Total In=%d Out=%d",
                        topic, tid, dot,
                        session_totals['entry'], session_totals['exit'],
                    )

        if event in ('ENTRY', 'EXIT'):
            # Storage overhaul: raw/annotated JPEG storage disabled — CSV-only output.
            # ts_tag = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
            # stem   = f"frame_{frame_index:08d}_{ts_tag}_{event}"
            #
            # cv2.imwrite(os.path.join(raw_frames_dir, f"{stem}_raw.jpg"), frame)
            # cv2.imwrite(os.path.join(ann_frames_dir, f"{stem}_ann.jpg"), display)

            bbox_csv_headers = [
                'frame_index', 'track_id', 'event',
                'x1', 'y1', 'x2', 'y2',
            ]
            static_bbox_csv = os.path.join(bbox_csv_dir, "bboxes.csv")
            dated_bbox_csv = _get_daily_csv_path(static_bbox_csv, bbox_csv_headers)
            with open(dated_bbox_csv, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    frame_index, tid, event,
                    int(tlwh[0]), int(tlwh[1]),
                    int(tlwh[0] + tlwh[2]), int(tlwh[1] + tlwh[3]),
                ])

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

    cv2.putText(
        display,
        f"IN:{session_totals['entry']}  OUT:{session_totals['exit']}",
        (6, FRAME_H - 10),
        cv2.FONT_HERSHEY_SIMPLEX, 0.55, (255, 107, 53), 2,
    )

    now = time.time()
    if now - ee_bucket['last_write'] >= EE_CSV_INTERVAL_SEC:
        ts_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with _csv_lock:
            with open(_get_daily_csv_path(ee_csv_path, [
                'Timestamp', 'CameraIP', 'TotalEntries', 'TotalExits',
            ]), 'a', newline='') as f:
                csv.writer(f).writerow([
                    ts_str, topic,
                    session_totals['entry'],
                    session_totals['exit'],
                ])
        ee_bucket['last_write'] = now
        logging.info(
            "[%s] EE CSV flush — IN=%d OUT=%d",
            topic, session_totals['entry'], session_totals['exit'],
        )


# ──────────────────────────────────────────────────────────────────────────────
# 11. MAIN PROCESSING THREAD
# ──────────────────────────────────────────────────────────────────────────────

def process_feed(cam_cfg: dict):
    """
    Main processing loop for a single camera feed.

    Lifecycle:
      1. Set up output directories and CSV files
      2. Load ML models for the pipelines this camera runs
      3. Detect dominant (live) partition via end offset query
      4. Connect to Kafka with per-camera group + static membership
      5. Seek to end on dominant partition only (single ordered stream)
      6. Consume frames in a loop:
           a. Decode frame from Kafka message
           b. Run enabled pipelines (AN, HC, EE) sequentially
           c. Annotate display frame
           d. Write to AVI and publish to dashboard
           e. Commit offset every COMMIT_EVERY_N_FRAMES frames

    Args:
        cam_cfg: One entry from CAMERA_REGISTRY dict.
    """
    topic      = cam_cfg['topic']
    folder     = cam_cfg['folder']
    run_AN     = cam_cfg['AN']
    run_HC     = cam_cfg['HC']
    run_EE     = cam_cfg['EE']
    cam_ip     = cam_cfg['ip']
    # Storage overhaul: video storage disabled — session_ts was only used for
    # AVI filenames, now unused (kept commented for context, not deleted).
    # session_ts = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Per-camera logger — all log lines tagged with topic name.
    log = logging.getLogger(topic)
    log.info("Thread starting — AN=%s HC=%s EE=%s", run_AN, run_HC, run_EE)

    # ── Per-camera Kafka group identifiers (FIX-P2-2) ──────────────────────────
    # group_id: Each camera is an independent consumer group.
    #   Crash/restart of one camera has zero effect on others.
    # NOTE: group_instance_id (FIX-P2-3) is intentionally NOT set here.
    #   The kafka-python library installed on this system (2.x) does not support
    #   the group_instance_id parameter and raises KafkaConfigurationError if it
    #   is passed. Static membership is therefore disabled; seek_to_end() on
    #   startup and FIX-E3 lag teleport handle live-edge positioning instead.
    safe_topic   = topic.replace('.', '_')
    cam_group_id = f"mlp_{safe_topic}"

    log.info("Kafka group config — group_id: %s", cam_group_id)

    # ── Create output directory structure ─────────────────────────────────────
    cam_root = os.path.join(BASE_OUTPUT_DIR, folder)
    dirs = {}
    for pipeline in ['animal_detection', 'head_count', 'entry_exit']:
        dirs[pipeline] = {
            'csv'         : os.path.join(cam_root, pipeline, 'csv'),
            'inference'   : os.path.join(cam_root, pipeline, 'inference'),
            'raw_frames'  : os.path.join(cam_root, pipeline, 'raw_frames'),
            'ann_frames'  : os.path.join(cam_root, pipeline, 'annotated_frames'),
            'bbox_csv'    : os.path.join(cam_root, pipeline, 'bbox_csv'),
        }
        os.makedirs(dirs[pipeline]['csv'],        exist_ok=True)
        # Storage overhaul: these dirs no longer created — video/image storage disabled.
        # os.makedirs(dirs[pipeline]['inference'],  exist_ok=True)
        # os.makedirs(dirs[pipeline]['raw_frames'], exist_ok=True)
        # os.makedirs(dirs[pipeline]['ann_frames'], exist_ok=True)
        os.makedirs(dirs[pipeline]['bbox_csv'],   exist_ok=True)

    # ── Initialise AN CSV ─────────────────────────────────────────────────────
    an_csv_path = None
    if run_AN:
        an_csv_path = os.path.join(
            dirs['animal_detection']['csv'],
            f"{folder}_animal_detection.csv",
        )

    # ── Initialise HC CSV ─────────────────────────────────────────────────────
    hc_stats_writer = None
    hc_bucket_path  = None
    hc_bucket       = {}
    hc_fps_tracker  = {}
    if run_HC:
        hc_stats_path  = os.path.join(
            dirs['head_count']['csv'],
            f"{folder}_headcount_stats.csv",
        )
        hc_bucket_path = os.path.join(
            dirs['head_count']['csv'],
            f"{folder}_headcount_bucket.csv",
        )
        hc_stats_writer = StreamingCSVWriter(hc_stats_path, [
            'wall_time_iso', 'frame_index', 'video_timestamp_sec',
            'inference_time_ms', 'running_avg_fps', 'head_count',
            'cuda_mem_allocated_mb', 'cuda_mem_reserved_mb', 'gpu_utilization_pct',
        ])
        hc_bucket = {
            'interval_start'    : time.time(),
            'interval_start_vid': 0.0,
            'counts'            : [],
        }
        hc_fps_tracker = {'warmup_done': False}

    # ── Initialise EE CSV and config ──────────────────────────────────────────
    ee_csv_path    = None
    ee_config      = None
    ee_tracker     = None
    ee_encoder     = None
    track_history  = {}
    track_state    = {}
    session_totals = {'entry': 0, 'exit': 0}
    ee_bucket      = {'last_write': time.time()}

    if run_EE:
        ee_csv_path = os.path.join(
            dirs['entry_exit']['csv'],
            f"{folder}_entry_exit.csv",
        )
        ee_config = EE_CAM_CONFIGS.get(cam_ip)
        if ee_config is None:
            log.warning(
                "No ROI config found for IP %s — EE pipeline will run "
                "without ROI gating (full frame).", cam_ip
            )

    # ── Load ML models ────────────────────────────────────────────────────────
    model_animal    = None
    model_headcount = None
    model_ee_yolo   = None

    if run_AN:
        log.info("Loading AN model: %s", MODEL_ANIMAL_PATH)
        model_animal = YOLO(MODEL_ANIMAL_PATH)
        model_animal.to('cuda')
        log.info("AN model loaded and moved to CUDA.")

    if run_HC:
        log.info("Loading HC model: %s", MODEL_HEADCOUNT_PATH)
        model_headcount = YOLO(MODEL_HEADCOUNT_PATH)
        model_headcount.to('cuda')
        log.info("HC model loaded and moved to CUDA.")

    if run_EE:
        log.info("Loading EE YOLO model: %s", MODEL_EE_YOLO_PATH)
        model_ee_yolo = YOLO(MODEL_EE_YOLO_PATH)
        log.info("Building EE ReID encoder: %s", REID_MODEL_PATH)
        ee_encoder    = build_ee_encoder()
        ee_tracker    = init_ee_tracker()
        log.info("EE pipeline fully initialised.")

    gpu_handle = init_nvml()

    # ── Step 1: Detect dominant (live) partition (FIX-E1 DELTA CHECK) ─────────
    # We now use the Delta Check to measure actual frame growth.
    dominant_partition = detect_dominant_partition(
        topic   = topic,
        brokers = BROKERS.split(','),
        log     = log,
    )

    # ── Step 2: Create Kafka consumer with Phase 2 config ─────────────────────
    consumer = None
    backoff  = 2  # exponential backoff starting at 2s, capped at 30s

    while consumer is None:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers     = BROKERS.split(','),
                # FIX-P2-2: Per-camera group ID — complete isolation between cameras.
                group_id              = cam_group_id,
                auto_offset_reset     = 'latest',
                # FIX-P2-6: Disable auto-commit — we commit manually every
                #   COMMIT_EVERY_N_FRAMES frames for precise crash recovery.
                enable_auto_commit    = False,
                # FIX-P2-4: Reduced from 60s — faster crash detection.
                session_timeout_ms    = 15000,
                heartbeat_interval_ms = 3000,
                # FIX-P2-5: One frame at a time — no batch accumulation.
                max_poll_records      = 1,
                max_poll_interval_ms  = 600000,
                fetch_max_bytes       = 52428800,
            )

            # ── Assign ONLY the dominant partition (FIX-P2-1) ─────────────────
            # Single partition = single ordered log = Kafka ordering guarantee.
            # Partitions with stale historical data are never polled.
            dominant_tp = TopicPartition(topic, dominant_partition)
            consumer.assign([dominant_tp])

            log.info(
                "Kafka consumer assigned — topic: %s | partition: %d (dominant only) | "
                "group: %s",
                topic, dominant_partition, cam_group_id,
            )

            # Seek to end of the dominant partition.
            consumer.seek_to_end(dominant_tp)
            log.info(
                "Seeking to end of partition %d for topic %s — "
                "consuming live frames only.",
                dominant_partition, topic,
            )

            consumer.poll(timeout_ms=5000)
            log.info(
                "Warm-up poll complete for topic %s — "
                "entering frame consumption loop.",
                topic,
            )

            backoff = 2  # reset on success

        except Exception as exc:
            log.error(
                "Kafka consumer init failed for topic %s: %s — "
                "retrying in %ds (exponential backoff).",
                topic, exc, backoff,
                exc_info=True,
            )
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass
            consumer = None
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

    # ── Video writer setup ────────────────────────────────────────────────────
    # Storage overhaul: video storage disabled entirely — see consumers/README.md.
    # video_writer = None
    # frames_in_current_file = 0
    # file_part_number = 1
    #
    # vid_pipeline = (
    #     'head_count'       if run_HC else
    #     'animal_detection' if run_AN else
    #     'entry_exit'
    # )

    frame_count   = 0
    frames_since_commit = 0   # tracks frames processed since last offset commit

    log.info("Entering frame consumption loop for topic %s.", topic)

    # ── Main frame consumption loop ───────────────────────────────────────────
    for msg in consumer:
        frame_count += 1

        # Static frame skip — Phase 3 will replace with dynamic lag-based skip.
        if frame_count % SKIP_FRAMES != 0:
            continue

        try:
            # ── Validate and unpack 8-byte nanosecond timestamp ───────────────
            if len(msg.value) < 8:
                log.warning(
                    "Frame %d: message too short (%d bytes) — skipping.",
                    frame_count, len(msg.value),
                )
                continue

            ts_ns      = struct.unpack('>Q', msg.value[:8])[0]
            ts_sec     = ts_ns / 1e9
            capture_ts = datetime.fromtimestamp(ts_sec).strftime(
                '%Y-%m-%d %H:%M:%S.%f')[:-3]
            wall_ts    = now_iso()

            # ── FIX-E3: DYNAMIC LAG-BASED SKIPPING (OFFSET-BASED) ─────────────
            highwater = consumer.highwater(dominant_tp)
            if highwater is not None:
                lag_frames = highwater - msg.offset
                
                if lag_frames > MAX_LAG_FRAMES:
                    # We are drastically behind. Instead of skipping frames one by one 
                    # over the network, instantly jump the Kafka pointer to the live edge.
                    log.warning("Lag Spike: %d frames behind. Teleporting to real-time.", lag_frames)
                    
                    consumer.seek_to_end(dominant_tp)
                    frames_since_commit = 0
                    try:
                        consumer.commit()
                    except Exception:
                        pass
                    continue

            # ── Decode JPEG frame ─────────────────────────────────────────────
            frame = cv2.imdecode(
                np.frombuffer(msg.value[8:], np.uint8),
                cv2.IMREAD_COLOR,
            )
            if frame is None:
                log.warning(
                    "Frame %d: JPEG decode returned None (corrupt payload?) — skipping.",
                    frame_count,
                )
                continue

            frame   = cv2.resize(frame, (FRAME_W, FRAME_H))
            display = frame.copy()

            # ── FIX-E2: VIDEO ROTATION LOGIC ──────────────────────────────────
            # Storage overhaul: video storage disabled entirely.
            # if SAVE_VIDEO:
            #     if video_writer is not None and frames_in_current_file >= MAX_FRAMES_PER_AVI:
            #         video_writer.release()
            #         video_writer = None
            #         log.info("Rotated video to part %d for %s", file_part_number + 1, topic)
            #         file_part_number += 1
            #         frames_in_current_file = 0
            #
            #     if video_writer is None:
            #         vid_path = os.path.join(
            #             dirs[vid_pipeline]['inference'],
            #             f"{folder}_annotated_{session_ts}_part{file_part_number}.avi",
            #         )
            #         video_writer = cv2.VideoWriter(
            #             vid_path,
            #             cv2.VideoWriter_fourcc(*'XVID'),
            #             18.0,
            #             (FRAME_W, FRAME_H),
            #         )
            #         log.info("AVI writer opened: %s", vid_path)

            video_ts_sec = frame_count / 18.0

            # ── Animal Detection pipeline ─────────────────────────────────────
            if run_AN and model_animal is not None:
                run_animal_detection(
                    topic, frame, display,
                    model_animal, frame_count,
                    capture_ts, wall_ts,
                    an_csv_path,
                    frame_count > AN_WARMUP_FRAMES,
                    dirs['animal_detection']['raw_frames'],
                    dirs['animal_detection']['ann_frames'],
                    dirs['animal_detection']['bbox_csv'],
                )

            # ── Head Count pipeline ───────────────────────────────────────────
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
                    dirs['head_count']['raw_frames'],
                    dirs['head_count']['ann_frames'],
                    dirs['head_count']['bbox_csv'],
                )

            # ── Entry/Exit pipeline ───────────────────────────────────────────
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
                    dirs['entry_exit']['raw_frames'],
                    dirs['entry_exit']['ann_frames'],
                    dirs['entry_exit']['bbox_csv'],
                )

            # ── Overlay capture timestamp ─────────────────────────────────────
            cv2.putText(
                display, capture_ts,
                (6, FRAME_H - 28),
                cv2.FONT_HERSHEY_SIMPLEX, 0.38, (180, 180, 180), 1,
            )

            # ── Write to AVI ──────────────────────────────────────────────────
            # Storage overhaul: video storage disabled entirely.
            # if video_writer:
            #     video_writer.write(display)
            #     frames_in_current_file += 1

            # ── Publish to dashboard ──────────────────────────────────────────
            publish_frame(topic, display, ts_sec)

            # ── Manual offset commit (FIX-P2-6) ──────────────────────────────
            # Commit the current offset to the broker every COMMIT_EVERY_N_FRAMES
            # successfully processed frames. This checkpoints the consumer's
            # position so that on crash+restart, the broker resumes from here
            # rather than discarding all crash-window frames via seek_to_end().
            # Maximum data loss on crash = COMMIT_EVERY_N_FRAMES frames (10).
            frames_since_commit += 1
            if frames_since_commit >= COMMIT_EVERY_N_FRAMES:
                try:
                    consumer.commit()
                    frames_since_commit = 0
                    log.debug(
                        "Committed offset — topic: %s | partition: %d | "
                        "frame: %d",
                        topic, dominant_partition, frame_count,
                    )
                except Exception as commit_exc:
                    # Commit failure is non-fatal — worst case we reprocess
                    # up to COMMIT_EVERY_N_FRAMES frames on next restart.
                    log.warning(
                        "Offset commit failed for topic %s: %s — "
                        "will retry on next commit cycle.",
                        topic, commit_exc,
                    )

            # ── Periodic memory cleanup ───────────────────────────────────────
            if frame_count % 500 == 0:
                gc.collect()
                torch.cuda.empty_cache()

            # ── Periodic status log ───────────────────────────────────────────
            if frame_count % 200 == 0:
                if run_EE:
                    log.info(
                        "Frame %d | partition: %d | EE IN=%d OUT=%d",
                        frame_count, dominant_partition,
                        session_totals['entry'], session_totals['exit'],
                    )
                else:
                    log.info(
                        "Frame %d | partition: %d",
                        frame_count, dominant_partition,
                    )

        except Exception as exc:
            # Log with full traceback but do not crash the thread.
            # The frame is skipped and consumption continues.
            log.error(
                "Frame %d processing error: %s",
                frame_count, exc,
                exc_info=True,
            )
            continue


# ──────────────────────────────────────────────────────────────────────────────
# 12. ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    logging.basicConfig(
        level   = logging.INFO,
        format  = '%(asctime)s [%(name)s] %(levelname)s %(message)s',
        datefmt = '%Y-%m-%d %H:%M:%S',
    )

    logging.info("=" * 70)
    logging.info("Multi-Pipeline Consumer — PHASE 2 BUILD")
    logging.info(
        "Phase 1: eventlet removed | threading SocketIO | "
        "split semaphores | AN cuda sync"
    )
    logging.info(
        "Phase 2: dominant partition detection | per-camera groups | "
        "static membership | session 15s | poll=1 | manual commits | "
        "WebSocket polling fix"
    )
    logging.info("=" * 70)

    load_ee_camera_configs()
    os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

    # ── Spawn one daemon thread per camera ────────────────────────────────────
    # daemon=True ensures threads exit cleanly when the main process receives
    # SIGTERM. The SocketIO server in the main thread anchors the process.
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
        # 3-second stagger between thread starts prevents simultaneous
        # partition detection queries from saturating the broker metadata API.
        time.sleep(3)

    logging.info(
        "All %d camera threads started. Dashboard: http://0.0.0.0:%d",
        len(threads), PORT,
    )

    # ── Start Flask-SocketIO server (blocks main thread) ─────────────────────
    # async_mode='threading': real OS threads, CUDA-safe (Phase 1 FIX-T2).
    # WebSocket upgrades not supported by Werkzeug in threading mode.
    # JS client configured to use polling transport (FIX-P2-7).
    socketio.run(
        app,
        host                  = '0.0.0.0',
        port                  = PORT,
        allow_unsafe_werkzeug = True,
        use_reloader          = False,
        log_output            = False,
    )
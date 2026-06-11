#!/usr/bin/env python3
"""
Kafka Producer — gate_2_exit_camera (G2-EX)
Topic   : video.raw.g2_ex
RTSP    : rtsp://admin1:Plaksha%402025123@10.1.34.239:554/cam/realmonitor?channel=1&subtype=1
Brokers : 10.1.40.43:9092, 10.1.40.44:9093, 10.1.40.45:9094
Message : 8-byte big-endian uint64 (nanoseconds) + JPEG bytes
FIXES   : stderr suppressed on FFmpeg | safe subprocess shutdown
"""

import subprocess
import select
import struct
import sys
import time
import logging

import cv2
import numpy as np
from kafka import KafkaProducer

# ── CONFIGURATION ─────────────────────────────────────────────────────────────
RTSP_URL          = "rtsp://admin1:Plaksha%402025123@10.1.34.239:554/cam/realmonitor?channel=1&subtype=1"
KAFKA_BROKERS     = "10.1.40.43:9092,10.1.40.44:9093,10.1.40.45:9094"
TOPIC_NAME        = "video.raw.g2_ex"
WIDTH             = 640
HEIGHT            = 480
JPEG_QUALITY      = 95
FRAME_TIMEOUT_SEC = 5.0
RESTART_DELAY_SEC = 2

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level   = logging.INFO,
    format  = "[%(asctime)s] [%(levelname)s] %(message)s",
    handlers= [logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("G2-EX")

# ── FUNCTIONS ─────────────────────────────────────────────────────────────────

def start_ffmpeg_pipe():
    """
    Spawn FFmpeg reading RTSP over TCP, outputting raw BGR24 frames.
    FIX: stderr=subprocess.DEVNULL — suppresses FFmpeg connection error spam
    on terminal; -loglevel quiet only mutes decoder messages, not stderr.
    """
    log.info("Starting FFmpeg stream...")
    cmd = [
        "ffmpeg",
        "-rtsp_transport", "tcp",
        "-i", RTSP_URL,
        "-vf", f"scale={WIDTH}:{HEIGHT}",
        "-f", "image2pipe",
        "-pix_fmt", "bgr24",
        "-vcodec", "rawvideo",
        "-loglevel", "quiet",
        "-"
    ]
    return subprocess.Popen(
        cmd,
        stdout = subprocess.PIPE,
        stderr = subprocess.DEVNULL,   # FIX: suppress stderr noise
        bufsize = 10**8,
    )


def create_kafka_producer():
    """
    KafkaProducer with acks=0 (fire-and-forget) for maximum throughput.
    Acceptable for video frames where an occasional dropped frame is tolerable.
    """
    return KafkaProducer(
        bootstrap_servers = KAFKA_BROKERS.split(","),
        acks              = 0,
        linger_ms         = 5,
        value_serializer  = lambda v: v,
    )


# ── MAIN PRODUCER LOOP ────────────────────────────────────────────────────────

def run_producer():
    """
    Main loop:
      1. Read raw BGR frame from FFmpeg pipe
      2. JPEG encode
      3. Prepend 8-byte big-endian uint64 timestamp (nanoseconds)
      4. Send to Kafka topic
    Restarts FFmpeg automatically on stall or incomplete frame.
    """
    producer    = create_kafka_producer()
    frame_size  = WIDTH * HEIGHT * 3
    frame_count = 0
    start_time  = time.time()
    pipe        = start_ffmpeg_pipe()

    try:
        while True:
            rlist, _, _ = select.select(
                [pipe.stdout], [], [], FRAME_TIMEOUT_SEC)

            if not rlist:
                log.warning("No frame in %.1fs — restarting FFmpeg...",
                            FRAME_TIMEOUT_SEC)
                _safe_kill(pipe)
                time.sleep(RESTART_DELAY_SEC)
                pipe = start_ffmpeg_pipe()
                continue

            raw = pipe.stdout.read(frame_size)
            if len(raw) != frame_size:
                log.warning("Incomplete frame (%d/%d bytes) — restarting FFmpeg...",
                            len(raw), frame_size)
                _safe_kill(pipe)
                time.sleep(RESTART_DELAY_SEC)
                pipe = start_ffmpeg_pipe()
                continue

            frame = np.frombuffer(raw, dtype=np.uint8).reshape((HEIGHT, WIDTH, 3))
            ok, jpeg = cv2.imencode(
                ".jpg", frame,
                [int(cv2.IMWRITE_JPEG_QUALITY), JPEG_QUALITY])
            if not ok:
                log.warning("JPEG encode failed — skipping frame")
                continue

            
            payload   = struct.pack(">Q", int(time.time() * 1e9)) + jpeg.tobytes()

            future = producer.send(
                TOPIC_NAME,
                key   = TOPIC_NAME.encode("utf-8"),
                value = payload,
            )
            future.add_errback(
                lambda exc: log.error("Kafka send failed: %s", exc))

            frame_count += 1
            if frame_count % 30 == 0:
                elapsed = time.time() - start_time
                fps     = frame_count / elapsed
                log.info("Frames sent: %d | Avg FPS: %.2f", frame_count, fps)

    except KeyboardInterrupt:
        log.info("Interrupted by user — shutting down")

    finally:
        log.info("Closing FFmpeg and Kafka producer...")
        _safe_kill(pipe)
        producer.flush()
        producer.close()


def _safe_kill(pipe):
    """
    FIX: Properly close FFmpeg subprocess.
    pipe.terminate() sends SIGTERM; if process ignores it, stdout.close()
    unblocks any pending read and wait() reaps the zombie.
    """
    try:
        pipe.stdout.close()
    except Exception:
        pass
    try:
        pipe.terminate()
        pipe.wait(timeout=5)
    except Exception:
        try:
            pipe.kill()
        except Exception:
            pass


# ── ENTRY POINT ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run_producer()

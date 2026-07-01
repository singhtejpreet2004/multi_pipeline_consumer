# CLAUDE.md

See CONTRIBUTING.md for branching, commit, and PR rules — follow them exactly when proposing changes.

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

A Kafka-driven, multi-threaded real-time video analytics pipeline deployed on an NVIDIA L40S GPU server. Producers ingest RTSP camera streams and publish encoded frames to Kafka. One consumer (`consumers/consumer.py`) spawns a daemon thread per camera and runs three concurrent ML pipelines per frame: Animal Detection (YOLOv8m + BotSORT), Head Count (YOLOv8 + BotSORT), and Entry/Exit (YOLOv5 + DeepSort + TF ReID). A Flask-SocketIO dashboard streams annotated frames to a browser at port 8675.

## Environment

This project uses a local `venv`, not conda:

```bash
source venv/bin/activate
```

Production runs on a remote GPU server (10.1.41.56) with CUDA, PyTorch, and TensorFlow. The venv there is `ee-venv`. ML model `.pt` files live under `/data/` on the server and are not committed to git.

## Running the System

```bash
# Start all producers
nohup ./scripts/run_producers.sh > logs/producers_master.log 2>&1 &

# Start the consumer (production standard)
nohup python consumers/consumer.py > logs/nohup_consumer.log 2>&1 &

# Monitor
tail -f logs/nohup_consumer.log
```

Stop with `kill -15 <PID>`. Use `kill -9` only as last resort — it prevents clean Kafka offset commits and CSV flushes.

## Running Tests

```bash
# Run all tests
pytest tests/

# Run a single test file
pytest tests/test_frame_saving.py -v

# Run a single test
pytest tests/test_frame_saving.py::test_animal_detection_frame_saving -v
```

Tests mock GPU/CUDA calls and import directly from `consumers/consumer`. They are designed to run on CPU-only CI environments — no GPU required.

## Critical Architecture Constraints

**Do not violate these — they exist because of production incidents:**

- **No eventlet**: `eventlet.monkey_patch()` causes SIGSEGV with CUDA on this system. A guard assertion at the top of the consumer script fires if eventlet is ever imported. Do not remove it.
- **SocketIO async_mode must be `'threading'`**: Required for CUDA safety. WebSocket upgrade is not supported in this mode; JS client uses `transports: ['polling']`.
- **GPU semaphores**: `_pytorch_sem = Semaphore(3)` gates AN and HC (PyTorch). `_tf_sem = Semaphore(2)` gates EE (TensorFlow). These values are tuned for L40S 48GB VRAM. Always acquire the correct semaphore before any GPU call and call `torch.cuda.synchronize()` before releasing `_pytorch_sem`.
- **Per-camera Kafka groups**: Each camera uses `group_id = f"mlp_{topic.replace('.','_')}"`. Never use a shared group ID — it causes rebalance cascades that kill all camera threads.
- **Manual offset commits**: `enable_auto_commit=False`. Offsets are committed every `COMMIT_EVERY_N_FRAMES=10` frames. The lag teleport (`seek_to_end` when lag > 54 frames) also commits immediately.

## Consumer Lifecycle (`process_feed`)

Each camera thread follows this sequence:
1. Create output directories under `BASE_OUTPUT_DIR/<camera_folder>/<pipeline>/{csv,inference,raw_frames,annotated_frames,bbox_csv}/`
2. Load ML models onto CUDA
3. Run `detect_dominant_partition()` — a delta-based probe that finds the live Kafka partition (critical: avoids historical dead partitions)
4. Assign consumer to dominant partition only and `seek_to_end`
5. Process frames: decode JPEG from `msg.value[8:]` (first 8 bytes are big-endian nanosecond timestamp), run enabled pipelines, write AVI, publish to dashboard
6. AVI rotates every 18,000 frames (FIX-E2) to prevent OpenCV container corruption

## Producer Message Format

Every Kafka message: `struct.pack(">Q", int(time.time() * 1e9)) + jpeg_bytes`

Consumer unpacks: `ts_ns = struct.unpack('>Q', msg.value[:8])[0]`, frame bytes from `msg.value[8:]`.

## Output Structure

All output is gitignored. On the GPU server:
```
/data/multi_pipeline_consumer/output/<camera_folder>/
    animal_detection/{csv,inference,raw_frames,annotated_frames,bbox_csv}/
    head_count/{csv,inference,raw_frames,annotated_frames,bbox_csv}/
    entry_exit/{csv,inference,raw_frames,annotated_frames,bbox_csv}/
```

CSV files are date-stamped via `_get_daily_csv_path()` — midnight rotation is automatic. Frame filenames follow `frame_{index:08d}_{YYYYMMDD_HHMMSS_uSec}_{raw|ann}.jpg`.

## File Roles

| File | Status |
|---|---|
| `consumers/consumer.py` | Production — current standard |
| `archive/consumer_v0.py`, `consumer_v0_phase1.py`, `consumer_v0_phase2.py` | Archived iterations — rollback only |
| `miscellaneous/daily_csv_rotation.py` | Utility for manual CSV rotation enforcement |
| `producers/producer_*.py` | One script per camera, all structurally identical |
| `scripts/run_producers.sh` | Starts all producers |
| `miscellaneous/setup_rsync_cron.sh` | Configures daily CSV rsync to HPC cluster |
| `miscellaneous/cleanup_old_output.sh` | Prunes old output directories |

## Camera Registry

`CAMERA_REGISTRY` in `consumer.py` is the single source of truth for camera topology. Each entry maps a Kafka topic → output folder name → camera IP → which pipelines (AN/HC/EE) are enabled. Adding a camera requires adding an entry here and creating a corresponding producer script.

## Dashboard

Flask-SocketIO at `http://10.1.40.46:8675`. Client subscribes to camera rooms; server emits base64-encoded JPEG frames after each inference. Snapshot endpoint: `/snapshot/<topic>`.

## Working agreement for code changes

Before applying any code change, always:
1. Show the full diff of what will change.
2. Explain the reasoning — what's changing and why — in plain terms.
3. Wait for explicit approval before applying.

This applies to every change, no exceptions, including changes that seem small or obviously correct.

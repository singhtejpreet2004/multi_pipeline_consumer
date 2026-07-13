# Multi-Pipeline Video Inference Consumer

Welcome to the **Multi-Pipeline Consumer** repository! This project is a highly scalable, Kafka-driven video analytics pipeline designed to run real-time Machine Learning models on RTSP video streams. It is optimized to run on powerful GPU hardware (specifically NVIDIA L40S servers).

---

## 🏛 Architecture Overview

The system is decoupled into **Producers** (ingestion) and **Consumers** (inference), communicating via **Apache Kafka**. This ensures that heavy GPU processing never blocks the real-time camera ingestion.

```mermaid
graph TD
    %% Define Styles
    classDef hardware fill:#2d3436,stroke:#74b9ff,stroke-width:2px,color:#fff
    classDef kafka fill:#e17055,stroke:#d63031,stroke-width:2px,color:#fff
    classDef consumer fill:#0984e3,stroke:#74b9ff,stroke-width:2px,color:#fff
    classDef output fill:#00b894,stroke:#55efc4,stroke-width:2px,color:#fff

    %% Nodes
    subgraph Edge Network
        C1[RTSP Camera 1]
        C2[RTSP Camera 2]
        C3[RTSP Camera N]
    end

    subgraph Ingestion Layer
        P1[Producer Script 1]
        P2[Producer Script 2]
        P3[Producer Script N]
    end

    K{Apache Kafka Broker}:::kafka

    subgraph GPU Server NVIDIA L40S
        Consumer[consumer.py]:::consumer
        T1((Thread 1))
        T2((Thread 2))
        TN((Thread N))
        
        Consumer --> T1 & T2 & TN
        
        subgraph ML Pipelines per Thread
            AN[Animal Detection YOLOv8]
            HC[Head Count YOLOv8]
            EE[Entry/Exit YOLOv5]
        end
        T1 -.-> AN & HC & EE
    end

    subgraph Outputs
        DB[(CSV Storage)]:::output
        FS[Saved Frames raw/ann]:::output
        WS[Flask Web Dashboard]:::output
    end

    %% Connections
    C1 --> P1
    C2 --> P2
    C3 --> P3
    
    P1 -->|Encodes frames| K
    P2 -->|Encodes frames| K
    P3 -->|Encodes frames| K
    
    K -->|Consumes batches| Consumer
    
    AN & HC & EE --> DB
    AN & HC & EE --> FS
    Consumer --> WS
```

---

## 📂 Codebase Structure

The repository is organized into specific domains to maintain separation of concerns.

| Directory | Purpose | Documentation |
| :--- | :--- | :--- |
| `consumers/` | The core intelligence engine. Contains scripts that pull from Kafka, manage GPU semaphores, and run ML inferences. | [Read More](consumers/README.md) |
| `producers/` | Lightweight ingestion scripts that connect to RTSP streams and push buffered frames to Kafka topics. | [Read More](producers/README.md) |
| `miscellaneous/` | Essential operational bash scripts for deployment, backups (`rsync`), and old output cleanup. | [Read More](miscellaneous/README.md) |
| `scripts/` | Entry-point scripts for starting producers and the consumer in production. | [Read More](scripts/README.md) |
| `archive/` | Retired consumer iterations kept for rollback reference only. | [Read More](archive/README.md) |
| `docs/` | Project governance docs (branching, versioning) and supplementary documentation. | [Read More](docs/README.md) |
| `tests/` | Automated unit tests utilizing extensive mocking to run GPU logic on CI/CD CPU environments. | [Read More](tests/README.md) |
| `output/` *(Ignored)* | Automatically generated directory containing saved frames, bounding box CSVs, and videos. | N/A |
| `logs/` *(Ignored)* | Runtime logging output from the daemons. | N/A |

> [!NOTE]
> `ES` and `ec` at the repo root are untracked, non-functional artifacts (three stray `local=''`
> lines and an empty file, respectively) — believed to be shell-redirection typos from a past
> session, not part of the codebase. Kept as-is per maintainer decision; do not treat as config or
> code.

---

## 🔧 Technical Specifications

| Parameter | Value | Notes |
|---|---|---|
| `_pytorch_sem` | `Semaphore(3)` | Gates Animal Detection + Head Count (PyTorch/YOLOv8) |
| `_tf_sem` | `Semaphore(2)` | Gates Entry/Exit (TensorFlow ReID) |
| `MAX_LAG_FRAMES` | `54` | Beyond this, consumer teleports via `seek_to_end()` instead of catching up frame-by-frame |
| `MAX_FRAMES_PER_AVI` | `18000` | AVI rotates every ~16 minutes to prevent OpenCV container corruption |
| Partition detection window | `1.5s` (docstring) / `3.5s` (actual sleep) | Docstring is stale — actual behaviour is 3.5s per `time.sleep()`, line 294. See `consumers/README.md` |
| Producer output resolution | `640×480` | `WIDTH`/`HEIGHT` in all producer scripts, piped from FFmpeg |
| Consumer working resolution | `640×360` | `FRAME_W`/`FRAME_H` in `consumer.py` line 120-121, used for all ML inference |
| JPEG quality | `95` | `cv2.IMWRITE_JPEG_QUALITY`, set in producer scripts |
| FFmpeg transport | `tcp` | `-rtsp_transport tcp`, forces TCP over UDP for reliability |
| Kafka `acks` | `0` | Fire-and-forget — occasional dropped frame tolerable for video |
| Kafka `linger_ms` | `5` | Client-side send buffering only, not frame batching |
| Dashboard port | `8675` | Flask-SocketIO, `PORT = 8675` in `consumer.py` |
| AVI codec | `XVID` | `cv2.VideoWriter_fourcc(*'XVID')` |
| Frame timeout | `5.0s` | Producer `FRAME_TIMEOUT_SEC` — triggers FFmpeg pipe restart if no frame received |
| Restart delay (producer, inner) | `2s` | Producer `RESTART_DELAY_SEC` before relaunching a stalled FFmpeg subprocess |
| Kafka message format | 8-byte big-endian nanosecond timestamp prefix + JPEG bytes | `struct.pack(">Q", int(time.time() * 1e9)) + jpeg_bytes` |

---

## 🚀 Production Deployment Guide

This system runs primarily via background Unix daemons. Below is the standard operating procedure for deploying, running, and stopping the pipelines in production.

### 1. Environment Setup
The project relies heavily on CUDA and PyTorch. Always activate the virtual environment before execution:
```bash
source venv/bin/activate
```

### 2. Starting the System
The system is deployed using `nohup` to ensure it continues running even if the SSH session disconnects.

**Step A: Start Producers**  
*(To start specific cameras, run their respective bash scripts)*
```bash
nohup ./scripts/run_producers.sh > logs/producers_master.log 2>&1 &
```

**Step B: Start the Multi-Pipeline Consumer**  
*(Currently using `consumer.py` as the production standard)*
```bash
nohup python consumers/consumer.py > logs/nohup_consumer.log 2>&1 &
```

---

## 📊 Monitoring & Logs

Once started, the system operates completely autonomously. You can monitor its health using the following tools:

| Monitor Target | Command / URL | What to look for |
| :--- | :--- | :--- |
| **Consumer Logs** | `tail -f logs/nohup_consumer.log` | FPS drops, GPU memory allocations, Kafka offset commits. |
| **Producer Logs** | `tail -f logs/producers_master.log` | RTSP connection drops, frame ingestion rates. |
| **Active Processes** | `ps aux \| grep python` | Ensure your specific consumer and producer PIDs are alive. |
| **Live Video Feed** | `http://10.1.40.46:8675` | The Flask-SocketIO dashboard streaming live annotated feeds. |

---

## 🛑 Stopping the System
If you need to stop the pipelines (e.g., for an upgrade or maintenance):

1. **Find the Process IDs (PIDs):**
   ```bash
   ps aux | grep python
   ```
2. **Kill the processes gracefully:**
   ```bash
   kill -15 <PID>
   ```
   > [!WARNING]
   > Avoid using `kill -9` unless absolutely necessary. A hard kill prevents the script from saving graceful Kafka offset markers and closing CSV files cleanly.

---

## 🛠 File Output & Backups

The system generates several artifacts during execution:
- **Daily CSVs**: Stored in `output/<camera_name>/<pipeline>/csv/`.
- **Saved Frames (Raw/Annotated)**: Triggered upon positive detections to train future models.
- **Videos (.avi)**: Saved during inference intervals.

### Automated Backups
We utilize cron jobs and `rsync` to back up all CSV data to an external HPC cluster daily.
To configure this, run the setup script:
```bash
bash miscellaneous/setup_rsync_cron.sh
```

---

## 🤝 Code Standards & Best Practices

- **Never Commit Output/Logs:** The `.gitignore` file explicitly blocks `output/`, `logs/`, and `.pt` model files. **Do not bypass this.** Doing so will crash the GitHub repository due to file size limits.
- **Resource Locking:** Since the consumer is highly multi-threaded, always respect the `_pytorch_sem` and `_tf_sem` semaphores when writing new pipeline logic to prevent GPU Out-Of-Memory (OOM) errors and race conditions. Bbox CSV writes still use `_csv_lock` (confirmed at 3 call sites in `consumer.py`: lines 983, 1144, 1388) — do not remove it without verifying thread-safety of concurrent CSV writes.

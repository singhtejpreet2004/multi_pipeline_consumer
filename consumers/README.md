# Consumer Module

This directory contains the central intelligence engine of the pipeline. The consumers connect to Kafka topics, pull incoming video frames, and process them through the Machine Learning pipelines.

## Scripts Overview

- **`test_consumer_phase02.py`**: The current **Production** script. It handles multi-threading (one thread per camera), dashboard streaming via Flask-SocketIO, and runs the 3 concurrent ML pipelines.
- **`test_consumer_phase2.py` / `test_consumer_phase1.py` / `test_consumer.py`**: Older iterations of the pipeline kept for archival/rollback purposes.
- **`test_daily_csv_rotation.py`**: A utility script used to enforce daily rotation logic for the tracking CSVs.

## The Architecture of `phase02`

### Multi-Threading
The script uses `threading.Thread` to spawn an isolated processing loop for each camera defined in the `CAMERA_REGISTRY`. This ensures that a laggy camera or broken stream does not halt the entire system.

### GPU Memory Management (The Semaphores)
Because we are running multiple heavy YOLO models across several camera threads concurrently on a single NVIDIA L40S GPU, memory must be strictly managed to prevent Out-Of-Memory (OOM) crashes.

We utilize two primary semaphores:
- `_pytorch_sem = threading.Semaphore(3)`: Limits the system to 3 concurrent PyTorch (YOLOv8/BotSORT) inferences at any given microsecond.
- `_tf_sem = threading.Semaphore(1)`: Limits the system to 1 concurrent TensorFlow (ReID) inference, as TF tends to aggressively pre-allocate GPU memory.

### Frame Saving Feature
Implemented by request from the ML team, the consumer automatically saves positive-detection frames to disk.
When an event triggers (e.g., an animal is detected, or a person enters a zone), the script writes three artifacts:
1. A raw `.jpg` frame.
2. An annotated `.jpg` frame.
3. An isolated bounding-box `.csv`.

*(For more details on frame saving, see `/frame_saving_docs/Frame_Saving_Handover.md`)*.

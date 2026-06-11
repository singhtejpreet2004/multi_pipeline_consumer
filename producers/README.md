# Producer Module

This directory contains the ingestion scripts responsible for capturing live RTSP feeds and forwarding them into the Kafka broker.

## How It Works

Each camera on campus has its own dedicated python script (e.g., `producer_gate_1_main_entry.py`). 
These scripts utilize OpenCV (`cv2.VideoCapture`) to connect to the RTSP URL.

1. **Frame Extraction**: Frames are pulled from the camera buffer.
2. **Batching**: To optimize Kafka network throughput, frames are not sent individually. They are batched and encoded before transmission.
3. **Kafka Publishing**: The frames are published to a specific Kafka topic matching the camera's location.

## Scalability and Design

The separation of Producers from Consumers allows the system to scale massively:
- Producers do very little computational work, meaning dozens can be run on a low-end CPU server.
- If the GPU Consumer crashes, the Producers continue buffering frames into Kafka, ensuring zero data loss during restarts.

## Adding a New Camera
To add a new camera stream to the network:
1. Duplicate an existing producer script.
2. Update the `RTSP_URL` and the Kafka `TOPIC` string inside the script.
3. Add the new topic to the `CAMERA_REGISTRY` in the consumer script.
4. Run the new producer script.

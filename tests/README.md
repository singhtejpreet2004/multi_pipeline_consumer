# Testing Suite

This directory contains automated unit tests and integration tests for the ML pipeline.

## Philosophy

Testing computer vision pipelines that rely on hardware acceleration (CUDA) and heavy models (YOLO, TensorFlow) is notoriously difficult in standard CI/CD pipelines (like GitHub Actions) because those environments rarely have GPU access. 

To solve this, our testing suite utilizes extensive mocking (`unittest.mock.patch` and `MagicMock`). We mock out the heavy YOLO inferences, the PyTorch semaphores, and the CUDA synchronization blocks. This allows us to test the *logic* (like file saving, CSV writing, and math logic) in milliseconds on any CPU machine.

Tests import directly from `consumers.consumer`. That module also imports `generate_detections` and
`deep_sort` from `/data/Entry_Exit`, which only exists on the production GPU server —
`tests/conftest.py` stubs those two specific imports (via `sys.modules`) so the import succeeds
everywhere else. **This does not stub `numpy`, `opencv-python`, `torch`, `tensorflow`, `ultralytics`,
`flask-socketio`, `pynvml`, or `kafka-python`** — those are real dependencies from `requirements.txt`
and must actually be installed (CPU builds are fine; no GPU/CUDA hardware is required) for
`consumers.consumer` to import successfully.

## Running the Tests

Install dependencies, then run `pytest`:

```bash
pip install -r requirements-dev.txt -r requirements.txt   # requirements.txt is large; CPU wheels are fine
pytest tests/
```

There is currently no CI workflow configured in this repository (no `.github/workflows/`) — tests
must be run manually.

### Key Tests
- **`test_frame_saving.py`**: Verifies that the implementation of the ML team's "Frame Saving Feature" correctly generates zero-padded filenames, writes the raw image, writes the annotated image, and generates an isolated bounding-box CSV without deadlocking the pipeline.
- **`conftest.py`**: Not a test — stubs `generate_detections` and `deep_sort` (normally loaded from
  `/data/Entry_Exit`) so `consumers.consumer` can be imported without that path existing.

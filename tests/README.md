# Testing Suite

This directory contains automated unit tests and integration tests for the ML pipeline.

## Philosophy

Testing computer vision pipelines that rely on hardware acceleration (CUDA) and heavy models (YOLO, TensorFlow) is notoriously difficult in standard CI/CD pipelines (like GitHub Actions) because those environments rarely have GPU access. 

To solve this, our testing suite utilizes extensive mocking (`unittest.mock.patch` and `MagicMock`). We mock out the heavy YOLO inferences, the PyTorch semaphores, and the CUDA synchronization blocks. This allows us to test the *logic* (like file saving, CSV writing, and math logic) in milliseconds on any CPU machine.

## Running the Tests

Ensure your virtual environment is active, and simply run `pytest`:

```bash
pytest tests/
```

### Key Tests
- **`test_frame_saving.py`**: Verifies that the implementation of the ML team's "Frame Saving Feature" correctly generates zero-padded filenames, writes the raw image, writes the annotated image, and generates an isolated bounding-box CSV without deadlocking the pipeline.

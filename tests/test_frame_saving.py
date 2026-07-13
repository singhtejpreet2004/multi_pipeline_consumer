import os
import numpy as np
import pytest
from unittest.mock import patch, MagicMock

@pytest.fixture
def mock_dirs(tmp_path):
    raw_dir = tmp_path / "raw_frames"
    ann_dir = tmp_path / "annotated_frames"
    csv_dir = tmp_path / "bbox_csv"
    raw_dir.mkdir()
    ann_dir.mkdir()
    csv_dir.mkdir()
    return str(raw_dir), str(ann_dir), str(csv_dir)

def test_animal_detection_frame_saving(mock_dirs):
    """
    Test that run_animal_detection saves frames and CSV when detections occur.
    """
    raw_dir, ann_dir, csv_dir = mock_dirs
    
    # Mock frame and display
    frame = np.zeros((360, 640, 3), dtype=np.uint8)
    display = np.zeros((360, 640, 3), dtype=np.uint8)
    
    # Mock YOLO model
    mock_model = MagicMock()
    mock_results = MagicMock()
    mock_box = MagicMock()
    mock_box.cls = [0]
    mock_box.conf = [0.9]
    mock_box.id = [1]
    mock_box.xyxy = [[10, 10, 50, 50]]
    mock_results.boxes = [mock_box]
    mock_model.track.return_value = [mock_results]
    
    try:
        from consumers.consumer import run_animal_detection

        with patch("consumers.consumer._pytorch_sem"), \
             patch("consumers.consumer.torch.no_grad"), \
             patch("consumers.consumer.torch.cuda.synchronize"), \
             patch("consumers.consumer._csv_lock"):
            
            run_animal_detection(
                topic="test_topic",
                frame=frame,
                display=display,
                model=mock_model,
                frame_index=42,
                capture_ts="12:00:00",
                wall_ts="12:00:01",
                csv_path=os.path.join(csv_dir, "main.csv"),
                warmup_done=True,
                raw_frames_dir=raw_dir,
                ann_frames_dir=ann_dir,
                bbox_csv_dir=csv_dir
            )
            
        raw_files = os.listdir(raw_dir)
        ann_files = os.listdir(ann_dir)
        csv_files = [f for f in os.listdir(csv_dir) if "bboxes" in f]
        
        assert len(raw_files) == 1, "Raw frame was not saved"
        assert len(ann_files) == 1, "Annotated frame was not saved"
        assert len(csv_files) == 1, "BBox CSV was not saved"
        
        assert "frame_00000042_" in raw_files[0]
        assert "frame_00000042_" in ann_files[0]
        
    except ImportError as e:
        pytest.skip(f"Skipping test due to import error: {e}")

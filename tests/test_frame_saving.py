import os
import time
from collections import deque
from unittest.mock import patch, MagicMock

import numpy as np
import pytest


@pytest.fixture
def mock_dirs(tmp_path):
    raw_dir = tmp_path / "raw_frames"
    ann_dir = tmp_path / "annotated_frames"
    csv_dir = tmp_path / "bbox_csv"
    raw_dir.mkdir()
    ann_dir.mkdir()
    csv_dir.mkdir()
    return str(raw_dir), str(ann_dir), str(csv_dir)


def test_animal_detection_no_image_storage(mock_dirs):
    """
    Storage overhaul: run_animal_detection must NOT write raw/annotated JPEGs
    on a positive detection, but must still write the main CSV and bbox CSV.
    """
    raw_dir, ann_dir, csv_dir = mock_dirs

    frame = np.zeros((360, 640, 3), dtype=np.uint8)
    display = np.zeros((360, 640, 3), dtype=np.uint8)

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
             patch("consumers.consumer._csv_lock"), \
             patch("consumers.consumer.cv2.imwrite") as mock_imwrite:

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
                bbox_csv_dir=csv_dir,
            )

        assert not mock_imwrite.called, "cv2.imwrite must not be called — image storage is disabled"
        assert os.listdir(raw_dir) == [], "raw_frames dir must stay empty"
        assert os.listdir(ann_dir) == [], "annotated_frames dir must stay empty"

        csv_files = [f for f in os.listdir(csv_dir) if "bboxes" in f]
        assert len(csv_files) == 1, "bbox CSV must still be written"

        main_csv_files = [f for f in os.listdir(csv_dir) if "main" in f]
        assert len(main_csv_files) == 1, "main detection CSV must still be written"

    except ImportError as e:
        pytest.skip(f"Skipping test due to import error: {e}")


def test_head_count_no_image_storage(mock_dirs):
    """
    Storage overhaul: run_head_count must NOT write raw/annotated JPEGs when
    heads are detected, but must still write the per-frame stats CSV and
    bbox CSV.
    """
    raw_dir, ann_dir, csv_dir = mock_dirs

    frame = np.zeros((360, 640, 3), dtype=np.uint8)
    display = np.zeros((360, 640, 3), dtype=np.uint8)

    mock_model = MagicMock()
    mock_results = MagicMock()
    mock_box = MagicMock()
    mock_box.conf = [0.9]
    mock_box.xyxy = [[10, 10, 50, 50]]
    mock_results.boxes = [mock_box]
    mock_model.track.return_value = [mock_results]

    stats_writer = MagicMock()
    hc_bucket = {"interval_start": time.time(), "interval_start_vid": 0.0, "counts": []}
    warmup_fps_tracker = {"warmup_done": False}

    try:
        from consumers.consumer import run_head_count

        with patch("consumers.consumer._pytorch_sem"), \
             patch("consumers.consumer.torch.no_grad"), \
             patch("consumers.consumer.torch.cuda.synchronize"), \
             patch("consumers.consumer.torch.cuda.memory_allocated", return_value=0), \
             patch("consumers.consumer.torch.cuda.memory_reserved", return_value=0), \
             patch("consumers.consumer._csv_lock"), \
             patch("consumers.consumer.cv2.imwrite") as mock_imwrite:

            run_head_count(
                topic="test_topic",
                frame=frame,
                display=display,
                model=mock_model,
                frame_index=42,
                video_ts_sec=2.33,
                wall_ts="12:00:01",
                hc_bucket=hc_bucket,
                stats_writer=stats_writer,
                bucket_csv_path=os.path.join(csv_dir, "bucket.csv"),
                warmup_fps_tracker=warmup_fps_tracker,
                gpu_handle=None,
                raw_frames_dir=raw_dir,
                ann_frames_dir=ann_dir,
                bbox_csv_dir=csv_dir,
            )

        assert not mock_imwrite.called, "cv2.imwrite must not be called — image storage is disabled"
        assert os.listdir(raw_dir) == [], "raw_frames dir must stay empty"
        assert os.listdir(ann_dir) == [], "annotated_frames dir must stay empty"

        csv_files = [f for f in os.listdir(csv_dir) if "bboxes" in f]
        assert len(csv_files) == 1, "bbox CSV must still be written"
        assert stats_writer.write.called, "per-frame stats CSV write must still happen"

    except ImportError as e:
        pytest.skip(f"Skipping test due to import error: {e}")


def test_entry_exit_no_image_storage(mock_dirs):
    """
    Storage overhaul: run_entry_exit must NOT write raw/annotated JPEGs on an
    ENTRY/EXIT crossing event, but must still write the bbox CSV for that event.
    """
    raw_dir, ann_dir, csv_dir = mock_dirs

    frame = np.zeros((360, 640, 3), dtype=np.uint8)
    display = np.zeros((360, 640, 3), dtype=np.uint8)

    # No raw YOLO boxes this frame — the deep_sort `Detection(...)` construction
    # path (skipped when `detections` is empty) isn't what this test is about;
    # the ENTRY event below is driven entirely by the mocked tracker/history.
    mock_yolo_results = MagicMock()
    mock_yolo_results.boxes = []
    yolo_model = MagicMock(return_value=[mock_yolo_results])

    encoder = MagicMock(return_value=[])

    mock_track = MagicMock()
    mock_track.is_confirmed.return_value = True
    mock_track.track_id = 7
    # tlwh -> center (cx=20, cy=20)
    mock_track.to_tlwh.return_value = np.array([10.0, 10.0, 20.0, 20.0])
    tracker = MagicMock()
    tracker.tracks = [mock_track]

    ee_config = {"roi": None, "vec_x": 0.0, "vec_y": 1.0}

    # Pre-populate lookback history so this single call crosses the ENTRY
    # threshold: dy = 20 - 5 = 15 > EE_DOT_THRESH (3.0), dot = dy * vec_y.
    track_history = {7: deque([(20, 5)] * 7, maxlen=13)}
    track_state = {}
    session_totals = {"entry": 0, "exit": 0}
    ee_bucket = {"last_write": time.time()}  # avoid tripping the 300s summary write

    try:
        from consumers.consumer import run_entry_exit

        with patch("consumers.consumer._tf_sem"), \
             patch("consumers.consumer._csv_lock"), \
             patch("consumers.consumer.cv2.imwrite") as mock_imwrite:

            run_entry_exit(
                topic="test_topic",
                frame=frame,
                display=display,
                yolo_model=yolo_model,
                encoder=encoder,
                tracker=tracker,
                ee_config=ee_config,
                track_history=track_history,
                track_state=track_state,
                session_totals=session_totals,
                ee_bucket=ee_bucket,
                ee_csv_path=os.path.join(csv_dir, "entry_exit.csv"),
                frame_index=42,
                raw_frames_dir=raw_dir,
                ann_frames_dir=ann_dir,
                bbox_csv_dir=csv_dir,
            )

        assert session_totals["entry"] == 1, "test setup must actually trigger an ENTRY event"
        assert not mock_imwrite.called, "cv2.imwrite must not be called — image storage is disabled"
        assert os.listdir(raw_dir) == [], "raw_frames dir must stay empty"
        assert os.listdir(ann_dir) == [], "annotated_frames dir must stay empty"

        csv_files = [f for f in os.listdir(csv_dir) if "bboxes" in f]
        assert len(csv_files) == 1, "bbox CSV must still be written for the ENTRY event"

    except ImportError as e:
        pytest.skip(f"Skipping test due to import error: {e}")


def test_no_active_video_or_image_writer_code():
    """
    Static guard: video/image storage must stay disabled. Fails if any
    non-commented line in consumer.py contains an active cv2.VideoWriter(),
    video_writer.write(), or cv2.imwrite( call.
    """
    consumer_path = os.path.join(
        os.path.dirname(__file__), "..", "consumers", "consumer.py"
    )
    with open(consumer_path) as f:
        lines = f.readlines()

    banned_patterns = ("cv2.VideoWriter(", "video_writer.write(", "cv2.imwrite(")

    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        for pattern in banned_patterns:
            assert pattern not in stripped, (
                f"Found active disabled-storage call {pattern!r}: {stripped!r}"
            )

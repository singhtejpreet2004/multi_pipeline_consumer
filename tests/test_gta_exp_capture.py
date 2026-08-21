"""
Tests for the TEMPORARY GTA-Exp raw-footage capture feature (remove after
2026-08-28 — see consumers/README.md's GTA-Exp section).

gta_exp_active_window() is a pure function of a datetime and the module-level
constants, so those tests need no mocking beyond the standard import-guard
used across this suite. GtaExpRecorder's writer lifecycle (open/write/close)
is tested with cv2.VideoWriter mocked out and GTA_EXP_DIR pointed at a tmp
directory, so no real video files or /data paths are touched.
"""
import logging
from datetime import datetime
from unittest.mock import patch, MagicMock

import numpy as np
import pytest


def _import_gta_exp():
    from consumers.consumer import (
        GTA_EXP_CAMERAS,
        gta_exp_active_window,
    )
    return gta_exp_active_window, GTA_EXP_CAMERAS


def test_gta_exp_cameras_are_exactly_the_four_gate_cameras():
    try:
        _, GTA_EXP_CAMERAS = _import_gta_exp()
    except ImportError as e:
        pytest.skip(f"Skipping test due to import error: {e}")
        return

    assert GTA_EXP_CAMERAS == {
        "gate_1_outside_left",
        "gate_1_main_entry",
        "gate_2_entry_camera",
        "gate_2_exit_camera",
    }


@pytest.mark.parametrize(
    "dt, expect_active, expect_label",
    [
        # Before the capture date range entirely.
        (datetime(2026, 8, 21, 9, 30), False, None),
        # After the capture date range entirely.
        (datetime(2026, 8, 29, 9, 30), False, None),
        # Valid date, but between windows (after 09:00-10:00, before 14:30-15:30).
        (datetime(2026, 8, 22, 12, 0), False, None),
        # Valid date, after the last window closes for the day.
        (datetime(2026, 8, 22, 21, 0), False, None),
        # Window 1 (09:00-10:00): start boundary inclusive.
        (datetime(2026, 8, 22, 9, 0, 0), True, "0900-1000"),
        # Window 1: mid-window.
        (datetime(2026, 8, 22, 9, 30, 0), True, "0900-1000"),
        # Window 1: end boundary exclusive -> inactive.
        (datetime(2026, 8, 22, 10, 0, 0), False, None),
        # Window 2 (14:30-15:30): start boundary inclusive.
        (datetime(2026, 8, 23, 14, 30, 0), True, "1430-1530"),
        # Window 2: mid-window.
        (datetime(2026, 8, 23, 15, 0, 0), True, "1430-1530"),
        # Window 2: end boundary exclusive -> inactive.
        (datetime(2026, 8, 23, 15, 30, 0), False, None),
        # Window 3 (19:00-20:00): start boundary inclusive.
        (datetime(2026, 8, 28, 19, 0, 0), True, "1900-2000"),
        # Window 3: mid-window, on the last valid day.
        (datetime(2026, 8, 28, 19, 45, 0), True, "1900-2000"),
        # Window 3: end boundary exclusive -> inactive.
        (datetime(2026, 8, 28, 20, 0, 0), False, None),
    ],
)
def test_gta_exp_active_window(dt, expect_active, expect_label):
    try:
        gta_exp_active_window, _ = _import_gta_exp()
    except ImportError as e:
        pytest.skip(f"Skipping test due to import error: {e}")
        return

    result = gta_exp_active_window(dt)

    if not expect_active:
        assert result is None
    else:
        assert result is not None
        result_date, result_label = result
        assert result_date == dt.date()
        assert result_label == expect_label


def _import_recorder():
    from consumers.consumer import GtaExpRecorder
    return GtaExpRecorder


def _make_frame():
    return np.zeros((360, 640, 3), dtype=np.uint8)


def test_recorder_does_not_open_writer_outside_any_window(tmp_path):
    try:
        GtaExpRecorder = _import_recorder()
        import consumers.consumer as consumer_module
    except ImportError as e:
        pytest.skip(f"Skipping test due to import error: {e}")
        return

    with patch.object(consumer_module, "GTA_EXP_DIR", str(tmp_path)), \
         patch("consumers.consumer.cv2.VideoWriter") as mock_video_writer:
        recorder = GtaExpRecorder("gate_1_main_entry", logging.getLogger("test"))
        recorder.update(_make_frame(), datetime(2026, 8, 22, 12, 0))  # between windows

        assert not mock_video_writer.called


def test_recorder_opens_once_and_writes_every_frame_in_same_window(tmp_path):
    try:
        GtaExpRecorder = _import_recorder()
        import consumers.consumer as consumer_module
    except ImportError as e:
        pytest.skip(f"Skipping test due to import error: {e}")
        return

    with patch.object(consumer_module, "GTA_EXP_DIR", str(tmp_path)), \
         patch("consumers.consumer.cv2.VideoWriter") as mock_video_writer:
        mock_video_writer.side_effect = lambda *a, **kw: MagicMock()
        recorder = GtaExpRecorder("gate_1_main_entry", logging.getLogger("test"))
        frame = _make_frame()

        recorder.update(frame, datetime(2026, 8, 22, 9, 5))
        recorder.update(frame, datetime(2026, 8, 22, 9, 10))
        recorder.update(frame, datetime(2026, 8, 22, 9, 15))

        assert mock_video_writer.call_count == 1, "must not reopen the writer within the same window"
        path_arg = mock_video_writer.call_args[0][0]
        assert "gate_1_main_entry_20260822_0900-1000" in path_arg
        assert recorder.session_ts in path_arg, "filename must include the session timestamp"

        writer_instance = recorder._writer
        assert writer_instance.write.call_count == 3, "must write every frame while the window is active"


def test_recorder_switches_writer_when_window_changes(tmp_path):
    try:
        GtaExpRecorder = _import_recorder()
        import consumers.consumer as consumer_module
    except ImportError as e:
        pytest.skip(f"Skipping test due to import error: {e}")
        return

    with patch.object(consumer_module, "GTA_EXP_DIR", str(tmp_path)), \
         patch("consumers.consumer.cv2.VideoWriter") as mock_video_writer:
        mock_video_writer.side_effect = lambda *a, **kw: MagicMock()
        recorder = GtaExpRecorder("gate_1_main_entry", logging.getLogger("test"))
        frame = _make_frame()

        recorder.update(frame, datetime(2026, 8, 22, 9, 30))  # window 1
        first_writer = recorder._writer

        recorder.update(frame, datetime(2026, 8, 22, 14, 45))  # window 2
        second_writer = recorder._writer

        assert mock_video_writer.call_count == 2, "a new window must open a new writer"
        assert first_writer.release.called, "the previous window's writer must be released"
        assert second_writer is not first_writer


def test_recorder_closes_writer_when_leaving_all_windows(tmp_path):
    try:
        GtaExpRecorder = _import_recorder()
        import consumers.consumer as consumer_module
    except ImportError as e:
        pytest.skip(f"Skipping test due to import error: {e}")
        return

    with patch.object(consumer_module, "GTA_EXP_DIR", str(tmp_path)), \
         patch("consumers.consumer.cv2.VideoWriter") as mock_video_writer:
        mock_video_writer.side_effect = lambda *a, **kw: MagicMock()
        recorder = GtaExpRecorder("gate_1_main_entry", logging.getLogger("test"))
        frame = _make_frame()

        recorder.update(frame, datetime(2026, 8, 22, 9, 30))  # window active
        writer_instance = recorder._writer

        recorder.update(frame, datetime(2026, 8, 22, 12, 0))  # now between windows

        assert writer_instance.release.called
        assert recorder._writer is None
        assert recorder._current_window is None


def test_recorder_restart_gets_a_distinct_filename(tmp_path):
    """
    Regression test: a consumer restart mid-window must not overwrite the
    prior session's file for the same camera/date/window — each recorder
    instance stamps its own session_ts once at construction.
    """
    try:
        GtaExpRecorder = _import_recorder()
        import consumers.consumer as consumer_module
    except ImportError as e:
        pytest.skip(f"Skipping test due to import error: {e}")
        return

    with patch.object(consumer_module, "GTA_EXP_DIR", str(tmp_path)), \
         patch("consumers.consumer.cv2.VideoWriter") as mock_video_writer:
        mock_video_writer.side_effect = lambda *a, **kw: MagicMock()
        frame = _make_frame()

        recorder1 = GtaExpRecorder("gate_1_main_entry", logging.getLogger("test"))
        recorder1.update(frame, datetime(2026, 8, 22, 9, 5))
        path1 = mock_video_writer.call_args[0][0]

        # Simulate a process restart mid-window: a brand new recorder instance.
        recorder2 = GtaExpRecorder("gate_1_main_entry", logging.getLogger("test"))
        recorder2.session_ts = recorder1.session_ts + "_1"  # force a distinct stamp deterministically
        recorder2.update(frame, datetime(2026, 8, 22, 9, 6))
        path2 = mock_video_writer.call_args[0][0]

        assert path1 != path2, "a restart mid-window must not reuse the same filename"

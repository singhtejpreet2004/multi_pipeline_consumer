"""
Tests for the TEMPORARY GTA-Exp raw-footage capture feature (remove after
2026-08-28 — see consumers/README.md's GTA-Exp section).

gta_exp_active_window() is a pure function of a datetime and the module-level
constants, so these tests need no mocking beyond the standard import-guard
used across this suite.
"""
from datetime import datetime

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

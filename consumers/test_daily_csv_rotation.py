#!/usr/bin/env python3
"""
Isolated test for daily CSV rotation logic.
Runs without GPU / Kafka / TF / YOLO dependencies.
Copies only _get_daily_csv_path and StreamingCSVWriter to test in isolation.
"""
import os
import csv
import threading
import tempfile
from datetime import datetime
from unittest.mock import patch

# ── Copies of the two changed components (no other imports needed) ────────────

def _get_daily_csv_path(static_path: str, headers: list) -> str:
    directory  = os.path.dirname(static_path)
    today      = datetime.now().strftime('%Y-%m-%d')
    dated_path = os.path.join(directory, f"{today}_{os.path.basename(static_path)}")
    if not os.path.exists(dated_path):
        with open(dated_path, 'w', newline='') as f:
            csv.writer(f).writerow(headers)
    return dated_path


class StreamingCSVWriter:
    def __init__(self, path: str, headers: list):
        self._base_path = path
        self._headers   = headers
        self._lock      = threading.Lock()

    def write(self, row: list):
        with self._lock:
            dated = _get_daily_csv_path(self._base_path, self._headers)
            with open(dated, 'a', newline='') as f:
                csv.writer(f).writerow(row)


# ── Test harness ──────────────────────────────────────────────────────────────

PASS_COUNT = 0
FAIL_COUNT = 0

def check(name: str, condition: bool):
    global PASS_COUNT, FAIL_COUNT
    if condition:
        print(f"  PASS  {name}")
        PASS_COUNT += 1
    else:
        print(f"  FAIL  {name}")
        FAIL_COUNT += 1


HEADERS = ['timestamp', 'value', 'count']

print("\n── _get_daily_csv_path ──────────────────────────────────────────────────")

with tempfile.TemporaryDirectory() as tmpdir:
    static_path = os.path.join(tmpdir, 'gate_1_outside_left_animal_detection.csv')
    today       = datetime.now().strftime('%Y-%m-%d')
    expected    = os.path.join(tmpdir, f"{today}_gate_1_outside_left_animal_detection.csv")

    # T1: correct dated filename
    result = _get_daily_csv_path(static_path, HEADERS)
    check("T1  dated filename matches YYYY-MM-DD_{basename}", result == expected)

    # T2: file created on disk
    check("T2  file exists after first call", os.path.exists(result))

    # T3: headers written as first row
    with open(result) as f:
        first_row = next(csv.reader(f))
    check("T3  headers written correctly", first_row == HEADERS)

    # T4: idempotent — repeat call does not overwrite existing file
    with open(result, 'a', newline='') as f:
        csv.writer(f).writerow(['2026-05-20T00:00:01Z', '42', '1'])
    _get_daily_csv_path(static_path, HEADERS)
    with open(result) as f:
        rows = list(csv.reader(f))
    check("T4  existing file not overwritten on repeat call", len(rows) == 2)

    # T5: undated static path is never created
    check("T5  undated static file is never created", not os.path.exists(static_path))

    # T6: date rotation — new date → new file
    with patch('__main__.datetime') as mock_dt:
        mock_dt.now.return_value.strftime.return_value = '2099-01-01'
        next_path = _get_daily_csv_path(static_path, HEADERS)
    expected_next = os.path.join(tmpdir, '2099-01-01_gate_1_outside_left_animal_detection.csv')
    check("T6  new dated file created on date change", next_path == expected_next)
    check("T7  new dated file exists on disk", os.path.exists(next_path))

    # T8: new file for new date has correct headers
    with open(next_path) as f:
        rotated_header = next(csv.reader(f))
    check("T8  rotated file has correct headers", rotated_header == HEADERS)

    # T9: old date file still intact after rotation
    with open(result) as f:
        old_rows = list(csv.reader(f))
    check("T9  previous day file still intact after rotation", len(old_rows) == 2)

print("\n── StreamingCSVWriter ───────────────────────────────────────────────────")

with tempfile.TemporaryDirectory() as tmpdir:
    static_path = os.path.join(tmpdir, 'gate_2_entry_camera_headcount_stats.csv')
    today       = datetime.now().strftime('%Y-%m-%d')
    dated_path  = os.path.join(tmpdir, f"{today}_gate_2_entry_camera_headcount_stats.csv")

    writer = StreamingCSVWriter(static_path, HEADERS)

    # T10: no file created at __init__ time (lazy creation)
    check("T10 no file created at __init__ (lazy)", not os.path.exists(dated_path))
    check("T11 undated static file not created at __init__", not os.path.exists(static_path))

    # T12: first write creates dated file
    writer.write(['2026-05-20T00:00:01Z', '5', '3'])
    check("T12 dated file created on first write", os.path.exists(dated_path))

    # T13: multiple writes accumulate correctly
    writer.write(['2026-05-20T00:00:02Z', '7', '4'])
    writer.write(['2026-05-20T00:00:03Z', '9', '5'])
    with open(dated_path) as f:
        rows = list(csv.reader(f))
    check("T13 3 data rows + 1 header = 4 total rows", len(rows) == 4)
    check("T14 header row correct", rows[0] == HEADERS)
    check("T15 first data row correct", rows[1] == ['2026-05-20T00:00:01Z', '5', '3'])

    # T16: concurrent writes from multiple threads do not corrupt
    extra_writer = StreamingCSVWriter(
        os.path.join(tmpdir, 'concurrent_test.csv'), HEADERS
    )
    threads = [
        threading.Thread(target=extra_writer.write, args=([f"ts_{i}", str(i), str(i)],))
        for i in range(20)
    ]
    for t in threads: t.start()
    for t in threads: t.join()
    conc_path = os.path.join(tmpdir, f"{today}_concurrent_test.csv")
    with open(conc_path) as f:
        conc_rows = list(csv.reader(f))
    check("T16 concurrent writes: 20 data rows + 1 header = 21 total", len(conc_rows) == 21)

print("\n── All-camera nomenclature sample ───────────────────────────────────────")

CAMERAS = [
    ('gate_1_outside_left',         'animal_detection'),
    ('gate_1_outside_left',         'headcount_stats'),
    ('gate_1_outside_left',         'headcount_bucket'),
    ('gate_1_outside_left',         'entry_exit'),
    ('dr2_1f_dining_area_1',        'headcount_stats'),
    ('gh_gf_outdoor_dining_area_1', 'animal_detection'),
]

with tempfile.TemporaryDirectory() as tmpdir:
    today = datetime.now().strftime('%Y-%m-%d')
    for folder, model in CAMERAS:
        static = os.path.join(tmpdir, f"{folder}_{model}.csv")
        dated  = _get_daily_csv_path(static, HEADERS)
        expected_name = f"{today}_{folder}_{model}.csv"
        check(f"T17 nomenclature: {expected_name}", os.path.basename(dated) == expected_name)

print(f"\n{'─'*70}")
print(f"  Results: {PASS_COUNT} passed, {FAIL_COUNT} failed")
print(f"{'─'*70}\n")

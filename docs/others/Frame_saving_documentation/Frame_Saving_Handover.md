# Frame Saving Feature — Deployment Handover
**Target Script:** `consumers/test_consumer_phase02.py`
**Date:** June 2026

## Overview
This document outlines the successful integration of the **Frame Saving Feature** as requested by the Machine Learning team. The system now actively records raw frames, annotated frames, and isolated bounding box coordinates to disk whenever a positive detection event occurs across any of the three ML pipelines (Animal Detection, Head Count, Entry/Exit).

## Structural Additions
The directory structure for output now automatically generates the following subdirectories for each pipeline:
- `raw_frames/`: `.jpg` format, no annotations.
- `annotated_frames/`: `.jpg` format, with pipeline-specific bounding boxes.
- `bbox_csv/`: `.csv` format, isolated bounding box data specifically linked to the saved frames.

## Code Integrity & Tradeoff Analysis
- **100% Additive Changes:** The modifications strictly adhered to the ML team's implementation guide. No existing CSV logic, Kafka handling, or dashboard WebSocket streaming code was modified or removed.
- **Zero-padded Frames:** Frame stems follow the strict `frame_{index:08d}_{YYYYMMDD}_{HHMMSS}_{uSec}` timestamping convention to ensure proper alphabetic sorting.
- **Performance Tradeoff:** Saving images to disk directly via `cv2.imwrite` introduces a slight blocking I/O penalty during active detection frames. Because this only triggers on *positive* detections, blank frames incur 0 overhead. If disk I/O becomes a bottleneck on the L40S server, we may need to evaluate asynchronous writing in the future, but current load should be fully absorbed by the lag-dropping mechanism.

## Testing Verification
Isolated testing for the file writing mechanism has been successfully implemented in `tests/test_frame_saving.py`. These tests verify that the `datetime.now()` strings are correctly formatted and that all 3 artifacts (`raw`, `ann`, `bboxes`) are generated atomically without breaking the pipeline.

## Handover Status
The feature has been committed to the `script_changes_frames` branch and is ready for production merge.

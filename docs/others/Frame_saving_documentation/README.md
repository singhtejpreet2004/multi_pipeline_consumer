# Frame Saving Documentation

This folder contains the official handover documentation regarding the **Frame Saving Feature**.

## History
During Phase 3 of the consumer rollout, the Machine Learning team requested that we begin saving raw and annotated frames to disk whenever an anomaly, animal, or person is detected by the pipelines. These frames are later used to re-train the models and improve confidence scores.

Please read `Frame_Saving_Handover.md` in this directory to understand:
- The precise folder structures generated (`raw_frames/`, `annotated_frames/`, `bbox_csv/`).
- The zero-padded timestamp logic used to ensure chronological sorting.
- The tradeoff analysis of disk I/O penalties during heavy detection windows.

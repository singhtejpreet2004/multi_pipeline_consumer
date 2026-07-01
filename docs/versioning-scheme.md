# Versioning Scheme — v1

Format: `vMAJOR.MINOR.PATCH` (e.g. `v1.4.2`), applied as a git tag at release-cut time.

## What bumps what

| Bump | Triggered by | Example |
|---|---|---|
| **MAJOR** | Breaking change to pipeline behavior, data format, or deployment requirements — anything that means "old assumptions no longer hold" | Changing the Kafka message schema; switching frame-saving folder structure |
| **MINOR** | New capability, no breaking change — driven by `feat:` commits since last release | Adding a new camera producer; adding daily CSV rotation |
| **PATCH** | Bug fix, no new capability — driven by `fix:` commits since last release | Correcting partition-detection logic; fixing AVI rotation threshold |

`docs:`, `chore:`, `refactor:`, `test:` commits alone do not trigger a version bump on their own — they ride along in whichever release happens next.

## Baseline

The current production consumer (`test_consumer_phase02.py`, once restructured and renamed in Phase 2) becomes the `v1.0.0` baseline at the end of Phase 6. This is a **documentation point, not a code change** — the file's behavior doesn't change to become `v1.0.0`, it's simply the first point we start tracking from.

The `phase1` / `phase2` / `phase02` naming history becomes archived context, referenced in the Wiki, rather than the live versioning mechanism. From `v1.0.0` onward, "what's running" is answered by a tag, not a filename.

## Release process (mechanics, executed in Phase 6+)

1. Confirm `develop` is stable and CI is green.
2. Determine bump level from accumulated commit types since last tag.
3. Tag: `git tag -a v1.x.x -m "..."`.
4. Push tag, create a GitHub Release with auto-generated or written notes.
5. Merge `develop` → `main`.
6. Deploy tagged version to the GPU server per the deployment checklist (defined in Phase 6).

## Pre-1.0 note

Until `v1.0.0` is cut, the project is implicitly `v0.x.x` — normal for active, not-yet-stabilized work. No tags are expected before Phase 6 completes.

---
*Status: Locked. Rule set, not yet executed — first tag happens in Phase 6.*

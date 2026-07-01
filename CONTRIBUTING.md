# Contributing to Plaksha Streams

This document is the enforceable rulebook for working in this repository. It is referenced from the root README and is the standard every restructuring, documentation, and code change is measured against, starting Phase 0.

## Before you branch

1. Pull latest `develop`.
2. Branch using `feature/<area>/<short-desc>` or `hotfix/<short-desc>` — see [branching-commit-strategy.md] for full rules.
3. Areas: `producer`, `consumer`, `docs`, `ci`, `infra`.

## Commit messages

Use Conventional Commits: `<type>: <short summary>`.
Types: `feat`, `fix`, `docs`, `chore`, `refactor`, `test`. Full reference in [branching-commit-strategy.md].

## Before opening a PR

- [ ] CI passes (lint + tests) — *enforced starting Phase 5; treat as mandatory personal discipline before then*
- [ ] No secrets, credentials, or `.env` values committed
- [ ] If code in a directory changed, that directory's `README.md` is updated in the **same PR** — this is non-negotiable and is what prevents documentation drift (see governance charter §4)
- [ ] If behavior changed, the relevant Wiki page is flagged for update (Wiki updates don't have to land in the same PR, but must be logged)

## File naming — hard rule

**No production file may be named `test_*.py`.** This repo previously had production consumer logic (`test_consumer_phase02.py`) sharing a naming pattern with real pytest test files, creating a collision risk during bare `pytest` collection. Test files live in `/tests` and `/consumers` test-area only, named `test_*.py`. Production logic never carries that prefix, regardless of directory.

## Dependencies

Any new package import must be added to `requirements.txt` in the same PR that introduces it. Non-pip-installable prerequisites (e.g. vendored code on the deployment server) must be documented in `requirements.txt`'s accompanying notes, not silently assumed.

## Versioning

This project uses `vMAJOR.MINOR.PATCH`. See `versioning-scheme.md` for exact bump rules. Contributors don't need to assign version numbers themselves — that happens at release-cut time (Phase 6 process) — but commit type (`feat`/`fix`/etc.) directly determines what that future version bump will be, so get the type right.

## Documentation placement — quick reference

| If your change is... | It belongs in... |
|---|---|
| Tied to a specific directory's code | That directory's `README.md` |
| About the whole system / operational / narrative | GitHub Wiki |
| About tasks, people, or reporting | Notion (not this repo) |

Full rationale in the Documentation & Tooling Governance Charter.

## Secrets

Never commit credentials, tokens, or `.env` files. Use environment variables. If you discover a committed secret, treat it as a `hotfix/` priority — see Phase 1 (Security Triage).

---
*This file evolves. Changes to it follow the same `docs:` commit convention and PR process as everything else.*

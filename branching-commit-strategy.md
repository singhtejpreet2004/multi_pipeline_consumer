# Branching & Commit Strategy — v1

> Locked in Phase 0 (Standards Lock-in). This governs every branch and commit from this point forward.

---

## 1. Branching Model: GitFlow-lite

Two permanent branches, everything else is temporary.

| Branch | Purpose | Lifespan |
|---|---|---|
| `main` | Always reflects what's deployed/deployable. Tagged for releases. | Permanent |
| `develop` | Integration branch — where finished feature branches land before a release cut. | Permanent |
| `feature/<area>/<short-desc>` | One unit of work. Branches off `develop`, merges back into `develop`. | Temporary — deleted after merge |
| `hotfix/<short-desc>` | Urgent production fix. Branches off `main`, merges into **both** `main` and `develop`. | Temporary — deleted after merge |

**Naming convention:** `feature/<area>/<short-desc>`
Area = `producer`, `consumer`, `docs`, `ci`, `infra` — matching your Notion area tags (consumer/producer/observability/other).

Examples:
- `feature/consumer/fix-partition-detection`
- `feature/docs/wiki-master-page`
- `hotfix/rotate-rtsp-credentials`

**Why GitFlow-lite (not full GitFlow, not trunk-based):** full GitFlow's release-branch ceremony is overkill for a solo owner. Pure trunk-based (straight to `main`) gives DE/IoT/ML teams nothing to integrate against safely later. `develop` as a staging ground is the middle path — costs one extra branch, buys a safe integration point before anything is release-tagged.

---

## 2. Commit Message Format: Conventional Commits

```
<type>: <short summary>

[optional longer body]
```

| Type | Use for | Versioning signal |
|---|---|---|
| `feat:` | New feature or capability | MINOR bump |
| `fix:` | Bug fix | PATCH bump |
| `docs:` | Documentation only (README, Wiki content, this file) | No version bump |
| `chore:` | Tooling, config, dependency bumps, non-functional cleanup | No version bump |
| `refactor:` | Code restructuring with no behavior change | No version bump (unless paired with feat/fix) |
| `test:` | Adding or fixing tests | No version bump |

Examples:
- `fix: correct dominant-partition detection using growth delta`
- `feat: add daily CSV rotation for head-count pipeline`
- `docs: update consumer README semaphore count to match code`

**Why this matters beyond tidiness:** these prefixes feed directly into Phase 6 (Versioning & Releases) — release notes and version bump decisions get derived from commit history instead of hand-written from memory.

---

## 3. Merge Policy: CI-Gated Self-Merge

**Current rule (effective now):** as sole contributor, you self-approve every PR. No second human reviewer exists yet — that's a fact, not a gap to apologize for.

**The gate:** a PR may only be merged into `develop` or `main` once CI passes (lint + tests). Until **Phase 5** ships an actual CI pipeline, this gate is **not yet technically enforced** — there is nothing to check against yet. Treat it as binding personal discipline starting today; it becomes machine-enforced (branch protection rule requiring a passing check) the moment Phase 5 lands.

**Forward path:** when DE/IoT/ML contributors join, this same structure absorbs a second human reviewer requirement with zero process redesign — just adding a required-reviewer rule on top of a gate that already exists.

---

*Status: Locked. Feeds CONTRIBUTING.md and the versioning scheme directly.*

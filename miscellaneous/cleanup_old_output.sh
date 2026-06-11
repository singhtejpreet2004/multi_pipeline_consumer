#!/bin/bash
# =============================================================================
# cleanup_old_output.sh
# Run on 10.1.41.56 AFTER the new consumer is confirmed running.
#
# What it does:
#   Deletes files in /data/multi_pipeline_consumer/output/ that are older than
#   (or same age as) the most recent backup in /data/Plaksha_streams_backup/.
#   These are the pre-new-consumer files — confirmed backed up.
#
# Safe contract:
#   - Mandatory dry-run first — shows every file + total size before deleting
#   - Uses backup directory mtime as the cutoff — no filename guessing
#   - Files written by the running consumer (newer than backup) are NEVER touched
#   - No changes to /data/Plaksha_streams_backup/ — backup is never touched
# =============================================================================

set -uo pipefail

OUTPUT_DIR="/data/multi_pipeline_consumer/output"
BACKUP_ROOT="/data/Plaksha_streams_backup"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
ok()   { echo -e "${GREEN}[OK]   $1${NC}"; }
info() { echo -e "${NC}[INFO] $1${NC}"; }
warn() { echo -e "${YELLOW}[WARN] $1${NC}"; }
fail() { echo -e "${RED}[FAIL] $1${NC}"; exit 1; }

echo ""
info "============================================================"
info " Plaksha Streams — cleanup_old_output.sh"
info " Output dir  : $OUTPUT_DIR"
info " Backup root : $BACKUP_ROOT"
info "============================================================"
echo ""

# ── FIND MOST RECENT BACKUP ───────────────────────────────────────────────────
info "Finding most recent backup directory..."
LATEST_BACKUP=$(ls -dt "$BACKUP_ROOT"/20*/ 2>/dev/null | head -1)

if [ -z "$LATEST_BACKUP" ]; then
    fail "No backup found in $BACKUP_ROOT — cannot determine safe cutoff. Aborting."
fi

ok "Using backup as cutoff: $LATEST_BACKUP"
info "  Files NOT newer than this = old files = safe to delete."
info "  Files newer than this     = written by new consumer = UNTOUCHED."
echo ""

# ── DRY RUN ───────────────────────────────────────────────────────────────────
info "DRY RUN — files that would be deleted:"
echo ""

OLD_FILES=$(find "$OUTPUT_DIR" -type f ! -newer "$LATEST_BACKUP" 2>/dev/null)

if [ -z "$OLD_FILES" ]; then
    ok "No old files found — nothing to delete. The output directory is already clean."
    exit 0
fi

# Show the files grouped by type with sizes
echo "$OLD_FILES" | while read -r f; do
    size=$(du -sh "$f" 2>/dev/null | cut -f1)
    printf "  %-12s  %s\n" "$size" "$f"
done

echo ""

# Count and total size
FILE_COUNT=$(echo "$OLD_FILES" | wc -l | tr -d ' ')
TOTAL_SIZE=$(echo "$OLD_FILES" | xargs du -shc 2>/dev/null | tail -1 | cut -f1)

info "  Total: $FILE_COUNT files, $TOTAL_SIZE"
echo ""

# Breakdown by type
CSV_COUNT=$(echo "$OLD_FILES" | grep -c "\.csv$" || true)
AVI_COUNT=$(echo "$OLD_FILES" | grep -c "\.avi$" || true)
info "  CSV files : $CSV_COUNT"
info "  AVI files : $AVI_COUNT"
echo ""

# ── VERIFY NEW CONSUMER FILES WOULD NOT BE TOUCHED ───────────────────────────
info "Verifying new consumer files are protected..."
NEW_FILES=$(find "$OUTPUT_DIR" -type f -newer "$LATEST_BACKUP" 2>/dev/null | wc -l | tr -d ' ')
ok "$NEW_FILES files written by the new consumer will NOT be touched."
echo ""

# ── CONFIRM ───────────────────────────────────────────────────────────────────
warn "THIS WILL PERMANENTLY DELETE $FILE_COUNT FILES ($TOTAL_SIZE) FROM $OUTPUT_DIR"
warn "The backup at $LATEST_BACKUP is the only copy of these files."
echo ""
read -r -p "Type DELETE to confirm permanent deletion: " CONFIRM

if [ "$CONFIRM" != "DELETE" ]; then
    info "Aborted — nothing deleted."
    exit 0
fi

# ── DELETE ────────────────────────────────────────────────────────────────────
info "Deleting old files..."

DELETED=0
FAILED=0

echo "$OLD_FILES" | while read -r f; do
    if rm "$f" 2>/dev/null; then
        DELETED=$((DELETED + 1))
    else
        warn "  Could not delete: $f"
        FAILED=$((FAILED + 1))
    fi
done

echo ""

# ── REMOVE EMPTY DIRECTORIES ──────────────────────────────────────────────────
info "Cleaning up empty directories left behind..."
find "$OUTPUT_DIR" -type d -empty -delete 2>/dev/null && true
ok "Empty directories removed."

# ── FINAL REPORT ─────────────────────────────────────────────────────────────
echo ""
info "============================================================"
ok " CLEANUP COMPLETE"
info "  Output dir now:"
du -sh "$OUTPUT_DIR" 2>/dev/null
echo ""
info "  Backup still intact:"
du -sh "$LATEST_BACKUP" 2>/dev/null
info "============================================================"

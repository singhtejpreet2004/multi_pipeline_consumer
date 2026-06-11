#!/bin/bash
# =============================================================================
# maintenance_backup.sh
# Run this on the remote server ONCE before deploying the new consumer.
#
# What it does:
#   1. Stops all running producers (via PID files + pkill fallback)
#   2. Stops the running consumer (shell loop + Python process)
#   3. Waits and confirms everything is dead
#   4. Creates a timestamped backup of output/ and logs/ to /data/Plaksha_streams_backup/
#   5. Verifies the backup by comparing file counts and disk sizes
#   6. Reports pass/fail — does NOT delete anything
#
# SAFE CONTRACT:
#   - No rm / no delete / no truncation anywhere in this script
#   - If backup verification fails the script exits non-zero and tells you
#   - Source data is never touched after the backup step
# =============================================================================

set -uo pipefail   # exit on undefined vars and pipe failures
                   # note: NOT -e so we can handle partial failures ourselves

# ── CONFIG ────────────────────────────────────────────────────────────────────
SOURCE_BASE="/data/multi_pipeline_consumer"
OUTPUT_DIR="$SOURCE_BASE/output"
LOG_DIR="$SOURCE_BASE/logs"

BACKUP_ROOT="/data/Plaksha_streams_backup"
TIMESTAMP=$(date +%Y-%m-%d_%H%M%S)
BACKUP_DIR="$BACKUP_ROOT/$TIMESTAMP"

GRACEFUL_WAIT=15   # seconds to wait after SIGTERM before SIGKILL
STOP_POLL=2        # seconds between liveness checks

# ── COLOUR HELPERS ────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${NC}[$(date '+%H:%M:%S')] $1${NC}"; }
ok()    { echo -e "${GREEN}[$(date '+%H:%M:%S')] OK    $1${NC}"; }
warn()  { echo -e "${YELLOW}[$(date '+%H:%M:%S')] WARN  $1${NC}"; }
fail()  { echo -e "${RED}[$(date '+%H:%M:%S')] FAIL  $1${NC}"; }

# ── SAFETY CHECK ─────────────────────────────────────────────────────────────
info "============================================================"
info " Plaksha Streams — maintenance_backup.sh"
info " Timestamp  : $TIMESTAMP"
info " Source     : $SOURCE_BASE"
info " Backup to  : $BACKUP_DIR"
info "============================================================"
echo ""
echo -e "${YELLOW}This script will STOP all producers and the consumer, then backup data.${NC}"
echo -e "${YELLOW}No files will be deleted. Source data is preserved.${NC}"
echo ""
read -r -p "Type YES to continue: " CONFIRM
if [ "$CONFIRM" != "YES" ]; then
    info "Aborted by user."
    exit 0
fi

echo ""
info "Starting stop + backup procedure..."

# ═════════════════════════════════════════════════════════════════════════════
# STEP 1 — STOP ALL PRODUCERS
# ═════════════════════════════════════════════════════════════════════════════
info "------------------------------------------------------------"
info "STEP 1: Stopping producers"
info "------------------------------------------------------------"

# Kill the shell restart-loop subshells via saved PID files
KILLED_PIDS=0
shopt -s nullglob
for pid_file in "$LOG_DIR"/producer_*.pid; do
    pid=$(cat "$pid_file" 2>/dev/null || true)
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        kill -SIGTERM "$pid" 2>/dev/null && info "  SIGTERM sent to producer subshell PID $pid (${pid_file##*/})"
        KILLED_PIDS=$((KILLED_PIDS + 1))
    else
        warn "  PID file ${pid_file##*/} — process not running or file empty"
    fi
done

# Also kill any Python producer processes still alive (handles restart-loop survivors)
info "  Sending SIGTERM to any running producer Python processes..."
pkill -SIGTERM -f "producer_gate_1_outside_left"    2>/dev/null && true
pkill -SIGTERM -f "producer_gate_1_main_entry"       2>/dev/null && true
pkill -SIGTERM -f "producer_gate_2_entry_camera"     2>/dev/null && true
pkill -SIGTERM -f "producer_gate_2_exit_camera"      2>/dev/null && true
pkill -SIGTERM -f "producer_a2_gf_electronic_zone"   2>/dev/null && true
pkill -SIGTERM -f "producer_a2_gf_makerspace"        2>/dev/null && true
pkill -SIGTERM -f "producer_dr2_1f_dining_area"      2>/dev/null && true
pkill -SIGTERM -f "producer_dr2_gf_dining_cam"       2>/dev/null && true
pkill -SIGTERM -f "producer_d58_summer_court"        2>/dev/null && true
pkill -SIGTERM -f "producer_gh_gf_outdoor_dining"    2>/dev/null && true
pkill -SIGTERM -f "run_producers.sh"                 2>/dev/null && true

info "  Waiting ${GRACEFUL_WAIT}s for producers to flush and exit..."
sleep "$GRACEFUL_WAIT"

# Force-kill anything that didn't exit cleanly
info "  SIGKILL pass for any producer processes still alive..."
pkill -SIGKILL -f "producer_gate_1_outside_left"    2>/dev/null && warn "  Force-killed: producer_gate_1_outside_left"   || true
pkill -SIGKILL -f "producer_gate_1_main_entry"       2>/dev/null && warn "  Force-killed: producer_gate_1_main_entry"      || true
pkill -SIGKILL -f "producer_gate_2_entry_camera"     2>/dev/null && warn "  Force-killed: producer_gate_2_entry_camera"    || true
pkill -SIGKILL -f "producer_gate_2_exit_camera"      2>/dev/null && warn "  Force-killed: producer_gate_2_exit_camera"     || true
pkill -SIGKILL -f "producer_a2_gf_electronic_zone"   2>/dev/null && warn "  Force-killed: producer_a2_gf_electronic_zone"  || true
pkill -SIGKILL -f "producer_a2_gf_makerspace"        2>/dev/null && warn "  Force-killed: producer_a2_gf_makerspace"       || true
pkill -SIGKILL -f "producer_dr2_1f_dining_area"      2>/dev/null && warn "  Force-killed: producer_dr2_1f_dining"          || true
pkill -SIGKILL -f "producer_dr2_gf_dining_cam"       2>/dev/null && warn "  Force-killed: producer_dr2_gf_dining_cam"      || true
pkill -SIGKILL -f "producer_d58_summer_court"        2>/dev/null && warn "  Force-killed: producer_d58_summer_court"       || true
pkill -SIGKILL -f "producer_gh_gf_outdoor_dining"    2>/dev/null && warn "  Force-killed: producer_gh_gf_outdoor_dining"   || true

sleep 2

# Verify all producers are dead
STILL_ALIVE=0
for pattern in "producer_gate_1" "producer_gate_2" "producer_a2_gf" "producer_dr2" "producer_d58" "producer_gh_gf"; do
    if pgrep -f "$pattern" > /dev/null 2>&1; then
        fail "  Process still running matching: $pattern"
        STILL_ALIVE=$((STILL_ALIVE + 1))
    fi
done

if [ "$STILL_ALIVE" -eq 0 ]; then
    ok "All producers stopped."
else
    fail "$STILL_ALIVE producer pattern(s) still alive. Investigate before proceeding."
    exit 1
fi

# ═════════════════════════════════════════════════════════════════════════════
# STEP 2 — STOP CONSUMER
# ═════════════════════════════════════════════════════════════════════════════
info "------------------------------------------------------------"
info "STEP 2: Stopping consumer"
info "------------------------------------------------------------"

# Kill the shell restart-loop
pkill -SIGTERM -f "run_consumer.sh"          2>/dev/null && info "  SIGTERM → run_consumer.sh shell loop"         || warn "  run_consumer.sh shell loop not found"
# Kill the Python consumer process
pkill -SIGTERM -f "test_consumer_phase"      2>/dev/null && info "  SIGTERM → test_consumer_phase*.py"            || warn "  test_consumer_phase*.py not found running"

info "  Waiting ${GRACEFUL_WAIT}s for consumer to flush Kafka offsets and exit..."
sleep "$GRACEFUL_WAIT"

# Force-kill if still alive
pkill -SIGKILL -f "test_consumer_phase"     2>/dev/null && warn "  Force-killed consumer Python process"          || true
pkill -SIGKILL -f "run_consumer.sh"         2>/dev/null && warn "  Force-killed run_consumer.sh loop"             || true

sleep 2

# Verify consumer is dead
if pgrep -f "test_consumer_phase" > /dev/null 2>&1; then
    fail "Consumer Python process still alive. Investigate before proceeding."
    exit 1
else
    ok "Consumer stopped."
fi

echo ""
info "All processes confirmed stopped. Beginning backup."

# ═════════════════════════════════════════════════════════════════════════════
# STEP 3 — CREATE BACKUP DIRECTORY
# ═════════════════════════════════════════════════════════════════════════════
info "------------------------------------------------------------"
info "STEP 3: Creating backup directory"
info "------------------------------------------------------------"

mkdir -p "$BACKUP_DIR" || { fail "Could not create $BACKUP_DIR — check permissions."; exit 1; }
ok "Backup directory created: $BACKUP_DIR"

# Record source sizes before copy
info "  Source sizes:"
du -sh "$OUTPUT_DIR" 2>/dev/null && true
du -sh "$LOG_DIR"    2>/dev/null && true
echo ""

# ═════════════════════════════════════════════════════════════════════════════
# STEP 4 — BACKUP OUTPUT
# ═════════════════════════════════════════════════════════════════════════════
info "------------------------------------------------------------"
info "STEP 4: Backing up output/ → $BACKUP_DIR/output"
info "------------------------------------------------------------"

if [ -d "$OUTPUT_DIR" ]; then
    info "  Copying output/ (this may take several minutes for large AVI files)..."
    rsync -a --info=progress2 "$OUTPUT_DIR" "$BACKUP_DIR/"
    RSYNC_RC=$?
    if [ "$RSYNC_RC" -eq 0 ]; then
        ok "output/ backed up successfully."
    else
        fail "rsync exited with code $RSYNC_RC — backup may be incomplete."
        exit 1
    fi
else
    warn "output/ directory not found at $OUTPUT_DIR — skipping."
fi

# ═════════════════════════════════════════════════════════════════════════════
# STEP 5 — BACKUP LOGS
# ═════════════════════════════════════════════════════════════════════════════
info "------------------------------------------------------------"
info "STEP 5: Backing up logs/ → $BACKUP_DIR/logs"
info "------------------------------------------------------------"

if [ -d "$LOG_DIR" ]; then
    info "  Copying logs/..."
    rsync -a --info=progress2 "$LOG_DIR" "$BACKUP_DIR/"
    RSYNC_RC=$?
    if [ "$RSYNC_RC" -eq 0 ]; then
        ok "logs/ backed up successfully."
    else
        fail "rsync exited with code $RSYNC_RC for logs."
        exit 1
    fi
else
    warn "logs/ directory not found at $LOG_DIR — skipping."
fi

# ═════════════════════════════════════════════════════════════════════════════
# STEP 6 — VERIFY BACKUP
# ═════════════════════════════════════════════════════════════════════════════
info "------------------------------------------------------------"
info "STEP 6: Verifying backup"
info "------------------------------------------------------------"

VERIFY_FAIL=0

# Compare file counts
if [ -d "$OUTPUT_DIR" ] && [ -d "$BACKUP_DIR/output" ]; then
    SRC_COUNT=$(find "$OUTPUT_DIR"        -type f | wc -l)
    DST_COUNT=$(find "$BACKUP_DIR/output" -type f | wc -l)
    if [ "$SRC_COUNT" -eq "$DST_COUNT" ]; then
        ok "output/ file count matches: $SRC_COUNT files"
    else
        fail "output/ file count MISMATCH — source: $SRC_COUNT, backup: $DST_COUNT"
        VERIFY_FAIL=$((VERIFY_FAIL + 1))
    fi
fi

if [ -d "$LOG_DIR" ] && [ -d "$BACKUP_DIR/logs" ]; then
    SRC_COUNT=$(find "$LOG_DIR"        -type f | wc -l)
    DST_COUNT=$(find "$BACKUP_DIR/logs" -type f | wc -l)
    if [ "$SRC_COUNT" -eq "$DST_COUNT" ]; then
        ok "logs/ file count matches: $SRC_COUNT files"
    else
        fail "logs/ file count MISMATCH — source: $SRC_COUNT, backup: $DST_COUNT"
        VERIFY_FAIL=$((VERIFY_FAIL + 1))
    fi
fi

# Show backup sizes for visual confirmation
info "  Backup directory sizes:"
du -sh "$BACKUP_DIR/output" 2>/dev/null && true
du -sh "$BACKUP_DIR/logs"   2>/dev/null && true
du -sh "$BACKUP_DIR"        2>/dev/null && true

echo ""
if [ "$VERIFY_FAIL" -eq 0 ]; then
    ok "Backup verification passed."
else
    fail "Backup verification failed ($VERIFY_FAIL check(s)). Do NOT proceed until resolved."
    exit 1
fi

# ═════════════════════════════════════════════════════════════════════════════
# DONE
# ═════════════════════════════════════════════════════════════════════════════
echo ""
info "============================================================"
ok " BACKUP COMPLETE"
info "  Backup location : $BACKUP_DIR"
info "  Source preserved: $SOURCE_BASE (untouched)"
info "============================================================"
echo ""
info "Next steps:"
info "  1. Visually confirm the sizes above look right"
info "  2. Optionally: ls -lh $BACKUP_DIR/output to spot-check"
info "  3. Then run the new consumer:"
info "       cd $SOURCE_BASE"
info "       nohup bash run_producers.sh > /dev/null 2>&1 &"
info "       nohup bash run_consumer.sh  > /dev/null 2>&1 &"
info "============================================================"

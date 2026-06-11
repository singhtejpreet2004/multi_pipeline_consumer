#!/bin/bash
# =============================================================================
# run_consumer.sh
# Starts the multi-pipeline consumer with full timestamped logging.
# Auto-restarts on crash, logs crash reason and restart time exactly.
# =============================================================================

# ── CONFIG ────────────────────────────────────────────────────────────────────
VENV_PATH="/data/Entry_Exit/ee-venv"
CONSUMER_SCRIPT="/data/multi_pipeline_consumer/consumers/test_consumer_phase02.py"
LOG_DIR="/data/multi_pipeline_consumer/logs"
RESTART_DELAY=10 # seconds to wait before restarting consumer on crash
SESSION_TS=$(date +%Y%m%d_%H%M%S)

# ── SETUP ─────────────────────────────────────────────────────────────────────
mkdir -p "$LOG_DIR"
source "$VENV_PATH/bin/activate"

LOG_FILE="$LOG_DIR/consumer_${SESSION_TS}.log"

log() {
  # Write timestamped line to both log file and stdout
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "=========================================="
log "Consumer launcher started"
log "Session timestamp : $SESSION_TS"
log "Consumer script   : $CONSUMER_SCRIPT"
log "Log file          : $LOG_FILE"
log "Venv              : $VENV_PATH"
log "Restart delay     : ${RESTART_DELAY}s"
log "Dashboard URL     : http://10.1.41.56:8675"
log "=========================================="

# ── VERIFY SCRIPT EXISTS ──────────────────────────────────────────────────────
if [ ! -f "$CONSUMER_SCRIPT" ]; then
  log "ERROR: Consumer script not found at $CONSUMER_SCRIPT"
  log "Exiting."
  exit 1
fi

# ── AUTO-RESTART LOOP ─────────────────────────────────────────────────────────
RUN_COUNT=0

while true; do
  RUN_COUNT=$((RUN_COUNT + 1))
  log "--- RUN #$RUN_COUNT ---"
  log "Launching consumer..."

  # Run consumer — both stdout and stderr appended to log file
  # The consumer's own Python logging also goes here via tee
  export XLA_FLAGS="--xla_gpu_cuda_data_dir=/usr/local/cuda"
  export PATH="/usr/local/cuda/bin:$PATH"
  python3 "$CONSUMER_SCRIPT" 2>&1 | while IFS= read -r line; do
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [CONSUMER] $line" | tee -a "$LOG_FILE"
  done

  EXIT_CODE=${PIPESTATUS[0]}
  CRASH_TIME=$(date '+%Y-%m-%d %H:%M:%S')

  log "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
  log "CONSUMER CRASHED at $CRASH_TIME"
  log "Exit code   : $EXIT_CODE"
  log "Run number  : $RUN_COUNT"
  log "Log file    : $LOG_FILE"
  log "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

  # Decode common exit codes for easier debugging
  case $EXIT_CODE in
  0) log "Exit reason : Clean exit (Ctrl+C or SIGTERM)" ;;
  1) log "Exit reason : Python exception / unhandled error" ;;
  137) log "Exit reason : Killed (SIGKILL) — likely OOM or manual kill" ;;
  139) log "Exit reason : Segmentation fault" ;;
  *) log "Exit reason : Unknown (code $EXIT_CODE)" ;;
  esac

  # If clean exit (0) — don't restart, user stopped it intentionally
  if [ $EXIT_CODE -eq 0 ]; then
    log "Clean exit detected — not restarting. Run manually to restart."
    break
  fi

  log "Restarting in ${RESTART_DELAY}s..."
  sleep "$RESTART_DELAY"
done

log "Consumer launcher exiting."

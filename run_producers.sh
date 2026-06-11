#!/bin/bash
# =============================================================================
# run_producers.sh
# Starts all 10 Kafka producers, one process each.
# Each producer gets its own timestamped log file.
# Auto-restarts any producer that crashes, logs the crash with timestamp.
# =============================================================================

# ── CONFIG ────────────────────────────────────────────────────────────────────
VENV_PATH="/data/Entry_Exit/ee-venv"
PRODUCERS_DIR="/data/multi_pipeline_consumer/producers"
LOG_DIR="/data/multi_pipeline_consumer/logs"
RESTART_DELAY=5       # seconds to wait before restarting a crashed producer
SESSION_TS=$(date +%Y%m%d_%H%M%S)   # timestamp for this run's log files

# ── PRODUCER LIST ─────────────────────────────────────────────────────────────
PRODUCERS=(
    "producer_gate_1_outside_left.py"
    "producer_gate_1_main_entry.py"
    "producer_gate_2_entry_camera.py"
    "producer_gate_2_exit_camera.py"
    "producer_a2_gf_electronic_zone.py"
    "producer_a2_gf_makerspace_worktops.py"
    "producer_dr2_1f_dining_area_1.py"
    "producer_dr2_gf_dining_cam_2.py"
    "producer_d58_summer_court_2.py"
    "producer_gh_gf_outdoor_dining_area_1.py"
)

# ── SETUP ─────────────────────────────────────────────────────────────────────
mkdir -p "$LOG_DIR"
source "$VENV_PATH/bin/activate"

MASTER_LOG="$LOG_DIR/producers_master_${SESSION_TS}.log"

log_master() {
    # Write a timestamped line to the master producers log
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$MASTER_LOG"
}

log_master "=========================================="
log_master "Producer launcher started"
log_master "Session timestamp : $SESSION_TS"
log_master "Producers dir     : $PRODUCERS_DIR"
log_master "Log dir           : $LOG_DIR"
log_master "Venv              : $VENV_PATH"
log_master "Producers count   : ${#PRODUCERS[@]}"
log_master "=========================================="

# ── WORKER FUNCTION ───────────────────────────────────────────────────────────
# Each producer runs in this function inside a background subshell.
# On crash it waits RESTART_DELAY seconds then restarts automatically.
run_producer_with_restart() {
    local script="$1"
    local name="${script%.py}"   # strip .py for cleaner log name
    local log_file="$LOG_DIR/${name}_${SESSION_TS}.log"

    {
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] [START] $script — logging to $log_file"
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO]  Venv: $VENV_PATH"
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO]  Script: $PRODUCERS_DIR/$script"
        echo "---"

        while true; do
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] [LAUNCH] Starting $script..."

            # Run the producer — stdout and stderr both go to this log
            python3 "$PRODUCERS_DIR/$script" 2>&1

            EXIT_CODE=$?
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] [CRASH] $script exited with code $EXIT_CODE"
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WAIT]  Restarting in ${RESTART_DELAY}s..."
            sleep "$RESTART_DELAY"
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] [RESTART] Relaunching $script..."
        done
    } >> "$log_file" 2>&1 &

    # Log the PID to master log so you can kill individual producers if needed
    local pid=$!
    log_master "Launched $script — PID=$pid — log: $log_file"
    echo "$pid" > "$LOG_DIR/${name}.pid"
}

# ── LAUNCH ALL PRODUCERS ──────────────────────────────────────────────────────
log_master "Launching all producers..."

for script in "${PRODUCERS[@]}"; do
    run_producer_with_restart "$script"
    sleep 1    # 1 second stagger between launches to avoid simultaneous RTSP hits
done

log_master "All ${#PRODUCERS[@]} producers launched. PIDs saved in $LOG_DIR/*.pid"
log_master "To stop all producers: kill \$(cat $LOG_DIR/producer_*.pid)"
log_master "To follow a producer log: tail -f $LOG_DIR/producer_<name>_${SESSION_TS}.log"
log_master "Monitoring active. Press Ctrl+C to stop this monitor (producers keep running)."

# ── KEEP SCRIPT ALIVE + PERIODIC STATUS ───────────────────────────────────────
# Prints a status line every 5 minutes showing which producers are still alive
while true; do
    sleep 300
    log_master "--- 5-min status check ---"
    for script in "${PRODUCERS[@]}"; do
        name="${script%.py}"
        pid_file="$LOG_DIR/${name}.pid"
        if [ -f "$pid_file" ]; then
            pid=$(cat "$pid_file")
            if kill -0 "$pid" 2>/dev/null; then
                log_master "  OK      $name (PID $pid)"
            else
                log_master "  DEAD    $name (PID $pid) — restart loop should be running"
            fi
        fi
    done
done

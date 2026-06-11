#!/bin/bash
# =============================================================================
# setup_rsync_cron.sh
# Run this ONCE on the L40S server (10.1.41.56) as the administrator user.
#
# What it does:
#   1. Generates an SSH key (if none exists) for passwordless rsync
#   2. Copies the key to HPC-1 (10.1.40.43) — asks for HPC-1 password ONCE
#   3. Tests the SSH connection
#   4. Creates the destination directory on HPC-1
#   5. Dry-runs rsync so you can see what would transfer
#   6. Installs the cron job (every 1 minute)
#
# After this script, rsync runs every minute automatically — no maintenance needed.
# =============================================================================

set -uo pipefail

# ── CONFIG ────────────────────────────────────────────────────────────────────
SOURCE_DIR="/data/multi_pipeline_consumer/output"
DEST_HOST="10.1.40.43"
DEST_USER="administrator"
DEST_DIR="/home/administrator/plaksha_streams_hpc1/plaksha_day_wise_csv"
LOG_FILE="/data/multi_pipeline_consumer/logs/rsync_backup.log"
SSH_KEY="$HOME/.ssh/id_ed25519"

# ── COLOUR HELPERS ────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
ok()   { echo -e "${GREEN}[OK]   $1${NC}"; }
info() { echo -e "${NC}[INFO] $1${NC}"; }
warn() { echo -e "${YELLOW}[WARN] $1${NC}"; }
fail() { echo -e "${RED}[FAIL] $1${NC}"; exit 1; }

echo ""
info "============================================================"
info " Plaksha Streams — rsync + cron setup"
info " Source : $SOURCE_DIR"
info " Dest   : $DEST_USER@$DEST_HOST:$DEST_DIR"
info " Log    : $LOG_FILE"
info "============================================================"
echo ""

# ── STEP 1: SSH KEY ───────────────────────────────────────────────────────────
info "STEP 1: SSH key setup"

if [ -f "$SSH_KEY" ]; then
    ok "SSH key already exists at $SSH_KEY — skipping generation."
else
    info "  Generating new Ed25519 SSH key (no passphrase — required for cron)..."
    ssh-keygen -t ed25519 -N "" -f "$SSH_KEY" -C "plaksha-rsync@$(hostname)"
    ok "SSH key generated: $SSH_KEY"
fi

# ── STEP 2: COPY KEY TO HPC-1 ─────────────────────────────────────────────────
info "STEP 2: Copying public key to HPC-1 ($DEST_USER@$DEST_HOST)"
info "  You will be asked for the HPC-1 administrator password once."
info "  After this, SSH will be passwordless for the cron job."
echo ""

ssh-copy-id -i "${SSH_KEY}.pub" "$DEST_USER@$DEST_HOST"
if [ $? -ne 0 ]; then
    fail "ssh-copy-id failed. Check HPC-1 password and network connectivity."
fi
ok "Public key copied to HPC-1."

# ── STEP 3: TEST SSH ──────────────────────────────────────────────────────────
info "STEP 3: Testing passwordless SSH to HPC-1..."

SSH_TEST=$(ssh -o BatchMode=yes -o ConnectTimeout=10 "$DEST_USER@$DEST_HOST" "echo SSH_OK" 2>&1)
if [ "$SSH_TEST" = "SSH_OK" ]; then
    ok "Passwordless SSH to $DEST_HOST is working."
else
    fail "SSH test failed: $SSH_TEST — key may not have been authorized. Check ~/.ssh/authorized_keys on HPC-1."
fi

# ── STEP 4: CREATE DESTINATION DIRECTORY ─────────────────────────────────────
info "STEP 4: Creating destination directory on HPC-1..."

ssh "$DEST_USER@$DEST_HOST" "mkdir -p '$DEST_DIR'"
ok "Destination directory ready: $DEST_DIR"

# ── STEP 5: DRY-RUN RSYNC ────────────────────────────────────────────────────
info "STEP 5: Dry-run rsync — showing what would transfer (no files sent yet)..."
echo ""

rsync -az --update --dry-run \
    --include="*/" \
    --include="*.csv" \
    --exclude="*" \
    "$SOURCE_DIR/" \
    "$DEST_USER@$DEST_HOST:$DEST_DIR/" \
    --itemize-changes

echo ""
info "  Lines above show files that would be synced."
info "  Format: >f = file to send, .d = directory, blank = already in sync."
echo ""
read -r -p "Does this look correct? Type YES to install the cron job: " CONFIRM
if [ "$CONFIRM" != "YES" ]; then
    info "Aborted. Cron NOT installed. Re-run this script when ready."
    exit 0
fi

# ── STEP 6: INSTALL CRON JOB ─────────────────────────────────────────────────
info "STEP 6: Installing cron job..."

CRON_CMD="* * * * * rsync -avz --update --itemize-changes --include=\"*/\" --include=\"*.csv\" --exclude=\"*\" $SOURCE_DIR/ $DEST_USER@$DEST_HOST:$DEST_DIR/ >> $LOG_FILE 2>&1"

# Remove any existing plaksha rsync cron entry, then add the new one
( crontab -l 2>/dev/null | grep -v "plaksha_streams_hpc1"; echo "$CRON_CMD" ) | crontab -

ok "Cron job installed."

# ── VERIFY CRON INSTALLED ─────────────────────────────────────────────────────
info "  Verifying crontab..."
crontab -l | grep "plaksha_streams_hpc1"
echo ""
ok "Cron entry confirmed in crontab."

# ── FIRST REAL RUN ────────────────────────────────────────────────────────────
info "Running first real rsync now..."

mkdir -p "$(dirname "$LOG_FILE")"

rsync -az --update \
    --include="*/" \
    --include="*.csv" \
    --exclude="*" \
    "$SOURCE_DIR/" \
    "$DEST_USER@$DEST_HOST:$DEST_DIR/" \
    >> "$LOG_FILE" 2>&1

ok "First sync complete. Log: $LOG_FILE"

# ── DONE ──────────────────────────────────────────────────────────────────────
echo ""
info "============================================================"
ok " SETUP COMPLETE"
info "  Cron runs every minute."
info "  CSVs sync: $SOURCE_DIR → $DEST_HOST:$DEST_DIR"
info "  Log file : $LOG_FILE"
echo ""
info "  Useful commands:"
info "    Watch live log   : tail -f $LOG_FILE"
info "    View cron entry  : crontab -l | grep plaksha"
info "    Remove cron      : crontab -l | grep -v plaksha_streams_hpc1 | crontab -"
info "    Check dest       : ssh $DEST_USER@$DEST_HOST 'ls -lh $DEST_DIR'"
info "============================================================"

#!/bin/bash
# =============================================================================
# Gimi Michi — DigitalOcean Droplet Setup Script
# Run this from your Mac:  bash do_setup.sh
# =============================================================================

set -e

DROPLET_IP="139.59.30.156"
DROPLET_USER="root"
REMOTE_DIR="/root/gimimichi-automation"
LOCAL_DIR="$HOME/Documents/gimimichi-automation"

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║   Gimi Michi — DigitalOcean Setup            ║"
echo "║   Target: $DROPLET_USER@$DROPLET_IP          ║"
echo "╚══════════════════════════════════════════════╝"
echo ""

# ── Step 1: Check local files exist ───────────────────────────────────────────
echo "▶ Checking local files…"
REQUIRED_FILES=(
    "main.py"
    "unicommerce_api.py"
    "split_shipments_api.py"
    "gdrive.py"
    "pdf_utils.py"
    "auth.py"
    "manual_split.py"
    "requirements.txt"
    ".env"
    "token.json"
    "credentials.json"
)

MISSING=()
for f in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$LOCAL_DIR/$f" ]; then
        MISSING+=("$f")
    fi
done

if [ ${#MISSING[@]} -gt 0 ]; then
    echo "  ✗ Missing files in $LOCAL_DIR:"
    for f in "${MISSING[@]}"; do
        echo "      - $f"
    done
    echo ""
    echo "  Fix missing files then re-run this script."
    exit 1
fi
echo "  ✓ All local files present"

# ── Step 2: Install server dependencies ───────────────────────────────────────
echo ""
echo "▶ Installing server dependencies…"
ssh "$DROPLET_USER@$DROPLET_IP" bash << 'ENDSSH'
set -e
apt-get update -qq
apt-get install -y -qq python3 python3-pip python3-venv
echo "  ✓ Python installed: $(python3 --version)"
ENDSSH

# ── Step 3: Create remote directory and copy files ─────────────────────────────
echo ""
echo "▶ Copying files to server…"
ssh "$DROPLET_USER@$DROPLET_IP" "mkdir -p $REMOTE_DIR/logs $REMOTE_DIR/output"

# Copy all script files
for f in "${REQUIRED_FILES[@]}"; do
    scp -q "$LOCAL_DIR/$f" "$DROPLET_USER@$DROPLET_IP:$REMOTE_DIR/$f"
done
echo "  ✓ Files copied"

# ── Step 4: Set up Python virtual environment and install packages ─────────────
echo ""
echo "▶ Setting up Python environment…"
ssh "$DROPLET_USER@$DROPLET_IP" bash << ENDSSH
set -e
cd $REMOTE_DIR
python3 -m venv venv
source venv/bin/activate
pip install -q --upgrade pip
pip install -q -r requirements.txt
echo "  ✓ Dependencies installed"
ENDSSH

# ── Step 5: Set up cron job (10 PM IST = 16:30 UTC) ───────────────────────────
echo ""
echo "▶ Setting up cron job (10:00 PM IST daily)…"
ssh "$DROPLET_USER@$DROPLET_IP" bash << ENDSSH
set -e
# 10 PM IST = UTC+5:30, so 16:30 UTC
CRON_LINE="30 16 * * * cd $REMOTE_DIR && $REMOTE_DIR/venv/bin/python3 main.py >> $REMOTE_DIR/logs/cron.log 2>&1"
# Remove any existing gimimichi cron entry and add fresh one
(crontab -l 2>/dev/null | grep -v "gimimichi\|main.py" ; echo "\$CRON_LINE") | crontab -
echo "  ✓ Cron job set: 10:00 PM IST daily"
crontab -l | grep main.py
ENDSSH

# ── Step 6: Test run with dry-run ─────────────────────────────────────────────
echo ""
echo "▶ Running dry-run test…"
ssh "$DROPLET_USER@$DROPLET_IP" bash << ENDSSH
set -e
cd $REMOTE_DIR
source venv/bin/activate
python3 main.py --dry-run
ENDSSH

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║   Setup complete ✅                           ║"
echo "╚══════════════════════════════════════════════╝"
echo ""
echo "  Droplet:   $DROPLET_USER@$DROPLET_IP"
echo "  Script:    $REMOTE_DIR/main.py"
echo "  Logs:      $REMOTE_DIR/logs/"
echo "  Schedule:  Daily at 10:00 PM IST"
echo ""
echo "  Useful commands:"
echo "    SSH in:       ssh $DROPLET_USER@$DROPLET_IP"
echo "    Manual run:   ssh $DROPLET_USER@$DROPLET_IP 'cd $REMOTE_DIR && venv/bin/python3 main.py'"
echo "    View logs:    ssh $DROPLET_USER@$DROPLET_IP 'tail -f $REMOTE_DIR/logs/cron.log'"
echo "    Edit cron:    ssh $DROPLET_USER@$DROPLET_IP 'crontab -e'"
echo ""

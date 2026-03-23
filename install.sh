#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="$(which python3)"
PLIST_SRC="$SCRIPT_DIR/launchd/com.gimimichi.orders.plist"
PLIST_DST="$HOME/Library/LaunchAgents/com.gimimichi.orders.plist"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Gimi Michi Order Automation — Setup"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo "[1/3] Installing Python dependencies…"
"$PYTHON" -m pip install -r "$SCRIPT_DIR/requirements.txt" --quiet
echo "      ✓ Done"

echo "[2/3] Setting up .env…"
if [ ! -f "$SCRIPT_DIR/.env" ]; then
    cp "$SCRIPT_DIR/.env.example" "$SCRIPT_DIR/.env"
    echo "      Created .env — fill it in before running"
else
    echo "      .env already exists — skipping"
fi

echo "[3/3] Installing launchd scheduler (10 PM daily)…"
mkdir -p "$SCRIPT_DIR/logs"
sed \
  -e "s|YOUR_PYTHON_PATH|$PYTHON|g" \
  -e "s|YOUR_PROJECT_PATH|$SCRIPT_DIR|g" \
  -e "s|YOUR_HOME_PATH|$HOME|g" \
  "$PLIST_SRC" > "$PLIST_DST"
launchctl unload "$PLIST_DST" 2>/dev/null || true
launchctl load   "$PLIST_DST"
echo "      ✓ Scheduled"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ✅ Setup complete!"
echo ""
echo "  Next steps:"
echo "  1. nano $SCRIPT_DIR/.env          ← fill in credentials"
echo "  2. python3 auth.py                ← one-time Google auth"
echo "  3. python3 main.py --cred-only --dry-run  ← test"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

#!/usr/bin/env bash
# Beelink bootstrap — one-shot installer for a fresh WSL2 Ubuntu 24.04
# (or bare-metal Ubuntu) environment.
#
# Run this AFTER:
#   1. WSL2 + Ubuntu 24.04 installed on Windows
#   2. /etc/wsl.conf has [boot] systemd=true (then `wsl --shutdown`)
#   3. This script is inside the repo, or you ran it via `bash <(curl ...)`
#
# What it does:
#   - Installs system deps (python, node, git, sqlite, ssh server, etc.)
#   - Creates ~/sports-model-bettor Python venv
#   - Installs pip requirements
#   - Installs Claude Code
#   - Installs + configures Tailscale
#   - Copies systemd units into ~/.config/systemd/user/
#   - Enables + starts the sync timer
#
# What it does NOT do (manual steps after):
#   - SCP your data/ files (*.db, odds_api_key.txt, hardrock_*.json)
#     from your current machine
#   - Restore ~/.claude/ (CLAUDE.md, memory/, settings.json)
#   - Tailscale auth (runs `tailscale up` interactively at the end)

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$HOME/sports-model-bettor}"

echo "════════════════════════════════════════════════════════════"
echo "  Beelink bootstrap for sports-model-bettor"
echo "  Target repo: ${REPO_ROOT}"
echo "  User: $(whoami)  Hostname: $(hostname)"
echo "════════════════════════════════════════════════════════════"
echo ""

# ── 1. Sanity: systemd running inside WSL? ──
if ! systemctl --user show-environment >/dev/null 2>&1; then
    echo "!! systemd --user isn't running. On WSL, make sure /etc/wsl.conf has:"
    echo "     [boot]"
    echo "     systemd=true"
    echo "   then run 'wsl --shutdown' on Windows and reopen the shell."
    echo "   Continuing — systemd steps at the end will fail, but you can re-run"
    echo "   them after fixing."
fi

# ── 2. System deps ──
echo ""
echo "── Installing system packages (sudo) ──"
sudo apt-get update -y
sudo apt-get install -y --no-install-recommends \
    build-essential ca-certificates curl git gnupg lsb-release \
    python3 python3-venv python3-pip python3-dev \
    sqlite3 libsqlite3-dev \
    openssh-server \
    jq unzip tzdata

# ── 3. Node + Claude Code ──
echo ""
echo "── Installing Node.js 20.x ──"
if ! command -v node >/dev/null 2>&1 || ! node -v | grep -qE '^v(2[0-9]|[3-9][0-9])'; then
    curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
    sudo apt-get install -y nodejs
fi
echo "Node: $(node -v)  npm: $(npm -v)"

echo ""
echo "── Installing Claude Code CLI ──"
if ! command -v claude >/dev/null 2>&1; then
    sudo npm install -g @anthropic-ai/claude-code
fi
echo "Claude: $(claude --version 2>/dev/null || echo 'installed')"

# ── 4. Tailscale ──
echo ""
echo "── Installing Tailscale ──"
if ! command -v tailscale >/dev/null 2>&1; then
    curl -fsSL https://tailscale.com/install.sh | sh
fi

# ── 5. Clone repo if missing ──
echo ""
echo "── Repo ──"
if [ ! -d "${REPO_ROOT}/.git" ]; then
    echo "!! Repo not found at ${REPO_ROOT}."
    echo "   Clone it first:"
    echo "     git clone git@github.com:coolmint65/sports-model-bettor.git ${REPO_ROOT}"
    echo "   Then re-run this script."
    exit 1
fi
cd "${REPO_ROOT}"
echo "At commit: $(git log --oneline -1)"

# ── 6. Python venv + requirements ──
echo ""
echo "── Python venv + deps ──"
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate
python -m pip install --upgrade pip wheel
if [ -f "requirements.txt" ]; then
    python -m pip install -r requirements.txt
else
    echo "(no requirements.txt — skipping pip install; make sure to pin deps later)"
fi
deactivate

# ── 7. Make sync scripts executable ──
echo ""
echo "── Permissions on sync scripts ──"
chmod +x scripts/sync.sh scripts/sync_mlb.sh scripts/sync_nhl.sh scripts/sync_nba.sh

# ── 8. systemd user units ──
echo ""
echo "── Installing systemd --user units ──"
mkdir -p "${HOME}/.config/systemd/user"
cp -v scripts/systemd/sports-sync.service "${HOME}/.config/systemd/user/"
cp -v scripts/systemd/sports-sync.timer   "${HOME}/.config/systemd/user/"

# Enable lingering so user services run even when you're not logged in.
# (Necessary for 24/7 on a headless Beelink where nobody's at a console.)
sudo loginctl enable-linger "$(whoami)" || true

systemctl --user daemon-reload || true
systemctl --user enable --now sports-sync.timer || {
    echo "!! Couldn't enable timer now — systemd --user may not be ready yet."
    echo "   After restarting WSL / logging in, run:"
    echo "     systemctl --user enable --now sports-sync.timer"
}

# ── 9. Lower MC sim count for N150 ──
echo ""
echo "── Suggestion: drop MLB_MC_N_SIMS to 20,000 for the N150 ──"
echo "   Edit engine/config.py or set via engine.model_overrides."
echo "   (Not doing this automatically — leaves your laptop config alone.)"

# ── 10. Tailscale up (interactive) ──
echo ""
echo "── Tailscale auth ──"
echo "   Run this when you're ready to join the tailnet:"
echo "     sudo tailscale up --ssh"
echo "   Then check: tailscale ip -4"

echo ""
echo "════════════════════════════════════════════════════════════"
echo "  Bootstrap complete."
echo "════════════════════════════════════════════════════════════"
echo ""
echo "Remaining manual steps:"
echo "  1. SCP your data/*.db, data/odds_api_key.txt, data/hardrock_*.json"
echo "     from your current machine."
echo "  2. Restore ~/.claude/ (CLAUDE.md, memory/, settings.json) via SCP"
echo "     so Claude Code has memory + settings."
echo "  3. sudo tailscale up --ssh"
echo "  4. Verify the timer: systemctl --user list-timers | grep sports-sync"
echo "  5. Kick a manual sync: ./scripts/sync.sh"

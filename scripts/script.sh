#!/bin/bash

# install_python.sh
# Run this script as root to update apt, install Python3/pip/venv,
# configure Scale MQTT Bridge as a systemd service,
# and set up log retention/cleanup.

set -euo pipefail

# Determine project directory: rely on script location first
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
if [ -f "$SCRIPT_DIR/bridge.py" ]; then
  PROJECT_DIR="$SCRIPT_DIR"
elif [ -f "$(dirname "$SCRIPT_DIR")/bridge.py" ]; then
  PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
elif [ -f "$PWD/bridge.py" ]; then
  PROJECT_DIR="$PWD"
elif [ -n "${SUDO_USER:-}" ]; then
  USER_HOME=$(getent passwd "$SUDO_USER" | cut -d: -f6)
  PROJECT_DIR="$USER_HOME/aurusguard-pi-python"
else
  USER_HOME=$HOME
  PROJECT_DIR="$USER_HOME/aurusguard-pi-python"
fi

# Verify the project directory exists and contains bridge.py
if [ ! -d "$PROJECT_DIR" ] || [ ! -f "$PROJECT_DIR/bridge.py" ]; then
  echo "Error: Could not locate project directory containing bridge.py."
  echo "Detected PROJECT_DIR=$PROJECT_DIR"
  exit 1
fi

LOG_FILE="/var/log/install_python.log"

exec > >(tee -a "$LOG_FILE")
exec 2>&1

echo "========================================="
echo "Installation started: $(date)"
echo "========================================="

if [[ "$EUID" -ne 0 ]]; then
  echo "This script must be run as root. Use sudo ./install_python.sh"
  exit 1
fi

if ! command -v apt >/dev/null 2>&1; then
  echo "apt is not available on this system. This script is intended for Debian/Ubuntu-based systems."
  exit 1
fi

echo "[1/8] Updating package lists..."
apt update

echo "[2/8] Installing Python3, pip and venv..."
if ! command -v python3 >/dev/null 2>&1 || ! python3 -m venv --help >/dev/null 2>&1; then
  apt install -y python3 python3-pip python3-venv
else
  echo "Python3 and venv are already installed. Skipping installation."
fi

echo "Python version: $(python3 --version)"

echo "[3/8] Creating bridge log file..."
touch /var/log/aurus.log
chmod 644 /var/log/aurus.log

echo "[4/8] Creating virtual environment and installing dependencies..."
cd "$PROJECT_DIR"
python3 -m venv "$PROJECT_DIR/venv"
"$PROJECT_DIR/venv/bin/pip" install --upgrade pip
"$PROJECT_DIR/venv/bin/pip" install -r "$PROJECT_DIR/requirements.txt"

echo "[5/8] Creating systemd service..."
cat >/etc/systemd/system/aurus.service <<EOF
[Unit]
Description=Scale MQTT Bridge
After=network.target

[Service]
Type=simple
WorkingDirectory=$PROJECT_DIR
ExecStart=$PROJECT_DIR/venv/bin/python3 $PROJECT_DIR/bridge.py
Restart=always
RestartSec=5
User=root

StandardOutput=append:/var/log/aurus.log
StandardError=append:/var/log/aurus.log

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable aurus
systemctl restart aurus

echo "[6/8] Configuring journald retention..."
mkdir -p /etc/systemd/journald.conf.d

cat >/etc/systemd/journald.conf.d/limits.conf <<EOF
[Journal]
SystemMaxUse=80M
MaxRetentionSec=14day
EOF

systemctl restart systemd-journald
journalctl --vacuum-size=80M
journalctl --vacuum-time=14d

echo "[7/8] Configuring log rotation..."
cat >/etc/logrotate.d/aurus <<'EOF'
/var/log/aurus.log {
    daily
    rotate 14
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
    maxsize 80M
}
EOF

echo "[8/8] Creating weekly cleanup task..."
tee /etc/cron.weekly/pi-cleanup >/dev/null <<'EOF'
#!/bin/sh

apt-get clean
apt-get autoclean -y
apt-get autoremove -y

find /tmp -type f -mtime +7 -delete
find /var/tmp -type f -mtime +7 -delete

journalctl --vacuum-time=14d
journalctl --vacuum-size=80M
EOF

chmod +x /etc/cron.weekly/pi-cleanup

echo
echo "========================================="
echo "Installation completed successfully"
echo "========================================="
echo
echo "Installer log:"
echo "  /var/log/install_python.log"
echo
echo "Bridge log:"
echo "  /var/log/aurus.log"
echo
echo "Service status:"
systemctl --no-pager --full status aurus || true

echo
echo "Useful commands:"
echo "  systemctl status aurus"
echo "  systemctl restart aurus"
echo "  tail -f /var/log/aurus.log"
echo "  journalctl -u aurus -f"

echo
echo "Completed at: $(date)"

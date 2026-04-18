#!/usr/bin/env bash
set -euo pipefail

TEAM_PASSWORD="${1:-}"
TEAM_USER="teamuser"
APP_USER="hackapr"
APP_GROUP="hackapr"
APP_DIR="/workspace/hack_apr"
SERVICE_NAME="hack-apr"
PORT="9000"

if [ -z "$TEAM_PASSWORD" ]; then
  echo "Usage: sudo bash init.sh <team_password>"
  exit 1
fi

if [ "$(id -u)" -ne 0 ]; then
  echo "Run this script as root"
  exit 1
fi

if [ ! -d "$APP_DIR" ]; then
  echo "Missing app directory: $APP_DIR"
  echo "Clone the repository first into $APP_DIR"
  exit 1
fi

if ! getent group "$APP_GROUP" >/dev/null; then
  groupadd --system "$APP_GROUP"
fi

if ! id -u "$APP_USER" >/dev/null 2>&1; then
  useradd --system --gid "$APP_GROUP" --home-dir /home/"$APP_USER" --shell /usr/sbin/nologin "$APP_USER"
fi

if ! id -u "$TEAM_USER" >/dev/null 2>&1; then
  useradd -m -s /bin/bash "$TEAM_USER"
fi

echo "${TEAM_USER}:${TEAM_PASSWORD}" | chpasswd

mkdir -p /var/log/hack_apr
chown "$APP_USER:$APP_GROUP" /var/log/hack_apr
chmod 750 /var/log/hack_apr

mkdir -p /workspace
chmod 755 /workspace

mkdir -p /workspace/tmp /workspace/hf /workspace/uv_cache /workspace/venvs
chown "$APP_USER:$APP_GROUP" /workspace/tmp /workspace/hf /workspace/uv_cache /workspace/venvs
chmod 755 /workspace/tmp /workspace/hf /workspace/uv_cache /workspace/venvs

apt-get update
apt-get install -y curl ca-certificates

if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi

if [ -x /root/.local/bin/uv ] && ! command -v uv >/dev/null 2>&1; then
  ln -sf /root/.local/bin/uv /usr/local/bin/uv
fi

chown -R "$APP_USER:$APP_GROUP" "$APP_DIR"
chmod 700 "$APP_DIR"
find "$APP_DIR" -type d -exec chmod 700 {} \;
find "$APP_DIR" -type f -exec chmod 600 {} \;

su -s /bin/bash "$APP_USER" -c "
export HOME=/home/${APP_USER}
export TMPDIR=/workspace/tmp
export HF_HOME=/workspace/hf
export UV_CACHE_DIR=/workspace/uv_cache
cd ${APP_DIR}
uv sync
"

cat >/etc/systemd/system/${SERVICE_NAME}.service <<EOF
[Unit]
Description=hack_apr server
After=network.target

[Service]
User=${APP_USER}
Group=${APP_GROUP}
WorkingDirectory=${APP_DIR}
Environment=PYTHONUNBUFFERED=1
Environment=TMPDIR=/workspace/tmp
Environment=HF_HOME=/workspace/hf
Environment=UV_CACHE_DIR=/workspace/uv_cache
Environment=SUBMIT_FINAL_SECRET=change-me
ExecStart=/usr/local/bin/uv run uvicorn main:app --host 0.0.0.0 --port ${PORT} --timeout-keep-alive 3600
Restart=always
RestartSec=3
StandardOutput=append:/var/log/hack_apr/server.log
StandardError=append:/var/log/hack_apr/server.log
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}"
systemctl restart "${SERVICE_NAME}"

mkdir -p /home/"$TEAM_USER"/share
chown -R "$TEAM_USER:$TEAM_USER" /home/"$TEAM_USER"
chmod 700 /home/"$TEAM_USER"

cat >/home/"$TEAM_USER"/README.txt <<EOF
Server running on port ${PORT}

Accessible:
  /workspace
  /home/${TEAM_USER}

Restricted:
  ${APP_DIR}
  /var/log/hack_apr

Example:
  curl http://127.0.0.1:${PORT}/
EOF

chown "$TEAM_USER:$TEAM_USER" /home/"$TEAM_USER"/README.txt
chmod 644 /home/"$TEAM_USER"/README.txt

IP_ADDR="$(hostname -I | awk '{print $1}')"

echo
echo "Done."
echo "User: ${TEAM_USER}"
echo "Password: ${TEAM_PASSWORD}"
echo "Host: ${IP_ADDR}"
echo "SSH: ssh ${TEAM_USER}@${IP_ADDR}"
echo "App: http://${IP_ADDR}:${PORT}"
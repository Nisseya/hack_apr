#!/usr/bin/env bash
set -euo pipefail

TEAM_PASSWORD="${1:-}"
TEAM_USER="teamuser"
APP_USER="hackapr"
APP_GROUP="hackapr"
APP_DIR="/var/lib/hack_apr"
SERVICE_NAME="hack-apr"
PORT="9000"

LOG_DIR="/var/log/hack_apr"

WORKSPACE_TMP="/workspace/tmp"
WORKSPACE_HF="/workspace/hf"
WORKSPACE_UV_CACHE="/workspace/uv_cache"
WORKSPACE_VENVS="/workspace/venvs"
WORKSPACE_UV_PYTHON="/workspace/uv_python"

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
  exit 1
fi

if ! getent group "$APP_GROUP" >/dev/null; then
  groupadd --system "$APP_GROUP"
fi

if ! id -u "$APP_USER" >/dev/null 2>&1; then
  useradd --system --create-home --gid "$APP_GROUP" --home-dir "/home/${APP_USER}" --shell /usr/sbin/nologin "$APP_USER"
fi

if ! id -u "$TEAM_USER" >/dev/null 2>&1; then
  useradd -m -s /bin/bash "$TEAM_USER"
fi

echo "${TEAM_USER}:${TEAM_PASSWORD}" | chpasswd

mkdir -p "/home/${APP_USER}"
chown -R "${APP_USER}:${APP_GROUP}" "/home/${APP_USER}"
chmod 755 "/home/${APP_USER}"

apt-get update
apt-get install -y curl ca-certificates

if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi

if [ -x /root/.local/bin/uv ]; then
  ln -sf /root/.local/bin/uv /usr/local/bin/uv
fi

mkdir -p "$LOG_DIR"
chown -R "$APP_USER:$APP_GROUP" "$LOG_DIR"
chmod 750 "$LOG_DIR"

mkdir -p \
  "$WORKSPACE_TMP" \
  "$WORKSPACE_HF" \
  "$WORKSPACE_UV_CACHE" \
  "$WORKSPACE_VENVS" \
  "$WORKSPACE_UV_PYTHON"

chmod 777 \
  "$WORKSPACE_TMP" \
  "$WORKSPACE_HF" \
  "$WORKSPACE_UV_CACHE" \
  "$WORKSPACE_VENVS" \
  "$WORKSPACE_UV_PYTHON"

chown -R "$APP_USER:$APP_GROUP" "$APP_DIR"
chmod 700 "$APP_DIR"
find "$APP_DIR" -type d -exec chmod 700 {} \;
find "$APP_DIR" -type f -exec chmod 600 {} \;

if [ -f "$APP_DIR/pyproject.toml" ]; then
  su -s /bin/bash "$APP_USER" -c "
    export HOME=/home/${APP_USER}
    export PATH=/usr/local/bin:/home/${APP_USER}/.local/bin:\$PATH
    export TMPDIR=${WORKSPACE_TMP}
    export HF_HOME=${WORKSPACE_HF}
    export UV_CACHE_DIR=${WORKSPACE_UV_CACHE}
    export UV_LINK_MODE=copy
    export VIRTUALENV_OVERRIDE_APP_DATA=${WORKSPACE_VENVS}
    export UV_PYTHON_INSTALL_DIR=${WORKSPACE_UV_PYTHON}
    export UV_MANAGED_PYTHON=1
    cd ${APP_DIR}
    uv python install 3.12
    uv sync --python 3.12
  "
fi

cat >/etc/systemd/system/${SERVICE_NAME}.service <<EOF
[Unit]
Description=hack_apr server
After=network.target

[Service]
User=${APP_USER}
Group=${APP_GROUP}
WorkingDirectory=${APP_DIR}
Environment=PYTHONUNBUFFERED=1
Environment=PATH=/usr/local/bin:/home/${APP_USER}/.local/bin:/usr/bin:/bin
Environment=TMPDIR=${WORKSPACE_TMP}
Environment=HF_HOME=${WORKSPACE_HF}
Environment=UV_CACHE_DIR=${WORKSPACE_UV_CACHE}
Environment=UV_LINK_MODE=copy
Environment=VIRTUALENV_OVERRIDE_APP_DATA=${WORKSPACE_VENVS}
Environment=UV_PYTHON_INSTALL_DIR=${WORKSPACE_UV_PYTHON}
Environment=UV_MANAGED_PYTHON=1
Environment=SUBMIT_FINAL_SECRET=change-me
ExecStart=/usr/local/bin/uv run --python 3.12 uvicorn main:app --host 0.0.0.0 --port ${PORT} --timeout-keep-alive 3600
Restart=always
RestartSec=3
StandardOutput=append:${LOG_DIR}/server.log
StandardError=append:${LOG_DIR}/server.log
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}"
systemctl restart "${SERVICE_NAME}"

mkdir -p "/home/${TEAM_USER}/share"
chown -R "${TEAM_USER}:${TEAM_USER}" "/home/${TEAM_USER}"
chmod 700 "/home/${TEAM_USER}"

cat >"/home/${TEAM_USER}/README.txt" <<EOF
Server running on port ${PORT}

Repo:
  ${APP_DIR}

Runtime:
  ${WORKSPACE_TMP}
  ${WORKSPACE_HF}
  ${WORKSPACE_UV_CACHE}
  ${WORKSPACE_VENVS}
  ${WORKSPACE_UV_PYTHON}

Python:
  uv-managed Python 3.12

Example:
  curl http://127.0.0.1:${PORT}/
EOF

chown "${TEAM_USER}:${TEAM_USER}" "/home/${TEAM_USER}/README.txt"
chmod 644 "/home/${TEAM_USER}/README.txt"

IP_ADDR="$(hostname -I | awk '{print $1}')"

echo
echo "Done."
echo "User: ${TEAM_USER}"
echo "Password: ${TEAM_PASSWORD}"
echo "Host: ${IP_ADDR}"
echo "SSH: ssh ${TEAM_USER}@${IP_ADDR}"
echo "App: http://${IP_ADDR}:${PORT}"
echo "Logs: tail -f ${LOG_DIR}/server.log"
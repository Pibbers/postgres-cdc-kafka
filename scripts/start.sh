#!/usr/bin/env bash
set -euo pipefail

if ! docker info >/dev/null 2>&1; then
  USER_ID="$(id -u)"
  SOCKET_PATH="/run/user/${USER_ID}/podman/podman.sock"
  export DOCKER_HOST="${DOCKER_HOST:-unix://${SOCKET_PATH}}"

  mkdir -p "/run/user/${USER_ID}/podman"

  if ! pgrep -f "podman system service .*${SOCKET_PATH}" >/dev/null 2>&1; then
    podman system service --time=0 "$DOCKER_HOST" >/tmp/podman-service.log 2>&1 &
  fi
fi

docker compose up -d "$@"

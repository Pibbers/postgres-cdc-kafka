#!/usr/bin/env bash
set -euo pipefail

if ! docker info >/dev/null 2>&1; then
  USER_ID="$(id -u)"
  SOCKET_PATH="/run/user/${USER_ID}/podman/podman.sock"
  export DOCKER_HOST="${DOCKER_HOST:-unix://${SOCKET_PATH}}"
fi

docker compose down "$@"

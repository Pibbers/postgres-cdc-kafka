#!/usr/bin/env bash
set -euo pipefail

USER_ID="$(id -u)"
SOCKET_PATH="/run/user/${USER_ID}/podman/podman.sock"
export DOCKER_HOST="${DOCKER_HOST:-unix://${SOCKET_PATH}}"

docker compose down "$@"

#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"

curl -s -X POST "${CONNECT_URL}/connectors" \
  -H "Content-Type: application/json" \
  -d @"${SCRIPT_DIR}/debezium-postgres.json"
echo
curl -s "${CONNECT_URL}/connectors/debezium-demo-postgres/status"
echo

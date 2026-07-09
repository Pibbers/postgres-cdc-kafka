#!/usr/bin/env bash
set -euo pipefail

TOPIC="${1:-orders}"
BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

python3 - <<'PY' "$BOOTSTRAP_SERVER" "$TOPIC"
import json
import sys
from kafka import KafkaConsumer

bootstrap_server, topic = sys.argv[1], sys.argv[2]
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[bootstrap_server],
    group_id='sample-consumer-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda message: json.loads(message.decode('utf-8')),
)

print(f'Listening on {topic} at {bootstrap_server}')
for message in consumer:
    print(json.dumps(message.value, indent=2))
PY

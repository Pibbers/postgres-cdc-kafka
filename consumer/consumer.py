import json
import os

from kafka import KafkaConsumer

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
topic = os.getenv("KAFKA_TOPIC", "orders")
group_id = os.getenv("KAFKA_CONSUMER_GROUP", "orders-consumer")

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[bootstrap_servers],
    group_id=group_id,
    auto_offset_reset="earliest",
    value_deserializer=lambda message: json.loads(message.decode("utf-8")),
)

print(f"Listening for CDC events on topic '{topic}' via {bootstrap_servers}")
for message in consumer:
    record = message.value
    operation = record.get("__op", record.get("op", "r"))
    print(
        json.dumps(
            {
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "operation": operation,
                "record": record,
            },
            indent=2,
        )
    )

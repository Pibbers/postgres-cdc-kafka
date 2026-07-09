# PostgreSQL CDC to Kafka with Debezium

This project builds a local end-to-end CDC pipeline using PostgreSQL, Apache Kafka, and Debezium Kafka Connect.

## What it includes
- PostgreSQL 15 with logical replication enabled
- Kafka broker and Kafka Connect
- Debezium PostgreSQL connector configured for `public.customers` and `public.orders`
- A small Python consumer that reads CDC events from Kafka

## Quick start

1. Start the stack:
   ```bash
   ./scripts/start.sh
   ```
2. Wait for Kafka Connect to become ready.
3. Register the connector:
   ```bash
   curl -X POST http://localhost:8083/connectors \
     -H "Content-Type: application/json" \
     -d @connectors/postgres-source.json
   ```
4. Check connector status:
   ```bash
   curl http://localhost:8083/connectors/postgres-source/status
   ```
5. Install the Python consumer dependencies:
   ```bash
   python3 -m pip install -r consumer/requirements.txt
   ```
6. Run the consumer locally:
   ```bash
   KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_TOPIC=orders python3 consumer/consumer.py
   ```
7. Or run the containerized consumer from the stack:
   ```bash
   docker compose up -d consumer
   ```
8. For a quick host-side topic consumer, use:
   ```bash
   ./consumer/consume_topic.sh orders localhost:9092
   ```
9. Open the browser-based Kafka UI at http://localhost:8080 to inspect topics, messages, consumer groups, and the Debezium connector.
10. Open the Airflow UI at http://localhost:8082 to schedule and monitor the example DAG.
11. In another terminal, insert or update rows in PostgreSQL to observe events:
   ```bash
   docker exec -it postgres psql -U postgres -d mydb -c "INSERT INTO public.customers (name, email) VALUES ('Linus Torvalds', 'linus@example.com');"
   ```

## Notes
- The connector uses the `pgoutput` logical decoding plugin.
- The Debezium transform `ExtractNewRecordState` unwraps the payload so Kafka records are easier to consume.
- The example consumer listens to the `orders` topic, which is produced by the Debezium transform.
- On this machine, the helper scripts use Podman’s API socket so `docker compose` can work even when Docker Desktop is not installed.
- The containerized consumer is defined in [consumer/Dockerfile](consumer/Dockerfile) and can be started with `docker compose up -d consumer`.

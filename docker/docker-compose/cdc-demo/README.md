# Debezium CDC Demo

Demonstrates end-to-end Change Data Capture from Postgres into XTDB via Debezium and Kafka.

## Prerequisites

Build the Docker image (includes the debezium module):

```bash
./docker/scripts/build-aws-image.sh --clean
```

## Start the stack

```bash
cd docker/docker-compose/cdc-demo
docker compose up -d
```

Wait ~30s for all services to stabilise.
Check status with `docker compose ps` — `debezium-init` and `minio-setup` will show as exited (expected, they're one-shot).

## Run the demo

To submit transactions to Postgres:

```bash
PGPASSWORD=postgres psql -h localhost -p 5434 -U postgres -d sourcedb
```

To watch messages on the kafka topic:

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic sourcedb.public.cdc_demo --from-beginning 2>/dev/null | jq .
```

To watch for changes in XTDB:
```bash
watch -n 0.5 "psql -h localhost -p 5433 -U xtdb -d xtdb -c \"SELECT *, _valid_time FROM public.cdc_demo FOR ALL VALID_TIME ORDER BY _id, _valid_from\""
```

### 1. Create a table and insert data in Postgres

```sql
CREATE TABLE cdc_demo (_id INT PRIMARY KEY, name TEXT, email TEXT);
INSERT INTO cdc_demo VALUES (1, 'Alice', 'alice@example.com');
INSERT INTO cdc_demo VALUES (2, 'Bob', 'bob@example.com');
```

### 2. More mutations

```sql
UPDATE cdc_demo SET email = 'alice-new@example.com' WHERE _id = 1;
DELETE FROM cdc_demo WHERE _id = 2;
INSERT INTO cdc_demo VALUES (3, 'Charlie', 'charlie@example.com');
```

## Tear down

```bash
docker compose down -v
```

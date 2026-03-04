#!/bin/sh
set -e

CONNECT_URL="http://debezium-connect:8083"

echo "Waiting for Debezium Connect to be ready..."
until curl -sf "$CONNECT_URL/connectors" > /dev/null 2>&1; do
  sleep 2
done
echo "Debezium Connect is ready."

curl -sf -X POST "$CONNECT_URL/connectors" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "sourcedb-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "postgres",
      "database.password": "postgres",
      "database.dbname": "sourcedb",
      "topic.prefix": "sourcedb",
      "schema.include.list": "public",
      "plugin.name": "pgoutput"
    }
  }'

echo ""
echo "Connector registered successfully."

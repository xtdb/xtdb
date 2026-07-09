---
title: Setting up a Kafka Connect external source
---

In this guide we will set up a [Kafka Connect external source](/ops/external-sources/kafka-connect/reference) from the Kafka topic `orders` into a database in XTDB called `kc_orders`.
Along the way we will apply a transform to mask the `email` field, gated by a predicate so it skips tombstones.

To do so we will:

1. Configure the Kafka cluster credentials on the XTDB node and redeploy
1. Run `ATTACH DATABASE` in XTDB

## Prerequisites

As with other [external sources](/ops/external-sources/overview) you will need:

- A [transaction log](/ops/config/log)
- An [object store](/ops/config/storage)

:::caution
Ensure that you have a transaction log and object store that do not conflict with other databases.
:::

Additionally you will need a single-partition Kafka topic carrying the records to sync.

## Deploy the Kafka cluster credentials

Configured under the [`remotes`](/ops/config#remotes) section of the node config like so:

```yaml
remotes:
  kafka_remote: !Kafka
    bootstrapServers: "localhost:9092"
```

For nodes to pick up this config change a rolling re-deploy is required.

:::note
If your [transaction log](/ops/config/log/kafka) is already a `!Kafka` remote, you can reuse those credentials for the source rather than declaring a new remote — and since the config is unchanged, no rolling re-deploy is needed.
:::

## Run `ATTACH DATABASE` in XTDB

Finally to attach the secondary database with the external source:

```sql
ATTACH DATABASE kc_orders WITH $$
# Set up in the prerequisites
log: !Local
  path: 'kc-orders/log'
storage: !Local
  path: 'kc-orders/storage'

externalSource: !KafkaConnect
  remote: kafka_remote
  topic: orders
  connectConfig:
    key.converter: org.apache.kafka.connect.storage.StringConverter
    value.converter: org.apache.kafka.connect.json.JsonConverter
    value.converter.schemas.enable: "false"
    transforms: mask
    transforms.mask.type: org.apache.kafka.connect.transforms.MaskField$Value
    transforms.mask.fields: email
    transforms.mask.replacement: REDACTED
    transforms.mask.predicate: notTombstone
    transforms.mask.negate: "true"
    predicates: notTombstone
    predicates.notTombstone.type: org.apache.kafka.connect.transforms.predicates.RecordIsTombstone
  indexer: !Docs
    table: orders
$$
```

You can now query the database by connecting to the `kc_orders` database and running:
```sql
SELECT * FROM orders;
```

Or from another database in XTDB by running:
```sql
SELECT * FROM kc_orders.public.orders;
```

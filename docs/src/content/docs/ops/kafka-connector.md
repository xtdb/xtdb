---
title: Kafka Sink Connector
---

XTDB provides a Sink Connector for the [Kafka Connect](https://kafka.apache.org/41/kafka-connect/overview/) platform, which allows ingesting records from Kafka topics into XTDB tables.

The XTDB Sink Connector features:

- Ingestion through XTDB's INSERT or PATCH operations
- Flexible topic-to-table configuration
- Data coercion to closest XTDB data types, based on Kafka record schema - tested with Avro and JSON schemas
- Ability to specify an exact XTDB data type as a schema extension, if needed
- Unrolling of record batches for isolating individual record errors
- Smart automatic retries, depending on the kind of error (transient errors / data errors / other)
- Errant records are reported to allow moving to dead-letter queues
- Packaged as an Uber-JAR and as Confluent Component ZIP package

You can find more information in the [XTDB Sink Connector Github repository](https://github.com/xtdb/xtdb-kafka-connect).
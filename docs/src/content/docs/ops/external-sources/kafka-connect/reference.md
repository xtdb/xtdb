---
title: Kafka Connect External Source
---

## Prerequisites

A Kafka topic carrying the records you want to sync to XTDB.

:::caution
Records on partitions other than partition 0 are not consumed, so the topic should have a single partition.
:::

## Configuration

Configured by [attaching a secondary database](/about/dbs-in-xtdb#attachingdetaching-secondary-databases-v21) with the additional `externalSource` options.
It consumes a topic via a [`!Kafka` remote](#remote), applies a [Connect config](#connect-config) to each record, and writes through an [indexer](#indexers):

```sql
ATTACH DATABASE my_db WITH $$
externalSource: !KafkaConnect
  # The alias of the !Kafka remote holding the cluster connection details.
  remote: my_kafka_remote

  # The Kafka topic to consume records from.
  topic: my_upstream_topic

  # Kafka Connect converter and transform options.
  # key.converter and value.converter are required.
  connectConfig:
    key.converter: org.apache.kafka.connect.storage.StringConverter
    value.converter: org.apache.kafka.connect.json.JsonConverter
    value.converter.schemas.enable: "false"
    transforms: unwrap
    transforms.unwrap.type: org.apache.kafka.connect.transforms.ExtractField$Value
    transforms.unwrap.field: payload

  # The indexer used to index records.
  indexer: !Docs
    table: my_table
$$
```

### Remote

Kafka cluster connection details are stored in a `!Kafka` [remote](/ops/config#remotes).
See the [Kafka](/ops/config/log/kafka) documentation for the available options.

```yaml
remotes:
  my_kafka_remote: !Kafka
    bootstrapServers: "localhost:9092"
```

### Connect config

`connectConfig` is a map of [Kafka Connect](https://kafka.apache.org/documentation/#connect) options applied to each record before it is indexed.
XTDB accepts the keys below; any other key is rejected.

```yaml
connectConfig:
  # Required: the fully-qualified converter class for record keys and values.
  key.converter: <converter class>
  value.converter: <converter class>

  # Options passed to a converter, with the key.converter./value.converter. prefix stripped.
  value.converter.<option>: <value>

  # A comma-separated list of transform aliases, applied in order.
  transforms: <alias>

  # The fully-qualified transform class.
  transforms.<alias>.type: <transform class>

  # Options passed to the transform.
  transforms.<alias>.<option>: <value>

  # Optional: the alias of a predicate (declared in `predicates`) that gates whether the transform applies.
  transforms.<alias>.predicate: <predicate alias>

  # Optional: apply the transform only when the predicate does not match. Requires `predicate`.
  transforms.<alias>.negate: "true"

  # A comma-separated list of predicate aliases.
  predicates: <alias>

  # The fully-qualified predicate class.
  predicates.<alias>.type: <predicate class>

  # Options passed to the predicate.
  predicates.<alias>.<option>: <value>
```

See [Included Kafka Connect classes](#included-kafka-connect-classes) for the bundled classes and the interfaces a custom class must implement.

XTDB runs converters, transforms, and predicates itself rather than in a Kafka Connect worker, so some Kafka Connect features are unavailable:
- Classes are loaded from the JVM classpath by name; there is no `plugin.path` isolation.
- Header conversion is fixed and cannot be configured.

:::caution
There is no error tolerance, dead-letter queue, or skip-and-continue, matching Kafka Connect's `errors.tolerance=none`.
A record that fails to convert or transform halts the source, and so does any record an indexer cannot process or declines to.
:::

## Included Kafka Connect classes

Converters, transforms, and predicates are loaded from the node's classpath by their fully-qualified class name.
The classes listed below are bundled with XTDB.

To use any other Kafka Connect class, for example [`io.confluent.connect.json.JsonSchemaConverter`](https://docs.confluent.io/platform/current/connect/userguide.html#json-schema-and-protobuf), add its jar to the node's classpath.
How you add a jar to the classpath depends on how you deploy XTDB, for example as a dependency in your build or bundled into your XTDB image.

A custom class must implement the relevant Kafka Connect interface.

Converters
:   Deserialize record keys and values — implement `org.apache.kafka.connect.storage.Converter`.
    XTDB bundles [`StringConverter`](https://docs.confluent.io/platform/current/connect/userguide.html#string-format-and-raw-bytes), [`JsonConverter`](https://docs.confluent.io/platform/current/connect/userguide.html#json-without-sr), and [`AvroConverter`](https://docs.confluent.io/platform/current/connect/userguide.html#avro).

Transforms
:   Single Message Transforms that modify each record before it is indexed — implement `org.apache.kafka.connect.transforms.Transformation<SinkRecord>`.
    XTDB bundles those that ship with [Kafka Connect 4.1.1](https://kafka.apache.org/41/kafka-connect/user-guide/#included-transformations).
    The target table is set by the [indexer](#indexers), so a transform that changes a record's topic has no effect on where it is written.

Predicates
:   Predicates that gate whether a transform applies to a record — implement `org.apache.kafka.connect.transforms.predicates.Predicate<SinkRecord>`.
    XTDB bundles those that ship with [Kafka Connect 4.1.1](https://kafka.apache.org/41/kafka-connect/user-guide/#predicates).

## Indexers

The following indexers are built into XTDB:
- Docs

### Docs

Maps each record to a document in the configured table.

#### Configuration

```yaml
indexer: !Docs
  # The XTDB table to write to, as `schema.table`.
  # An unqualified name is taken as the table, in the `public` schema.
  # The table part must not itself contain a `.`.
  table: my_table
```

#### Document `_id`

The `_id` of each document is derived from the record key; an `_id` field in the value is overwritten.
A single-field Struct key is unwrapped to its inner value automatically.
A record with no usable key (none at all, a multi-field key, or a binary key) halts the source.

For a topic whose id lives in the value, promote it to the key with the standard Connect transforms:

```yaml
connectConfig:
  # ...
  transforms: keyFromValue,extractKey
  transforms.keyFromValue.type: org.apache.kafka.connect.transforms.ValueToKey
  transforms.keyFromValue.fields: id
  transforms.extractKey.type: org.apache.kafka.connect.transforms.ExtractField$Key
  transforms.extractKey.field: id
```

#### Deletes

A tombstone (a record with a null value) is indexed as a delete of the document with that key.

#### Document values

A non-null value must convert to a Struct or Map at the top level; a scalar or array value halts the source.

Record values are translated into XTDB [types](/reference/main/data-types) as follows:

| Kafka Connect type | XTDB type |
| --- | --- |
| Struct | nested document |
| Map | nested document (keys converted to text) |
| Array | list |
| Decimal | `DECIMAL` |
| Timestamp | `TIMESTAMP WITH TIMEZONE` |
| Date | `DATE` |
| Time | `TIME` |
| Bytes | `VARBINARY` |
| int, long, float, double, boolean, string | the corresponding XTDB scalar |

#### Errors

The `!Docs` indexer halts on any record it can't index, such as one with an unusable key or a non-document value.

#### System time

The system time of each row is the timestamp of the Kafka record, so ensure the topic carries meaningful timestamps — for example by setting `message.timestamp.type=LogAppendTime` on the topic, or by having producers set an explicit timestamp.

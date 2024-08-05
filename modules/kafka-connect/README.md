# kafka-connect

## kafka-sink

## Config

### `url`

Destination URL of XTDB HTTP end point.

Type: `String`
Default: `None`
Importance: `High`

### `id.mode`

Where to get the `_id` from.

One of:
- `record_key`
  - The record key must be either a Struct or a primitive value
  - If the key is a struct then `id.field` must be used to select a field to use as the `_id`
  - **Required** if you want [tombstones](https://kafka.apache.org/documentation/#design_compactionbasics) to delete records
- `record_value`
  - `id.field` must be used to select a field to use as the `_id`

Type: `String`
Default: `None`
Importance: `High`

### `id.field`

The field name to use as the `_id`.
Leave blank if using a primitive `record_key`.

The behaviour depends on `id.mode`:
- `record_key`
  - If the key is primitive then `id.field` *must* be empty string
  - If the key is a Struct then must select a field from the Struct
- `record_value`
  - Must select a key from the Struct

Type: `String`
Default: `""`
Importance: `Medium`

### `validFrom.field`

The field name to use as `_valid_from`.
Leave blank to use xtdb's default `_valid_from`.

Default: `""`
Type: `String`
Importance: `Low`

### `validTo.field`

The field name to use as `_valid_from`.
Leave blank to use xtdb's default `_valid_from`.

Type: `String`
Default: `""`
Importance: `Low`

### `table.name.format`

A format string for the destination table name, which may contain `${topic}` as a placeholder for the originating topic name.

Type: `String`
Default: `"${topic}"`
Importance: `Medium`

## Example config

```json
{
    "name": "xtdb-jdbc-sink-connector",
    "config": {
        "connector.class": "xtdb.kafka.connect.XtdbSinkConnector",
        "tasks.max": 1,
        "topics": "readings",
        "url": "http://xtdb:3000",
        "id.mode": "record_value",
        "id.field": "xt/id",
        "validFrom.field": "validFrom",
        "validTo.field": "validTo",
        "table.name.format": "companyco_${topic}",
        "transforms": "convertValidFrom,convertValidTo",
        "transforms.convertValidFrom.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
        "transforms.convertValidFrom.field": "validFrom",
        "transforms.convertValidFrom.target.type": "Timestamp",
        "transforms.convertValidFrom.format": "yyyy-MM-dd'T'HH:mm:ssX",
        "transforms.convertValidTo.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
        "transforms.convertValidTo.field": "validTo",
        "transforms.convertValidTo.target.type": "Timestamp",
        "transforms.convertValidTo.format": "yyyy-MM-dd'T'HH:mm:ssX",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "true",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "true"
    }
}
```

With some data that looks like:
```json
{
    "schema": {
        "type": "struct",
        "fields": [
            { "type": "int64", "optional": false, "field": "xt/id" },
            { "type": "string", "optional": false, "field": "metric" },
            { "type": "int32", "optional": false, "field": "measurement" },
            { "type": "string", "optional": false, "field": "validFrom" },
            { "type": "string", "optional": false, "field": "validTo" }
        ]
    },
    "payload": {
        "xt/id": 1,
        "metric": "Humidity",
        "validFrom": "2024-01-01T00:15:00Z",
        "validTo": "2024-01-01T00:20:00Z",
        "measurement": 0.8
    }
}
```

If storing datetimes with timezones is important to you, I would suggest
writing a [custom transform](https://docs.confluent.io/platform/current/connect/transforms/custom.html).

## Development

REPL:

```bash
$ clj -M:dev
```

Build:

```bash
$ clj -T:build uber
```

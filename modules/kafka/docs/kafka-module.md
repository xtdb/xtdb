# Kafka Module

[Apache Kafka](https://kafka.apache.org/) can be used as an XTDB transaction log.

For an overview of the operation and requirements of the XTDB Kafka module, see the [Kafka reference](/reference/main/modules/kafka).

## Configuration:

To configure it, add the following to your node options map:

```clojure
{:log [:kafka <opts>]}
```

Connection options:

* `:bootstrap-servers` (string, required, default `"localhost:9092"`): URL for connecting to Kafka
* `:properties-file` (string/`File`/`Path`, optional): Kafka connection properties file, supplied directly to Kafka
* `:properties-map` (map, optional): Kafka connection properties map, supplied directly to Kafka
* `poll-wait-duration` (string/`Duration`, optional, default 1 second, `"PT1S"`): time to wait on each Kafka poll.

Topic options:

* `:topic-name` (string, required).
* `:replication-factor` (int, optional, default 1): level of durability for Kafka
* `:create-topics?` (boolean, optional, default true): whether to create topics if they do not exist
* `:topic-config` (map, optional): any further topic configuration to pass directly to Kafka

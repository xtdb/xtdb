package crux.api;

import clojure.lang.Keyword;
import java.lang.Boolean;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.util.HashMap;
import java.util.Map;

public class KafkaNode {
  public static final Keyword DB_DIR = Keyword.intern("crux.kv/db-dir");

  public static final String DB_DIR_DEFAULT = "data";

  public static final Keyword KAFKA_PROPERTIES_MAP = Keyword.intern("crux.kafka/kafka-properties-map");

  public static final Keyword BOOTSTRAP_SERVERS = Keyword.intern("crux.kafka/bootstrap-servers");

  public static final String BOOTSTRAP_SERVERS_DEFAULT = "localhost:9092";

  public static final Keyword KV_STORE = Keyword.intern("crux.node/kv-store");

  public static final Keyword KAFKA_PROPERTIES_FILE = Keyword.intern("crux.kafka/kafka-properties-file");

  public static final Keyword REPLICATION_FACTOR = Keyword.intern("crux.kafka/replication-factor");

  public static final Long REPLICATION_FACTOR_DEFAULT = 1l;

  public static final Keyword DOC_PARTITIONS = Keyword.intern("crux.kafka/doc-partitions");

  public static final Long DOC_PARTITIONS_DEFAULT = 1l;

  public static final Keyword SYNC = Keyword.intern("crux.kv/sync?");

  public static final Boolean SYNC_DEFAULT = false;

  public static final Keyword TX_TOPIC = Keyword.intern("crux.kafka/tx-topic");

  public static final String TX_TOPIC_DEFAULT = "crux-transaction-log";

  public static final Keyword GROUP_ID = Keyword.intern("crux.kafka/group-id");

  public static final String GROUP_ID_DEFAULT = "9545aff2-e102-464a-819f-03b6345a672e";

  public static final Keyword CREATE_TOPICS = Keyword.intern("crux.kafka/create-topics");

  public static final Boolean CREATE_TOPICS_DEFAULT = true;

  public static final Keyword CHECK_AND_STORE_INDEX_VERSION = Keyword.intern("crux.kv/check-and-store-index-version");

  public static final Boolean CHECK_AND_STORE_INDEX_VERSION_DEFAULT = true;

  public static final Keyword INDEXER = Keyword.intern("crux.node/indexer");

  public static final Keyword DOC_TOPIC = Keyword.intern("crux.kafka/doc-topic");

  public static final String DOC_TOPIC_DEFAULT = "crux-docs";

  public static final Keyword OBJECT_STORE = Keyword.intern("crux.node/object-store");

  public static class Builder {
    public Map<Keyword, Object> KafkaNodeProperties;

    public Builder() {
      KafkaNodeProperties = new HashMap<Keyword, Object>();
      KafkaNodeProperties.put(Keyword.intern("crux.node/topology"),Keyword.intern("crux.kafka/topology"));
    }

    public Builder withDbDir(String val) {
      KafkaNodeProperties.put(DB_DIR, val);
      return this;
    }

    public Builder withKafkaPropertiesMap(String val) {
      KafkaNodeProperties.put(KAFKA_PROPERTIES_MAP, val);
      return this;
    }

    public Builder withBootstrapServers(String val) {
      KafkaNodeProperties.put(BOOTSTRAP_SERVERS, val);
      return this;
    }

    public Builder withKvStore(String val) {
      KafkaNodeProperties.put(KV_STORE, val);
      return this;
    }

    public Builder withKafkaPropertiesFile(String val) {
      KafkaNodeProperties.put(KAFKA_PROPERTIES_FILE, val);
      return this;
    }

    public Builder withReplicationFactor(Long val) {
      KafkaNodeProperties.put(REPLICATION_FACTOR, val);
      return this;
    }

    public Builder withDocPartitions(Long val) {
      KafkaNodeProperties.put(DOC_PARTITIONS, val);
      return this;
    }

    public Builder withSync(Boolean val) {
      KafkaNodeProperties.put(SYNC, val);
      return this;
    }

    public Builder withTxTopic(String val) {
      KafkaNodeProperties.put(TX_TOPIC, val);
      return this;
    }

    public Builder withGroupId(String val) {
      KafkaNodeProperties.put(GROUP_ID, val);
      return this;
    }

    public Builder withCreateTopics(Boolean val) {
      KafkaNodeProperties.put(CREATE_TOPICS, val);
      return this;
    }

    public Builder withCheckAndStoreIndexVersion(Boolean val) {
      KafkaNodeProperties.put(CHECK_AND_STORE_INDEX_VERSION, val);
      return this;
    }

    public Builder withIndexer(String val) {
      KafkaNodeProperties.put(INDEXER, val);
      return this;
    }

    public Builder withDocTopic(String val) {
      KafkaNodeProperties.put(DOC_TOPIC, val);
      return this;
    }

    public Builder withObjectStore(String val) {
      KafkaNodeProperties.put(OBJECT_STORE, val);
      return this;
    }

    public Map<Keyword, Object> build() {
      return KafkaNodeProperties;
    }
  }
}

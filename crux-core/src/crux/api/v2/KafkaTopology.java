package crux.api.v2;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

import java.util.HashMap;
import java.util.Map;

import static crux.api.v2.Util.kw;

public class KafkaTopology extends EdnTopology {
    private static final Keyword KAFKA_TOPOLOGY = kw("crux.kafka/topology");
    private static final Keyword NODE_TOPOLOGY = kw("crux.node/topology");
    private static final Keyword KV_STORE = kw("crux.node/kv-store");
    private static final Keyword OBJECT_STORE = kw("crux.node/object-store");
    private static final Keyword DB_DIR = kw("crux.kv/db-dir");
    private static final Keyword KV_SYNC = kw("crux.kv/sync");
    private static final Keyword CHECK_AND_STORE_INDEX_VERSION = kw("crux.kv/check-and-store-index-version");
    private static final Keyword BOOTSTRAP_SERVERS = kw("crux.kafka/bootstrap-servers");
    private static final Keyword TX_TOPIC = kw("crux.kafka/tx-topic");
    private static final Keyword DOC_TOPIC = kw("crux.kafka/doc-topic");
    private static final Keyword CREATE_TOPICS = kw("crux.kafka/create-topics");
    private static final Keyword DOC_PARTITIONS = kw("crux.kafka/doc-partitions");
    private static final Keyword REPLICATION_FACTOR = kw("crux.kafka/replication-factor");
    private static final Keyword GROUP_ID = kw("crux.kafka/group-id");
    private static final Keyword KAFKA_PROPERTIES_FILE = kw("crux.kafka/kafka-properties-file");

    protected final Map<Keyword, Object> topologyAttrs;

    private KafkaTopology(Map<Keyword, Object> topologyAttrs) {
        this.topologyAttrs = topologyAttrs;
    }

    public Object getObject(Keyword attr) {
        return topologyAttrs.get(attr);
    }

    public static KafkaTopology kafkaTopology() {
        return new KafkaTopology(Map.of(NODE_TOPOLOGY, KAFKA_TOPOLOGY));
    }

    @SuppressWarnings("unchecked")
    Map<Keyword, Object> toEdn() {
        IPersistentMap ednMap = PersistentArrayMap.EMPTY;
        for (Keyword key : topologyAttrs.keySet()) {
            ednMap = ednMap.assoc(key, topologyAttrs.get(key));
        }
        return (PersistentArrayMap) ednMap;
    }

    public KafkaTopology withTopologyMap(Map<Keyword, ?> topologyAttrs) {
        Map<Keyword, Object> newTopologyAttrs = new HashMap<>(this.topologyAttrs);
        newTopologyAttrs.putAll(topologyAttrs);
        return new KafkaTopology(newTopologyAttrs);
    }

    private KafkaTopology with(Keyword k, Object v) {
        Map<Keyword, Object> newTopologyAttrs = new HashMap<>(this.topologyAttrs);
        newTopologyAttrs.put(k, v);
        return new KafkaTopology(newTopologyAttrs);
    }

    public KafkaTopology withKvStore(String kvStore) {
        return with(KV_STORE, kvStore);
    }

    public KafkaTopology withObjectStore(String objectStore) {
        return with(OBJECT_STORE, objectStore);
    }

    public KafkaTopology withDbDir(String dbDir) {
        return with(DB_DIR, dbDir);
    }

    public KafkaTopology withSync(boolean sync) {
        return with(KV_SYNC, sync);
    }

    public KafkaTopology withCheckAndStoreIndexVersion(boolean checkAndStoreIndexVersion) {
        return with(CHECK_AND_STORE_INDEX_VERSION, checkAndStoreIndexVersion);
    }

    public KafkaTopology withBootstrapServers(String bootstrapServers) {
        return with(BOOTSTRAP_SERVERS, bootstrapServers);
    }

    public KafkaTopology withTxTopic(String txTopic) {
        return with(TX_TOPIC, txTopic);
    }

    public KafkaTopology withDocTopic(String docTopic) {
        return with(DOC_TOPIC, docTopic);
    }

    public KafkaTopology withCreateTopics(boolean createTopics) {
        return with(CREATE_TOPICS, createTopics);
    }

    public KafkaTopology withDocPartitions(int docPartitions) {
        return with(DOC_PARTITIONS, docPartitions);
    }

    public KafkaTopology withReplicationFactor(int replicationFactor) {
        return with(REPLICATION_FACTOR, replicationFactor);
    }

    public KafkaTopology withGroupId(String groupId) {
        return with(GROUP_ID, groupId);
    }

    public KafkaTopology withKafkaPropertiesFile(String kafkaPropertiesFile) {
        return with(KAFKA_PROPERTIES_FILE, kafkaPropertiesFile);
    }
}
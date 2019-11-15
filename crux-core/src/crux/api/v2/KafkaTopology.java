package crux.api.v2;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

import java.util.HashMap;
import java.util.Map;

import static crux.api.v2.Attribute.attr;

public class KafkaTopology extends Topology {
    protected final Map<Attribute, Object> topologyAttrs;

    private KafkaTopology(Map<Attribute, Object> topologyAttrs) {
        this.topologyAttrs = topologyAttrs;
    }

    public Object getObject(Attribute attr) {
        return topologyAttrs.get(attr);
    }

    public static KafkaTopology kafkaTopology() {
        Map<Attribute, Object> newTopologyAttrs = new HashMap<>();
        newTopologyAttrs.put(Attribute.attr("crux.node/topology"), Keyword.intern("crux.kafka/topology"));
        return new KafkaTopology(newTopologyAttrs);
    }

    protected Map<Keyword, Object> toEdn() {
        IPersistentMap ednMap = PersistentArrayMap.EMPTY;
        for (Attribute key : topologyAttrs.keySet()) {
            ednMap = ednMap.assoc(key.toEdn(), topologyAttrs.get(key));
        }
        return (PersistentArrayMap) ednMap;
    }

    public KafkaTopology withTopologyMap(Map<Attribute, ?> topologyAttrs) {
        Map<Attribute, Object> newTopologyAttrs = new HashMap<>(this.topologyAttrs);
        newTopologyAttrs.putAll(topologyAttrs);
        return new KafkaTopology(newTopologyAttrs);
    }

    private KafkaTopology with(String putAt, Object toPut) {
        Map<Attribute, Object> newTopologyAttrs = new HashMap<>(this.topologyAttrs);
        newTopologyAttrs.put(attr(putAt), toPut);
        return new KafkaTopology(newTopologyAttrs);
    }

    public KafkaTopology withKvStore(String kvStore) {
        return with("crux.node/kv-store", kvStore);
    }

    public KafkaTopology withObjectStore(String objectStore) {
        return with("crux.node/object-store", objectStore);
    }

    public KafkaTopology withDbDir(String dbDir) {
        return with("crux.kv/db-dir", dbDir);
    }

    public KafkaTopology withSync(boolean sync) {
        return with("crux.kv/sync", sync);
    }

    public KafkaTopology withCheckAndStoreIndexVersion(boolean checkAndStoreIndexVersion) {
        return with("crux.kv/check-and-store-index-version", checkAndStoreIndexVersion);
    }

    public KafkaTopology withBootstrapServers(String bootstrapServers) {
        return with("crux.kafka/bootstrap-servers", bootstrapServers);
    }

    public KafkaTopology withTxTopic(String txTopic) {
        return with("crux.kafka/tx-topic", txTopic);
    }

    public KafkaTopology withDocTopic(String docTopic) {
        return with("crux.kafka/doc-topic", docTopic);
    }

    public KafkaTopology withCreateTopics(boolean createTopics) {
        return with("crux.kafka/create-topics", createTopics);
    }

    public KafkaTopology withDocPartitions(int docPartitions) {
        return with("crux.kafka/doc-partitions", docPartitions);
    }

    public KafkaTopology withReplicationFactor(int replicationFactor) {
        return with("crux.kafka/replication-factor", replicationFactor);
    }

    public KafkaTopology withGroupId(String groupId) {
        return with("crux.kafka/group-id", groupId);
    }

    public KafkaTopology withKafkaPropertiesFile(String kafkaPropertiesFile) {
        return with("crux.kafka/kafka-properties-file", kafkaPropertiesFile);
    }
}
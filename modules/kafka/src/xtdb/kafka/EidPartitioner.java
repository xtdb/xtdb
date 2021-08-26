package xtdb.kafka;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import clojure.java.api.Clojure;
import clojure.lang.Keyword;
import clojure.lang.IFn;

/**
 * Aims to group all version of an entity into the same partition.
 * To use, add this to the producer config:
 *
 * partitioner.class=xtdb.kafka.EidPartitioner
 */
public class EidPartitioner implements Partitioner {
    private static final Keyword xtId = (Keyword) Clojure.read(":xt/id");
    private static final IFn newId;
    private static final IFn toIdBuffer;
    private static final IFn toOnHeap;

    public void configure(Map<String, ?> configs) {
    }

    static {
        Clojure.var("clojure.core/require").invoke(Clojure.read("crux.codec"));
        Clojure.var("clojure.core/require").invoke(Clojure.read("crux.memory"));
        newId = Clojure.var("crux.codec/new-id");
        toIdBuffer = Clojure.var("crux.codec/->id-buffer");
        toOnHeap = Clojure.var("crux.memory/->on-heap");
    }

    @SuppressWarnings("unchecked")
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (keyBytes == null || value == null) {
            return 0;
        }
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        return partitionForDoc((Map<Keyword,Object>) value, numPartitions);
    }

    public int partitionForDoc(Map<Keyword,Object> doc, int numPartitions) {
        Object eid = doc.get(xtId);
        byte[] eidBytes = (byte[]) toOnHeap.invoke(toIdBuffer.invoke(newId.invoke(eid)));
        return Utils.toPositive(Utils.murmur2(eidBytes)) % numPartitions;
    }

    public void close() {
    }
}

package crux.api.v2;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

import java.util.HashMap;
import java.util.Map;

import static crux.api.v2.Attribute.attr;

public class StandaloneTopology extends Topology {
    protected final Map<Attribute, Object> topologyAttrs;

    private StandaloneTopology(Map<Attribute, Object> topologyAttrs) {
        this.topologyAttrs = topologyAttrs;
    }

    public Object getObject(Attribute attr) {
        return topologyAttrs.get(attr);
    }

    public static StandaloneTopology standaloneTopology() {
        Map<Attribute, Object> newTopologyAttrs = new HashMap<>();
        newTopologyAttrs.put(attr("crux.node/topology"), Keyword.intern("crux.standalone/topology"));
        return new StandaloneTopology(newTopologyAttrs);
    }

    @SuppressWarnings("unchecked")
    protected Map<Keyword, Object> toEdn() {
        IPersistentMap ednMap = PersistentArrayMap.EMPTY;
        for (Attribute key : topologyAttrs.keySet()) {
            ednMap = ednMap.assoc(key.toEdn(), topologyAttrs.get(key));
        }
        return (PersistentArrayMap) ednMap;
    }

    public StandaloneTopology withTopologyMap(Map<Attribute, ?> topologyAttrs) {
        Map<Attribute, Object> newTopologyAttrs = new HashMap<>(this.topologyAttrs);
        newTopologyAttrs.putAll(topologyAttrs);
        return new StandaloneTopology(newTopologyAttrs);
    }

    private StandaloneTopology with(String putAt, Object toPut) {
        Map<Attribute, Object> newTopologyAttrs = new HashMap<>(this.topologyAttrs);
        newTopologyAttrs.put(attr(putAt), toPut);
        return new StandaloneTopology(newTopologyAttrs);
    }

    public StandaloneTopology withKvStore(String kvStore) {
        return with("crux.node/kv-store", kvStore);
    }

    public StandaloneTopology withObjectStore(String objectStore) {
        return with("crux.node/object-store", objectStore);
    }

    public StandaloneTopology withDbDir(String dbDir) {
        return with("crux.kv/db-dir", dbDir);
    }

    public StandaloneTopology withSync(boolean sync) {
        return with("crux.kv/sync", sync);
    }

    public StandaloneTopology withCheckAndStoreIndexVersion(boolean checkAndStoreIndexVersion) {
        return with("crux.kv/check-and-store-index-version", checkAndStoreIndexVersion);
    }

    public StandaloneTopology withEventLogKvStore(String eventLogKvStore) {
        return with("crux.standalone/event-log-kv-store", eventLogKvStore);
    }

    public StandaloneTopology withEventLogDir(String eventLogDir) {
        return with("crux.standalone/event-log-dir", eventLogDir);
    }

    public StandaloneTopology withEventLogSync(boolean eventLogSync) {
        return with("crux.standalone/event-log-sync?", eventLogSync);
    }
}

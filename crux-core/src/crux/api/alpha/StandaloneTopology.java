package crux.api.alpha;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

import java.util.HashMap;
import java.util.Map;

import static crux.api.alpha.Util.keyword;

public class StandaloneTopology extends EdnTopology {
    private static final Keyword STANDALONE_TOPOLOGY = keyword("crux.standalone/topology");
    private static final Keyword NODE_TOPOLOGY = keyword("crux.node/topology");
    private static final Keyword KV_STORE = keyword("crux.node/kv-store");
    private static final Keyword DOCUMENT_STORE = keyword("crux.node/document-store");
    private static final Keyword DB_DIR = keyword("crux.kv/db-dir");
    private static final Keyword SYNC = keyword("crux.kv/sync");
    private static final Keyword EVENT_LOG_KV_STORE = keyword("crux.standalone/event-log-kv-store");
    private static final Keyword EVENT_LOG_DIR = keyword("crux.standalone/event-log-dir");
    private static final Keyword EVENT_LOG_SYNC = keyword("crux.standalone/event-log-sync?");

    private final Map<Keyword, Object> topologyAttrs;

    private StandaloneTopology(Map<Keyword, Object> topologyAttrs) {
        this.topologyAttrs = topologyAttrs;
    }

    public Object getObject(Keyword attr) {
        return topologyAttrs.get(attr);
    }

    public static StandaloneTopology standaloneTopology() {
        Map<Keyword, Object> attrs = new HashMap<>();
        attrs.put(NODE_TOPOLOGY, STANDALONE_TOPOLOGY);
        return new StandaloneTopology(attrs);
    }

    @SuppressWarnings("unchecked")
    Map<Keyword, Object> toEdn() {
        IPersistentMap ednMap = PersistentArrayMap.EMPTY;
        for (Keyword key : topologyAttrs.keySet()) {
            ednMap = ednMap.assoc(key, topologyAttrs.get(key));
        }
        return (PersistentArrayMap) ednMap;
    }

    public StandaloneTopology with(Map<Keyword, ?> topologyAttrs) {
        Map<Keyword, Object> newTopologyAttrs = new HashMap<>(this.topologyAttrs);
        newTopologyAttrs.putAll(topologyAttrs);
        return new StandaloneTopology(newTopologyAttrs);
    }

    private StandaloneTopology with(Keyword k, Object v) {
        Map<Keyword, Object> newTopologyAttrs = new HashMap<>(this.topologyAttrs);
        newTopologyAttrs.put(k, v);
        return new StandaloneTopology(newTopologyAttrs);
    }

    public StandaloneTopology withKvStore(String kvStore) {
        return with(KV_STORE, kvStore);
    }

    public StandaloneTopology withDocumentStore(String documentStore) {
        return with(DOCUMENT_STORE, documentStore);
    }

    public StandaloneTopology withDbDir(String dbDir) {
        return with(DB_DIR, dbDir);
    }

    public StandaloneTopology withSync(boolean sync) {
        return with(SYNC, sync);
    }

    public StandaloneTopology withEventLogKvStore(String eventLogKvStore) {
        return with(EVENT_LOG_KV_STORE, eventLogKvStore);
    }

    public StandaloneTopology withEventLogDir(String eventLogDir) {
        return with(EVENT_LOG_DIR, eventLogDir);
    }

    public StandaloneTopology withEventLogSync(boolean eventLogSync) {
        return with(EVENT_LOG_SYNC, eventLogSync);
    }
}

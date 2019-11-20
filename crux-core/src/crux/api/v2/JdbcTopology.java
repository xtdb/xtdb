package crux.api.v2;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

import java.util.HashMap;
import java.util.Map;

import static crux.api.v2.Util.kw;

public class JdbcTopology extends EdnTopology {
    private static final Keyword JDBC_TOPOLOGY = kw("crux.jdbc/topology");
    private static final Keyword NODE_TOPOLOGY = kw("crux.node/topology");
    private static final Keyword JDBC_DBTYPE = kw("crux.jdbc/dbtype");
    private static final Keyword JDBC_DBNAME = kw("crux.jdbc/dbname");
    private static final Keyword KV_STORE = kw("crux.node/kv-store");
    private static final Keyword OBJECT_STORE = kw("crux.node/object-store");
    private static final Keyword DB_DIR = kw("crux.kv/db-dir");
    private static final Keyword SYNC = kw("crux.kv/sync");
    private static final Keyword CHECK_AND_STORE_INDEX_VERSION = kw("crux.kv/check-and-store-index-version");
    private static final Keyword HOST = kw("crux.jdbc/host");
    private static final Keyword USER = kw("crux.jdbc/user");
    private static final Keyword PASSWORD = kw("crux.jdbc/password");

    private final Map<Keyword, Object> topologyAttrs;

    private JdbcTopology(Map<Keyword, Object> topologyAttrs) {
        this.topologyAttrs = topologyAttrs;
    }

    public Object getObject(Keyword attr) {
        return topologyAttrs.get(attr);
    }

    public static JdbcTopology jdbcTopology(String dbType, String dbName) {
        Map<Keyword, Object> newTopologyAttrs = new HashMap<>();
        newTopologyAttrs.put(NODE_TOPOLOGY, JDBC_TOPOLOGY);
        newTopologyAttrs.put(JDBC_DBTYPE, dbType);
        newTopologyAttrs.put(JDBC_DBNAME, dbName);
        return new JdbcTopology(newTopologyAttrs);
    }

    @Override
    @SuppressWarnings("unchecked")
    Map<Keyword, Object> toEdn() {
        IPersistentMap ednMap = PersistentArrayMap.EMPTY;
        for (Keyword key : topologyAttrs.keySet()) {
            ednMap = ednMap.assoc(key, topologyAttrs.get(key));
        }
        return (PersistentArrayMap) ednMap;
    }

    public JdbcTopology withTopologyMap(Map<Keyword, ?> topologyAttrs) {
        Map<Keyword, Object> newTopologyAttrs = new HashMap<>(this.topologyAttrs);
        newTopologyAttrs.putAll(topologyAttrs);
        return new JdbcTopology(newTopologyAttrs);
    }

    private JdbcTopology with(Keyword k, Object v) {
        Map<Keyword, Object> newTopologyAttrs = new HashMap<>(this.topologyAttrs);
        newTopologyAttrs.put(k, v);
        return new JdbcTopology(newTopologyAttrs);
    }

    public JdbcTopology withKvStore(String kvStore) {
        return with(KV_STORE, kvStore);
    }

    public JdbcTopology withObjectStore(String objectStore) {
        return with(OBJECT_STORE, objectStore);
    }

    public JdbcTopology withDbDir(String dbDir) {
        return with(DB_DIR, dbDir);
    }

    public JdbcTopology withSync(boolean sync) {
        return with(SYNC, sync);
    }

    public JdbcTopology withCheckAndStoreIndexVersion(boolean checkAndStoreIndexVersion) {
        return with(CHECK_AND_STORE_INDEX_VERSION, checkAndStoreIndexVersion);
    }

    public JdbcTopology withDbType(String dbType) {
        return with(JDBC_DBTYPE, dbType);
    }

    public JdbcTopology withDbName(String dbName) {
        return with(JDBC_DBNAME, dbName);
    }

    public JdbcTopology withHost(String host) {
        return with(HOST, host);
    }

    public JdbcTopology asUser(String user) {
        return with(USER, user);
    }

    public JdbcTopology withPassword(String password) {
        return with(PASSWORD, password);
    }
}

package crux.api.v2;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

import java.util.HashMap;
import java.util.Map;

public class JdbcTopology extends EdnTopology {
    protected final Map<Keyword, Object> topologyAttrs;

    private JdbcTopology(Map<Keyword, Object> topologyAttrs) {
        this.topologyAttrs = topologyAttrs;
    }

    public Object getObject(Keyword attr) {
        return topologyAttrs.get(attr);
    }

    public static JdbcTopology jdbcTopology(String dbType, String dbName) {
        Map<Keyword, Object> newTopologyAttrs = new HashMap<>();
        newTopologyAttrs.put(Util.kw("crux.node/topology"), Keyword.intern("crux.jdbc/topology"));
        newTopologyAttrs.put(Util.kw("crux.jdbc/dbtype"), dbType);
        newTopologyAttrs.put(Util.kw("crux.jdbc/dbname"), dbName);
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

    private JdbcTopology with(String putAt, Object toPut) {
        Map<Keyword, Object> newTopologyAttrs = new HashMap<>(this.topologyAttrs);
        newTopologyAttrs.put(Util.kw(putAt), toPut);
        return new JdbcTopology(newTopologyAttrs);
    }

    public JdbcTopology withKvStore(String kvStore) {
        return with("crux.node/kv-store", kvStore);
    }

    public JdbcTopology withObjectStore(String objectStore) {
        return with("crux.node/object-store", objectStore);
    }

    public JdbcTopology withDbDir(String dbDir) {
        return with("crux.kv/db-dir", dbDir);
    }

    public JdbcTopology withSync(boolean sync) {
        return with("crux.kv/sync", sync);
    }

    public JdbcTopology withCheckAndStoreIndexVersion(boolean checkAndStoreIndexVersion) {
        return with("crux.kv/check-and-store-index-version", checkAndStoreIndexVersion);
    }

    public JdbcTopology withDbType(String dbType) {
        return with("crux.jdbc/dbtype", dbType);
    }

    public JdbcTopology withDbName(String dbName) {
        return with("crux.jdbc/dbname", dbName);
    }

    public JdbcTopology withHost(String host) {
        return with("crux.jdbc/host", host);
    }

    public JdbcTopology asUser(String user) {
        return with("crux.jdbc/user", user);
    }

    public JdbcTopology withPassword(String password) {
        return with("crux.jdbc/password", password);
    }
}

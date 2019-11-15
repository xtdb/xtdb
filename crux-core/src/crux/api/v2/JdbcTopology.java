package crux.api.v2;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

import java.util.HashMap;
import java.util.Map;

import static crux.api.v2.Attribute.attr;

public class JdbcTopology extends Topology {
    protected final Map<Attribute, Object> topologyAttrs;

    private JdbcTopology(Map<Attribute, Object> topologyAttrs) {
        this.topologyAttrs = topologyAttrs;
    }

    public Object getObject(Attribute attr) {
        return topologyAttrs.get(attr);
    }

    public static JdbcTopology jdbcTopology(String dbType, String dbName) {
        Map<Attribute, Object> newTopologyAttrs = new HashMap<>();
        newTopologyAttrs.put(Attribute.attr("crux.node/topology"), Keyword.intern("crux.jdbc/topology"));
        newTopologyAttrs.put(Attribute.attr("crux.jdbc/dbtype"), dbType);
        newTopologyAttrs.put(Attribute.attr("crux.jdbc/dbname"), dbName);
        return new JdbcTopology(newTopologyAttrs);
    }

    protected Map<Keyword, Object> toEdn() {
        IPersistentMap ednMap = PersistentArrayMap.EMPTY;
        for (Attribute key : topologyAttrs.keySet()) {
            ednMap = ednMap.assoc(key.toEdn(), topologyAttrs.get(key));
        }
        return (PersistentArrayMap) ednMap;
    }

    public JdbcTopology withTopologyMap(Map<Attribute, ?> topologyAttrs) {
        Map<Attribute, Object> newTopologyAttrs = new HashMap<>(this.topologyAttrs);
        newTopologyAttrs.putAll(topologyAttrs);
        return new JdbcTopology(newTopologyAttrs);
    }

    private JdbcTopology with(String putAt, Object toPut) {
        Map<Attribute, Object> newTopologyAttrs = new HashMap<>(this.topologyAttrs);
        newTopologyAttrs.put(attr(putAt), toPut);
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

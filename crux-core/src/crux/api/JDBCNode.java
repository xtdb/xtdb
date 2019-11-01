package crux.api;

import clojure.lang.Keyword;
import java.lang.Boolean;
import java.lang.Object;
import java.lang.String;
import java.util.HashMap;
import java.util.Map;

public class JDBCNode {
  public static final Keyword DBTYPE = Keyword.intern("crux.jdbc/dbtype");

  public static final Keyword DBNAME = Keyword.intern("crux.jdbc/dbname");

  public static final Keyword KV_STORE = Keyword.intern("crux.node/kv-store");

  public static final Keyword OBJECT_STORE = Keyword.intern("crux.node/object-store");

  public static final Keyword INDEXER = Keyword.intern("crux.node/indexer");

  public static final Keyword DB_DIR = Keyword.intern("crux.kv/db-dir");

  public static final String DB_DIR_DEFAULT = "data";

  public static final Keyword SYNC = Keyword.intern("crux.kv/sync?");

  public static final Boolean SYNC_DEFAULT = false;

  public static final Keyword CHECK_AND_STORE_INDEX_VERSION = Keyword.intern("crux.kv/check-and-store-index-version");

  public static final Boolean CHECK_AND_STORE_INDEX_VERSION_DEFAULT = true;

  public static class Builder {
    public Map<Keyword, Object> JDBCNodeProperties;

    public Builder() {
      JDBCNodeProperties = new HashMap<Keyword, Object>();
      JDBCNodeProperties.put(Keyword.intern("crux.node/topology"),Keyword.intern("crux.jdbc/topology"));
    }

    public Builder withDbtype(String val) {
      JDBCNodeProperties.put(DBTYPE, val);
      return this;
    }

    public Builder withDbname(String val) {
      JDBCNodeProperties.put(DBNAME, val);
      return this;
    }

    public Builder withKvStore(String val) {
      JDBCNodeProperties.put(KV_STORE, val);
      return this;
    }

    public Builder withObjectStore(String val) {
      JDBCNodeProperties.put(OBJECT_STORE, val);
      return this;
    }

    public Builder withIndexer(String val) {
      JDBCNodeProperties.put(INDEXER, val);
      return this;
    }

    public Builder withDbDir(String val) {
      JDBCNodeProperties.put(DB_DIR, val);
      return this;
    }

    public Builder withSync(Boolean val) {
      JDBCNodeProperties.put(SYNC, val);
      return this;
    }

    public Builder withCheckAndStoreIndexVersion(Boolean val) {
      JDBCNodeProperties.put(CHECK_AND_STORE_INDEX_VERSION, val);
      return this;
    }

    public Map<Keyword, Object> build() {
      return JDBCNodeProperties;
    }
  }
}

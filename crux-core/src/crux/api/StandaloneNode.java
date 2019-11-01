package crux.api;

import clojure.lang.Keyword;
import java.lang.Boolean;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.util.HashMap;
import java.util.Map;

public class StandaloneNode {
  public static final Keyword DB_DIR = Keyword.intern("crux.kv/db-dir");

  public static final String DB_DIR_DEFAULT = "data";

  public static final Keyword EVENT_LOG_SYNC = Keyword.intern("crux.standalone/event-log-sync?");

  public static final Boolean EVENT_LOG_SYNC_DEFAULT = false;

  public static final Keyword KV_STORE = Keyword.intern("crux.node/kv-store");

  public static final Keyword EVENT_LOG_DIR = Keyword.intern("crux.standalone/event-log-dir");

  public static final Keyword SYNC = Keyword.intern("crux.kv/sync?");

  public static final Boolean SYNC_DEFAULT = false;

  public static final Keyword EVENT_LOG_SYNC_INTERVAL_MS = Keyword.intern("crux.standalone/event-log-sync-interval-ms");

  public static final Keyword EVENT_LOG_KV_STORE = Keyword.intern("crux.standalone/event-log-kv-store");

  public static final String EVENT_LOG_KV_STORE_DEFAULT = "crux.kv.rocksdb/kv";

  public static final Keyword CHECK_AND_STORE_INDEX_VERSION = Keyword.intern("crux.kv/check-and-store-index-version");

  public static final Boolean CHECK_AND_STORE_INDEX_VERSION_DEFAULT = true;

  public static final Keyword INDEXER = Keyword.intern("crux.node/indexer");

  public static final Keyword OBJECT_STORE = Keyword.intern("crux.node/object-store");

  public static class Builder {
    public Map<Keyword, Object> StandaloneNodeProperties;

    public Builder() {
      StandaloneNodeProperties = new HashMap<Keyword, Object>();
      StandaloneNodeProperties.put(Keyword.intern("crux.node/topology"),Keyword.intern("crux.standalone/topology"));
    }

    public Builder withDbDir(String val) {
      StandaloneNodeProperties.put(DB_DIR, val);
      return this;
    }

    public Builder withEventLogSync(Boolean val) {
      StandaloneNodeProperties.put(EVENT_LOG_SYNC, val);
      return this;
    }

    public Builder withKvStore(String val) {
      StandaloneNodeProperties.put(KV_STORE, val);
      return this;
    }

    public Builder withEventLogDir(String val) {
      StandaloneNodeProperties.put(EVENT_LOG_DIR, val);
      return this;
    }

    public Builder withSync(Boolean val) {
      StandaloneNodeProperties.put(SYNC, val);
      return this;
    }

    public Builder withEventLogSyncIntervalMs(Long val) {
      StandaloneNodeProperties.put(EVENT_LOG_SYNC_INTERVAL_MS, val);
      return this;
    }

    public Builder withEventLogKvStore(String val) {
      StandaloneNodeProperties.put(EVENT_LOG_KV_STORE, val);
      return this;
    }

    public Builder withCheckAndStoreIndexVersion(Boolean val) {
      StandaloneNodeProperties.put(CHECK_AND_STORE_INDEX_VERSION, val);
      return this;
    }

    public Builder withIndexer(String val) {
      StandaloneNodeProperties.put(INDEXER, val);
      return this;
    }

    public Builder withObjectStore(String val) {
      StandaloneNodeProperties.put(OBJECT_STORE, val);
      return this;
    }

    public Map<Keyword, Object> build() {
      return StandaloneNodeProperties;
    }
  }
}

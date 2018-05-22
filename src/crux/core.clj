(ns crux.core
  (:require [crux.kv :as kv]
            [crux.rocksdb :as rocksdb]
            [crux.db])
  (:import [java.util Date]))

(defrecord KvDatasource [kv ts attributes]
  crux.db/Datasource
  (entities [this]
    (kv/entity-ids kv))

  (entities-for-attribute-value [this ident v]
    (kv/entity-ids-for-value kv ident v ts))

  (attr-val [this eid ident]
    (kv/-get-at kv eid ident ts)))

(defrecord KvIndexer [kv]
  crux.db/Indexer
  (index [_ txs transact-time]
    (kv/-put kv txs transact-time))

  (store-index-meta [_ k v]
    (kv/store-meta kv k v))

  (read-index-meta [_ k]
    (kv/read-meta kv k)))

(defn kv
  "Open a connection to the underlying KV data-store."
  ([db-dir]
   (kv db-dir {}))
  ([db-dir {:keys [kv-store] :as opts}]
   (merge (or kv-store (rocksdb/map->CruxRocksKv {}))
          {:db-dir db-dir :state (atom {})})))

(defn as-of [kv ts]
  (map->KvDatasource {:kv kv :ts ts}))

(defn db [kv]
  (as-of kv (Date.)))

(defn indexer [kv]
  (map->KvIndexer {:kv kv}))

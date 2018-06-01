(ns crux.core
  (:require [crux.kv :as kv]
            [crux.rocksdb :as rocksdb]
            [crux.db])
  (:import [java.util Date]))

(defrecord KvEntity [kv eid ts]
  crux.db/Entity
  (attr-val [this ident]
    (kv/seek-first kv eid ident ts nil))
  (->id [this]
    (kv/attr-aid->ident kv eid)))

(defrecord KvDatasource [kv ts attributes]
  crux.db/Datasource
  (entities [this]
    (map (fn [eid] (KvEntity. kv eid ts))
         (kv/entity-ids kv)))

  (entities-for-attribute-value [this ident min-v max-v]
    (map (fn [eid] (KvEntity. kv eid ts))
         (kv/entity-ids-for-range-value kv ident min-v max-v ts))))

(defrecord KvIndexer [kv]
  crux.db/Indexer
  (index-docs [_ docs]
    (throw (UnsupportedOperationException.)))

  (index-tx [_ tx-ops transact-time tx-id]
    (kv/-put kv tx-ops transact-time))

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

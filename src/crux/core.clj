(ns crux.core
  (:require [crux.kv :as kv]
            [crux.rocksdb :as rocksdb]
            [crux.db])
  (:import [java.util Date]))

(defrecord KvEntity [kv eid ts fields]
  crux.db/Entity
  (attr-val [this ident]
    (let [v  (or (@fields ident)
                 (get (swap! fields assoc ident (kv/seek-first kv eid ident ts nil)) ident))]
      v))
  (->id [this]
    (kv/attr-aid->ident kv eid))
  (eq? [this another]
    (= (:eid this) (:eid another))))

(defrecord KvDatasource [kv ts attributes]
  crux.db/Datasource
  (entities [this]
    (map (fn [eid] (KvEntity. kv eid ts (atom {})))
         (kv/entity-ids kv)))

  (entities-for-attribute-value [this ident min-v max-v]
    (map (fn [eid] (KvEntity. kv eid ts (atom {})))
         (kv/entity-ids-for-range-value kv ident min-v max-v ts))))

(defrecord KvIndexer [kv]
  crux.db/Indexer
  (index-doc [_ content-hash doc]
    (throw (UnsupportedOperationException.)))

  (index-tx [_ tx-ops tx-time tx-id]
    (kv/-put kv tx-ops tx-time))

  (store-index-meta [_ k v]
    (kv/store-meta kv k v))

  (read-index-meta [_ k]
    (kv/read-meta kv k)))

(defn kv
  "Open a connection to the underlying KV data-store."
  ([db-dir]
   (kv db-dir {}))
  ([db-dir {:keys [kv-store] :as opts}]
   (merge (or kv-store (rocksdb/map->RocksKv {}))
          {:db-dir db-dir :state (atom {})})))

(defn as-of [kv ts]
  (map->KvDatasource {:kv kv :ts ts}))

(defn db [kv]
  (as-of kv (Date.)))

(defn indexer [kv]
  (map->KvIndexer {:kv kv}))

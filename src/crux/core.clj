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

(defn kv
  "Open a connection to the underlying KV data-store."
  [db-name]
  (rocksdb/map->CruxRocksKv {:db-name db-name :attributes (atom {})}))

(defn as-of [kv ts]
  (map->KvDatasource {:kv kv :ts ts}))

(defn db [kv]
  (as-of kv (Date.)))

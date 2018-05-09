(ns crux.core
  (:require [crux.kv :as kv]
            [crux.rocksdb :as rocksdb])
  (:import [java.util Date]))

(defn kv
  "Open a connection to the underlying KV data-store."
  [db-name]
  (rocksdb/map->CruxRocksKv {:db-name db-name}))

(defn as-of [kv ts]
  (let [attributes (kv/attributes kv)]
    (kv/map->KvDatasource {:kv kv :ts ts :attributes attributes})))

(defn db [kv]
  (as-of kv (Date.)))

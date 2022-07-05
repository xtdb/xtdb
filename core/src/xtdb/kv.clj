(ns ^:no-doc xtdb.kv
  "Protocols for KV backend implementations."
  (:refer-clojure :exclude [next])
  (:require [xtdb.io :as xio]
            [xtdb.status :as status]
            [xtdb.system :as sys]))

(defprotocol KvIterator
  (seek [this k])
  (next [this])
  (prev [this])
  (value [this]))

(defprotocol KvSnapshot
  (new-iterator ^java.io.Closeable [this])
  (get-value [this k]))

(defprotocol KvStoreTx
  (new-tx-snapshot ^java.io.Closeable [this])
  (abort-kv-tx [this])
  (commit-kv-tx [this])
  (put-kv [this k v]))

;; tag::KvStore[]
(defprotocol KvStore
  (new-snapshot ^java.io.Closeable [this])
  (store [this kvs])
  (fsync [this])
  (compact [this])
  (count-keys [this])
  (db-dir [this])
  (kv-name [this]))
;; end::KvStore[]

(defprotocol KvStoreWithReadTransaction
  (begin-kv-tx ^java.io.Closeable [this]))

(def args
  {:db-dir {:doc "Directory to store K/V files"
            :required? false
            :spec ::sys/path}
   :sync? {:doc "Sync the KV store to disk after every write."
           :default false
           :spec ::sys/boolean}})

(extend-protocol status/Status
  xtdb.kv.KvStore
  (status-map [this]
    {:xtdb.kv/kv-store (kv-name this)
     :xtdb.kv/estimate-num-keys (count-keys this)
     :xtdb.kv/size (some-> (db-dir this) (xio/folder-size))}))

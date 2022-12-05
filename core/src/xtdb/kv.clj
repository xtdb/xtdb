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

;; interface rather than protocol because made optional via fast instance? checks.
(definterface KvSnapshotPrefixSupport
  (newPrefixSeekOptimisedIterator []))

(defn new-prefix-seek-optimised-iterator
  "Opens a new iterator, allowing for prefix-only search optimisations (if they are possible).

  Callers must be ok for your iterator to only see keys sharing the prefix supplied as the `k` arg to `(seek iterator k)`.

  WARNING: Depending on the capabilities of the KV store implementation you may or may not see keys outside the prefix, do not depend on either."
  [kv-snapshot]
  (if (instance? KvSnapshotPrefixSupport kv-snapshot)
    (.newPrefixSeekOptimisedIterator ^KvSnapshotPrefixSupport kv-snapshot)
    (new-iterator kv-snapshot)))

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

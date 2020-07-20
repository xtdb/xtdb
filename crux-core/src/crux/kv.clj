(ns ^:no-doc crux.kv
  "Protocols for KV backend implementations."
  (:require [crux.io :as cio]
            [crux.status :as status]
            [crux.system :as sys])
  (:refer-clojure :exclude [next])
  (:import java.io.Closeable))

(defprotocol KvIterator
  (seek [this k])
  (next [this])
  (prev [this])
  (value [this]))

(defprotocol KvSnapshot
  (new-iterator ^java.io.Closeable [this])
  (get-value [this k]))

;; tag::KvStore[]
(defprotocol KvStore
  (new-snapshot ^java.io.Closeable [this])
  (store [this kvs])
  (delete [this ks])
  (fsync [this])
  (compact [this])
  (count-keys [this])
  (db-dir [this])
  (kv-name [this]))
;; end::KvStore[]

(def args
  {:db-dir {:doc "Directory to store K/V files"
            :required? false
            :spec ::sys/path}
   :sync? {:doc "Sync the KV store to disk after every write."
           :default false
           :spec ::sys/boolean}})

(extend-protocol status/Status
  crux.kv.KvStore
  (status-map [this]
    {:crux.kv/kv-store (kv-name this)
     :crux.kv/estimate-num-keys (count-keys this)
     :crux.kv/size (some-> (db-dir this) (cio/folder-size))}))

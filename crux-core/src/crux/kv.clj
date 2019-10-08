(ns crux.kv
  "Protocols for KV backend implementations."
  (:require [clojure.spec.alpha :as s]
            [crux.io :as cio]
            [crux.status :as status])
  (:refer-clojure :exclude [next])
  (:import java.io.Closeable
           clojure.lang.IRecord))

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
  (open ^crux.kv.KvStore [this options])
  (new-snapshot ^java.io.Closeable [this])
  (store [this kvs])
  (delete [this ks])
  (fsync [this])
  (backup [this dir])
  (count-keys [this])
  (db-dir [this])
  (kv-name [this]))
;; end::KvStore[]

(s/def ::db-dir string?)
(s/def ::sync? boolean?)
(s/def ::check-and-store-index-version boolean?)

(s/def ::options (s/keys :req [::db-dir]
                         :opt [::sync?
                               ::check-and-store-index-version]))

(def options
  {:crux.kv/db-dir
   {:doc "Directory to store K/V files"
    :default "data"}
   :crux.kv/sync?
   {:doc "Sync the KV store to disk after every write."
    :default false}
   :crux.kv/check-and-store-index-version
   {:doc "Check and store index version upon start"
    :default true}})

(extend-protocol status/Status
  crux.kv.KvStore
  (status-map [this]
    {:crux.kv/kv-backend (kv-name this)
     :crux.kv/estimate-num-keys (count-keys this)
     :crux.kv/size (some-> (db-dir this) (cio/folder-size))}))

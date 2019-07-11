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
(s/def ::kv-backend string?)
(s/def ::sync? boolean?)
(s/def :crux.index/check-and-store-index-version boolean?)

(s/def ::options (s/keys :req-un [::db-dir ::kv-backend]
                         :opt-un [::sync?]
                         :opt [:crux.index/check-and-store-index-version]))

(defn require-and-ensure-kv-record ^Class [record-class-name]
  (cio/require-and-ensure-record @#'crux.kv/KvStore record-class-name))

(defn new-kv-store ^java.io.Closeable [kv-backend]
  (->> (require-and-ensure-kv-record kv-backend)
       (cio/new-record)))

(extend-protocol status/Status
  crux.kv.KvStore
  (status-map [this]
    {:crux.kv/kv-backend (kv-name this)
     :crux.kv/estimate-num-keys (count-keys this)
     :crux.kv/size (some-> (db-dir this) (cio/folder-size))}))

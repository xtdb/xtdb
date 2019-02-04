(ns crux.kv
  "Protocols for KV backend implementations."
  (:require [clojure.spec.alpha :as s]
            [crux.io :as cio])
  (:refer-clojure :exclude [next])
  (:import java.io.Closeable
           clojure.lang.IRecord))

(defprotocol KvIterator
  (seek [this k])
  (next [this])
  (prev [this])
  (value [this])
  (refresh [this]))

(defprotocol KvSnapshot
  (new-iterator ^java.io.Closeable [this]))

;; tag::KvStore[]
(defprotocol KvStore
  (open ^crux.kv.KvStore [this options])
  (new-snapshot ^java.io.Closeable [this])
  (store [this kvs])
  (delete [this ks])
  (backup [this dir])
  (count-keys [this])
  (db-dir [this])
  (kv-name [this]))
;; end::KvStore[]

(s/def ::db-dir string?)
(s/def ::sync? boolean?)

(defn require-and-ensure-kv-record ^Class [record-class-name]
  (let [[_ record-ns] (re-find #"(.+)(:?\..+)" record-class-name)]
    (require (symbol record-ns))
    (let [record-class ^Class (eval (symbol record-class-name))]
      (when (and (extends? (eval 'crux.kv/KvStore) record-class)
                 (.isAssignableFrom ^Class IRecord record-class))
        record-class))))

(defn new-kv-store ^java.io.Closeable [kv-backend]
  (let [kv-record-class (require-and-ensure-kv-record kv-backend)]
    (.invoke (.getMethod kv-record-class "create"
                         (into-array [clojure.lang.IPersistentMap]))
             nil (object-array [{}]))))

(defn kv-status [kv]
  {:crux.kv/kv-backend (kv-name kv)
   :crux.kv/estimate-num-keys (count-keys kv)
   :crux.kv/size (some-> (db-dir kv) (cio/folder-size))})

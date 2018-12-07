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
  (value [this])
  (refresh [this]))

(defprotocol KvSnapshot
  (new-iterator ^java.io.Closeable [this]))

(defprotocol KvStore
  (open ^crux.kv.KvStore [this options])
  (new-snapshot ^java.io.Closeable [this])
  (store [this kvs])
  (delete [this ks])
  (backup [this dir])
  (count-keys [this])
  (db-dir [this])
  (kv-name [this]))

(defn require-and-ensure-kv-record ^Class [record-class-name]
  (let [[_ record-ns] (re-find #"(.+)(:?\..+)" record-class-name)]
    (require (symbol record-ns))
    (let [record-class ^Class (resolve (symbol record-class-name))]
      (when (and (extends? KvStore record-class)
                 (.isAssignableFrom ^Class IRecord record-class))
        record-class))))

(s/def ::db-dir string?)
(s/def ::kv-backend #'require-and-ensure-kv-record)

(s/def ::options (s/keys :req-un [::kv-backend]
                         :opt-un [::db-dir]))

(defn new-kv-store ^java.io.Closeable [{:keys [db-dir
                                               kv-backend]
                                        :as options}]
  (s/assert ::options options)
  (let [kv-record-class (require-and-ensure-kv-record kv-backend)]
    (.invoke (.getMethod kv-record-class "create"
                         (into-array [clojure.lang.IPersistentMap]))
             nil (object-array [{}]))))

(defn kv-status [kv]
  {:crux.kv/kv-backend (kv-name kv)
   :crux.kv/estimate-num-keys (count-keys kv)
   :crux.kv/size (some-> (db-dir kv) (cio/folder-size))})

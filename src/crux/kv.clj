(ns crux.kv
  "Protocols for KV backend implementations."
  (:require [clojure.spec.alpha :as s])
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
(s/def ::kv-backend require-and-ensure-kv-record)

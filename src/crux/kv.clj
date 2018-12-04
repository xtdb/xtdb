(ns crux.kv
  "Protocols for KV backend implementations."
  (:refer-clojure :exclude [next])
  (:import java.io.Closeable))

(defprotocol KvIterator
  (seek [this k])
  (next [this])
  (value [this])
  (refresh [this]))

(defprotocol KvSnapshot
  (new-iterator ^java.io.Closeable [this]))

(defprotocol KvStore
  (open ^crux.kv.KvStore [this])
  (new-snapshot ^java.io.Closeable [this])
  (store [this kvs])
  (delete [this ks])
  (backup [this dir])
  (count-keys [this])
  (db-dir [this])
  (kv-name [this]))

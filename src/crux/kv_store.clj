(ns crux.kv-store
  (:refer-clojure :exclude [next])
  (:import [java.io Closeable]))

(defprotocol KvIterator
  (seek [this k])
  (next [this])
  (value [this]))

(defprotocol KvSnapshot
  (^java.io.Closeable new-iterator [this]))

(defprotocol KvStore
  (open [this])
  (^java.io.Closeable new-snapshot [this])
  (store [this kvs])
  (delete [this ks])
  (backup [this dir])
  (count-keys [this]))

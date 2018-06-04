(ns crux.kv-store)

(defprotocol KvIterator
  (-seek [this k])
  (-next [this]))

(defprotocol KvSnapshot
  (^Closeable new-iterator [this])
  (iterate-with [this f]))

(defprotocol KvStore
  (open [this])
  (^Closeable new-snapshot [this])
  (store [this kvs])
  (delete [this ks])
  (backup [this dir]))

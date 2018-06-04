(ns crux.kv-store)

(defprotocol KvIterator
  (-seek [this k])
  (-next [this]))

(defprotocol KvSnapshot
  (iterate-with [this f]))

(defprotocol KvStore
  (open [this])
  (new-snapshot [this])
  (store [this kvs])
  (delete [this ks])
  (backup [this dir]))

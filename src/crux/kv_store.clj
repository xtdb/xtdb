(ns crux.kv-store)

(defprotocol KvIterator
  (-seek [this k])
  (-next [this]))

(defprotocol CruxKvStore
  (open [this])
  (iterate-with [this f])
  (store [this kvs])
  (close [this])
  (backup [this dir]))

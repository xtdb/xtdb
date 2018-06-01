(ns crux.kv-store)

(defprotocol KvIterator
  (-seek [this k])
  (-next [this]))

(defprotocol CruxKvStore
  (open [this])
  (iterate-with [this f])
  (store [this kvs])
  (delete [this ks])
  (backup [this dir]))

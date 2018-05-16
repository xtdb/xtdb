(ns crux.kv-store)

(defprotocol KvIterator
  (-seek [this k])
  (-next [this]))

(defprotocol CruxKvStore
  (open [this])

  (iterate-with [this f])

  (store [this k v])

  (store-all! [this kvs])

  (close [this])

  (destroy [this])

  (backup [this dir]))

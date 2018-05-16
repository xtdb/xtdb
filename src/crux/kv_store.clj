(ns crux.kv-store)

(defprotocol CruxKvStore
  (open [this])

  (value [this k])

  (seek [this k])

  (seek-first [this prefix-pred key-pred k])

  (seek-and-iterate [this key-pred k])

  (store [this k v])

  (store-all! [this kvs])

  (close [this])

  (destroy [this])

  (backup [this dir]))

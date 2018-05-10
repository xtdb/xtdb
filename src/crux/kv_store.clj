(ns crux.kv-store)

(defprotocol CruxKvStore
  (open [this])

  (seek [db k])

  (seek-and-iterate [this k upper-bound])

  (seek-and-iterate-bounded [this k])

  (store [this k v])

  (merge! [this k v])

  (close [this])

  (destroy [this]))

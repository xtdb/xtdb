(ns crux.kv)

(defprotocol CruxKv
  (open [this])

  (seek [db k])

  (seek-and-iterate [this k upper-bound])

  (store [this k v])

  (merge! [this k v])

  (close [this])

  (destroy [this]))

(ns xtdb.grpc-server.utils)

(defn assoc-some
  "Associates a key with a value in a map, if and only if the value is not nil."
  ([m k v]
   (if (nil? v) m (assoc m k {:option {:some v}}))))

(defn nil->default
  "Associates a key with a value in a map, if and only if the value is not nil."
  ([m k v default]
   (if (nil? v) (assoc m k default) (assoc m k v))))
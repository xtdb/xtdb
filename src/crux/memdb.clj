(ns crux.memdb
  (:require [crux.byte-utils :as bu]
            [crux.kv-store :as ks])
  (:import [java.util TreeMap]
           [java.util.function BiFunction]))

(defrecord CruxMemKv [db-name ^TreeMap db]
  ks/CruxKvStore
  (open [this]
    (assoc this :db (TreeMap. bu/bytes-comparator)))

  (seek [_ k]
    (first (.tailMap db k)))

  (value [_ k]
    (get db k))

  (seek-and-iterate [_ k upper-bound]
    (for [[k2 v] (.subMap db k upper-bound)]
      [k2 v]))

  (seek-and-iterate-bounded [_ k]
    (for [[^bytes k2 v] (.tailMap db k)
          :while (zero? (bu/compare-bytes k k2 (count k)))]
      [k2 v]))

  (store [_ k v]
    (.put db k v))

  (merge! [_ k v]
    (.compute db k (reify BiFunction
                     (apply [_ k old-value]
                       (if old-value
                         (bu/long->bytes (+ (bu/bytes->long old-value)
                                            (bu/bytes->long v)))
                         v)))))

  (close [_])

  (destroy [this]
    (dissoc this :db)))

(defn crux-mem-kv [db-name]
  (map->CruxMemKv {:db-name db-name}))

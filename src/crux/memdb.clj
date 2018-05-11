(ns crux.memdb
  (:require [crux.byte-utils :as bu]
            [crux.kv-store :as ks])
  (:import [java.util SortedMap TreeMap]
           [java.util.function BiFunction]))

(defrecord CruxMemKv [^SortedMap db]
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
    (locking db
      (.merge db k v (reify BiFunction
                       (apply [_ old-value new-value]
                         (bu/long->bytes (+ (bu/bytes->long old-value)
                                            (bu/bytes->long new-value))))))))

  (close [_])

  (destroy [this]
    (.clear db)
    (dissoc this :db)))

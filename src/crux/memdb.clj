(ns crux.memdb
  (:require [crux.byte-utils :as bu]
            [crux.kv-store :as ks])
  (:import [java.util TreeMap]
           [java.util.function BiFunction]))

(defrecord CruxMemKv [db-name db]
  ks/CruxKv
  (open [this]
    (assoc this :db (TreeMap. bu/bytes-comparator)))

  (seek [{:keys [db]} k]
    (get db k))

  (seek-and-iterate [{:keys [^TreeMap db]} k upper-bound]
    (for [[k2 v] (.subMap db k upper-bound)]
      [k2 v]))

  (seek-and-iterate-bounded [{:keys [^TreeMap db]} k]
    (for [[^bytes k2 v] (.tailMap db k)
          :while (zero? (bu/compare-bytes k k2 (count k)))]
      [k2 v]))

  (store [{:keys [^TreeMap db]} k v]
    (.put db k v))

  (merge! [{:keys [^TreeMap db]} k v]
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

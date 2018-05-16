(ns crux.memdb
  (:require [crux.byte-utils :as bu]
            [crux.kv-store :as ks])
  (:import java.io.Closeable
           [java.util SortedMap TreeMap]))

(defn- atom-cursor->next! [cursor]
  (let [[^bytes k2 v :as kv] (first @cursor)]
    (swap! cursor rest)
    kv))

(defrecord CruxMemKv [^SortedMap db]
  ks/CruxKvStore
  (open [this]
    (assoc this :db (TreeMap. bu/bytes-comparator)))

  (iterate-with [_ f]
    (f
     (let [c (atom nil)]
       (reify
         ks/KvIterator
         (ks/-seek [this k]
           (reset! c (.tailMap db k))
           (atom-cursor->next! c))
         (ks/-next [this]
           (atom-cursor->next! c))))))

  (store [_ k v]
    (.put db k v))

  (store-all! [_ kvs]
    (locking db
      (doseq [[k v] kvs]
        (.put db k v))))

  (destroy [this]
    (.clear db)
    (dissoc this :db))

  (backup [_ dir]
    (throw (UnsupportedOperationException.)))

  Closeable
  (close [_]))

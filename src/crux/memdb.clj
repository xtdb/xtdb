(ns crux.memdb
  (:require [crux.byte-utils :as bu]
            [crux.kv-store :as ks]
            [clojure.java.io :as io]
            [taoensso.nippy :as nippy])
  (:import java.io.Closeable))

(defn- atom-cursor->next! [cursor]
  (let [[k v :as kv] (first @cursor)]
    (swap! cursor rest)
    (when kv
      [k v])))

(defn- persist-db [dir db]
  (let [file (io/file dir)]
    (.mkdirs file)
    (nippy/freeze-to-file (io/file file "memdb") @db)))

(defn- restore-db [dir]
  (->> (nippy/thaw-from-file (io/file dir "memdb"))
       (into (sorted-map-by bu/bytes-comparator))))

(def ^:dynamic ^:private *current-iterator* nil)

(defrecord MemKv [db db-dir persist-on-close?]
  ks/KvStore
  (open [this]
    (if (.isFile (io/file db-dir "memdb"))
      (assoc this :db (atom (restore-db db-dir)))
      (assoc this :db (atom (sorted-map-by bu/bytes-comparator)))))

  (iterate-with [_ f]
    (if *current-iterator*
      (f *current-iterator*)
      (let [c (atom nil)
            i (reify
                ks/KvIterator
                (ks/-seek [this k]
                  (reset! c (subseq @db >= k))
                  (atom-cursor->next! c))
                (ks/-next [this]
                  (atom-cursor->next! c)))]
       (binding [*current-iterator* i]
         (f i)))))

  (store [_ kvs]
    (swap! db merge (into {} kvs)))

  (delete [_ ks]
    (swap! db #(apply dissoc % ks)))

  (backup [_ dir]
    (let [file (io/file dir)]
      (when (.exists file)
        (throw (IllegalArgumentException. (str "Directory exists: " (.getAbsolutePath file)))))
      (persist-db dir db)))

  Closeable
  (close [_]
    (when (and db-dir persist-on-close?)
      (persist-db db-dir db))))

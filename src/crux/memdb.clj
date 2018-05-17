(ns crux.memdb
  (:require [crux.byte-utils :as bu]
            [crux.kv-store :as ks]
            [clojure.java.io :as io]
            [taoensso.nippy :as nippy])
  (:import java.io.Closeable
           [java.util SortedMap TreeMap]))

(defn- atom-cursor->next! [cursor]
  (let [[^bytes k2 v :as kv] (first @cursor)]
    (swap! cursor rest)
    kv))

(defn- persist-db [db dir]
  (let [file (io/file dir)]
    (.mkdirs file)
    (nippy/freeze-to-file (io/file file "memdb") (into {} db))))

(defn- restore-db [dir]
  (doto (TreeMap. bu/bytes-comparator)
    (.putAll (nippy/thaw-from-file (io/file dir "memdb")))))

(defrecord CruxMemKv [^SortedMap db db-dir persist-on-close?]
  ks/CruxKvStore
  (open [this]
    (if (.isFile (io/file db-dir "memdb"))
      (assoc this :db (restore-db db-dir))
      (assoc this :db (TreeMap. bu/bytes-comparator))))

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
    (let [file (io/file dir)]
      (when (.exists file)
        (throw (IllegalArgumentException. (str "Directory exists: " (.getAbsolutePath file)))))
      (persist-db db dir)))

  Closeable
  (close [_]
    (when (and db-dir persist-on-close?)
      (persist-db db db-dir))))

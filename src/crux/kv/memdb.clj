(ns crux.kv.memdb
  "In-memory KV backend for Crux."
  (:require [clojure.spec.alpha :as s]
            [clojure.java.io :as io]
            [crux.byte-utils :as bu]
            [crux.kv :as kv]
            [taoensso.nippy :as nippy])
  (:import java.io.Closeable
           clojure.lang.Box))

(defn- persist-db [dir db]
  (let [file (io/file dir)]
    (.mkdirs file)
    (->> (into {} @db)
         (nippy/freeze-to-file (io/file file "memdb")))))

(defn- restore-db [dir]
  (->> (nippy/thaw-from-file (io/file dir "memdb"))
       (into (sorted-map-by bu/bytes-comparator))))

;; NOTE: Using Box here to hide the db from equals/hashCode, otherwise
;; unusable in practice.
(defrecord MemKvIterator [^Box db cursor]
  kv/KvIterator
  (kv/seek [this k]
    (let [[x & xs] (subseq (.val db) >= k)]
      (some->> (reset! cursor {:first x :rest xs})
               :first
               (key))))
  (kv/next [this]
    (some->> (swap! cursor (fn [{[x & xs] :rest}]
                             {:first x :rest xs}))
             :first
             (key)))
  (kv/value [this]
    (some->> @cursor
             :first
             (val)))

  (kv/refresh [this]
    this)

  Closeable
  (close [_]))

(defrecord MemKvSnapshot [db]
  kv/KvSnapshot
  (new-iterator [_]
    (->MemKvIterator (Box. db) (atom {:rest (seq db)})))

  Closeable
  (close [_]))

(s/def ::persist-on-close? boolean?)

(s/def ::options (s/keys :opt-un [:crux.kv/db-dir]
                         :opt [::persist-on-close?]))

(defrecord MemKv [db db-dir persist-on-close?]
  kv/KvStore
  (open [this {:keys [db-dir crux.memdb.kv/persist-on-close?] :as options}]
    (s/assert ::options options)
    (let [this (assoc this :db-dir db-dir :persist-on-close? persist-on-close?)]
      (if (.isFile (io/file db-dir "memdb"))
        (assoc this :db (atom (restore-db db-dir)))
        (assoc this :db (atom (sorted-map-by bu/bytes-comparator))))))

  (new-snapshot [_]
    (->MemKvSnapshot @db))

  (store [_ kvs]
    (swap! db into kvs))

  (delete [_ ks]
    (swap! db #(apply dissoc % ks)))

  (backup [_ dir]
    (let [file (io/file dir)]
      (when (.exists file)
        (throw (IllegalArgumentException. (str "Directory exists: " (.getAbsolutePath file)))))
      (persist-db dir db)))

  (count-keys [_]
    (count @db))

  (db-dir [_]
    (str db-dir))

  (kv-name [this]
    (.getName (class this)))

  Closeable
  (close [_]
    (when (and db-dir persist-on-close?)
      (persist-db db-dir db))))

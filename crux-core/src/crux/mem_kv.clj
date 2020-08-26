(ns ^:no-doc crux.mem-kv
  "In-memory KV backend for Crux."
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [crux.kv :as kv]
            [crux.memory :as mem]
            [crux.system :as sys]
            [taoensso.nippy :as nippy])
  (:import [clojure.lang Box MapEntry]
           java.io.Closeable
           java.nio.file.Path))

(defn- persist-db [dir db]
  (let [file (io/file dir)]
    (.mkdirs file)
    (->> (for [[k v] @db]
           [(mem/->on-heap k)
            (mem/->on-heap v)])
         (into {})
         (nippy/freeze-to-file (io/file file "memkv")))))

(defn- restore-db [dir]
  (->> (for [[k v] (nippy/thaw-from-file (io/file dir "memkv"))]
         [(mem/->off-heap k)
          (mem/->off-heap v)])
       (into (sorted-map-by mem/buffer-comparator))))

;; NOTE: Using Box here to hide the db from equals/hashCode, otherwise
;; unusable in practice.
(defrecord MemKvIterator [^Box db cursor]
  kv/KvIterator
  (seek [this k]
    (let [[x & xs] (subseq (.val db) >= (mem/as-buffer k))]
      (some->> (reset! cursor {:first x :rest xs})
               :first
               (key))))

  (next [this]
    (some->> (swap! cursor (fn [{[x & xs] :rest}]
                             {:first x :rest xs}))
             :first
             (key)))

  (prev [this]
    (when-let [prev (first (rsubseq (.val db) < (key (:first @cursor))))]
      (kv/seek this (key prev))))

  (value [this]
    (some->> @cursor
             :first
             (val)))

  Closeable
  (close [_]))

(defrecord MemKvSnapshot [db]
  kv/KvSnapshot
  (new-iterator [_]
    (->MemKvIterator (Box. db) (atom {:rest (seq db)})))

  (get-value [_ k]
    (get db (mem/as-buffer k)))

  Closeable
  (close [_]))

(defrecord MemKv [db db-dir]
  kv/KvStore
  (new-snapshot [_]
    (->MemKvSnapshot @db))

  (store [_ kvs]
    (swap! db into (for [[k v] kvs]
                     (MapEntry/create (mem/copy-to-unpooled-buffer (mem/as-buffer k))
                                      (mem/copy-to-unpooled-buffer (mem/as-buffer v)))))
    nil)

  (delete [_ ks]
    (swap! db #(apply dissoc % (map mem/->off-heap ks)))
    nil)

  (compact [_])

  (fsync [_]
    (log/debug "Using fsync on MemKv has no effect."))

  (count-keys [_]
    (count @db))

  (db-dir [_]
    (str db-dir))

  (kv-name [this]
    (.getName (class this)))

  Closeable
  (close [_]
    (when db-dir
      (persist-db db-dir db))))

(defn ->kv-store {::sys/args {:db-dir {:required? false
                                       :doc "Directory to (optionally) store K/V files"
                                       :spec ::sys/path}
                              :persist-on-close? {:required? true
                                                  :default false
                                                  :spec ::sys/boolean}}}
  ([] (->kv-store {}))

  ([{:keys [^Path db-dir persist-on-close?]}]
   (let [db-dir (some-> db-dir (.toFile))]
     (map->MemKv {:db-dir (when persist-on-close?
                            db-dir)
                  :db (atom (if (.isFile (io/file db-dir "memkv"))
                              (restore-db db-dir)
                              (sorted-map-by mem/buffer-comparator)))}))))

(ns ^:no-doc crux.kv.memdb
  "In-memory KV backend for Crux."
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [crux.kv :as kv]
            [crux.memory :as mem]
            [taoensso.nippy :as nippy]
            [crux.index :as idx])
  (:import clojure.lang.Box
           java.io.Closeable))

(defn- persist-db [dir db]
  (let [file (io/file dir)]
    (.mkdirs file)
    (->> (for [[k v] @db]
           [(mem/->on-heap k)
            (mem/->on-heap v)])
         (into {})
         (nippy/freeze-to-file (io/file file "memdb")))))

(defn- restore-db [dir]
  (->> (for [[k v] (nippy/thaw-from-file (io/file dir "memdb"))]
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

(defrecord MemKv [db db-dir persist-on-close?]
  kv/KvStore
  (new-snapshot [_]
    (->MemKvSnapshot @db))

  (store [_ kvs]
    (swap! db into (vec (for [[k v] kvs]
                          [(mem/copy-to-unpooled-buffer (mem/as-buffer k))
                           (mem/copy-to-unpooled-buffer (mem/as-buffer v))])))
    nil)

  (delete [_ ks]
    (swap! db #(apply dissoc % (map mem/->off-heap ks)))
    nil)

  (compact [_])

  (fsync [_]
    (log/debug "Using fsync on MemKv has no effect."))

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

(def kv
  {:start-fn (fn [_ {:keys [::kv/db-dir ::kv/sync? ::kv/check-and-store-index-version ::persist-on-close?]
                     :as options}]
               (when sync?
                 (log/warn "Using sync? on MemKv has no effect."
                           (if (and db-dir persist-on-close?)
                             "Will persist on close."
                             "Persistence is disabled.")))
               (-> (map->MemKv {:db-dir db-dir
                                :persist-on-close? persist-on-close?
                                :db (atom (if (.isFile (io/file db-dir "memdb"))
                                            (restore-db db-dir)
                                            (sorted-map-by mem/buffer-comparator)))})
                   (cond-> check-and-store-index-version idx/check-and-store-index-version)))

   :args (merge kv/options
                {::persist-on-close? {:doc "Persist Mem Db on close"
                                      :default false
                                      :crux.config/type :crux.config/boolean}})})

(def kv-store {:crux.node/kv-store kv})

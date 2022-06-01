(ns ^:no-doc xtdb.mem-kv
  "In-memory KV backend for XTDB."
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [xtdb.checkpoint :as cp]
            [xtdb.codec :as c]
            [xtdb.io :as xio]
            [xtdb.kv :as kv]
            [xtdb.kv.index-store :as kvi]
            [xtdb.memory :as mem]
            [xtdb.system :as sys]
            [juxt.clojars-mirrors.nippy.v3v1v1.taoensso.nippy :as nippy])
  (:import clojure.lang.Box
           [java.io Closeable File]
           java.nio.file.Path))

(defn- persist-db [dir db]
  (let [file (io/file dir)]
    (.mkdirs file)
    (->> (for [[k v] db]
           [(mem/->on-heap k)
            (mem/->on-heap v)])
         (into {})
         (nippy/freeze-to-file (io/file file "memkv")))))

(defn- restore-db [dir]
  (xio/with-nippy-thaw-all
    (->> (for [[k v] (nippy/thaw-from-file (io/file dir "memkv"))]
           [(mem/->off-heap k)
            (mem/->off-heap v)])
         (into (sorted-map-by mem/buffer-comparator)))))

;; NOTE: Using Box here to hide the db from equals/hashCode, otherwise
;; unusable in practice.
(deftype MemKvIterator [^Box db cursor]
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

(deftype MemKvSnapshot [db]
  kv/KvSnapshot
  (new-iterator [_]
    (MemKvIterator. (Box. db) (atom {:rest (seq db)})))

  (get-value [_ k]
    (get db (mem/as-buffer k)))

  Closeable
  (close [_]))

(defrecord MemKvTx [!db !db2]
  kv/KvStoreTx
  (new-tx-snapshot [_]
    (->MemKvSnapshot @!db2))

  (put-kv [_ k v]
    (swap! !db2 (fn [db]
                 (let [k-buf (mem/as-buffer k)]
                   (if v
                     (assoc db (mem/copy-to-unpooled-buffer k-buf) (mem/copy-to-unpooled-buffer (mem/as-buffer v)))
                     (dissoc db k-buf))))))

  (commit-kv-tx [_]
    (reset! !db @!db2))

  Closeable
  (close [_]))

(defrecord MemKv [!db db-dir cp-job]
  kv/KvStore
  (new-snapshot [_]
    (MemKvSnapshot. @!db))

  (begin-kv-tx [_]
    (->MemKvTx !db (atom @!db)))

  (compact [_])

  (fsync [_]
    (log/debug "Using fsync on MemKv has no effect."))

  (count-keys [_]
    (count @!db))

  (db-dir [_] nil)

  (kv-name [this]
    (.getName (class this)))

  cp/CheckpointSource
  (save-checkpoint [this dir]
    (persist-db dir @!db)
    {:tx (kvi/latest-completed-tx this)})

  Closeable
  (close [_]
    (xio/try-close cp-job)
    (when db-dir
      (persist-db db-dir @!db))))

(def ^:private cp-format
  {:index-version c/index-version
   ::version "1"})

(defn- try-restore-from-checkpoint [checkpointer]
  (let [db-dir (xio/create-tmpdir "memkv-cp")]
    (try
      (when (cp/try-restore checkpointer db-dir cp-format)
        (restore-db db-dir))
      (finally
        (xio/delete-dir db-dir)))))

(defn ->kv-store {::sys/deps {:checkpointer (fn [_])}
                  ::sys/args {:db-dir {:required? false
                                       :spec ::sys/path}}}
  ([] (->kv-store {}))

  ([{:keys [checkpointer ^Path db-dir]}]
   (let [^File db-dir (some-> db-dir .toFile)
         db (or (when (and db-dir (.exists db-dir))
                  ;; for xtdb.kv-test/test-checkpoint-and-restore-db
                  (restore-db db-dir))
                (when checkpointer
                  (try-restore-from-checkpoint checkpointer))
                (sorted-map-by mem/buffer-comparator))
         kv-store (map->MemKv {:!db (atom db)
                               :db-dir db-dir})]
     (cond-> kv-store
       checkpointer (assoc :cp-job (cp/start checkpointer kv-store {::cp/cp-format cp-format}))))))

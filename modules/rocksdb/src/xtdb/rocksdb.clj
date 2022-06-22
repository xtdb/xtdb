(ns ^:no-doc xtdb.rocksdb
  "RocksDB KV backend for XTDB."
  (:require [clojure.tools.logging :as log]
            [xtdb.kv :as kv]
            [xtdb.rocksdb.loader]
            [xtdb.memory :as mem]
            [xtdb.system :as sys]
            [xtdb.io :as xio]
            [xtdb.checkpoint :as cp]
            [xtdb.kv.index-store :as kvi]
            [xtdb.codec :as c])
  (:import (java.io Closeable File)
           (java.nio.file Files Path)
           java.nio.file.attribute.FileAttribute
           (org.rocksdb BlockBasedTableConfig Checkpoint CompressionType FlushOptions LRUCache
                        Options ReadOptions RocksDB RocksIterator
                        WriteBatchWithIndex WriteBatch WriteOptions Statistics StatsLevel)))

(set! *unchecked-math* :warn-on-boxed)

(defn- iterator->key [^RocksIterator i]
  (when (.isValid i)
    (mem/as-buffer (.key i))))

(defrecord RocksKvIterator [^RocksIterator i]
  kv/KvIterator
  (seek [_ k]
    (.seek i (mem/direct-byte-buffer k))
    (iterator->key i))

  (next [_]
    (.next i)
    (iterator->key i))

  (prev [_]
    (.prev i)
    (iterator->key i))

  (value [_]
    (mem/as-buffer (.value i)))

  Closeable
  (close [_]
    (.close i)))

(defrecord RocksKvTxSnapshot [^RocksDB db ^ReadOptions read-options, snapshot, ^WriteBatchWithIndex wb]
  kv/KvSnapshot
  (new-iterator [_]
    (->RocksKvIterator (.newIteratorWithBase wb (.newIterator db read-options) read-options)))

  (get-value [_ k]
    (some-> (.getFromBatchAndDB wb db read-options (mem/->on-heap k)) (mem/as-buffer)))

  Closeable
  (close [_]
    (.close read-options)
    (.releaseSnapshot db snapshot)))

(defrecord RocksKvSnapshot [^RocksDB db ^ReadOptions read-options snapshot]
  kv/KvSnapshot
  (new-iterator [_]
    (->RocksKvIterator (.newIterator db read-options)))

  (get-value [_ k]
    (some-> (.get db read-options (mem/->on-heap k))
            (mem/as-buffer)))

  Closeable
  (close [_]
    (.close read-options)
    (.releaseSnapshot db snapshot)))

(defrecord RocksKvTx [^RocksDB db, ^WriteOptions write-options, ^WriteBatchWithIndex wb]
  kv/KvStoreTx
  (new-tx-snapshot [_]
    (let [snapshot (.getSnapshot db)]
      (->RocksKvTxSnapshot db
                           (doto (ReadOptions.)
                             (.setSnapshot snapshot))
                           snapshot
                           wb)))

  (put-kv [_ k v]
    (if v
      (.put wb (mem/direct-byte-buffer k) (mem/direct-byte-buffer v))
      (.remove wb (mem/direct-byte-buffer k))))

  (commit-kv-tx [_]
    (.write db write-options wb))

  Closeable
  (close [_]
    (.close wb)))

(defrecord RocksKv [^RocksDB db, ^WriteOptions write-options, ^Options options, ^Closeable metrics, ^Closeable cp-job, db-dir]
  kv/KvStoreWithReadTransaction
  (begin-kv-tx [_]
    (->RocksKvTx db write-options (WriteBatchWithIndex.)))

  kv/KvStore
  (new-snapshot [_]
    (let [snapshot (.getSnapshot db)]
      (->RocksKvSnapshot db
                         (doto (ReadOptions.)
                           (.setSnapshot snapshot))
                         snapshot)))

  (store [_ kvs]
    (with-open [wb (WriteBatch.)]
      (doseq [[k v] kvs]
        (if v
          (.put wb (mem/direct-byte-buffer k) (mem/direct-byte-buffer v))
          (.remove wb (mem/direct-byte-buffer k))))
      (.write db write-options wb)))

  (compact [_]
    (.compactRange db))

  (fsync [_]
    (when (and (not (.sync write-options))
               (.disableWAL write-options))
      (with-open [flush-options (doto (FlushOptions.)
                                  (.setWaitForFlush true))]
        (.flush db flush-options))))

  (count-keys [_]
    (-> (.getProperty db "rocksdb.estimate-num-keys")
        (Long/parseLong)))

  (db-dir [_]
    (str db-dir))

  (kv-name [this]
    (.getName (class this)))

  cp/CheckpointSource
  (save-checkpoint [this dir]
    (xio/delete-dir dir)
    (let [tx (kvi/latest-completed-tx this)]
      (with-open [checkpoint (Checkpoint/create db)]
        (.createCheckpoint checkpoint (.getAbsolutePath ^File dir)))
      {:tx tx}))

  Closeable
  (close [_]
    (xio/try-close db)
    (xio/try-close options)
    (xio/try-close write-options)
    (xio/try-close metrics)
    (xio/try-close cp-job)))

(def ^:private cp-format {:index-version c/index-version, ::version "7"})

(defn ->lru-block-cache {::sys/args {:cache-size {:doc "Cache size"
                                                  :default (* 8 1024 1024)
                                                  :spec ::sys/nat-int}}}
  [{:keys [cache-size]}]
  (LRUCache. cache-size))

(defn ->kv-store {::sys/deps {:metrics (fn [_])
                              :checkpointer (fn [_])
                              :block-cache `->lru-block-cache}
                  ::sys/args {:db-dir {:doc "Directory to store K/V files"
                                       :required? true
                                       :spec ::sys/path}
                              :sync? {:doc "Sync the KV store to disk after every write."
                                      :default false
                                      :spec ::sys/boolean}
                              :db-options {:doc "RocksDB Options"
                                           :spec #(instance? Options %)}
                              :disable-wal? {:doc "Disable Write Ahead Log"
                                             :default false
                                             :spec ::sys/boolean}}}
  [{:keys [^Path db-dir sync? disable-wal? metrics checkpointer ^Options db-options block-cache] :as options}]

  (RocksDB/loadLibrary)

  (when (and (nil? @xio/malloc-arena-max)
             (xio/glibc?))
    #_{:clj-kondo/ignore [:inline-def]}
    (defonce warn-once-on-malloc-arena-max
      (log/warn "MALLOC_ARENA_MAX not set, memory usage might be high, recommended setting for XTDB is 2")))

  (when checkpointer
    (cp/try-restore checkpointer (.toFile db-dir) cp-format))

  (let [stats (when metrics (doto (Statistics.) (.setStatsLevel (StatsLevel/EXCEPT_DETAILED_TIMERS))))
        opts (doto (or ^Options db-options (Options.))
               (cond-> metrics (.setStatistics stats))
               (.setCompressionType CompressionType/LZ4_COMPRESSION)
               (.setBottommostCompressionType CompressionType/ZSTD_COMPRESSION)
               (.setCreateIfMissing true))
        opts (cond-> opts
               (and block-cache (nil? (.tableFormatConfig opts))) (.setTableFormatConfig (doto (BlockBasedTableConfig.)
                                                                                           (.setBlockCache block-cache))))

        db (try
             (RocksDB/open opts (-> (Files/createDirectories db-dir (make-array FileAttribute 0))
                                    (.toAbsolutePath)
                                    (str)))
             (catch Throwable t
               (.close opts)
               (throw t)))
        metrics (when metrics (metrics db stats))
        kv-store (map->RocksKv {:db-dir db-dir
                                :options opts
                                :db db
                                :metrics metrics
                                :write-options (doto (WriteOptions.)
                                                 (.setSync (boolean sync?))
                                                 (.setDisableWAL (boolean disable-wal?)))})]
    (cond-> kv-store
      checkpointer (assoc :cp-job (cp/start checkpointer kv-store {::cp/cp-format cp-format})))))

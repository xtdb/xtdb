(ns ^:no-doc crux.kv.rocksdb.nio
  "RocksDB KV backend for Crux using direct ByteBuffers:
  https://github.com/facebook/rocksdb/pull/2283"
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [crux.kv.rocksdb.loader]
            [crux.memory :as mem]
            [crux.index :as idx])
  (:import java.io.Closeable
           java.nio.ByteBuffer
           java.util.function.ToIntFunction
           (org.rocksdb Checkpoint CompressionType FlushOptions LRUCache
                        Options ReadOptions RocksDB RocksIterator
                        BlockBasedTableConfig WriteBatch WriteOptions
                        Statistics StatsLevel)))

(set! *unchecked-math* :warn-on-boxed)

(defn- read-value [^ToIntFunction f]
  (loop [limit 32]
    (let [out (mem/direct-byte-buffer (mem/allocate-buffer limit))
          result (.applyAsInt f out)]
      (cond
        (= result RocksDB/NOT_FOUND)
        nil

        (< limit result)
        (recur result)

        :else
        (mem/as-buffer out)))))

(defn- iterator->key [^RocksIterator i]
  (when (.isValid i)
    (read-value (reify ToIntFunction
                  (applyAsInt [_ out]
                    (.key i ^ByteBuffer out))))))

(defrecord RocksNIOKvIterator [^RocksIterator i]
  kv/KvIterator
  (seek [this k]
    (.seek i (mem/direct-byte-buffer k))
    (iterator->key i))

  (next [this]
    (.next i)
    (iterator->key i))

  (prev [this]
    (.prev i)
    (iterator->key i))

  (value [this]
    (read-value (reify ToIntFunction
                  (applyAsInt [_ out]
                    (.value i ^ByteBuffer out)))))

  Closeable
  (close [this]
    (.close i)))

(defrecord RocksNIOKvSnapshot [^RocksDB db ^ReadOptions read-options snapshot]
  kv/KvSnapshot
  (new-iterator [this]
    (->RocksNIOKvIterator (.newIterator db read-options)))

  (get-value [this k]
    (read-value (reify ToIntFunction
                  (applyAsInt [_ out]
                    (.get db read-options (mem/direct-byte-buffer k) ^ByteBuffer out)))))

  Closeable
  (close [_]
    (.close read-options)
    (.releaseSnapshot db snapshot)))

(def ^:private default-block-cache-size (* 128 1024 1024))
(def ^:private default-block-size (* 16 1024))

;; See https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning
(defn- apply-recommended-options ^org.rocksdb.Options [^Options options]
  (doto options
    (.setTableFormatConfig (doto (BlockBasedTableConfig.)
                             (.setBlockCache (LRUCache. default-block-cache-size))
                             (.setBlockSize default-block-size)
                             (.setCacheIndexAndFilterBlocks true)
                             (.setPinL0FilterAndIndexBlocksInCache true)))
    (.setIncreaseParallelism (max (.availableProcessors (Runtime/getRuntime)) 2))))

(defrecord RocksNIOKv [db-dir]
  kv/KvStore
  (new-snapshot [{:keys [^RocksDB db]}]
    (let [snapshot (.getSnapshot db)]
      (->RocksNIOKvSnapshot db
                            (doto (ReadOptions.)
                              (.setPinData true)
                              (.setSnapshot snapshot))
                            snapshot)))

  (store [{:keys [^RocksDB db ^WriteOptions write-options]} kvs]
    (with-open [wb (WriteBatch.)]
      (doseq [[k v] kvs]
        (.put wb (mem/direct-byte-buffer k) (mem/direct-byte-buffer v)))
      (.write db write-options wb)))

  (delete [{:keys [^RocksDB db ^WriteOptions write-options]} ks]
    (with-open [wb (WriteBatch.)]
      (doseq [k ks]
        (.remove wb (mem/direct-byte-buffer k)))
      (.write db write-options wb)))

  (compact [{:keys [^RocksDB db]}]
    (.compactRange db))

  (fsync [{:keys [^RocksDB db]}]
    (with-open [flush-options (doto (FlushOptions.)
                                (.setWaitForFlush true))]
      (.flush db flush-options)))

  (backup [{:keys [^RocksDB db]} dir]
    (let [dir (io/file dir)]
      (when (.exists dir)
        (throw (IllegalArgumentException. (str "Directory exists: " (.getAbsolutePath dir)))))
      (with-open [checkpoint (Checkpoint/create db)]
        (.createCheckpoint checkpoint (.getAbsolutePath dir)))))

  (count-keys [{:keys [^RocksDB db]}]
    (-> (.getProperty db "rocksdb.estimate-num-keys")
        (Long/parseLong)))

  (db-dir [_]
    (str db-dir))

  (kv-name [this]
    (.getName (class this)))

  Closeable
  (close [{:keys [^RocksDB db ^Options options ^WriteOptions write-options]}]
    (.close db)
    (.close options)
    (.close write-options)))

(def kv
  {:start-fn (fn [_ {:keys [::kv/db-dir ::kv/sync? ::kv/check-and-store-index-version
                            ::disable-wal? ::metrics? ::db-options]
                     :as options}]
               (RocksDB/loadLibrary)
               (let [stats (when metrics? (doto (Statistics.) (.setStatsLevel (StatsLevel/EXCEPT_DETAILED_TIMERS))))
                     opts (doto (or ^Options db-options (Options.))
                            (cond-> metrics? (.setStatistics stats))
                            (.setCompressionType CompressionType/LZ4_COMPRESSION)
                            (.setBottommostCompressionType CompressionType/ZSTD_COMPRESSION)
                            (.setCreateIfMissing true))
                     db (try
                          (RocksDB/open opts (.getAbsolutePath (doto (io/file db-dir)
                                                                 (.mkdirs))))
                          (catch Throwable t
                            (.close opts)
                            (throw t)))
                     write-opts (doto (WriteOptions.)
                                  (.setSync (boolean sync?))
                                  (.setDisableWAL (boolean disable-wal?)))]
                 (-> (map->RocksNIOKv {:db-dir db-dir
                                       :db db
                                       :options opts
                                       :stats stats
                                       :write-options write-opts})
                     (cond-> check-and-store-index-version idx/check-and-store-index-version)
                     (lru/wrap-lru-cache options))))

   :args (-> (merge kv/options
                    lru/options
                    {::db-options {:doc "RocksDB Options"
                                   :crux.config/type [#(instance? Options %) identity]}
                     ::disable-wal? {:doc "Disable Write Ahead Log"
                                     :crux.config/type :crux.config/boolean}
                     ::metrics? {:doc "Enable RocksDB metrics"
                                 :default false
                                 :crux.config/type :crux.config/boolean}})
             (update ::kv/db-dir assoc :required? true, :default "data"))})

(def kv-store {:crux.node/kv-store kv})

(def kv-store-with-metrics
  {:crux.node/kv-store (update-in kv [:args ::metrics? :default] not)
   :crux.metrics/registry 'crux.metrics/registry-module
   :crux.metrics/all-metrics-loaded 'crux.metrics/all-metrics-loaded
   ::metrics 'crux.kv.rocksdb.metrics/metrics-module})

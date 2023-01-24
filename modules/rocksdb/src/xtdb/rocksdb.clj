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
  (:import java.util.Map
           java.util.HashMap
           java.util.function.Function
           (java.io Closeable File)
           (java.nio.file Files Path)
           java.nio.file.attribute.FileAttribute
           (org.rocksdb BlockBasedTableConfig Checkpoint CompressionType FlushOptions LRUCache
                        DBOptions Options ReadOptions RocksDB RocksIterator
                        WriteBatchWithIndex WriteBatch WriteOptions Statistics StatsLevel
                        ColumnFamilyOptions ColumnFamilyDescriptor ColumnFamilyHandle BloomFilter)
           (org.agrona DirectBuffer)
           (xtdb.kv KvSnapshotPrefixSupport)))

(defprotocol CfId
  (->cf-id [this]))

(extend-protocol CfId
  (class (byte-array 0))
  (->cf-id [this]
    (aget ^"[B" this 0))

  org.agrona.DirectBuffer
  (->cf-id [this]
    (.getByte this 0))

  org.agrona.concurrent.UnsafeBuffer
  (->cf-id [this]
    (.getByte this 0))

  java.nio.ByteBuffer
  (->cf-id [this]
    (.get this 0)))

(set! *unchecked-math* :warn-on-boxed)

(def column-family-defs
  [{:id-byte c/entity+vt+tt+tx-id->content-hash-index-id, :prefix-len (+ c/id-size c/index-id-size)}
   {:id-byte c/entity+z+tx-id->content-hash-index-id}])

(defn- iterator->key [^RocksIterator i]
  (when (.isValid i)
    (mem/as-buffer (.key i))))

(deftype RocksKvIterator [^:unsynchronized-mutable ^RocksIterator i, ^Map is, ^Function k->i]
  kv/KvIterator
  (seek [_ k]
    (set! i (.computeIfAbsent is (->cf-id k) k->i))
    (.seek i (mem/direct-byte-buffer k))
    (iterator->key i))

  (next [_]
    (when (.isValid i)
      (.next i)
      (iterator->key i)))

  (prev [_]
    (when (.isValid i)
      (.prev i)
      (iterator->key i)))

  (value [_]
    (mem/as-buffer (.value i)))

  Closeable
  (close [_]
    (doseq [^RocksIterator i (.values is)]
      (.close i))))

(defrecord RocksKvTxSnapshot [^RocksDB db, ->column-family-handle,
                              ^ReadOptions read-options,
                              ^ReadOptions prefix-read-options,
                              snapshot,
                              ^WriteBatchWithIndex wb]
  kv/KvSnapshot
  (new-iterator [_]
    (let [k->i (reify Function
                 (apply [_ k]
                   (let [cfh (->column-family-handle k)]
                     (.newIteratorWithBase wb cfh (.newIterator db cfh read-options) read-options))))]
      (->RocksKvIterator nil (HashMap.) k->i)))

  (get-value [_ k]
    (some-> (.getFromBatchAndDB wb db ^ColumnFamilyHandle (->column-family-handle (->cf-id k)) read-options (mem/->on-heap k))
            (mem/as-buffer)))

  KvSnapshotPrefixSupport
  (newPrefixSeekOptimisedIterator [_]
    (let [k->i (reify Function
                 (apply [_ k]
                   (let [cfh (->column-family-handle k)]
                     (.newIteratorWithBase wb cfh (.newIterator db cfh prefix-read-options) prefix-read-options))))]
      (->RocksKvIterator nil (HashMap.) k->i)))

  Closeable
  (close [_]
    (.close read-options)
    (.close prefix-read-options)
    (.releaseSnapshot db snapshot)))

(defrecord RocksKvSnapshot [^RocksDB db, ->column-family-handle,
                            ^ReadOptions read-options,
                            ^ReadOptions prefix-read-options,
                            snapshot]
  kv/KvSnapshot
  (new-iterator [_]
    (let [k->i (reify Function
                 (apply [_ k]
                   (.newIterator db (->column-family-handle k) read-options)))]
      (->RocksKvIterator nil (HashMap.) k->i)))

  (get-value [_ k]
    (some-> (.get db ^ColumnFamilyHandle (->column-family-handle (->cf-id k)) read-options (mem/->on-heap k))
            (mem/as-buffer)))

  KvSnapshotPrefixSupport
  (newPrefixSeekOptimisedIterator [_]
    (let [k->i (reify Function
                 (apply [_ k]
                   (.newIterator db (->column-family-handle k) read-options)))]
      (->RocksKvIterator nil (HashMap.) k->i)))

  Closeable
  (close [_]
    (.close read-options)
    (.close prefix-read-options)
    (.releaseSnapshot db snapshot)))

(defrecord RocksKvTx [^RocksDB db, ->column-family-handle, ^WriteOptions write-options, ^WriteBatchWithIndex wb]
  kv/KvStoreTx
  (new-tx-snapshot [_]
    (let [snapshot (.getSnapshot db)]
      (->RocksKvTxSnapshot db
                           ->column-family-handle
                           (doto (ReadOptions.)
                             (.setSnapshot snapshot))
                           (doto (ReadOptions.)
                             (.setPrefixSameAsStart true)
                             (.setSnapshot snapshot))
                           snapshot
                           wb)))

  (put-kv [_ k v]
    (if (and (instance? DirectBuffer k)
             (or (nil? v) (instance? DirectBuffer v)))
      (let [^DirectBuffer k k
            ^DirectBuffer v v
            koffset (.wrapAdjustment k)
            kcap (.capacity k)
            kbb (.byteBuffer k)
            kpos (.position kbb)
            klim (.limit kbb)
            _ (.position kbb koffset)
            _ (.limit kbb (+ koffset kcap))
            cfh ^ColumnFamilyHandle (->column-family-handle (.get kbb koffset))]

        (if v
          (let [voffset (.wrapAdjustment v)
                vcap (.capacity v)
                vbb (.byteBuffer v)
                vpos (.position vbb)
                vlim (.limit vbb)]
            (.position vbb voffset)
            (.limit vbb (+ voffset vcap))
            (.put wb cfh kbb vbb)
            (.position vbb vpos)
            (.limit vbb vlim))
          (.delete wb cfh kbb))

        (.position kbb kpos)
        (.limit kbb klim))
      (let [kb (mem/direct-byte-buffer k)
            cfh ^ColumnFamilyHandle (->column-family-handle (.get kb (.position kb)))]
        (if v
          (.put wb cfh kb (mem/direct-byte-buffer v))
          (.delete wb cfh kb)))))

  (commit-kv-tx [this]
    (.write db write-options wb)
    (.close this))

  (abort-kv-tx [this]
    (.close this))

  Closeable
  (close [_]
    (.close wb)))

(defrecord RocksKv [^RocksDB db, ->column-family-handle, ^WriteOptions write-options, ^Options options, ^Closeable metrics, ^Closeable cp-job, db-dir]
  kv/KvStoreWithReadTransaction
  (begin-kv-tx [_]
    (->RocksKvTx db ->column-family-handle write-options (WriteBatchWithIndex.)))

  kv/KvStore
  (new-snapshot [_]
    (let [snapshot (.getSnapshot db)]
      (->RocksKvSnapshot db
                         ->column-family-handle
                         (doto (ReadOptions.)
                           (.setSnapshot snapshot))
                         (doto (ReadOptions.)
                           (.setPrefixSameAsStart true)
                           (.setSnapshot snapshot))
                         snapshot)))

  (store [_ kvs]
    (with-open [wb (WriteBatch.)]
      (doseq [[k v] kvs
              :let [kbb (mem/direct-byte-buffer k)
                    cfh ^ColumnFamilyHandle (->column-family-handle (->cf-id kbb))]]
        (if v
          (.put wb cfh kbb (mem/direct-byte-buffer v))
          (.delete wb cfh kbb)))
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
                                             :spec ::sys/boolean}
                              :enable-filters?
                              {:doc
                               "Enables additional probabilistic filtering of certain index seeks, and caching of filters.

                               Can reduce I/O at the cost of increased demands on block cache, this option may increase the size of SST files on disk slightly, expect around 2-3% increase in size.

                               Some effects may require new SST files to be written by RocksDB, so may not have an immediate impact.

                               If this option is enabled, a block cache size of at least 256MB is recommended. See https://docs.xtdb.com/storage/rocksdb/#blocks-cache for instructions on
                               configuring the block cache."
                               :default false
                               :spec ::sys/boolean}}}
  [{:keys [^Path db-dir sync? disable-wal? metrics checkpointer ^Options db-options block-cache enable-filters?] :as options}]

  (RocksDB/loadLibrary)

  (when (and (nil? @xio/malloc-arena-max)
             (xio/glibc?))
    #_{:clj-kondo/ignore [:inline-def :clojure-lsp/unused-public-var]}
    (defonce warn-once-on-malloc-arena-max
      (log/warn "MALLOC_ARENA_MAX not set, memory usage might be high, recommended setting for XTDB is 2")))

  (when checkpointer
    (cp/try-restore checkpointer (.toFile db-dir) cp-format))

  (let [default-cfo (doto (ColumnFamilyOptions.)
                      (.setCompressionType CompressionType/LZ4_COMPRESSION)
                      (.setBottommostCompressionType CompressionType/ZSTD_COMPRESSION))

        configure-table
        (fn [^BlockBasedTableConfig cfg]
          (when block-cache
            (.setBlockCache cfg block-cache))
          (when enable-filters?
            (.setCacheIndexAndFilterBlocks cfg true)
            ;; true by default (but requires CacheIndexAndFilterBlocks to do anything), however not specified as such in docs so setting here just in case it is redefined in
            ;; a subsequent rocks jni release.
            (.setCacheIndexAndFilterBlocksWithHighPriority cfg true)))

        _ (when block-cache
            (.setTableFormatConfig default-cfo (doto (BlockBasedTableConfig.) (configure-table))))

        column-family-descriptors
        (into [(ColumnFamilyDescriptor. RocksDB/DEFAULT_COLUMN_FAMILY default-cfo)]
              (for [{:keys [id-byte, prefix-len]} column-family-defs
                    :let [cfo (doto (ColumnFamilyOptions.)
                                (.setCompressionType CompressionType/LZ4_COMPRESSION)
                                (.setBottommostCompressionType CompressionType/ZSTD_COMPRESSION))

                          table-config (BlockBasedTableConfig.)

                          _ (configure-table table-config)

                          _ (when (and enable-filters? block-cache prefix-len)
                              (.useFixedLengthPrefixExtractor cfo (int prefix-len))
                              (.setFilterPolicy table-config (BloomFilter.))
                              (.setWholeKeyFiltering table-config false)
                              (.setMemtablePrefixBloomSizeRatio cfo 0.05))

                          _ (.setTableFormatConfig cfo table-config)
                          ]]
                (ColumnFamilyDescriptor. (byte-array [id-byte]) cfo)))

        column-family-handles-vector (java.util.Vector.)

        stats (when metrics (doto (Statistics.) (.setStatsLevel (StatsLevel/EXCEPT_DETAILED_TIMERS))))

        opts (doto (or ^DBOptions db-options (DBOptions.))
               (cond-> metrics (.setStatistics stats))
               (.setCreateIfMissing true)
               (.setCreateMissingColumnFamilies true)
               (cond-> (nil? db-options) (.setMaxBackgroundJobs (max 2 (dec (.availableProcessors (Runtime/getRuntime)))))))

        db (try
             (RocksDB/open opts (-> (Files/createDirectories db-dir (make-array FileAttribute 0))
                                    (.toAbsolutePath)
                                    (str))
                           column-family-descriptors
                           column-family-handles-vector)
             (catch Throwable t
               (.close opts)
               (throw t)))
        metrics (when metrics (metrics db stats))
        column-family-handles (into {}
                                    (for [[^int i cfd] (map-indexed vector column-family-defs)]
                                      [(:id-byte cfd) (.get column-family-handles-vector (inc i))]))

        ^objects cf-handle-array
        (let [max-cf-id (apply max (keys column-family-handles))
              arr (object-array (inc (int max-cf-id)))]
          (doseq [[k v] column-family-handles]
            (aset arr k v))
          arr)

        default-column-family-handle (first column-family-handles-vector)
        ->column-family-handle
        (fn [cf-id]
          (let [cf-id (int cf-id)]
            (if (< cf-id (alength cf-handle-array))
              (or (aget cf-handle-array cf-id) default-column-family-handle)
              default-column-family-handle)))

        kv-store (map->RocksKv {:db-dir db-dir
                                :options opts
                                :db db
                                :metrics metrics
                                :write-options (doto (WriteOptions.)
                                                 (.setSync (boolean sync?))
                                                 (.setDisableWAL (boolean disable-wal?)))
                                :->column-family-handle ->column-family-handle})]
    (cond-> kv-store
      checkpointer (assoc :cp-job (cp/start checkpointer kv-store {::cp/cp-format cp-format})))))


(comment

  (cond-> 1 (.setStatistics stats))

  :ok)

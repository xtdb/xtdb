(ns ^:no-doc xtdb.rocksdb.jnr
  "RocksDB KV backend for Crux using JNR:
  https://github.com/jnr/jnr-ffi"
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [xtdb.io :as cio]
            [xtdb.kv :as kv]
            [xtdb.rocksdb.loader]
            [xtdb.memory :as mem]
            [xtdb.checkpoint :as cp]
            [xtdb.system :as sys]
            [xtdb.kv.index-store :as kvi]
            [xtdb.codec :as c])
  (:import java.io.Closeable
           [org.agrona DirectBuffer MutableDirectBuffer ExpandableDirectByteBuffer]
           org.agrona.concurrent.UnsafeBuffer
           java.nio.file.Path
           [jnr.ffi LibraryLoader Memory NativeType Pointer]))

;;; ***** N.B. *****
;; NOTE: This backend doesn't currently work, as it requires
;; defensive copies in `xtdb.kv.index-store` after calls to `key-suffix`.

;; This isn't currently done, as this does an needless copy for other stores.
;; This store is about 10% faster than RocksJava JNI.

;; This backend is about 10% faster than the normal JNI one when defensive copies are made.

;; // Return the key for the current entry. The underlying storage for
;; // the returned slice is valid only until the next modification of
;; // the iterator.

(set! *unchecked-math* :warn-on-boxed)

(definterface RocksDB
  (^jnr.ffi.Pointer rocksdb_options_create [])
  (^void rocksdb_options_set_create_if_missing [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} opt
                                                ^byte v])
  (^void rocksdb_options_set_compression [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} opt
                                          ^int t])
  (^void rocksdb_options_set_block_based_table_factory [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} opt
                                                        ^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} table_options])
  (^void rocksdb_options_destroy [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} opt])

  (^jnr.ffi.Pointer rocksdb_block_based_options_create [])
  (^void rocksdb_block_based_options_set_block_cache [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} options
                                                      ^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} block_cache])
  (^void rocksdb_block_based_options_destroy [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} opt])

  (^jnr.ffi.Pointer rocksdb_cache_create_lru [^{jnr.ffi.types.size_t true :tag long} capacity])
  (^void rocksdb_cache_destroy [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} cache])

  (^jnr.ffi.Pointer rocksdb_writeoptions_create [])
  (^void rocksdb_writeoptions_disable_WAL [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} opt
                                           ^byte disable])
  (^void rocksdb_writeoptions_set_sync [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} opt
                                        ^byte v])
  (^void rocksdb_writeoptions_destroy [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} opt])


  (^jnr.ffi.Pointer rocksdb_flushoptions_create [])
  (^void rocksdb_flushoptions_set_wait [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} opt
                                        ^byte v])
  (^void rocksdb_flushoptions_destroy [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} opt])
  (^void rocksdb_flush [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} db
                        ^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} flush_options
                        ^{jnr.ffi.annotations.Out true :tag "[Ljava.lang.String;"} errptr])

  (^jnr.ffi.Pointer rocksdb_open [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} options
                                  ^{jnr.ffi.annotations.In true :tag String} name
                                  ^{jnr.ffi.annotations.Out true :tag "[Ljava.lang.String;"} errptr])
  (^String rocksdb_property_value [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} db
                                   ^{jnr.ffi.annotations.In true :tag String} propname])
  (^void rocksdb_write [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} db
                        ^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} options
                        ^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} batch
                        ^{jnr.ffi.annotations.Out true :tag "[Ljava.lang.String;"} errptr])
  (^jnr.ffi.Pointer rocksdb_get [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} db
                                 ^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} options
                                 ^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} key
                                 ^{jnr.ffi.types.size_t true  :tag long} keylen
                                 ^{jnr.ffi.annotations.Out true :tag jnr.ffi.Pointer} vlen
                                 ^{jnr.ffi.annotations.Out true :tag "[Ljava.lang.String;"} errptr])
  (^void rocksdb_close [^jnr.ffi.Pointer db])

  (^jnr.ffi.Pointer rocksdb_checkpoint_object_create [^jnr.ffi.Pointer db
                                                      ^"[Ljava.lang.String;" errptr])
  (^void rocksdb_checkpoint_create [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} checkpoint
                                    ^{jnr.ffi.annotations.In true :tag String} checkpoint_dir
                                    ^{jnr.ffi.types.u_int64_t true :tag long} log_size_for_flush
                                    ^{jnr.ffi.annotations.Out true :tag "[Ljava.lang.String;"} errptr])
  (^void rocksdb_checkpoint_object_destroy [^jnr.ffi.Pointer checkpoint])

  (^jnr.ffi.Pointer rocksdb_create_snapshot [^jnr.ffi.Pointer db])
  (^void rocksdb_release_snapshot [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} db
                                   ^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} snapshot])

  (^jnr.ffi.Pointer rocksdb_writebatch_create [])
  (^void rocksdb_writebatch_put [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} b
                                 ^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} key
                                 ^{jnr.ffi.types.size_t true :tag long} klen
                                 ^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} val
                                 ^{jnr.ffi.types.size_t true :tag long} vlen])
  (^void rocksdb_writebatch_delete [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} b
                                    ^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} key
                                    ^{jnr.ffi.types.size_t true :tag long} klen])
  (^void rocksdb_writebatch_destroy [^jnr.ffi.Pointer opt])

  (^jnr.ffi.Pointer rocksdb_readoptions_create [])
  (^void rocksdb_readoptions_set_snapshot [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} opt
                                           ^jnr.ffi.Pointer snap])
  (^void rocksdb_readoptions_set_pin_data [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} opt
                                           ^byte v])
  (^void rocksdb_readoptions_destroy [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} opt])

  (^jnr.ffi.Pointer rocksdb_create_iterator [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} db
                                             ^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} options])
  (^byte rocksdb_iter_valid [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} iter])
  (^void rocksdb_iter_next [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} iter])
  (^void rocksdb_iter_prev [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} iter])
  (^void rocksdb_iter_seek [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} iter
                            ^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} k
                            ^{jnr.ffi.types.size_t true  :tag long} klen])
  (^jnr.ffi.Pointer rocksdb_iter_key [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} iter
                                      ^{jnr.ffi.annotations.Out true :tag jnr.ffi.Pointer} klen])
  (^jnr.ffi.Pointer rocksdb_iter_value [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} iter
                                        ^{jnr.ffi.annotations.Out true :tag jnr.ffi.Pointer} vlen])
  (^void rocksdb_iter_get_error [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} iter
                                 ^{jnr.ffi.annotations.Out true :tag "[Ljava.lang.String;"} errptr])
  (^void rocksdb_iter_destroy [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} iter]))

(defn- load-rocksdb-native-lib ^xtdb.rocksdb.jnr.RocksDB []
  (.load (LibraryLoader/create RocksDB) xtdb.rocksdb.loader/rocksdb-library-path))

(def ^:private ^RocksDB rocksdb)
(def ^:private ^jnr.ffi.Runtime rt)

(defn- init-rocksdb-jnr! []
  (when-not (bound? #'rocksdb)
    (alter-var-root #'rocksdb (constantly
                                (load-rocksdb-native-lib)))
    (alter-var-root #'rt (constantly
                           (jnr.ffi.Runtime/getRuntime rocksdb)))))

(defn- check-error [errptr-out]
  (when-let [error (first errptr-out)]
    (throw (RuntimeException. (str error)))))

(defn- check-iterator-error [^Pointer i]
  (let [errptr (make-array String 1)]
    (.rocksdb_iter_get_error rocksdb i errptr)
    (check-error errptr)))

;; From Iterator::key(), value() is the same:
;; // Return the key for the current entry.  The underlying storage for
;; // the returned slice is valid only until the next modification of
;; // the iterator.
(defn- pointer+len->buffer ^org.agrona.DirectBuffer [^Pointer address ^Pointer len-out]
  (let [len (.getInt len-out 0)]
    (UnsafeBuffer. (.address address) len)))

(defn- iterator->key ^org.agrona.DirectBuffer [^Pointer i ^Pointer len-out]
  (when (= 1 (.rocksdb_iter_valid rocksdb i))
    (let [p-k (.rocksdb_iter_key rocksdb i len-out)]
      (pointer+len->buffer p-k len-out))))

(defn- buffer->pointer ^jnr.ffi.Pointer [^DirectBuffer b]
  (Pointer/wrap rt (.addressOffset b)))

(defrecord RocksJNRKvIterator [^Pointer i ^ExpandableDirectByteBuffer eb ^Pointer len-out]
  kv/KvIterator
  (seek [this k]
    (let [k (mem/ensure-off-heap k eb)]
      (.rocksdb_iter_seek rocksdb i (buffer->pointer k) (.capacity k))
      (iterator->key i len-out)))

  (next [this]
    (.rocksdb_iter_next rocksdb i)
    (iterator->key i len-out))

  (prev [this]
    (.rocksdb_iter_prev rocksdb i)
    (iterator->key i len-out))

  (value [this]
    (let [p-v (.rocksdb_iter_value rocksdb i len-out)]
      (pointer+len->buffer p-v len-out)))

  Closeable
  (close [this]
    (.rocksdb_iter_destroy rocksdb i)))

(defrecord RocksJNRKvSnapshot [^Pointer db ^Pointer read-options ^Pointer snapshot len-out]
  kv/KvSnapshot
  (new-iterator [this]
    (->RocksJNRKvIterator (.rocksdb_create_iterator rocksdb db read-options)
                          (ExpandableDirectByteBuffer.)
                          (Memory/allocateTemporary rt NativeType/ULONG)))

  (get-value [this k]
    (let [k (mem/->off-heap k)
          errptr-out (make-array String 1)
          v (.rocksdb_get rocksdb db read-options (buffer->pointer k) (.capacity k) len-out errptr-out)]
      (check-error errptr-out)
      (when v
        (pointer+len->buffer v len-out))))

  Closeable
  (close [_]
    (.rocksdb_readoptions_destroy rocksdb read-options)
    (.rocksdb_release_snapshot rocksdb db snapshot)))

(def ^:private rocksdb_lz4_compression 4)

(defrecord RocksJNRKv [^Pointer db, ^Pointer options, ^Pointer write-options, ^Pointer block-based-options, db-dir, cp-job, disable-wal? sync?]
  kv/KvStore
  (new-snapshot [_]
    (let [snapshot (.rocksdb_create_snapshot rocksdb db)
          read-options (.rocksdb_readoptions_create rocksdb)]
      (.rocksdb_readoptions_set_pin_data rocksdb read-options 1)
      (.rocksdb_readoptions_set_snapshot rocksdb read-options snapshot)
      (->RocksJNRKvSnapshot db
                            read-options
                            snapshot
                            (Memory/allocateTemporary rt NativeType/ULONG))))

  (store [_ kvs]
    (let [wb (.rocksdb_writebatch_create rocksdb)
          errptr-out (make-array String 1)
          kb (ExpandableDirectByteBuffer.)
          vb (ExpandableDirectByteBuffer.)]
      (try
        (doseq [[k v] kvs
                :let [k (mem/ensure-off-heap k kb)
                      v (some-> v (mem/ensure-off-heap vb))]]
          (if v
            (.rocksdb_writebatch_put rocksdb wb (buffer->pointer k) (.capacity k) (buffer->pointer v) (.capacity v))
            (.rocksdb_writebatch_delete rocksdb wb (buffer->pointer k) (.capacity k))))
        (.rocksdb_write rocksdb db write-options wb errptr-out)
        (finally
          (.rocksdb_writeoptions_destroy rocksdb wb)
          (check-error errptr-out)))))

  (fsync [_]
    (when (and (not sync?) disable-wal?)
      (let [errptr-out (make-array String 1)
            flush-options (.rocksdb_flushoptions_create rocksdb)]
        (try
          (.rocksdb_flushoptions_set_wait rocksdb flush-options 1)
          (.rocksdb_flush rocksdb db flush-options errptr-out)
          (finally
            (.rocksdb_flushoptions_destroy rocksdb flush-options)
            (check-error errptr-out))))))

  (compact [_])

  (count-keys [_]
    (-> (.rocksdb_property_value rocksdb db "rocksdb.estimate-num-keys")
        (Long/parseLong)))

  (db-dir [_]
    (str db-dir))

  (kv-name [this]
    (.getName (class this)))

  cp/CheckpointSource
  (save-checkpoint [kv-store dir]
    (cio/delete-dir dir)
    (let [tx (kvi/latest-completed-tx kv-store)
          errptr-out (make-array String 1)
          checkpoint (try
                       (.rocksdb_checkpoint_object_create rocksdb db errptr-out)
                       (finally
                         (check-error errptr-out)))]
      (try
        (.rocksdb_checkpoint_create rocksdb checkpoint (str dir) 0 errptr-out)
        {:tx tx}
        (finally
          (.rocksdb_checkpoint_object_destroy rocksdb checkpoint)
          (check-error errptr-out)))))

  Closeable
  (close [_]
    (.rocksdb_close rocksdb db)
    (.rocksdb_options_destroy rocksdb options)
    (.rocksdb_block_based_options_destroy rocksdb block-based-options)
    (.rocksdb_writeoptions_destroy rocksdb write-options)
    (cio/try-close cp-job)))

(def ^:private cp-format
  {:index-version c/index-version
   ::version "6"})

(defrecord RocksJNRLRUCache [^Pointer cache]
  Closeable
  (close [_]
    (.rocksdb_cache_destroy rocksdb cache)))

(defn ->lru-block-cache {::sys/args {:cache-size {:doc "Cache size"
                                                  :default (* 8 1024 1024)
                                                  :spec ::sys/nat-int}}}
  [{:keys [cache-size]}]
  (init-rocksdb-jnr!)
  (->RocksJNRLRUCache (.rocksdb_cache_create_lru rocksdb cache-size)))

(defn ->kv-store {::sys/deps {:checkpointer (fn [_])
                              :block-cache `->lru-block-cache}
                  ::sys/args (merge (-> kv/args
                                        (update :db-dir assoc :required? true, :default "data"))
                                    {:db-options {:doc "RocksDB Options"
                                                  :spec ::sys/string}
                                     :disable-wal? {:doc "Disable Write Ahead Log"
                                                    :spec ::sys/boolean}})}
  [{:keys [^Path db-dir sync? db-options disable-wal? checkpointer ^RocksJNRLRUCache block-cache]}]
  (init-rocksdb-jnr!)
  (let [db-dir (.toFile db-dir)
        _ (some-> checkpointer (cp/try-restore db-dir cp-format))

        block-based-options (.rocksdb_block_based_options_create rocksdb)
        _ (when (and block-cache (.cache block-cache))
            (.rocksdb_block_based_options_set_block_cache rocksdb block-based-options (.cache block-cache)))
        opts (.rocksdb_options_create rocksdb)
        _ (.rocksdb_options_set_create_if_missing rocksdb opts 1)
        _ (.rocksdb_options_set_compression rocksdb opts rocksdb_lz4_compression)
        _ (.rocksdb_options_set_block_based_table_factory rocksdb opts block-based-options)
        errptr-out (make-array String 1)

        db (try
             (let [db (.rocksdb_open rocksdb
                                     opts
                                     (.getAbsolutePath (doto (io/file db-dir)
                                                         (.mkdirs)))
                                     errptr-out)]
               (check-error errptr-out)
               db)
             (catch Throwable t
               (.rocksdb_options_destroy rocksdb opts)
               (throw t)))

        write-options (.rocksdb_writeoptions_create rocksdb)
        _ (when sync?
            (.rocksdb_writeoptions_set_sync rocksdb write-options 1))
        _ (when disable-wal?
            (.rocksdb_writeoptions_disable_WAL rocksdb write-options 1))

        kv-store (map->RocksJNRKv {:db-dir db-dir
                                   :db db
                                   :options opts
                                   :write-options write-options
                                   :block-based-options block-based-options
                                   :sync? sync?
                                   :disable-wal? disable-wal?})]
    (cond-> kv-store
      checkpointer (assoc :cp-job (cp/start checkpointer kv-store {::cp/cp-format cp-format})))))

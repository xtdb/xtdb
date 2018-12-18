(ns crux.kv.rocksdb.jnr
  "RocksDB KV backend for Crux using JNR:
  https://github.com/jnr/jnr-ffi

  Requires org.rocksdb/rocksdbjni and com.github.jnr/jnr-ffi on the
  classpath."
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [crux.io :as cio]
            [crux.kv :as kv])
  (:import java.io.Closeable
           java.util.function.Supplier
           org.agrona.concurrent.UnsafeBuffer
           org.rocksdb.util.Environment
           [jnr.ffi LibraryLoader Memory Pointer]))

(set! *unchecked-math* :warn-on-boxed)

(definterface RocksDB
  (^jnr.ffi.Pointer rocksdb_options_create [])
  (^void rocksdb_options_set_create_if_missing [^jnr.ffi.Pointer opt ^byte v])
  (^void rocksdb_options_set_compression [^jnr.ffi.Pointer opt ^int t])
  (^void rocksdb_options_destroy [^jnr.ffi.Pointer opt])

  (^jnr.ffi.Pointer rocksdb_writeoptions_create [])
  (^void rocksdb_writeoptions_disable_WAL [^jnr.ffi.Pointer opt ^byte disable])
  (^void rocksdb_writeoptions_destroy [^jnr.ffi.Pointer opt])

  (^jnr.ffi.Pointer rocksdb_open [^jnr.ffi.Pointer options ^String name ^{jnr.ffi.annotations.Out true :tag "[Ljava.lang.String;"} errptr])
  (^String rocksdb_property_value [^jnr.ffi.Pointer db ^String propname])
  (^void rocksdb_write [^jnr.ffi.Pointer db ^jnr.ffi.Pointer options ^jnr.ffi.Pointer batch ^{jnr.ffi.annotations.Out true :tag "[Ljava.lang.String;"} errptr])
  (^void rocksdb_close [^jnr.ffi.Pointer db])

  (^jnr.ffi.Pointer rocksdb_checkpoint_object_create [^jnr.ffi.Pointer db ^"[Ljava.lang.String;" errptr])
  (^void rocksdb_checkpoint_create [^jnr.ffi.Pointer checkpoint ^String checkpoint_dir ^{jnr.ffi.types.u_int64_t true :tag long} log_size_for_flush ^{jnr.ffi.annotations.Out true :tag "[Ljava.lang.String;"} errptr])
  (^void rocksdb_checkpoint_object_destroy [^jnr.ffi.Pointer checkpoint])

  (^jnr.ffi.Pointer rocksdb_create_snapshot [^jnr.ffi.Pointer db])
  (^void rocksdb_release_snapshot [^jnr.ffi.Pointer db ^jnr.ffi.Pointer snapshot])

  (^jnr.ffi.Pointer rocksdb_writebatch_create [])
  (^void rocksdb_writebatch_put [^jnr.ffi.Pointer b ^{jnr.ffi.annotations.In true :tag bytes} key ^{jnr.ffi.types.size_t true :tag long} klen ^{jnr.ffi.annotations.In true  :tag bytes} val ^{jnr.ffi.types.size_t true :tag long} vlen])
  (^void rocksdb_writebatch_delete [^jnr.ffi.Pointer b ^{jnr.ffi.annotations.In true :tag bytes} key ^{jnr.ffi.types.size_t true :tag long} klen])
  (^void rocksdb_writebatch_destroy [^jnr.ffi.Pointer opt])

  (^jnr.ffi.Pointer rocksdb_readoptions_create [])
  (^void rocksdb_readoptions_set_snapshot [^jnr.ffi.Pointer opt ^jnr.ffi.Pointer snap])
  (^void rocksdb_readoptions_set_pin_data [^jnr.ffi.Pointer opt ^byte v])
  (^void rocksdb_readoptions_destroy [^jnr.ffi.Pointer opt])

  (^jnr.ffi.Pointer rocksdb_create_iterator [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} db ^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} options])
  (^byte rocksdb_iter_valid [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} iter])
  (^void rocksdb_iter_next [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} iter])
  (^void rocksdb_iter_seek [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} iter ^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} k ^{jnr.ffi.types.size_t true  :tag long} klen])
  (^jnr.ffi.Pointer rocksdb_iter_key [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} iter ^{jnr.ffi.annotations.Out true :tag jnr.ffi.Pointer} klen])
  (^jnr.ffi.Pointer rocksdb_iter_value [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} iter ^{jnr.ffi.annotations.Out true :tag jnr.ffi.Pointer} vlen])
  (^void rocksdb_iter_get_error [^jnr.ffi.Pointer iter ^{jnr.ffi.annotations.Out true :tag "[Ljava.lang.String;"} errptr])
  (^void rocksdb_iter_destroy [^jnr.ffi.Pointer iter]))

(defn- extract-rocksdb-native-lib [to-dir]
  (let [lib-name (Environment/getJniLibraryFileName "rocksdb")
        f (io/file to-dir lib-name)]
    (io/make-parents f)
    (with-open [in (io/input-stream (io/resource lib-name))
                out (io/output-stream f)]
      (io/copy in out))
    f))

(defn- load-rocksdb-native-lib ^crux.kv.rocksdb.jnr.RocksDB []
  (let [tmp (cio/create-tmpdir "jnr_rocksdb")
        f (extract-rocksdb-native-lib tmp)]
    (.load (LibraryLoader/create RocksDB) (str f))))

(defn- check-error [errptr]
  (when-let [error (first errptr)]
    (throw (RuntimeException. (str error)))))

(def ^RocksDB rocksdb (load-rocksdb-native-lib))
(def ^jnr.ffi.Runtime rt (jnr.ffi.Runtime/getRuntime rocksdb))

(defn- check-iterator-error [^Pointer i]
  (let [errptr (make-array String 1)]
    (.rocksdb_iter_get_error rocksdb i errptr)
    (check-error errptr)))

(def ^:private ^ThreadLocal pointer-to-pointer (ThreadLocal/withInitial
                                                (reify Supplier
                                                  (get [_]
                                                    (Memory/allocateDirect rt Long/BYTES)))))

(defn- allocate-pointer-to-pointer ^Pointer []
  (.get pointer-to-pointer))

(def ^:private ^ThreadLocal buffer-pool (ThreadLocal/withInitial
                                         (reify Supplier
                                           (get [_]
                                             (Memory/allocateDirect rt 32)))))

(defn- allocate-buffer ^Pointer [^long size]
  (let [buffer ^Pointer (.get buffer-pool)]
    (if (and buffer (>= (.size buffer) size))
      buffer
      (let [buffer (Memory/allocateDirect rt (* (.size buffer) 2))]
        (.set buffer-pool buffer)
        (recur size)))))

(defn- deallocate-buffer [^Pointer buffer])

(defn- pointer+len->bytes [^Pointer address ^Pointer len]
  (let [len (.getInt len 0)
        b (byte-array len)]
    (.getBytes (UnsafeBuffer. (.address address) len) 0 b)
    b))

(defn- iterator->key [^Pointer i]
  (when (= 1 (.rocksdb_iter_valid rocksdb i))
    (let [p-len ^Pointer (allocate-pointer-to-pointer)
          p-k (.rocksdb_iter_key rocksdb i p-len)]
      (pointer+len->bytes p-k p-len))))

(defrecord RocksJNRKvIterator [^Pointer i]
  kv/KvIterator
  (seek [this k]
    (let [k ^bytes k
          klen (alength k)
          p-key ^Pointer (allocate-buffer klen)]
      (try
        (.putBytes (UnsafeBuffer. (.address p-key) klen) 0 k)
        (.rocksdb_iter_seek rocksdb i p-key klen)
        (iterator->key i)
        (finally
          (deallocate-buffer p-key)))))

  (next [this]
    (.rocksdb_iter_next rocksdb i)
    (iterator->key i))

  (value [this]
    (let [p-len ^Pointer (allocate-pointer-to-pointer)
          p-v (.rocksdb_iter_value rocksdb i p-len)]
      (pointer+len->bytes p-v p-len)))

  (kv/refresh [this]
    ;; TODO: https://github.com/facebook/rocksdb/pull/3465
    this)

  Closeable
  (close [this]
    (.rocksdb_iter_destroy rocksdb i)))

(defrecord RocksJNRKvSnapshot [^Pointer db ^Pointer read-options ^Pointer snapshot]
  kv/KvSnapshot
  (new-iterator [this]
    (->RocksJNRKvIterator (.rocksdb_create_iterator rocksdb db read-options)))

  Closeable
  (close [_]
    (.rocksdb_readoptions_destroy rocksdb read-options)
    (.rocksdb_release_snapshot rocksdb db snapshot)))

(s/def ::db-options string?)

(s/def ::options (s/keys :req-un [:crux.kv/db-dir]
                         :opt [::db-options]))

(def ^:private rocksdb_lz4_compression 4)

(defrecord RocksJNRKv [db-dir]
  kv/KvStore
  (open [this {:keys [db-dir
                      crux.kv.rocksdb.jnr/db-options] :as options}]
    (s/assert ::options options)
    (let [opts (.rocksdb_options_create rocksdb)
          _ (.rocksdb_options_set_create_if_missing rocksdb opts 1)
          _ (.rocksdb_options_set_compression rocksdb opts rocksdb_lz4_compression)
          errptr (make-array String 1)
          db (try
               (let [db (.rocksdb_open rocksdb
                                       opts
                                       (.getAbsolutePath (doto (io/file db-dir)
                                                           (.mkdirs)))
                                       errptr)]
                 (check-error errptr)
                 db)
               (catch Throwable t
                 (.rocksdb_options_destroy rocksdb opts)
                 (throw t)))
          write-options (.rocksdb_writeoptions_create rocksdb)]
      (.rocksdb_writeoptions_disable_WAL rocksdb write-options 1)
      (assoc this
             :db-dir db-dir
             :db db
             :options opts
             :write-options write-options)))

  (new-snapshot [{:keys [^Pointer db]}]
    (let [snapshot (.rocksdb_create_snapshot rocksdb db)
          read-options (.rocksdb_readoptions_create rocksdb)]
      (.rocksdb_readoptions_set_pin_data rocksdb read-options 1)
      (.rocksdb_readoptions_set_snapshot rocksdb read-options snapshot)
      (->RocksJNRKvSnapshot db
                            read-options
                            snapshot)))

  (store [{:keys [^Pointer db ^Pointer write-options]} kvs]
    (let [wb (.rocksdb_writebatch_create rocksdb)
          errptr (make-array String 1)]
      (try
        (doseq [[^bytes k ^bytes v] kvs]
          (.rocksdb_writebatch_put rocksdb wb k (alength k) v (alength v)))
        (.rocksdb_write rocksdb db write-options wb errptr)
        (finally
          (.rocksdb_writeoptions_destroy rocksdb wb)
          (check-error errptr)))))

  (delete [{:keys [^Pointer db ^Pointer write-options]} ks]
    (let [wb (.rocksdb_writebatch_create rocksdb)
          errptr (make-array String 1)]
      (try
        (doseq [^bytes k ks]
          (.rocksdb_writebatch_delete rocksdb wb k (alength k)))
        (.rocksdb_write rocksdb db write-options wb errptr)
        (finally
          (.rocksdb_writeoptions_destroy rocksdb wb)
          (check-error errptr)))))

  (backup [{:keys [^Pointer db]} dir]
    (let [errptr (make-array String 1)
          checkpoint (try
                       (.rocksdb_checkpoint_object_create rocksdb db errptr)
                       (finally
                         (check-error errptr)))]
      (try
        (.rocksdb_checkpoint_create rocksdb checkpoint (str dir) 0 errptr)
        (finally
          (.rocksdb_checkpoint_object_destroy rocksdb checkpoint)
          (check-error errptr)))))

  (count-keys [{:keys [^Pointer db]}]
    (-> (.rocksdb_property_value rocksdb db "rocksdb.estimate-num-keys")
        (Long/parseLong)))

  (db-dir [_]
    (str db-dir))

  (kv-name [this]
    (.getName (class this)))

  Closeable
  (close [{:keys [^Pointer db ^Pointer options ^Pointer write-options]}]
    (.rocksdb_close rocksdb db)
    (.rocksdb_options_destroy rocksdb options)
    (.rocksdb_writeoptions_destroy rocksdb write-options)))

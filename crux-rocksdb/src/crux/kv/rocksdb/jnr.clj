(ns crux.kv.rocksdb.jnr
  "RocksDB KV backend for Crux using JNR:
  https://github.com/jnr/jnr-ffi"
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.kv.rocksdb.loader]
            [crux.memory :as mem]
            [crux.lru :as lru])
  (:import java.io.Closeable
           [org.agrona DirectBuffer MutableDirectBuffer ExpandableDirectByteBuffer]
           org.agrona.concurrent.UnsafeBuffer
           [jnr.ffi LibraryLoader Memory NativeType Pointer]))

(set! *unchecked-math* :warn-on-boxed)

(definterface RocksDB
  (^jnr.ffi.Pointer rocksdb_options_create [])
  (^void rocksdb_options_set_create_if_missing [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} opt
                                                ^byte v])
  (^void rocksdb_options_set_compression [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} opt
                                          ^int t])
  (^void rocksdb_options_destroy [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} opt])

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

  (^jnr.ffi.Pointer rocksdb_get_pinned [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} db
                                        ^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} options
                                        ^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} key
                                        ^{jnr.ffi.types.size_t true  :tag long} keylen
                                        ^{jnr.ffi.annotations.Out true :tag "[Ljava.lang.String;"} errptr])
  (^jnr.ffi.Pointer rocksdb_pinnableslice_value [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} v
                                                 ^{jnr.ffi.annotations.Out true :tag jnr.ffi.Pointer} vlen])
  (^void rocksdb_pinnableslice_destroy [^{jnr.ffi.annotations.In true :tag jnr.ffi.Pointer} v])


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

(defn- load-rocksdb-native-lib ^crux.kv.rocksdb.jnr.RocksDB []
  (.load (LibraryLoader/create RocksDB) crux.kv.rocksdb.loader/rocksdb-library-path))

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

(defrecord RocksJNRKvSnapshot [^Pointer db ^Pointer read-options ^Pointer snapshot pinned]
  kv/KvSnapshot
  (new-iterator [this]
    (->RocksJNRKvIterator (.rocksdb_create_iterator rocksdb db read-options)
                          (ExpandableDirectByteBuffer.)
                          (Memory/allocateTemporary rt NativeType/ULONG)))

  (get-value [this k]
    (let [k (mem/->off-heap k)
          errptr-out (make-array String 1)
          p (.rocksdb_get_pinned rocksdb db read-options (buffer->pointer k) (.capacity k) errptr-out)]
      (try
        (check-error errptr-out)
        (catch Throwable t
          (some->> p (.rocksdb_pinnableslice_destroy rocksdb))
          (throw t)))
      (when p
        (swap! pinned conj p)
        (let [len-out (Memory/allocateTemporary rt NativeType/ULONG)]
          (-> (.rocksdb_pinnableslice_value rocksdb p len-out)
              (pointer+len->buffer len-out))))))

  Closeable
  (close [_]
    (doseq [p @pinned]
      (.rocksdb_pinnableslice_destroy rocksdb p))
    (.rocksdb_readoptions_destroy rocksdb read-options)
    (.rocksdb_release_snapshot rocksdb db snapshot)))

(def ^:private rocksdb_lz4_compression 4)

(defrecord RocksJNRKv [db-dir]
  kv/KvStore
  (open [this {:keys [crux.kv/db-dir crux.kv/sync? crux.kv.rocksdb.jnr/db-options crux.kv.rocksdb.jnr/disable-wal?] :as options}]
    (init-rocksdb-jnr!)
    (let [opts (.rocksdb_options_create rocksdb)
          _ (.rocksdb_options_set_create_if_missing rocksdb opts 1)
          _ (.rocksdb_options_set_compression rocksdb opts rocksdb_lz4_compression)
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
          write-options (.rocksdb_writeoptions_create rocksdb)]
      (when sync?
        (.rocksdb_writeoptions_set_sync rocksdb write-options 1))
      (when disable-wal?
        (.rocksdb_writeoptions_disable_WAL rocksdb write-options 1))
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
                            snapshot
                            (atom []))))

  (store [{:keys [^Pointer db ^Pointer write-options]} kvs]
    (let [wb (.rocksdb_writebatch_create rocksdb)
          errptr-out (make-array String 1)
          kb (ExpandableDirectByteBuffer.)
          vb (ExpandableDirectByteBuffer.)]
      (try
        (doseq [[k v] kvs
                :let [k (mem/ensure-off-heap k kb)
                      v (mem/ensure-off-heap v vb)]]
          (.rocksdb_writebatch_put rocksdb wb (buffer->pointer k) (.capacity k) (buffer->pointer v) (.capacity v)))
        (.rocksdb_write rocksdb db write-options wb errptr-out)
        (finally
          (.rocksdb_writeoptions_destroy rocksdb wb)
          (check-error errptr-out)))))

  (delete [{:keys [^Pointer db ^Pointer write-options]} ks]
    (let [wb (.rocksdb_writebatch_create rocksdb)
          errptr-out (make-array String 1)
          kb (ExpandableDirectByteBuffer.)]
      (try
        (doseq [k ks
                :let [k (mem/ensure-off-heap k kb)]]
          (.rocksdb_writebatch_delete rocksdb wb (buffer->pointer k) (.capacity k)))
        (.rocksdb_write rocksdb db write-options wb errptr-out)
        (finally
          (.rocksdb_writeoptions_destroy rocksdb wb)
          (check-error errptr-out)))))

  (fsync [{:keys [^Pointer db]}]
    (let [errptr-out (make-array String 1)
          flush-options (.rocksdb_flushoptions_create rocksdb)]
      (try
        (.rocksdb_flushoptions_set_wait rocksdb flush-options 1)
        (.rocksdb_flush rocksdb db flush-options errptr-out)
        (finally
          (.rocksdb_flushoptions_destroy rocksdb flush-options)
          (check-error errptr-out)))))

  (backup [{:keys [^Pointer db]} dir]
    (let [dir (io/file dir)]
      (when (.exists dir)
        (throw (IllegalArgumentException. (str "Directory exists: " (.getAbsolutePath dir)))))
      (let [errptr-out (make-array String 1)
            checkpoint (try
                         (.rocksdb_checkpoint_object_create rocksdb db errptr-out)
                         (finally
                           (check-error errptr-out)))]
        (try
          (.rocksdb_checkpoint_create rocksdb checkpoint (str dir) 0 errptr-out)
          (finally
            (.rocksdb_checkpoint_object_destroy rocksdb checkpoint)
            (check-error errptr-out))))))

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

(def kv {:start-fn (fn [_ {:keys [crux.kv/db-dir] :as options}]
                     (lru/start-kv-store (map->RocksJNRKv {:db-dir db-dir}) options))
         :args (merge lru/options
                      {::db-options {:doc "RocksDB Options"
                                     :crux.config/type :crux.config/string}
                       ::disable-wal? {:doc "Disable Write Ahead Log"
                                       :crux.config/type :crux.config/boolean}})})

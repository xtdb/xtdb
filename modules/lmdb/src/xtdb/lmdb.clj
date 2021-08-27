(ns xtdb.lmdb
  "LMDB KV backend for Crux."
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [xtdb.checkpoint :as cp]
            [xtdb.codec :as c]
            [xtdb.error :as err]
            [xtdb.kv :as kv]
            [xtdb.kv.index-store :as kvi]
            [xtdb.memory :as mem]
            [xtdb.system :as sys])
  (:import clojure.lang.ExceptionInfo
           java.io.Closeable
           [java.nio.file Files Path]
           java.nio.file.attribute.FileAttribute
           java.util.concurrent.locks.StampedLock
           java.util.concurrent.TimeUnit
           org.agrona.concurrent.UnsafeBuffer
           org.agrona.ExpandableDirectByteBuffer
           [org.lwjgl.system MemoryStack MemoryUtil]
           [org.lwjgl.util.lmdb LMDB MDBEnvInfo MDBStat MDBVal]))

(set! *unchecked-math* :warn-on-boxed)

;; Based on
;; https://github.com/LWJGL/lwjgl3/blob/master/modules/samples/src/test/java/org/lwjgl/demo/util/lmdb/LMDBDemo.java

(defn- success? [rc]
  (when-not (= LMDB/MDB_SUCCESS rc)
    (throw (ex-info (LMDB/mdb_strerror rc) {:error rc}))))

(defn- env-set-mapsize [^long env ^long size]
  (success? (LMDB/mdb_env_set_mapsize env size)))

(defn- acquire-write-lock ^long [^StampedLock mapsize-lock]
  (let [stamp (.tryWriteLock mapsize-lock 120 TimeUnit/SECONDS)]
    (assert (pos? stamp) "LMDB write lock timeout")
    stamp))

;; TODO: Note, this has to be done when there are no open
;; transactions. Also, when file reached 4Gb it crashed. MDB_WRITEMAP
;; and MDB_MAPASYNC might solve this, but doesn't allow nested
;; transactions. See: https://github.com/dw/py-lmdb/issues/113
(defn- increase-mapsize [^StampedLock mapsize-lock env ^long factor]
  (let [stamp (acquire-write-lock mapsize-lock)]
    (try
      (with-open [stack (MemoryStack/stackPush)]
        (let [info (MDBEnvInfo/mallocStack stack)]
          (success? (LMDB/mdb_env_info env info))
          (let [new-mapsize (* factor (.me_mapsize info))]
            (log/debug "Increasing mapsize to:" new-mapsize)
            (env-set-mapsize env new-mapsize))))
      (finally
        (.unlock mapsize-lock stamp)))))

(defrecord LMDBTransaction [^long txn close-fn]
  Closeable
  (close [_]
    (try
      (let [rc (LMDB/mdb_txn_commit txn)]
        (when-not (= LMDB/MDB_BAD_TXN rc)
          (success? rc)))
      (finally
        (close-fn)))))

(defn- new-transaction ^xtdb.lmdb.LMDBTransaction [^StampedLock mapsize-lock env flags]
  (let [txn-stamp (.readLock mapsize-lock)]
    (try
      (with-open [stack (MemoryStack/stackPush)]
        (let [pp (.mallocPointer stack 1)
              rc (LMDB/mdb_txn_begin env MemoryUtil/NULL flags pp)]
          (if (= LMDB/MDB_MAP_RESIZED rc)
            (env-set-mapsize env 0)
            (success? rc))
          (->LMDBTransaction (.get pp) #(.unlock mapsize-lock txn-stamp))))
      (catch Throwable t
        (.unlock mapsize-lock txn-stamp)
        (throw t)))))

(defn- env-create []
  (with-open [stack (MemoryStack/stackPush)]
    (let [pp (.mallocPointer stack 1)]
      (success? (LMDB/mdb_env_create pp))
      (.get pp))))

(defn- env-open [^long env dir ^long flags]
  (success? (LMDB/mdb_env_open env
                               (-> (Files/createDirectories ^Path dir (make-array FileAttribute 0))
                                   (.toAbsolutePath)
                                   (str))
                               flags
                               0664)))

(defn- env-set-maxreaders [^long env ^long maxreaders]
  (success? (LMDB/mdb_env_set_maxreaders env maxreaders)))

(defn- env-close [^long env]
  (LMDB/mdb_env_close env))

(defn- env-copy [^long env path]
  (let [file (io/file path)]
    (when (.exists file)
      (throw (err/illegal-arg :directory-exists
                              {::err/message (str "Directory exists: " (.getAbsolutePath file))})))
    (.mkdirs file)
    (success? (LMDB/mdb_env_copy env (.getAbsolutePath file)))))

(defn- dbi-open [mapsize-lock env]
  (with-open [stack (MemoryStack/stackPush)
              tx (new-transaction mapsize-lock env LMDB/MDB_RDONLY)]
    (let [{:keys [^long txn]} tx
          ip (.mallocInt stack 1)
          ^CharSequence name nil]
      (success? (LMDB/mdb_dbi_open txn name 0 ip))
      (.get ip 0))))

(defrecord LMDBCursor [^long cursor]
  Closeable
  (close [_]
    (LMDB/mdb_cursor_close cursor)))

(defn- new-cursor ^xtdb.lmdb.LMDBCursor [dbi txn]
  (with-open [stack (MemoryStack/stackPush)]
    (let [pp (.mallocPointer stack 1)]
      (success? (LMDB/mdb_cursor_open txn dbi pp))
      (->LMDBCursor (.get pp)))))

(defn- cursor->key [cursor ^MDBVal kv ^MDBVal dv flags]
  (let [rc (LMDB/mdb_cursor_get cursor kv dv flags)]
    (when (not= LMDB/MDB_NOTFOUND rc)
      (success? rc)
      (UnsafeBuffer. (.mv_data kv) 0 (.mv_size kv)))))

(defn- cursor-put [mapsize-lock env dbi kvs]
  (with-open [stack (MemoryStack/stackPush)
              tx (new-transaction mapsize-lock env 0)
              cursor (new-cursor dbi (:txn tx))]
    (let [{:keys [cursor]} cursor
          kv (MDBVal/mallocStack stack)
          dv (MDBVal/mallocStack stack)
          kb (ExpandableDirectByteBuffer.)
          vb (ExpandableDirectByteBuffer.)]
      (doseq [[k v] kvs]
        (let [k (mem/ensure-off-heap k kb)
              v (some-> v (mem/ensure-off-heap vb))
              kv (-> kv
                     (.mv_data (MemoryUtil/memByteBuffer (.addressOffset k) (.capacity k)))
                     (.mv_size (.capacity k)))]
          (if v
            (let [dv (.mv_size dv (.capacity v))]
              (success? (LMDB/mdb_cursor_put cursor kv dv LMDB/MDB_RESERVE))
              (.getBytes v 0 (.mv_data dv) (.mv_size dv)))
            (let [rc (LMDB/mdb_del (:txn tx) dbi kv nil)]
              (when-not (= LMDB/MDB_NOTFOUND rc)
                (success? rc)))))))))

(defn- tx-get [dbi ^LMDBTransaction tx k]
  (with-open [stack (MemoryStack/stackPush)]
    (let [k (mem/->off-heap k)
          kv (-> (MDBVal/mallocStack stack)
                 (.mv_data (MemoryUtil/memByteBuffer (.addressOffset k) (.capacity k)))
                 (.mv_size (.capacity k)))
          dv (MDBVal/mallocStack stack)
          rc (LMDB/mdb_get (.txn tx) dbi kv dv)]
      (when-not (= LMDB/MDB_NOTFOUND rc)
        (success? rc)
        (UnsafeBuffer. (.mv_data dv) 0 (.mv_size dv))))))

(def ^:const default-env-flags (bit-or LMDB/MDB_NOTLS
                                       LMDB/MDB_NORDAHEAD))

(def ^:const no-sync-env-flags (bit-or LMDB/MDB_MAPASYNC
                                       LMDB/MDB_NOSYNC
                                       LMDB/MDB_NOMETASYNC))

(defrecord LMDBKvIterator [^LMDBCursor cursor ^LMDBTransaction tx ^MDBVal kv ^MDBVal dv ^ExpandableDirectByteBuffer eb]
  kv/KvIterator
  (seek [_ k]
    (let [k (mem/ensure-off-heap k eb)
          kv (-> kv
                 (.mv_data (MemoryUtil/memByteBuffer (.addressOffset k) (.capacity k)))
                 (.mv_size (.capacity k)))]
      (cursor->key (.cursor cursor) kv dv LMDB/MDB_SET_RANGE)))

  (next [this]
    (cursor->key (.cursor cursor) kv dv LMDB/MDB_NEXT))

  (prev [this]
    (cursor->key (.cursor cursor) kv dv LMDB/MDB_PREV))

  (value [this]
    (UnsafeBuffer. (.mv_data dv) 0 (.mv_size dv)))

  Closeable
  (close [_]
    (.close cursor)))

(defrecord LMDBKvSnapshot [env dbi ^LMDBTransaction tx]
  kv/KvSnapshot
  (new-iterator [_]
    (->LMDBKvIterator (new-cursor dbi (.txn tx))
                      tx
                      (MDBVal/create)
                      (MDBVal/create)
                      (ExpandableDirectByteBuffer.)))

  (get-value [_ k]
    (tx-get dbi tx k))

  Closeable
  (close [_]
    (.close tx)))

(def ^:dynamic ^{:tag 'long} *mapsize-increase-factor* 1)
(def ^:const max-mapsize-increase-factor 32)

(defrecord LMDBKv [db-dir env env-flags dbi ^StampedLock mapsize-lock sync?]
  kv/KvStore
  (new-snapshot [_]
    (let [tx (new-transaction mapsize-lock env LMDB/MDB_RDONLY)]
      (->LMDBKvSnapshot env dbi tx)))

  (store [this kvs]
    (try
      (cursor-put mapsize-lock env dbi kvs)
      (catch ExceptionInfo e
        (if (= LMDB/MDB_MAP_FULL (:error (ex-data e)))
          (binding [*mapsize-increase-factor* (* 2 *mapsize-increase-factor*)]
            (when (> *mapsize-increase-factor* max-mapsize-increase-factor)
              (throw (IllegalStateException. "Too large size of key values to store at once.")))
            (increase-mapsize mapsize-lock env *mapsize-increase-factor*)
            (kv/store this kvs))
          (throw e)))))

  (compact [_])

  (fsync [this]
    (when-not sync?
      (success? (LMDB/mdb_env_sync env true))))

  (count-keys [_]
    (with-open [stack (MemoryStack/stackPush)
                tx (new-transaction mapsize-lock env LMDB/MDB_RDONLY)]
      (let [stat (MDBStat/mallocStack stack)]
        (LMDB/mdb_stat (.txn tx) dbi stat)
        (.ms_entries stat))))

  (db-dir [this]
    (str db-dir))

  (kv-name [this]
    (.getName (class this)))

  cp/CheckpointSource
  (save-checkpoint [this dir]
    (let [tx (kvi/latest-completed-tx this)]
      (env-copy env dir)
      {:tx tx}))

  Closeable
  (close [_]
    (let [stamp (acquire-write-lock mapsize-lock)]
      (try
        (env-close env)
        (finally
          (.unlock mapsize-lock stamp))))))

(def ^:private cp-format
  {:index-version c/index-version
   ::version "3"})

(defn ->kv-store {::sys/deps {:checkpointer (fn [_])}
                  ::sys/args (-> {:db-dir {:doc "Directory to store K/V files"
                                           :required? true
                                           :spec ::sys/path}
                                  :sync? {:doc "Sync the KV store to disk after every write."
                                          :default false
                                          :spec ::sys/boolean}
                                  :env-flags {:doc "LMDB Flags"
                                              :spec ::sys/nat-int}
                                  :env-mapsize {:doc "LMDB Map size"
                                                :spec ::sys/nat-int}
                                  :env-maxreaders {:doc "LMDB Max readers"
                                                   :default 1024
                                                   :spec ::sys/nat-int}})}
  [{:keys [^Path db-dir checkpointer sync? env-flags env-mapsize env-maxreaders]}]

  (some-> checkpointer (cp/try-restore (.toFile db-dir) cp-format))

  (let [env-flags (or env-flags
                      (bit-or default-env-flags
                              (if sync?
                                0
                                no-sync-env-flags)))
        env (env-create)
        mapsize-lock (StampedLock.)]
    (try
      (when env-set-maxreaders
        (env-set-maxreaders env env-maxreaders))
      (env-open env db-dir env-flags)
      (when env-mapsize
        (env-set-mapsize env env-mapsize))
      (let [kv-store (map->LMDBKv {:db-dir db-dir
                                   :env env
                                   :env-flags env-flags
                                   :dbi (dbi-open mapsize-lock env)
                                   :mapsize-lock mapsize-lock
                                   :sync? sync?})]
        (cond-> kv-store
          checkpointer (assoc :cp-job (cp/start checkpointer kv-store {::cp/cp-format cp-format}))))
      (catch Throwable t
        (env-close env)
        (throw t)))))

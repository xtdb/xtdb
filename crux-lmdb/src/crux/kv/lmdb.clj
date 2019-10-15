(ns crux.kv.lmdb
  "LMDB KV backend for Crux."
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.memory :as mem]
            [crux.lru :as lru])
  (:import clojure.lang.ExceptionInfo
           java.io.Closeable
           [org.agrona DirectBuffer MutableDirectBuffer ExpandableDirectByteBuffer]
           org.agrona.concurrent.UnsafeBuffer
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

;; TODO: Note, this has to be done when there are no open
;; transactions. Also, when file reached 4Gb it crashed. MDB_WRITEMAP
;; and MDB_MAPASYNC might solve this, but doesn't allow nested
;; transactions. See: https://github.com/dw/py-lmdb/issues/113
(defn- increase-mapsize [env ^long factor]
  (with-open [stack (MemoryStack/stackPush)]
    (let [info (MDBEnvInfo/mallocStack stack)]
      (success? (LMDB/mdb_env_info env info))
      (let [new-mapsize (* factor (.me_mapsize info))]
        (log/debug "Increasing mapsize to:" new-mapsize)
        (env-set-mapsize env new-mapsize)))))

(defrecord LMDBTransaction [^long txn]
  Closeable
  (close [_]
    (let [rc (LMDB/mdb_txn_commit txn)]
      (when-not (= LMDB/MDB_BAD_TXN rc)
        (success? rc)))))

(defn- new-transaction ^crux.kv.lmdb.LMDBTransaction [env flags]
  (with-open [stack (MemoryStack/stackPush)]
    (let [pp (.mallocPointer stack 1)
          rc (LMDB/mdb_txn_begin env MemoryUtil/NULL flags pp)]
      (if (= LMDB/MDB_MAP_RESIZED rc)
        (env-set-mapsize env 0)
        (success? rc))
      (->LMDBTransaction (.get pp)))))

(defn- env-create []
  (with-open [stack (MemoryStack/stackPush)]
    (let [pp (.mallocPointer stack 1)]
      (success? (LMDB/mdb_env_create pp))
      (.get pp))))

(defn- env-open [^long env dir ^long flags]
  (success? (LMDB/mdb_env_open env
                               (.getAbsolutePath (doto (io/file dir)
                                                   (.mkdirs)))
                               flags
                               0664)))

(defn- env-close [^long env]
  (LMDB/mdb_env_close env))

(defn- env-copy [^long env path]
  (let [file (io/file path)]
    (when (.exists file)
      (throw (IllegalArgumentException. (str "Directory exists: " (.getAbsolutePath file)))))
    (.mkdirs file)
    (success? (LMDB/mdb_env_copy env (.getAbsolutePath file)))))

(defn- dbi-open [env]
  (with-open [stack (MemoryStack/stackPush)
              tx (new-transaction env LMDB/MDB_RDONLY)]
    (let [{:keys [^long txn]} tx
          ip (.mallocInt stack 1)
          ^CharSequence name nil]
      (success? (LMDB/mdb_dbi_open txn name 0 ip))
      (.get ip 0))))

(defrecord LMDBCursor [^long cursor]
  Closeable
  (close [_]
    (LMDB/mdb_cursor_close cursor)))

(defn- new-cursor ^crux.kv.lmdb.LMDBCursor [dbi txn]
  (with-open [stack (MemoryStack/stackPush)]
    (let [pp (.mallocPointer stack 1)]
      (success? (LMDB/mdb_cursor_open txn dbi pp))
      (->LMDBCursor (.get pp)))))

(defn- cursor->key [cursor ^MDBVal kv ^MDBVal dv flags]
  (let [rc (LMDB/mdb_cursor_get cursor kv dv flags)]
    (when (not= LMDB/MDB_NOTFOUND rc)
      (success? rc)
      (UnsafeBuffer. (.mv_data kv) 0 (.mv_size kv)))))

(defn- cursor-put [env dbi kvs]
  (with-open [stack (MemoryStack/stackPush)
              tx (new-transaction env 0)
              cursor (new-cursor dbi (:txn tx))]
    (let [{:keys [cursor]} cursor
          kv (MDBVal/mallocStack stack)
          dv (MDBVal/mallocStack stack)
          kb (ExpandableDirectByteBuffer.)
          vb (ExpandableDirectByteBuffer.)]
      (doseq [[k v] kvs]
        (let [k (mem/ensure-off-heap k kb)
              v (mem/ensure-off-heap v vb)
              kv (-> kv
                     (.mv_data (MemoryUtil/memByteBuffer (.addressOffset k) (.capacity k)))
                     (.mv_size (.capacity k)))
              dv (.mv_size dv (.capacity v))]
          (success? (LMDB/mdb_cursor_put cursor kv dv LMDB/MDB_RESERVE))
          (.getBytes v 0 (.mv_data dv) (.mv_size dv)))))))

;; TODO: Figure out why LMDB crashes if we remove the sort here. The
;; root cause is likely something else.
(defn- tx-delete [env dbi ks]
  (with-open [stack (MemoryStack/stackPush)
              tx (new-transaction env 0)]
    (let [{:keys [^long txn]} tx
          kv (MDBVal/mallocStack stack)
          kb (ExpandableDirectByteBuffer.)]
      (doseq [k ks]
        (let [k (mem/ensure-off-heap k kb)
              kv (-> kv
                     (.mv_data (MemoryUtil/memByteBuffer (.addressOffset k) (.capacity k)))
                     (.mv_size (.capacity k)))
              rc (LMDB/mdb_del txn dbi kv nil)]
          (when-not (= LMDB/MDB_NOTFOUND rc)
            (success? rc)))))))

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

(defrecord LMDBKv [db-dir env env-flags dbi]
  kv/KvStore
  (open [this {:keys [crux.kv/db-dir crux.kv/sync? crux.kv.lmdb/env-flags] :as options}]
    (let [env-flags (or env-flags
                        (bit-or default-env-flags
                                (if sync?
                                  0
                                  no-sync-env-flags)))
          env (env-create)]
      (try
        (env-open env db-dir env-flags)
        (assoc this
               :db-dir db-dir
               :env env
               :env-flags env-flags
               :dbi (dbi-open env))
        (catch Throwable t
          (env-close env)
          (throw t)))))

  (new-snapshot [_]
    (let [tx (new-transaction env LMDB/MDB_RDONLY)]
      (->LMDBKvSnapshot env dbi tx)))

  (store [this kvs]
    (try
      (cursor-put env dbi kvs)
      (catch ExceptionInfo e
        (if (= LMDB/MDB_MAP_FULL (:error (ex-data e)))
          (binding [*mapsize-increase-factor* (* 2 *mapsize-increase-factor*)]
            (when (> *mapsize-increase-factor* max-mapsize-increase-factor)
              (throw (IllegalStateException. "Too large size of key values to store at once.")))
            (increase-mapsize env *mapsize-increase-factor*)
            (kv/store this kvs))
          (throw e)))))

  (delete [this ks]
    (try
      (tx-delete env dbi ks)
      (catch ExceptionInfo e
        (if (= LMDB/MDB_MAP_FULL (:error (ex-data e)))
          (binding [*mapsize-increase-factor* (* 2 *mapsize-increase-factor*)]
            (when (> *mapsize-increase-factor* max-mapsize-increase-factor)
              (throw (IllegalStateException. "Too large size of keys to delete at once.")))
            (increase-mapsize env *mapsize-increase-factor*)
            (kv/delete this ks))
          (throw e)))))

  (fsync [this]
    (success? (LMDB/mdb_env_sync env true)))

  (backup [_ dir]
    (env-copy env dir))

  (count-keys [_]
    (with-open [stack (MemoryStack/stackPush)
                tx (new-transaction env LMDB/MDB_RDONLY)]
      (let [stat (MDBStat/mallocStack stack)]
        (LMDB/mdb_stat (.txn tx) dbi stat)
        (.ms_entries stat))))

  (db-dir [this]
    (str db-dir))

  (kv-name [this]
    (.getName (class this)))

  Closeable
  (close [_]
    (env-close env)))

(def kv {:start-fn (fn [_ {:keys [crux.kv/db-dir] :as options}]
                     (lru/start-kv-store (map->LMDBKv {:db-dir db-dir}) options))
         :args (merge lru/options
                      {::env-flags {:doc "LMDB Flags"
                                    :crux.config/type :crux.config/nat-int}})})

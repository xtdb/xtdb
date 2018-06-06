(ns crux.lmdb
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [crux.byte-utils :as bu]
            [crux.io :as cio]
            [crux.kv-store :as ks])
  (:import [clojure.lang ExceptionInfo MapEntry]
           java.io.Closeable
           [org.lwjgl.system MemoryStack MemoryUtil]
           [org.lwjgl.util.lmdb LMDB MDBEnvInfo MDBVal]))

;; Based on
;; https://github.com/LWJGL/lwjgl3/blob/master/modules/samples/src/test/java/org/lwjgl/demo/util/lmdb/LMDBDemo.java

(defn- success? [rc]
  (when-not (= LMDB/MDB_SUCCESS rc)
    (throw (ex-info (LMDB/mdb_strerror rc) {:error rc}))))

(defn- env-set-mapsize [^long env ^long size]
  (success? (LMDB/mdb_env_set_mapsize env size)))

(defn- increase-mapsize [env ^long factor]
  (with-open [stack (MemoryStack/stackPush)]
    (let [info (MDBEnvInfo/callocStack stack)]
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

(defn- ^LMDBTransaction new-transaction [^MemoryStack stack env flags]
  (let [pp (.mallocPointer stack 1)
        rc (LMDB/mdb_txn_begin env MemoryUtil/NULL flags pp)]
    (if (= LMDB/MDB_MAP_RESIZED rc)
      (env-set-mapsize env 0)
      (success? rc))
    (->LMDBTransaction (.get pp))))

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
              tx (new-transaction stack env LMDB/MDB_RDONLY)]
    (let [{:keys [^long txn]} tx
          ip (.mallocInt stack 1)
          ^CharSequence name nil]
      (success? (LMDB/mdb_dbi_open txn  name 0 ip))
      (.get ip 0))))

(defrecord LMDBCursor [^long cursor]
  Closeable
  (close [_]
    (LMDB/mdb_cursor_close cursor)))

(defn- ^LMDBCursor new-cursor [^MemoryStack stack dbi txn]
  (with-open [stack (.push stack)]
    (let [pp (.mallocPointer stack 1)]
      (success? (LMDB/mdb_cursor_open txn dbi pp))
      (->LMDBCursor (.get pp)))))

(defn- cursor->kv [cursor ^MDBVal kv ^MDBVal dv flags]
  (let [rc (LMDB/mdb_cursor_get cursor kv dv flags)]
    (when (not=  LMDB/MDB_NOTFOUND rc)
      (success? rc)
      (MapEntry. (bu/byte-buffer->bytes (.mv_data kv))
                 (bu/byte-buffer->bytes (.mv_data dv))))))

(defn- cursor-put [env dbi kvs]
  (with-open [stack (MemoryStack/stackPush)
              tx (new-transaction stack env 0)
              cursor (new-cursor stack dbi (:txn tx))]
    (let [{:keys [cursor]} cursor
          kv (MDBVal/callocStack stack)
          dv (MDBVal/callocStack stack)]
      (doseq [[^bytes k ^bytes v] (sort-by first bu/bytes-comparator kvs)]
        (with-open [stack (.push stack)]
          (let [kb (.flip (.put (.malloc stack (alength k)) k))
                kv (.mv_data kv kb)
                dv (.mv_size dv (alength v))]
            (success? (LMDB/mdb_cursor_put cursor kv dv LMDB/MDB_RESERVE))
            (.put (.mv_data dv) v)))))))

(defn- tx-delete [env dbi ks]
  (with-open [stack (MemoryStack/stackPush)
              tx (new-transaction stack env 0)]
    (let [{:keys [^long txn]} tx
          kv (MDBVal/callocStack stack)]
      (doseq [^bytes k (sort bu/bytes-comparator ks)]
        (with-open [stack (.push stack)]
          (let [kb (.flip (.put (.malloc stack (alength k)) k))
                kv (.mv_data kv kb)
                rc (LMDB/mdb_del txn dbi kv nil)]
            (when-not (= LMDB/MDB_NOTFOUND rc)
              (success? rc))))))))

(def default-env-flags (bit-or LMDB/MDB_NOSYNC
                               LMDB/MDB_NOMETASYNC))

(defrecord LMDBKvIterator [^MemoryStack stack ^LMDBCursor cursor kv dv]
  ks/KvIterator
  (-seek [_ k]
    (with-open [stack (.push stack)]
      (let [k ^bytes k
            kb (.flip (.put (.malloc stack (alength k)) k))
            kv (.mv_data (MDBVal/callocStack stack) kb)]
        (cursor->kv (:cursor cursor) kv dv LMDB/MDB_SET_RANGE))))
  (-next [_]
    (cursor->kv (:cursor cursor) kv dv LMDB/MDB_NEXT))

  Closeable
  (close [_]
    (.close cursor)
    (.close stack)))

(defrecord LMDBKvSnapshot [^MemoryStack stack env dbi ^LMDBTransaction tx]
  ks/KvSnapshot
  (new-iterator [_]
    (let [stack (.push stack)]
      (->LMDBKvIterator stack
                        (new-cursor stack dbi (:txn tx))
                        (MDBVal/callocStack stack)
                        (MDBVal/callocStack stack))))

  Closeable
  (close [_]
    (.close tx)
    (.close stack)))

(defrecord LMDBKv [db-dir env env-flags dbi]
  ks/KvStore
  (open [this]
    (let [env-flags (or env-flags default-env-flags)
          env (env-create)]
      (try
        (env-open env db-dir env-flags)
        (assoc this
               :env env
               :env-flags env-flags
               :dbi (dbi-open env))
        (catch Throwable t
          (env-close env)
          (throw t)))))

  (new-snapshot [_]
    (let [stack (MemoryStack/stackPush)
          tx (new-transaction stack env LMDB/MDB_RDONLY)]
      (->LMDBKvSnapshot stack env dbi tx)))

  (store [_ kvs]
    (try
      (cursor-put env dbi kvs)
      (catch ExceptionInfo e
        (if (= LMDB/MDB_MAP_FULL (:error (ex-data e)))
          (do (increase-mapsize env 2)
              (cursor-put env dbi kvs))
          (throw e)))))

  (delete [_ ks]
    (tx-delete env dbi ks))

  (backup [_ dir]
    (env-copy env dir))

  Closeable
  (close [_]
    (env-close env)))

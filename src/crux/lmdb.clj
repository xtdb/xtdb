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

(defn- with-transaction [env f flags]
  (with-open [stack (MemoryStack/stackPush)]
    (let [pp (.mallocPointer stack 1)
          rc (LMDB/mdb_txn_begin env MemoryUtil/NULL flags pp)]
      (if (= LMDB/MDB_MAP_RESIZED rc)
        (env-set-mapsize env 0)
        (success? rc))
      (let [txn (.get pp)
            result (try
                     (f stack txn)
                     (catch Throwable t
                       (LMDB/mdb_txn_abort txn)
                       (throw t)))]
        (success? (LMDB/mdb_txn_commit txn))
        result))))

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
  (with-transaction
    env
    (fn [^MemoryStack stack ^long txn]
      (let [ip (.mallocInt stack 1)
            ^CharSequence name nil]
        (success? (LMDB/mdb_dbi_open txn name 0 ip))
        (.get ip 0)))
    LMDB/MDB_RDONLY))

(defn- with-cursor [env dbi f txn-flags]
  (with-transaction
    env
    (fn [^MemoryStack stack txn]
      (let [pp (.mallocPointer stack 1)]
        (success? (LMDB/mdb_cursor_open txn dbi pp))
        (let [cursor (.get pp 0)]
          (try
            (f stack cursor)
            (finally
              (LMDB/mdb_cursor_close cursor))))))
    txn-flags))

(defn- cursor->kv [cursor ^MDBVal kv ^MDBVal dv flags]
  (let [rc (LMDB/mdb_cursor_get cursor kv dv flags)]
    (when (not=  LMDB/MDB_NOTFOUND rc)
      (success? rc)
      (MapEntry. (bu/byte-buffer->bytes (.mv_data kv))
                 (bu/byte-buffer->bytes (.mv_data dv))))))

(defn- cursor-iterate [env dbi f]
  (with-cursor env dbi
    (fn [^MemoryStack stack cursor]
      (let [kv (MDBVal/callocStack stack)
            dv (MDBVal/callocStack stack)
            i (reify
                ks/KvIterator
                (-seek [this k]
                  (with-open [stack (.push stack)]
                    (let [k ^bytes k
                          kb (.flip (.put (.malloc stack (alength k)) k))
                          kv (.mv_data (MDBVal/callocStack stack) kb)]
                      (cursor->kv cursor kv dv LMDB/MDB_SET_RANGE))))
                (-next [this]
                  (cursor->kv cursor kv dv LMDB/MDB_NEXT)))]
        (f i)))
    LMDB/MDB_RDONLY))

(defn- cursor-put [env dbi kvs]
  (with-cursor env dbi
    (fn [^MemoryStack stack cursor]
      (let [kv (MDBVal/callocStack stack)
            dv (MDBVal/callocStack stack)]
        (doseq [[^bytes k ^bytes v] (sort-by first bu/bytes-comparator kvs)]
          (with-open [stack (.push stack)]
            (let [kb (.flip (.put (.malloc stack (alength k)) k))
                  kv (.mv_data kv kb)
                  dv (.mv_size dv (alength v))]
              (success? (LMDB/mdb_cursor_put cursor kv dv LMDB/MDB_RESERVE))
              (.put (.mv_data dv) v))))))
    0))

(defn- tx-delete [env dbi ks]
  (with-transaction env
    (fn [^MemoryStack stack txn]
      (let [kv (MDBVal/callocStack stack)]
        (doseq [^bytes k (sort-by bu/bytes-comparator ks)]
          (with-open [stack (.push stack)]
            (let [kb (.flip (.put (.malloc stack (alength k)) k))
                  kv (.mv_data kv kb)
                  rc (LMDB/mdb_del txn dbi kv nil)]
              (when-not (= LMDB/MDB_NOTFOUND rc)
                (success? rc)))))))
    0))

(def default-env-flags (bit-or LMDB/MDB_NOSYNC
                               LMDB/MDB_NOMETASYNC))

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

  (new-snapshot [this]
    (reify
      ks/KvSnapshot
      (new-iterator [this]
        (throw (UnsupportedOperationException.)))

      (iterate-with [this f]
        (cursor-iterate env dbi f))

      Closeable
      (close [_])))

  (store [this kvs]
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

(ns crux.lmdb
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [crux.byte-utils :as bu]
            [crux.kv-store :as ks]
            [crux.io :as cio])
  (:import [java.io Closeable]
           [java.nio ByteBuffer]
           [clojure.lang ExceptionInfo]
           [org.lwjgl.system MemoryStack MemoryUtil]
           [org.lwjgl.util.lmdb LMDB MDBVal MDBEnvInfo]))

;; Based on
;; https://github.com/LWJGL/lwjgl3/blob/master/modules/samples/src/test/java/org/lwjgl/demo/util/lmdb/LMDBDemo.java

(defn success? [rc]
  (when-not (= LMDB/MDB_SUCCESS rc)
    (throw (ex-info (LMDB/mdb_strerror rc) {:error rc}))))

(defn env-set-mapsize [^long env ^long size]
  (success? (LMDB/mdb_env_set_mapsize env size)))

(defn increase-mapsize [env ^long factor]
  (with-open [stack (MemoryStack/stackPush)]
    (let [info (MDBEnvInfo/callocStack stack)]
      (success? (LMDB/mdb_env_info env info))
      (let [new-mapsize (* factor (.me_mapsize info))]
        (log/info "Increasing mapsize to:" new-mapsize)
        (env-set-mapsize env new-mapsize)))))

(defn transaction
  ([env f]
   (transaction env f LMDB/MDB_RDONLY))
  ([env f flags]
   (try
     (with-open [stack (MemoryStack/stackPush)]
       (let [pp (.mallocPointer stack 1)
             rc (LMDB/mdb_txn_begin env MemoryUtil/NULL flags pp)]
         (if (= LMDB/MDB_MAP_RESIZED rc)
           (env-set-mapsize env 0)
           (success? rc))
         (let [txn (.get pp)
               [result
                commit-rc] (try
                             [(f stack txn)
                              (LMDB/mdb_txn_commit txn)]
                             (catch Throwable t
                               (LMDB/mdb_txn_abort txn)
                               (throw t)))]
           (success? commit-rc)
           result)))
     (catch ExceptionInfo e
       (if (= LMDB/MDB_MAP_FULL (:error (ex-data e)))
         (do (increase-mapsize env 2)
             (transaction env f flags))
         (throw e))))))

(defn env-create []
  (with-open [stack (MemoryStack/stackPush)]
    (let [pp (.mallocPointer stack 1)]
      (success? (LMDB/mdb_env_create pp))
      (.get pp))))

(defn env-open [^long env dir ^long flags]
  (success? (LMDB/mdb_env_open env
                               (.getAbsolutePath (doto (io/file dir)
                                                   .mkdirs))
                               flags
                               0664)))

(defn env-close [^long env]
  (LMDB/mdb_env_close env))

(defn env-copy [^long env path]
  (let [file (io/file path)]
    (when (.exists file)
      (throw (IllegalArgumentException. (str "Directory exists: " (.getAbsolutePath file)))))
    (.mkdirs file)
    (success? (LMDB/mdb_env_copy env (.getAbsolutePath file)))))

(defn dbi-open [env]
  (transaction
   env
   (fn [^MemoryStack stack ^long txn]
     (let [ip (.mallocInt stack 1)
           ^CharSequence name nil]
       (success? (LMDB/mdb_dbi_open txn name 0 ip))
       (.get ip 0)))))

(defn get-bytes->bytes ^bytes [^MemoryStack stack txn dbi ^bytes k]
  (let [kb (.flip (.put (.malloc stack (alength k)) k))
        kv (.mv_data (MDBVal/callocStack stack) kb)
        dv (MDBVal/callocStack stack)
        rc (LMDB/mdb_get txn dbi kv dv)]
    (when-not (= LMDB/MDB_NOTFOUND rc)
      (success? rc)
      (bu/byte-buffer->bytes (.mv_data dv)))))

(defn cursor [^MemoryStack stack txn dbi f]
  (let [pp (.mallocPointer stack 1)]
    (success? (LMDB/mdb_cursor_open txn dbi pp))
    (let [cursor (.get pp 0)]
      (try
        (f stack cursor)
        (finally
          (LMDB/mdb_cursor_close cursor))))))

(defn cursor->vec [env dbi ^bytes k pred]
  (transaction
   env
   (fn [stack txn]
     (cursor stack txn dbi
             (fn [^MemoryStack stack cursor]
               (let [kb (.flip (.put (.malloc stack (alength k)) k))
                     kv (.mv_data (MDBVal/callocStack stack) kb)
                     dv (MDBVal/callocStack stack)]
                 (loop [acc []
                        flags LMDB/MDB_SET_RANGE]
                   (let [rc (LMDB/mdb_cursor_get cursor kv dv flags)]
                     (if (= LMDB/MDB_NOTFOUND rc)
                       acc
                       (do (success? rc)
                           (let [k (bu/byte-buffer->bytes (.mv_data kv))]
                             (if (pred k)
                               (recur (conj acc [k (bu/byte-buffer->bytes (.mv_data dv))])
                                      LMDB/MDB_NEXT)
                               acc))))))) )))))

(defn cursor-put [env dbi kvs]
  (transaction
   env
   (fn [stack txn]
     (cursor stack txn dbi
             (fn [^MemoryStack stack cursor]
               (let [kv (MDBVal/callocStack stack)
                     dv (MDBVal/callocStack stack)]
                 (doseq [[^bytes k ^bytes v] (sort-by first bu/bytes-comparator kvs)]
                   (with-open [stack (.push stack)]
                     (let [kb (.flip (.put (.malloc stack (alength k)) k))
                           kv (.mv_data kv kb)
                           dv (.mv_size dv (alength v))])
                     (success? (LMDB/mdb_cursor_put cursor kv dv LMDB/MDB_RESERVE))
                     (.put (.mv_data dv) v)))))))
   0))

(def default-env-flags (bit-or LMDB/MDB_NOSYNC
                               LMDB/MDB_NOMETASYNC))

(defrecord CruxLMDBKv [db-dir env env-flags dbi max-size]
  ks/CruxKvStore
  (open [this]
    (let [env-flags (or env-flags default-env-flags)
          env (env-create)]
      (try
        (env-open env db-dir env-flags)
        (assoc this
               :env env
               :env-flags env-flags
               :max-size max-size
               :dbi (dbi-open env))
        (catch Throwable t
          (env-close env)
          (throw t)))))

  (value [_ k]
    (transaction
     env
     (fn [^MemoryStack stack ^long txn]
       (get-bytes->bytes stack txn dbi k))))

  (seek [_ k]
    (let [keep-going? (atom true)]
      (first (cursor->vec env dbi k (fn [_]
                                      (let [done? @keep-going?]
                                        (reset! keep-going? false)
                                        done?))))))

  (seek-and-iterate [_ key-pred k]
    (cursor->vec env dbi k key-pred))

  (store [_ k v]
    (cursor-put env dbi [[k v]]))

  (store-all! [_ kvs]
    (cursor-put env dbi kvs))

  (destroy [_]
    (cio/delete-dir db-dir))

  (backup [_ dir]
    (env-copy env dir))

  Closeable
  (close [_]
    (env-close env)))

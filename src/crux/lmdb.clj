(ns crux.lmdb
  (:require [clojure.java.io :as io]
            [crux.byte-utils :as bu]
            [crux.kv-store :as ks])
  (:import [java.io Closeable File]
           [java.nio ByteBuffer]
           [org.lwjgl.system MemoryStack MemoryUtil]
           [org.lwjgl.util.lmdb LMDB MDBVal]))

(defn success? [rc]
  (when-not (= LMDB/MDB_SUCCESS rc)
    (throw (IllegalStateException. (LMDB/mdb_strerror rc)))))

(defn transaction [env f]
  (with-open [stack (MemoryStack/stackPush)]
    (let [pp (.mallocPointer stack 1)]
      (success? (LMDB/mdb_txn_begin env MemoryUtil/NULL 0 pp))
      (let [txn (.get pp)
            [result
             error] (try
                      [(f stack txn)
                       (LMDB/mdb_txn_commit txn)]
                      (catch Throwable t
                        (LMDB/mdb_txn_abort txn)
                        (throw t)))]
        (success? error)
        result))))

(defn env-create []
  (with-open [stack (MemoryStack/stackPush)]
    (let [pp (.mallocPointer stack 1)]
      (success? (LMDB/mdb_env_create pp))
      (.get pp))))

(defn env-open [^long env ^File dir]
  (success? (LMDB/mdb_env_open env (.getAbsolutePath dir) 0 0664)))

(defn env-close [^long env]
  (LMDB/mdb_env_close env))

(defn dbi-open [env]
  (transaction
   env
   (fn [^MemoryStack stack ^long txn]
     (let [ip (.mallocInt stack 1)
           ^CharSequence name nil]
       (success? (LMDB/mdb_dbi_open txn name 0 ip))
       (.get ip 0)))))

(defn put-bytes->bytes-in-txn [^MemoryStack stack txn dbi ^bytes k ^bytes v]
  (let [kb (.flip (.put (.malloc stack (alength k)) k))
        kv (.mv_data (MDBVal/callocStack stack) kb)
        vb (.flip (.put (.malloc stack (alength v)) v))
        dv (.mv_data (MDBVal/callocStack stack) vb)]
    (success? (LMDB/mdb_put txn dbi kv dv 0))))

(defn put-bytes->bytes [env dbi ^bytes k ^bytes v]
  (transaction
   env
   (fn [^MemoryStack stack ^long txn]
     (put-bytes->bytes-in-txn stack txn dbi k v))))

(defn get-bytes->bytes-in-txn ^bytes [^MemoryStack stack txn dbi ^bytes k]
  (let [kb (.flip (.put (.malloc stack (alength k)) k))
        kv (.mv_data (MDBVal/callocStack stack) kb)
        dv (MDBVal/callocStack stack)]
    (success? (LMDB/mdb_get txn dbi kv dv))
    (let [bb (.mv_data dv)]
      (doto (byte-array (.remaining bb))
        (->> (.get bb))))))

(defn get-bytes->bytes ^bytes [env dbi ^bytes k]
  (transaction
   env
   (fn [^MemoryStack stack ^long txn]
     (get-bytes->bytes-in-txn stack txn dbi k))))

;; TODO: this should be configurable.
(defn- db-path [db-name]
  (str "/tmp/" (name db-name) ".db"))

(defrecord CruxLMDBKv [db-name db-dir env dbi]
  ks/CruxKvStore
  (open [this]
    (let [env (env-create)]
      (env-open env (or db-dir (io/file (db-path db-name))))
      (assoc this :env env :dbi (dbi-open env))))

  (seek [_ k]
    (get-bytes->bytes env dbi k))

  (seek-and-iterate [_ k upper-bound]
    )

  (seek-and-iterate-bounded [_ k]
    )

  (store [_ k v]
    (put-bytes->bytes env dbi k v))

  (merge! [_ k v]
    (transaction
     env
     (fn [^MemoryStack stack ^long txn]
       (put-bytes->bytes-in-txn
        stack
        txn
        dbi
        k
        (bu/long->bytes (+ (bu/bytes->long (get-bytes->bytes-in-txn stack txn dbi k))
                           (bu/bytes->long v)))))))

  (destroy [this]
    (transaction
     env
     (fn [_ txn]
       (success? (LMDB/mdb_drop txn dbi true)))))

  Closeable
  (close [_]
    (env-close env)))

(defn crux-lmdb-kv [db-name]
  (map->CruxLMDBKv {:db-name db-name}))

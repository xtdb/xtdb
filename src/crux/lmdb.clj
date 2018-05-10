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

(defn transaction
  ([env f]
   (transaction env f LMDB/MDB_RDONLY))
  ([env f flags]
   (with-open [stack (MemoryStack/stackPush)]
     (let [pp (.mallocPointer stack 1)]
       (success? (LMDB/mdb_txn_begin env MemoryUtil/NULL flags pp))
       (let [txn (.get pp)
             [result
              error] (try
                       [(f stack txn)
                        (LMDB/mdb_txn_commit txn)]
                       (catch Throwable t
                         (LMDB/mdb_txn_abort txn)
                         (throw t)))]
         (success? error)
         result)))))

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
     (put-bytes->bytes-in-txn stack txn dbi k v))
   0))

(defn get-bytes->bytes-in-txn ^bytes [^MemoryStack stack txn dbi ^bytes k]
  (let [kb (.flip (.put (.malloc stack (alength k)) k))
        kv (.mv_data (MDBVal/callocStack stack) kb)
        dv (MDBVal/callocStack stack)]
    (success? (LMDB/mdb_get txn dbi kv dv))
    (bu/byte-buffer->bytes (.mv_data dv))))

(defn get-bytes->bytes ^bytes [env dbi ^bytes k]
  (transaction
   env
   (fn [^MemoryStack stack ^long txn]
     (get-bytes->bytes-in-txn stack txn dbi k))))

(defn cursor->vec [env dbi ^bytes k pred]
  (transaction
   env
   (fn [^MemoryStack stack ^long txn]
     (let [pp (.mallocPointer stack 1)]
       (success? (LMDB/mdb_cursor_open txn dbi pp))
       (let [cursor (.get pp 0)
             kb (.flip (.put (.malloc stack (alength k)) k))
             kv (.mv_data (MDBVal/callocStack stack) kb)
             dv (MDBVal/callocStack stack)]
         (try
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
                         acc))))))
           (finally
             (LMDB/mdb_cursor_close cursor))))))))

;; TODO: this should be configurable.
(defn- db-path [db-name]
  (str "/tmp/" (name db-name) ".db"))

(defrecord CruxLMDBKv [db-name db-dir env dbi]
  ks/CruxKvStore
  (open [this]
    (let [env (env-create)]
      (env-open env (or db-dir (doto (io/file (db-path db-name))
                                 .mkdirs)))
      (assoc this :env env :dbi (dbi-open env))))

  (seek [_ k]
    (get-bytes->bytes env dbi k))

  (seek-and-iterate [_ k upper-bound]
    (cursor->vec env dbi k #(neg? (bu/compare-bytes % upper-bound Integer/MAX_VALUE))))

  (seek-and-iterate-bounded [_ k]
    (cursor->vec env dbi k #(zero? (bu/compare-bytes k % (alength ^bytes k)))))

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
                           (bu/bytes->long v)))))
     0))

  (destroy [this]
    (transaction
     env
     (fn [_ txn]
       (success? (LMDB/mdb_drop txn dbi true)))))

  Closeable
  (close [_]
    (env-close env)))

(defn crux-lmdb-kv [db-name]
  (map->CruxLMDBKv {:db-name db-name :attributes (atom nil)}))

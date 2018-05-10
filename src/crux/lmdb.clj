(ns crux.lmdb
  (:require [crux.byte-utils :as bu]
            [crux.kv-store :as ks])
  (:import [java.io File]
           [org.lwjgl.system MemoryStack MemoryUtil]
           [org.lwjgl.util.lmdb LMDB MDBVal]))

(defrecord CruxLMDBKv [db-name db]
  ks/CruxKvStore
  (open [this]
    )

  (seek [{:keys [db]} k]
    )

  (seek-and-iterate [{:keys [db]} k upper-bound]
    )

  (seek-and-iterate-bounded [{:keys [db]} k]
    )

  (store [{:keys [db]} k v])

  (merge! [{:keys [db]} k v])

  (close [_])

  (destroy [this]))

(defn crux-lmdb-kv [db-name]
  (map->CruxLMDBKv {:db-name db-name}))

(defn success? [rc]
  (when-not (= LMDB/MDB_SUCCESS rc)
    (throw (IllegalStateException. (LMDB/mdb_strerror rc)))))

(defn transaction [env f]
  (with-open [s (MemoryStack/stackPush)]
    (let [pp (.mallocPointer s 1)]
      (success? (LMDB/mdb_txn_begin env MemoryUtil/NULL 0 pp))
      (let [txn (.get pp)
            [result
             error] (try
                      [(f s txn)
                       (LMDB/mdb_txn_commit txn)]
                      (catch Throwable t
                        (LMDB/mdb_txn_abort txn)
                        (throw t)))]
        (success? error)
        result))))

(defn env-create []
  (with-open [s (MemoryStack/stackPush)]
    (let [pp (.mallocPointer s 1)]
      (success? (LMDB/mdb_env_create pp))
      (.get pp))))

(defn env-open [^long env ^File dir]
  (success? (LMDB/mdb_env_open env (.getAbsolutePath dir) 0 0664)))

(defn env-close [^long env]
  (LMDB/mdb_env_close env))

(defn dbi-open [env]
  (transaction
   env
   (fn [^MemoryStack s ^long txn]
     (let [ip (.mallocInt s 1)
           ^CharSequence name nil]
       (success? (LMDB/mdb_dbi_open txn name LMDB/MDB_INTEGERKEY ip))
       (.get ip 0)))))

(defn put-int->string [env dbi k v]
  (transaction
   env
   (fn [^MemoryStack s ^long txn]
     (let [kv (.mv_data (MDBVal/callocStack s)
                        (.putInt (.malloc s 4) 0 k))
           dv (.mv_data (MDBVal/callocStack s)
                        (.UTF8 s v false))]
       (success? (LMDB/mdb_put txn dbi kv dv 0))))))

(defn get-int->string [env dbi k]
  (transaction
   env
   (fn [^MemoryStack s ^long txn]
     (let [kv (.mv_data (MDBVal/callocStack s)
                        (.putInt (.malloc s 4) 0 k))
           dv (MDBVal/callocStack s)]
       (success? (LMDB/mdb_get txn dbi kv dv))
       (some-> (.mv_data dv)
               (MemoryUtil/memUTF8))))))

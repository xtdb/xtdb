(ns ^:no-doc xtdb.lmdb.jnr
  "LMDB KV backend for Crux (alternative)."
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.memory :as mem]
            [crux.system :as sys]
            [crux.checkpoint :as cp]
            [crux.kv.index-store :as kvi]
            [crux.codec :as c])
  (:import java.io.Closeable
           java.nio.file.Path
           java.util.concurrent.locks.StampedLock
           org.agrona.ExpandableDirectByteBuffer
           [org.lmdbjava CopyFlags Cursor Dbi DbiFlags DirectBufferProxy Env EnvFlags Env$MapFullException GetOp PutFlags Txn]))

(defrecord LMDBJNRIterator [^Txn tx ^Cursor cursor ^ExpandableDirectByteBuffer eb]
  kv/KvIterator
  (seek [this k]
    (when (.get cursor (mem/ensure-off-heap k eb) GetOp/MDB_SET_RANGE)
      (.key cursor)))
  (next [this]
    (when (.next cursor)
      (.key cursor)))
  (prev [this]
    (when (.prev cursor)
      (.key cursor)))
  (value [this]
    (.val cursor))

  Closeable
  (close [_]
    (.close cursor)))

(defrecord LMDBJNRSnapshot [^Env env ^Dbi dbi ^Txn tx close-fn]
  kv/KvSnapshot
  (new-iterator [_]
    (->LMDBJNRIterator tx (.openCursor dbi tx) (ExpandableDirectByteBuffer.)))

  (get-value [_ k]
    (.get dbi tx (mem/->off-heap k)))

  Closeable
  (close [_]
    (try
      (.close tx)
      (finally
        (close-fn)))))

(def ^:dynamic ^{:tag 'long} *mapsize-increase-factor* 1)
(def ^:const max-mapsize-increase-factor 32)

(def ^:private default-env-flags [EnvFlags/MDB_NOTLS
                                  EnvFlags/MDB_NORDAHEAD])

(def ^:private no-sync-env-flags [EnvFlags/MDB_MAPASYNC
                                  EnvFlags/MDB_NOSYNC
                                  EnvFlags/MDB_NOMETASYNC])

(defn- increase-mapsize [^StampedLock mapsize-lock ^Env env ^long factor]
  (cio/with-write-lock mapsize-lock
    (let [new-mapsize (* factor (.mapSize (.info env)))]
      (log/debug "Increasing mapsize to:" new-mapsize)
      (.setMapSize env new-mapsize))))

(defrecord LMDBJNRKv [db-dir ^Env env ^Dbi dbi ^StampedLock mapsize-lock, cp-job, sync?]
  kv/KvStore
  (new-snapshot [_]
    (let [txn-stamp (.readLock mapsize-lock)]
      (try
        (->LMDBJNRSnapshot env dbi (.txnRead env) #(.unlock mapsize-lock txn-stamp))
        (catch Throwable t
          (.unlock mapsize-lock txn-stamp)
          (throw t)))))

  (store [this kvs]
    (try
      (cio/with-read-lock mapsize-lock
        (with-open [tx (.txnWrite env)]
          (let [kb (ExpandableDirectByteBuffer.)
                vb (ExpandableDirectByteBuffer.)]
            (doseq [[k v] kvs]
              (if v
                (.put dbi tx (mem/ensure-off-heap k kb) (mem/ensure-off-heap v vb) (make-array PutFlags 0))
                (.delete dbi tx (mem/ensure-off-heap k kb))))
            (.commit tx))))
      (catch Env$MapFullException e
        (binding [*mapsize-increase-factor* (* 2 *mapsize-increase-factor*)]
          (when (> *mapsize-increase-factor* max-mapsize-increase-factor)
            (throw (IllegalStateException. "Too large size of keys to store at once.")))
          (increase-mapsize mapsize-lock env *mapsize-increase-factor*)
          (kv/store this kvs)))))

  (fsync [this]
    (when-not sync?
      (.sync env true)))

  (compact [_])

  (count-keys [_]
    (cio/with-read-lock mapsize-lock
      (with-open [tx (.txnRead env)]
        (.entries (.stat dbi tx)))))

  (db-dir [this]
    (str db-dir))

  (kv-name [this]
    (.getName (class this)))

  cp/CheckpointSource
  (save-checkpoint [this dir]
    (let [tx (kvi/latest-completed-tx this)
          file (io/file dir)]
      (when (.exists file)
        (throw (IllegalArgumentException. (str "Directory exists: " (.getAbsolutePath file)))))
      (.mkdirs file)
      (.copy env file (make-array CopyFlags 0))
      {:tx tx}))

  Closeable
  (close [_]
    (cio/with-write-lock mapsize-lock
      (.close env))
    (cio/try-close cp-job)))

(def ^:private cp-format
  {:index-version c/index-version
   ::version "3"})

(defn ->kv-store {::sys/deps {:checkpointer (fn [_])}
                  ::sys/args (merge (-> kv/args
                                        (update :db-dir assoc :required? true, :default "data"))
                                    {:env-flags {:doc "LMDB Flags"
                                                 :spec ::sys/nat-int}
                                     :env-mapsize {:doc "LMDB Map size"
                                                   :spec ::sys/nat-int}
                                     :env-maxreaders {:doc "LMDB Max readers"
                                                      :default 1024
                                                      :spec ::sys/nat-int}})}
  [{:keys [^Path db-dir checkpointer sync? env-flags env-mapsize env-maxreaders]}]
  (let [db-dir (.toFile db-dir)
        _ (some-> checkpointer (cp/try-restore db-dir cp-format))
        env (.open (cond-> (Env/create DirectBufferProxy/PROXY_DB)
                     env-maxreaders (.setMaxReaders env-maxreaders))
                   (doto db-dir (.mkdirs))
                   (into-array EnvFlags (cond-> default-env-flags
                                          (not sync?) (concat no-sync-env-flags))))
        ^String db-name nil]
    (try
      (when env-mapsize
        (.setMapSize env env-mapsize))
      (let [kv-store (map->LMDBJNRKv {:db-dir db-dir
                                      :env env
                                      :dbi (.openDbi env db-name ^"[Lorg.lmdbjava.DbiFlags;" (make-array DbiFlags 0))
                                      :mapsize-lock (StampedLock.)
                                      :sync? sync?})]
        (cond-> kv-store
          checkpointer (assoc :cp-job (cp/start checkpointer kv-store {::cp/cp-format cp-format}))))
      (catch Throwable t
        (.close env)
        (throw t)))))

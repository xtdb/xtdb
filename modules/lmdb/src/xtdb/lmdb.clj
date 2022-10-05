(ns xtdb.lmdb
  "LMDB KV backend for XTDB."
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [xtdb.checkpoint :as cp]
            [xtdb.codec :as c]
            [xtdb.error :as err]
            [xtdb.kv :as kv]
            [xtdb.kv.index-store :as kvi]
            [xtdb.memory :as mem]
            [xtdb.system :as sys]
            [xtdb.io :as xio])
  (:import clojure.lang.ExceptionInfo
           java.io.Closeable
           [java.nio.file Files Path]
           java.nio.file.attribute.FileAttribute
           org.agrona.concurrent.UnsafeBuffer
           org.agrona.ExpandableDirectByteBuffer
           [org.lwjgl.system MemoryStack MemoryUtil]
           [org.lwjgl.util.lmdb LMDB MDBEnvInfo MDBStat MDBVal]
           (clojure.lang IDeref)
           (xtdb MapResizeSync)))

(set! *unchecked-math* :warn-on-boxed)

;; Based on
;; https://github.com/LWJGL/lwjgl3/blob/master/modules/samples/src/test/java/org/lwjgl/demo/util/lmdb/LMDBDemo.java

(defn- success? [rc]
  (when-not (= LMDB/MDB_SUCCESS rc)
    (throw (ex-info (LMDB/mdb_strerror rc) {:error rc}))))

(defn- env-set-mapsize [^long env ^long size]
  (success? (LMDB/mdb_env_set_mapsize env size)))

(defn new-mapsize-sync
  "Provides concurrency control to LMDB access to avoid races/segfault when map resizes are required.

  Occasionally writes to LMDB will encounter a 'MDB_MAP_FULL' error, xtdb responds by increasing the size of the lmdb map, retrying the write.
  This can only be done if there are no open transactions, otherwise the JVM will segfault.

  We track open transactions by acquiring/releasing 'open transaction permits' via (acquire-open-transaction-permit sync), (release-open-transaction-permit sync).

  When a resize is needed the (kv/store) call will acquire an exclusive (resize) lock, ensuring no concurrent transactions,
  new transactions must wait (block) for the resize to finish. See calls of (acquire-map-resize-permit sync), (release-map-resize-permit sync)

  The reason over StampedLock (the original control mechanism) is to provide a fairness/barging policy so that writers are prioritised over readers, and that older readers get a chance to run if under high contention.
  We cannot use a ReentrantReadWriteLock as open transactions lifetimes are not tied to any particular thread.

  NOTE: resize permits must be released on acquiring thread, transaction permits can be released on any thread."
  ^MapResizeSync []
  (MapResizeSync.))

;; see new-mapsize-sync def above
(defn- acquire-map-resize-permit [^MapResizeSync mapsize-sync] (.acquireInterruptibly mapsize-sync -1))
(defn- release-map-resize-permit [^MapResizeSync mapsize-sync] (.release mapsize-sync -1))
(defn- acquire-open-transaction-permit [^MapResizeSync mapsize-sync] (.acquireSharedInterruptibly mapsize-sync 1))
(defn- release-open-transaction-permit [^MapResizeSync mapsize-sync] (.releaseShared mapsize-sync 1))

;; TODO: Note, this has to be done when there are no open
;; transactions. Also, when file reached 4Gb it crashed. MDB_WRITEMAP
;; and MDB_MAPASYNC might solve this, but doesn't allow nested
;; transactions. See: https://github.com/dw/py-lmdb/issues/113
(defn- increase-mapsize [^long env ^long factor]
  (with-open [stack (MemoryStack/stackPush)]
    (let [info (MDBEnvInfo/malloc stack)]
      (success? (LMDB/mdb_env_info env info))
      (let [new-mapsize (* factor (.me_mapsize info))]
        (log/debug "Increasing mapsize to:" new-mapsize)
        (env-set-mapsize env new-mapsize)))))

(defn- txn-close [^long txn]
  (let [rc (LMDB/mdb_txn_commit txn)]
    (when-not (= LMDB/MDB_BAD_TXN rc)
      (success? rc))))

(defn- txn-begin ^long [^long env ^long flags]
  (with-open [stack (MemoryStack/stackPush)]
    (let [pp (.mallocPointer stack 1)
          rc (LMDB/mdb_txn_begin env MemoryUtil/NULL flags pp)]
      (if (= LMDB/MDB_MAP_RESIZED rc)
        (env-set-mapsize env 0)
        (success? rc))
      (.get pp))))

(deftype CloseableBox [obj close-fn]
  IDeref
  (deref [_] obj)
  Closeable
  (close [_] (close-fn obj)))

(defn- closeable ^Closeable [obj close-fn] (->CloseableBox obj close-fn))

(defn- new-transaction-no-lock ^Closeable [env flags]
  (closeable (txn-begin env flags) txn-close))

(defn- new-transaction-owning-lock ^Closeable [mapsize-sync env flags]
  (acquire-open-transaction-permit mapsize-sync)
  (try
    (closeable (txn-begin env flags) #(try (txn-close %) (finally (release-open-transaction-permit mapsize-sync))))
    (catch Throwable t
      (release-open-transaction-permit mapsize-sync)
      (throw t))))

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

(defn- dbi-open [mapsize-sync ^long env]
  (with-open [stack (MemoryStack/stackPush)
              tx (new-transaction-owning-lock mapsize-sync env LMDB/MDB_RDONLY)]
    (let [^long txn @tx
          ip (.mallocInt stack 1)
          ^CharSequence name nil]
      (success? (LMDB/mdb_dbi_open txn name 0 ip))
      (.get ip 0))))

(defn cursor-close [^long cursor] (LMDB/mdb_cursor_close cursor))

(defn- cursor-open ^long [^long dbi ^long txn]
  (with-open [stack (MemoryStack/stackPush)]
    (let [pp (.mallocPointer stack 1)]
      (success? (LMDB/mdb_cursor_open txn dbi pp))
      (.get pp))))

(defn- cursor->key [^long cursor ^MDBVal kv ^MDBVal dv flags]
  (let [rc (LMDB/mdb_cursor_get cursor kv dv flags)]
    (when (not= LMDB/MDB_NOTFOUND rc)
      (success? rc)
      (UnsafeBuffer. (.mv_data kv) 0 (.mv_size kv)))))

(defn- cursor-put [env dbi mapsize-sync kvs]
  (with-open [stack (MemoryStack/stackPush)
              ;; if we are writing under an exclusive lock during a map resize, mapsize-sync is nil.
              tx (if mapsize-sync
                   (new-transaction-owning-lock mapsize-sync env 0)
                   (new-transaction-no-lock env 0))
              cursor (closeable (cursor-open dbi @tx) cursor-close)]
    (let [kv (MDBVal/malloc stack)
          dv (MDBVal/malloc stack)
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
              (success? (LMDB/mdb_cursor_put @cursor kv dv LMDB/MDB_RESERVE))
              (.getBytes v 0 (.mv_data dv) (.mv_size dv)))
            (let [rc (LMDB/mdb_del @tx dbi kv nil)]
              (when-not (= LMDB/MDB_NOTFOUND rc)
                (success? rc)))))))))

(defn- tx-get [dbi txn k]
  (with-open [stack (MemoryStack/stackPush)]
    (let [k (mem/->off-heap k)
          kv (-> (MDBVal/malloc stack)
                 (.mv_data (MemoryUtil/memByteBuffer (.addressOffset k) (.capacity k)))
                 (.mv_size (.capacity k)))
          dv (MDBVal/malloc stack)
          rc (LMDB/mdb_get txn dbi kv dv)]
      (when-not (= LMDB/MDB_NOTFOUND rc)
        (success? rc)
        (UnsafeBuffer. (.mv_data dv) 0 (.mv_size dv))))))

(def ^:const default-env-flags (bit-or LMDB/MDB_NOTLS
                                       LMDB/MDB_NORDAHEAD))

(def ^:const no-sync-env-flags (bit-or LMDB/MDB_MAPASYNC
                                       LMDB/MDB_NOSYNC
                                       LMDB/MDB_NOMETASYNC))

(defrecord LMDBKvIterator [^long cursor ^MDBVal kv ^MDBVal dv ^ExpandableDirectByteBuffer eb]
  kv/KvIterator
  (seek [_ k]
    (let [k (mem/ensure-off-heap k eb)
          kv (-> kv
                 (.mv_data (MemoryUtil/memByteBuffer (.addressOffset k) (.capacity k)))
                 (.mv_size (.capacity k)))]
      (cursor->key cursor kv dv LMDB/MDB_SET_RANGE)))

  (next [this]
    (cursor->key cursor kv dv LMDB/MDB_NEXT))

  (prev [this]
    (cursor->key cursor kv dv LMDB/MDB_PREV))

  (value [this]
    (UnsafeBuffer. (.mv_data dv) 0 (.mv_size dv)))

  Closeable
  (close [_] (cursor-close cursor)))

(defrecord LMDBKvSnapshot [^long env ^long dbi closeable-txn]
  kv/KvSnapshot
  (new-iterator [_]
    (->LMDBKvIterator (cursor-open dbi @closeable-txn)
                      (MDBVal/create)
                      (MDBVal/create)
                      (ExpandableDirectByteBuffer.)))

  (get-value [_ k]
    (tx-get dbi @closeable-txn k))

  Closeable
  (close [_] (.close ^Closeable closeable-txn)))

(def ^:dynamic ^{:tag 'long} *mapsize-increase-factor* 1)
(def ^:const max-mapsize-increase-factor 32)

(defn- put-if-map-not-full [env dbi mapsize-sync kvs]
  (try
    (cursor-put env dbi mapsize-sync kvs)
    true
    (catch ExceptionInfo e
      (if (= LMDB/MDB_MAP_FULL (:error (ex-data e)))
        false
        (throw e)))))

(defn- put-resizing-if-map-full [env dbi kvs]
  ;; assume we have exclusive access if resizing, nil signals to the (put-if-map-not-full) to use no lock.
  (when-not (put-if-map-not-full env dbi nil kvs)
    (binding [*mapsize-increase-factor* (* 2 *mapsize-increase-factor*)]
      (when (> *mapsize-increase-factor* max-mapsize-increase-factor)
        (throw (IllegalStateException. "Too large size of key values to store at once.")))
      (increase-mapsize env *mapsize-increase-factor*)
      ;; non-tail recur to carry binding for max-mapsize-increase error
      (put-resizing-if-map-full env dbi kvs))))

(defrecord LMDBKv [db-dir env env-flags dbi mapsize-sync sync? cp-job]
  kv/KvStore
  (store [this kvs]
    (when-not (put-if-map-not-full env dbi mapsize-sync kvs)
      ;; on MDB_MAP_FULL we must ensure no open transactions to resize, or segfault - see (new-mapsize-sync)
      (acquire-map-resize-permit mapsize-sync)
      (try
        (put-resizing-if-map-full env dbi kvs)
        (finally
          (release-map-resize-permit mapsize-sync)))))

  (new-snapshot [_]
    (->LMDBKvSnapshot env dbi (new-transaction-owning-lock mapsize-sync env LMDB/MDB_RDONLY)))

  (compact [_])

  (fsync [this]
    (when-not sync?
      (success? (LMDB/mdb_env_sync env true))))

  (count-keys [_]
    (with-open [stack (MemoryStack/stackPush)
                tx (new-transaction-owning-lock mapsize-sync env LMDB/MDB_RDONLY)]
      (let [stat (MDBStat/malloc stack)]
        (LMDB/mdb_stat @tx dbi stat)
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
    (acquire-map-resize-permit mapsize-sync)
    (try
      (env-close env)
      (finally
        (release-map-resize-permit mapsize-sync)))
    (xio/try-close cp-job)))

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
        mapsize-sync (new-mapsize-sync)]
    (try
      (env-set-maxreaders env env-maxreaders)
      (env-open env db-dir env-flags)
      (when env-mapsize
        (env-set-mapsize env env-mapsize))
      (let [kv-store (map->LMDBKv {:db-dir db-dir
                                   :env env
                                   :env-flags env-flags
                                   :dbi (dbi-open mapsize-sync env)
                                   :mapsize-sync mapsize-sync
                                   :sync? sync?})]
        (cond-> kv-store
                checkpointer (assoc :cp-job (cp/start checkpointer kv-store {::cp/cp-format cp-format}))))
      (catch Throwable t
        (env-close env)
        (throw t)))))

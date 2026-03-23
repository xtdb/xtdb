(ns xtdb.db-catalog
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.error :as err]
            [xtdb.util :as util])
  (:import [java.lang AutoCloseable]
           [java.util HashMap]
           xtdb.api.IndexerConfig
           xtdb.api.Xtdb$Config
           [xtdb.api.log Watchers]
           [xtdb.database Database DatabaseState DatabaseStorage Database$Catalog Database$Config Database$Mode]
           [xtdb.database.proto DatabaseConfig DatabaseConfig$LogCase DatabaseConfig$StorageCase DatabaseMode]
           [xtdb.util MsgIdUtil]))

;; Database components follow a hexagonal architecture pattern:
;;
;; - Storage (::storage): I/O layer - logs, buffer pool, metadata manager
;; - State (::state): In-memory state holders - catalogs (block, table, trie), live index
;; - Services: Orchestration components that depend on storage and state -
;;             LogProcessor, Compactor, GarbageCollector (no explicit aggregate)
;;
;; Dependencies flow inward: Services → State, Services → Storage
;; State will eventually have no I/O dependencies at runtime (only at startup to hydrate).

(defmethod ig/init-key ::allocator [_ {{:keys [allocator]} :base, :keys [db-name]}]
  (util/->child-allocator allocator (format "database/%s" db-name)))

(defmethod ig/halt-key! ::allocator [_ allocator]
  (util/close allocator))

(defmethod ig/expand-key ::state [k opts]
  {k (into {:block-cat (ig/ref :xtdb/block-catalog)
            :table-cat (ig/ref :xtdb/table-catalog)
            :trie-cat (ig/ref :xtdb/trie-catalog)
            :live-index (ig/ref :xtdb.indexer/live-index)}
           opts)})

(defmethod ig/init-key ::state [_ {:keys [db-name block-cat table-cat trie-cat live-index]}]
  (DatabaseState. db-name block-cat table-cat trie-cat live-index))

(defmethod ig/expand-key ::storage [k _opts]
  {k {:source-log (ig/ref :xtdb/source-log)
      :replica-log (ig/ref :xtdb/replica-log)
      :external-source (ig/ref :xtdb/external-source-log)
      :buffer-pool (ig/ref :xtdb/buffer-pool)
      :metadata-manager (ig/ref :xtdb.metadata/metadata-manager)}})

(defmethod ig/init-key ::storage [_ {:keys [source-log replica-log external-source buffer-pool metadata-manager]}]
  (DatabaseStorage. source-log replica-log external-source buffer-pool metadata-manager))

(defmethod ig/expand-key :xtdb/db-catalog [k _]
  {k {:base {:allocator (ig/ref :xtdb/allocator)
             :config (ig/ref :xtdb/config)
             :mem-cache (ig/ref :xtdb.cache/memory)
             :disk-cache (ig/ref :xtdb.cache/disk)
             :meter-registry (ig/ref :xtdb.metrics/registry)
             :log-clusters (ig/ref :xtdb.log/clusters)
             :indexer (ig/ref :xtdb/indexer)
             :compactor (ig/ref :xtdb/compactor)}}})

(defmethod ig/expand-key ::watchers [k opts]
  {k (into {:db-storage (ig/ref ::storage)
            :db-state (ig/ref ::state)}
           opts)})

(defmethod ig/init-key ::watchers [_ {:keys [^DatabaseStorage db-storage, ^DatabaseState db-state]}]
  (let [block-cat (.getBlockCatalog db-state)
        source-msg-id (max (or (.getLatestProcessedMsgId block-cat) -1)
                           (MsgIdUtil/offsetToMsgId (.getEpoch (.getSourceLog db-storage)) -1))]
    (Watchers. source-msg-id source-msg-id)))

(defmethod ig/halt-key! ::watchers [_ ^Watchers watchers]
  (util/close watchers))

(defmethod ig/expand-key ::database [k opts]
  {k (into {:allocator (ig/ref ::allocator)
            :storage (ig/ref ::storage)
            :db-state (ig/ref ::state)
            :watchers (ig/ref ::watchers)
            :compactor-for-db (ig/ref :xtdb.compactor/for-db)}
           opts)})

(defmethod ig/init-key ::database [_ {:keys [allocator ^IndexerConfig indexer-conf db-config storage db-state watchers
                                             compactor-for-db]}]
  (Database. allocator db-config storage db-state (.getEnabled indexer-conf) watchers compactor-for-db))

(defn single-writer? []
  (some-> (System/getenv "XTDB_SINGLE_WRITER") Boolean/parseBoolean))

(defn- db-system [db-name base ^Database$Config db-config]
  (let [^Xtdb$Config conf (get-in base [:config :config])
        indexer-conf (.getIndexer conf)
        opts {:base base, :db-name db-name, :mode (.getMode db-config), :db-config db-config, :indexer-conf indexer-conf}]
    (-> (cond-> {::allocator opts
                 :xtdb.metadata/metadata-manager opts
                 :xtdb/log (assoc opts :factory (.getLog db-config))
                 :xtdb/source-log opts
                 :xtdb/replica-log (assoc opts :factory (.getLog db-config))
                 :xtdb/external-source-log opts
                 :xtdb/buffer-pool (assoc opts :factory (.getStorage db-config))

                 ::storage opts

                 :xtdb/block-catalog opts
                 :xtdb/table-catalog opts
                 :xtdb/trie-catalog opts
                 :xtdb.indexer/live-index opts
                 ::state opts

                 ::watchers opts

                 :xtdb.indexer/crash-logger opts
                 :xtdb.indexer/for-db opts

                 :xtdb.compactor/for-db opts

                 ::database opts}

          (single-writer?)
          (assoc :xtdb.log.processor/block-uploader (cond-> opts
                                                      (:db-catalog base) (assoc :db-catalog (:db-catalog base)))
                 :xtdb.log/processor (cond-> opts
                                       (:db-catalog base) (assoc :db-catalog (:db-catalog base))))

          (not (single-writer?))
          (assoc :xtdb.log.processor/source (cond-> (assoc opts
                                                            :block-flush-duration (.getFlushDuration indexer-conf))
                                              (:db-catalog base) (assoc :db-catalog (:db-catalog base)))))
        (doto ig/load-namespaces))))

(defn- open-db [db-name base db-config]
  (try
    (-> (db-system db-name base db-config)
        ig/expand
        ig/init)
    (catch clojure.lang.ExceptionInfo e
      (log/debug "Failed to initialize database system" {:db-name db-name, :exception (class e)})
      (when-let [cause (.getCause e)]
        (log/debug "Cause:" {:class (class cause), :message (.getMessage cause)}))
      (when-let [data (ex-data e)]
        (log/debug "Ex-data:" data))
      (try
        (ig/halt! (:system (ex-data e)))
        (catch Throwable t
          (let [^Throwable e (or (ex-cause e) e)]
            (throw (doto e (.addSuppressed t))))))

      (throw (ex-cause e)))))

(defmethod ig/init-key :xtdb/db-catalog [_ {:keys [base]}]
  (util/with-close-on-catch [!dbs (HashMap.)]
    (let [^Xtdb$Config conf (get-in base [:config :config])
          db-cat (reify
                   Database$Catalog
                   (getDatabaseNames [_] (set (keys !dbs)))

                   (databaseOrNull [_ db-name]
                     (::database (.get !dbs db-name)))

                   (attach [_ db-name db-config]
                     (when (.containsKey !dbs db-name)
                       (throw (err/conflict :xtdb/db-exists "Database already exists" {:db-name db-name})))

                     (util/with-close-on-catch [db (try
                                                     (open-db db-name base (or db-config (Database$Config.)))
                                                     (catch Throwable t
                                                       (log/debug "Failed to open database"
                                                                 {:db-name db-name
                                                                  :exception (class t)
                                                                  :message (.getMessage t)})
                                                       (when-let [cause (.getCause t)]
                                                         (log/debug "Cause:" {:class (class cause), :message (.getMessage cause)}))
                                                       (when-let [root-cause (and (.getCause t) (.getCause (.getCause t)))]
                                                         (log/debug "Root cause:" {:class (class root-cause), :message (.getMessage root-cause)}))
                                                       (throw (err/incorrect ::invalid-db-config "Failed to open database"
                                                                             {::err/cause t}))))]
                       (.put !dbs db-name db)
                       (::database db)))

                   (detach [_ db-name]
                     (when (= "xtdb" db-name)
                       (throw (err/incorrect :xtdb/cannot-detach-primary "Cannot detach the primary 'xtdb' database" {:db-name db-name})))

                     (when-not (.containsKey !dbs db-name)
                       (throw (err/not-found :xtdb/no-such-db "Database does not exist" {:db-name db-name})))

                     (when-some [sys (.remove !dbs db-name)]
                       (ig/halt! sys)))

                   AutoCloseable
                   (close [_]
                     (doseq [[_ sys] !dbs]
                       (ig/halt! sys))))]

      (let [xtdb-db-config (cond-> (-> (Database$Config.)
                                       (.log (.getLog conf))
                                       (.storage (.getStorage conf)))
                             (.getReadOnlyDatabases conf) (.mode Database$Mode/READ_ONLY))]
        (util/with-close-on-catch [xtdb-sys (open-db "xtdb" (assoc base :db-catalog db-cat) xtdb-db-config)]
          (.put !dbs "xtdb" xtdb-sys)

          (let [^Database xtdb-db (::database xtdb-sys)]
            (doseq [[db-name ^Database$Config db-config] (-> (.getSecondaryDatabases (.getBlockCatalog xtdb-db))
                                                             (update-vals Database$Config/fromProto))
                    :when (not= db-name "xtdb")]
              (let [db-config (cond-> db-config
                                (.getReadOnlyDatabases conf) (.mode Database$Mode/READ_ONLY))]
                (util/with-close-on-catch [db (open-db db-name base db-config)]
                  (.put !dbs db-name db)))))))

      db-cat)))

(defmethod ig/halt-key! :xtdb/db-catalog [_ db-cat]
  (util/close db-cat))

(defn <-node ^xtdb.database.Database$Catalog [node]
  (:db-cat node))

(defn primary-db ^xtdb.database.Database [node]
  ;; HACK a temporary util to just pull the primary DB out of the node
  ;; chances are the non-test callers of this will need to know which database they're interested in.
  (.getPrimary (<-node node)))

(defn <-DatabaseConfig [^DatabaseConfig conf]
  {:log (condp = (.getLogCase conf)
          DatabaseConfig$LogCase/IN_MEMORY_LOG :memory

          DatabaseConfig$LogCase/LOCAL_LOG
          [:local {:path (.hasPath (.getLocalLog conf))}])

   :storage (condp = (.getStorageCase conf)
              DatabaseConfig$StorageCase/IN_MEMORY_STORAGE
              [:memory {:epoch (.getEpoch (.getInMemoryStorage conf))}]

              DatabaseConfig$StorageCase/LOCAL_STORAGE
              (let [ls (.getLocalStorage conf)]
                [:local {:path (.hasPath ls), :epoch (.getEpoch ls)}]))

   :mode (condp = (.getMode conf)
           DatabaseMode/READ_WRITE :read-write
           DatabaseMode/READ_ONLY :read-only
           :read-write)})

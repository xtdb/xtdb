(ns xtdb.db-catalog
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.error :as err]
            [xtdb.util :as util])
  (:import [java.lang AutoCloseable]
           [java.util HashMap]
           xtdb.api.Xtdb$Config
           [xtdb.database Database DatabaseState DatabaseStorage Database$Catalog Database$Config Database$Mode]
           [xtdb.database.proto DatabaseConfig DatabaseConfig$LogCase DatabaseConfig$StorageCase DatabaseMode]))

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

(defmethod ig/expand-key ::state [k {:keys [db-name]}]
  {k {:db-name db-name
      :block-cat (ig/ref :xtdb/block-catalog)
      :table-cat (ig/ref :xtdb/table-catalog)
      :trie-cat (ig/ref :xtdb/trie-catalog)
      :live-index (ig/ref :xtdb.indexer/live-index)}})

(defmethod ig/init-key ::state [_ {:keys [db-name block-cat table-cat trie-cat live-index]}]
  (DatabaseState. db-name block-cat table-cat trie-cat live-index))

(defmethod ig/expand-key ::storage [k _]
  {k {:source-log (ig/ref :xtdb/source-log)
      :replica-log (ig/ref :xtdb/replica-log)
      :buffer-pool (ig/ref :xtdb/buffer-pool)
      :metadata-manager (ig/ref :xtdb.metadata/metadata-manager)}})

(defmethod ig/init-key ::storage [_ {:keys [source-log replica-log buffer-pool metadata-manager]}]
  (DatabaseStorage. source-log replica-log buffer-pool metadata-manager))

(defmethod ig/expand-key :xtdb/db-catalog [k _]
  {k {:base {:allocator (ig/ref :xtdb/allocator)
             :config (ig/ref :xtdb/config)
             :mem-cache (ig/ref :xtdb.cache/memory)
             :disk-cache (ig/ref :xtdb.cache/disk)
             :meter-registry (ig/ref :xtdb.metrics/registry)
             :log-clusters (ig/ref :xtdb.log/clusters)
             :indexer (ig/ref :xtdb/indexer)
             :compactor (ig/ref :xtdb/compactor)}}})

(defn- db-system [db-name base ^Database$Config db-config]
  (let [^Xtdb$Config conf (get-in base [:config :config])
        indexer-conf (.getIndexer conf)
        mode (.getMode db-config)
        opts {:base base, :db-name db-name}]
    (-> {::allocator opts
         :xtdb/block-catalog opts
         :xtdb/table-catalog opts
         :xtdb/trie-catalog opts
         :xtdb.metadata/metadata-manager opts
         :xtdb/source-log (assoc opts :factory (.getLog db-config) :mode mode)
         :xtdb/replica-log (assoc opts :factory (.getLog db-config) :mode mode)
         :xtdb/buffer-pool (assoc opts :factory (.getStorage db-config) :mode mode)
         :xtdb.indexer/live-index (assoc opts :indexer-conf indexer-conf)
         :xtdb.indexer/crash-logger opts

         ::storage opts
         ::state opts

         :xtdb.tx-source/for-db (assoc opts :tx-source-conf (.getTxSource conf))
         :xtdb.indexer/for-db opts
         :xtdb.compactor/for-db (assoc opts :mode mode)
         :xtdb.log/processor (assoc opts :indexer-conf indexer-conf :mode mode)}
        (doto ig/load-namespaces))))

(defn- open-db [db-name base db-config]
  (let [sys (try
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

                 (throw (ex-cause e))))]
    {:db (Database. (::allocator sys) db-config (::storage sys) (::state sys)
                    (:processor (:xtdb.log/processor sys)) (:xtdb.compactor/for-db sys) (:xtdb.tx-source/for-db sys))
     :sys sys}))

(defmethod ig/init-key :xtdb/db-catalog [_ {:keys [base]}]
  (util/with-close-on-catch [!dbs (HashMap.)]
    (let [^Xtdb$Config conf (get-in base [:config :config])
          db-cat (reify
                   Database$Catalog
                   (getDatabaseNames [_] (set (keys !dbs)))

                   (databaseOrNull [_ db-name]
                     (:db (.get !dbs db-name)))

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
                       (:db db)))

                   (detach [_ db-name]
                     (when (= "xtdb" db-name)
                       (throw (err/incorrect :xtdb/cannot-detach-primary "Cannot detach the primary 'xtdb' database" {:db-name db-name})))
                       
                     (when-not (.containsKey !dbs db-name)
                       (throw (err/not-found :xtdb/no-such-db "Database does not exist" {:db-name db-name})))
                     
                     (when-some [{:keys [sys]} (.remove !dbs db-name)]
                       (ig/halt! sys)))

                   AutoCloseable
                   (close [_]
                     (doseq [[_ {:keys [sys]}] !dbs]
                       (ig/halt! sys))))]

      (let [xtdb-db-config (cond-> (-> (Database$Config.)
                                       (.log (.getLog conf))
                                       (.storage (.getStorage conf)))
                             (.getReadOnlyDatabases conf) (.mode Database$Mode/READ_ONLY))]
        (util/with-close-on-catch [xtdb-db (open-db "xtdb" (assoc base :db-catalog db-cat) xtdb-db-config)]
          (.put !dbs "xtdb" xtdb-db)

          (let [^Database xtdb-db (:db xtdb-db)]
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

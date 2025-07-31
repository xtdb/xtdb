(ns xtdb.db-catalog
  (:require [integrant.core :as ig]
            [xtdb.error :as err]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import [java.lang AutoCloseable]
           [java.util HashMap]
           xtdb.api.Xtdb$Config
           [xtdb.database Database Database$Catalog Database$Config]))

(defprotocol CreateDatabase
  ;; currently just used by the playground to create a new in-memory database
  (create-database [this db-name]))

(defmulti ->log-factory
  #_{:clj-kondo/ignore [:unused-binding]}
  (fn [tag opts]
    (when-let [ns (namespace tag)]
      (doseq [k [(symbol ns)
                 (symbol (str ns "." (name tag)))]]

        (try
          (require k)
          (catch Throwable _))))

    tag))

(defmulti ->storage-factory
  #_{:clj-kondo/ignore [:unused-binding]}
  (fn [tag opts]
    (when-let [ns (namespace tag)]
      (doseq [k [(symbol ns)
                 (symbol (str ns "." (name tag)))]]

        (try
          (require k)
          (catch Throwable _))))

    tag))

(defmethod xtn/apply-config! ::databases [^Xtdb$Config config _ databases]
  (doseq [[db-name {:keys [log storage]}] databases]
    (.database config (str (symbol db-name))
               (cond-> (Database$Config.)
                 log (.log (->log-factory (first log) (second log)))
                 storage (.storage (->storage-factory (first storage) (second storage))))))

  config)

(defmethod ig/init-key ::allocator [_ {{:keys [allocator]} :base, :keys [db-name]}]
  (util/->child-allocator allocator (format "database/%s" db-name)))

(defmethod ig/halt-key! ::allocator [_ allocator]
  (util/close allocator))

(defmethod ig/prep-key ::for-query [_ {:keys [db-name]}]
  {:db-name db-name
   :allocator (ig/ref ::allocator)
   :block-cat (ig/ref :xtdb/block-catalog)
   :table-cat (ig/ref :xtdb/table-catalog)
   :trie-cat (ig/ref :xtdb/trie-catalog)
   :log (ig/ref :xtdb/log)
   :buffer-pool (ig/ref :xtdb/buffer-pool)
   :metadata-manager (ig/ref :xtdb.metadata/metadata-manager)
   :live-index (ig/ref :xtdb.indexer/live-index)})

(defmethod ig/init-key ::for-query [_ {:keys [allocator db-name block-cat table-cat
                                             trie-cat log buffer-pool metadata-manager
                                             live-index]}]
  (Database. db-name allocator block-cat table-cat trie-cat
             log buffer-pool metadata-manager live-index
             live-index ; snap-src
             nil nil))

(defmethod ig/prep-key :xtdb/db-catalog [_ _]
  {:base {:allocator (ig/ref :xtdb/allocator)
          :config (ig/ref :xtdb/config)
          :mem-cache (ig/ref :xtdb.cache/memory)
          :disk-cache (ig/ref :xtdb.cache/disk)
          :meter-registry (ig/ref :xtdb.metrics/registry)
          :log-clusters (ig/ref :xtdb.log/clusters)
          :indexer (ig/ref :xtdb/indexer)
          :compactor (ig/ref :xtdb/compactor)}})

(defn- db-system [db-name base ^Database$Config db-config]
  (let [^Xtdb$Config conf (get-in base [:config :config])
        indexer-conf (.getIndexer conf)
        opts {:base base, :db-name db-name}]
    (-> {::allocator opts
         :xtdb/block-catalog opts
         :xtdb/table-catalog opts
         :xtdb/trie-catalog opts
         :xtdb.metadata/metadata-manager opts
         :xtdb/log (assoc opts :factory (.getLog db-config))
         :xtdb/buffer-pool (assoc opts :factory (.getStorage db-config))
         :xtdb.indexer/live-index (assoc opts :indexer-conf indexer-conf)

         ::for-query opts

         :xtdb.indexer/for-db opts
         :xtdb.compactor/for-db opts
         :xtdb.log/processor (assoc opts :indexer-conf indexer-conf)}
        (doto ig/load-namespaces))))

(defn- open-db [db-name base db-config]
  (let [sys (try
               (-> (db-system db-name base db-config)
                   ig/prep
                   ig/init)
               (catch clojure.lang.ExceptionInfo e
                 (try
                   (ig/halt! (:system (ex-data e)))
                   (catch Throwable t
                     (let [^Throwable e (or (ex-cause e) e)]
                       (throw (doto e (.addSuppressed t))))))

                 (throw (ex-cause e))))]
    {:db (try
           (-> ^Database (::for-query sys)
               (.withComponents (:xtdb.log/processor sys)
                                (:xtdb.compactor/for-db sys)))
           (catch Throwable t
             (ig/halt! sys)
             (throw t)))
     :sys sys}))

(defmethod ig/init-key :xtdb/db-catalog [_ {:keys [base]}]
  (util/with-close-on-catch [!dbs (HashMap.)]
    (let [^Xtdb$Config conf (get-in base [:config :config])
          db-configs (-> (.getDatabases conf)
                         (update-keys (comp util/str->normal-form-str str symbol)))]

      (when-not (get db-configs "xtdb")
        (throw (err/incorrect ::missing-xtdb-database "The 'xtdb' database is required but not found in the configuration.")))

      (doseq [[db-name ^Database$Config db-config] db-configs]
        (util/with-close-on-catch [db (open-db db-name base db-config)]
          (.put !dbs db-name db)))

      (reify
        Database$Catalog
        (getDatabaseNames [_] (set (keys !dbs)))

        (databaseOrNull [_ db-name]
          (:db (.get !dbs db-name)))

        CreateDatabase
        (create-database [_ db-name]
          (util/with-close-on-catch [db (open-db db-name base (Database$Config.))]
            (.put !dbs db-name db)
            (:db db)))

        AutoCloseable
        (close [_]
          (doseq [[_ {:keys [sys]}] !dbs]
            (ig/halt! sys)))))))

(defmethod ig/halt-key! :xtdb/db-catalog [_ db-cat]
  (util/close db-cat))

(defn <-node ^xtdb.database.Database$Catalog [node]
  (:db-cat node))

(defn primary-db ^xtdb.database.Database [node]
  ;; HACK a temporary util to just pull the primary DB out of the node
  ;; chances are the non-test callers of this will need to know which database they're interested in.
  (.getPrimary (<-node node)))

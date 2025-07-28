(ns xtdb.db-catalog
  (:require [integrant.core :as ig]
            [xtdb.error :as err]
            [xtdb.util :as util])
  (:import [java.util HashMap LinkedList]
           (xtdb.api.log Log)
           (xtdb.api.storage Storage)
           xtdb.api.Xtdb$Config
           [xtdb.database Database DatabaseCatalog]
           xtdb.database.proto.DatabaseConfig))

(defmethod ig/init-key ::allocator [_ {{:keys [allocator]} :base, :keys [db-name]}]
  (util/->child-allocator allocator (format "database/%s" db-name)))

(defmethod ig/halt-key! ::allocator [_ allocator]
  (util/close allocator))

(defmethod ig/prep-key ::for-query [_ {:keys [db-name part-idx]}]
  {:db-name db-name
   :part-idx part-idx
   :allocator (ig/ref ::allocator)
   :block-cat (ig/ref :xtdb/block-catalog)
   :table-cat (ig/ref :xtdb/table-catalog)
   :trie-cat (ig/ref :xtdb/trie-catalog)
   :log (ig/ref :xtdb/log)
   :buffer-pool (ig/ref :xtdb/buffer-pool)
   :metadata-manager (ig/ref :xtdb.metadata/metadata-manager)
   :live-index (ig/ref :xtdb.indexer/live-index)})

(defmethod ig/init-key ::for-query [_ {:keys [allocator db-name part-idx block-cat table-cat
                                             trie-cat log buffer-pool metadata-manager
                                             live-index]}]
  (Database. db-name part-idx allocator block-cat table-cat trie-cat
             log buffer-pool metadata-manager live-index
             nil nil))

(defmethod ig/prep-key :xtdb/db-catalog [_ _]
  {:base {:allocator (ig/ref :xtdb/allocator)
          :config (ig/ref :xtdb/config)
          :mem-cache (ig/ref :xtdb.cache/memory)
          :disk-cache (ig/ref :xtdb.cache/disk)
          :meter-registry (ig/ref :xtdb.metrics/registry)
          :indexer (ig/ref :xtdb/indexer)
          :compactor (ig/ref :xtdb/compactor)}})

(defn- db-system [base {:keys [db-name log-conf storage-conf]}]
  (let [^Xtdb$Config conf (get-in base [:config :config])
        indexer-conf (.getIndexer conf)
        opts {:base base, :db-name db-name, :part-idx 0}]
    (-> {::allocator opts
         :xtdb/block-catalog opts
         :xtdb/table-catalog opts
         :xtdb/trie-catalog opts
         :xtdb.metadata/metadata-manager opts
         :xtdb/log (assoc opts :factory log-conf)
         :xtdb/buffer-pool (assoc opts :factory storage-conf)
         :xtdb.indexer/live-index (assoc opts :indexer-conf indexer-conf)

         ::for-query opts

         :xtdb.indexer/for-db opts
         :xtdb.compactor/for-db opts
         :xtdb.log/processor (assoc opts :indexer-conf indexer-conf)}
        (doto ig/load-namespaces))))

(defn- open-db [db-cat base db-config]
  (let [sys (try
              (-> (db-system (assoc base :db-cat db-cat) db-config)
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

(defn <-DatabaseConfig [^DatabaseConfig db-config]
  {:db-name (.getDbName db-config)})

(defmethod ig/init-key :xtdb/db-catalog [_ {:keys [base]}]
  (let [!dbs (HashMap.)]

    (util/with-close-on-catch [db-cat (reify DatabaseCatalog
                                        (getPrimary [this] (first (.databaseOrNull this "xtdb")))

                                        (getDatabaseNames [_] (set (keys !dbs)))

                                        (databaseOrNull [_ db-name]
                                          (some->> (.get !dbs db-name)
                                                   (mapv :db)))

                                        (createDatabase [this db-name]
                                          (when (.databaseOrNull this db-name)
                                            (throw (err/fault ::already-exists (str "Database already exists: " db-name)
                                                              {:db-name db-name})))

                                          (util/with-close-on-catch [db (open-db this base {:db-name db-name
                                                                                            :log-conf (Log/getInMemoryLog)
                                                                                            :storage-conf (Storage/inMemoryStorage)})]
                                            (:db (.put !dbs db-name [db]))
                                            [(:db db)]))

                                        (close [_]
                                          (doseq [[_ dbs] !dbs
                                                  {:keys [sys]} dbs]
                                            (ig/halt! sys))))]

      (let [primary-db (let [^Xtdb$Config conf (get-in base [:config :config])]
                         (open-db db-cat base {:db-name "xtdb"
                                               :log-conf (.getLog conf)
                                               :storage-conf (.getStorage conf)}))]

        (.put !dbs "xtdb" [primary-db])

        (doseq [db (.getDatabases (.getBlockCatalog ^Database (:db primary-db)))
                :let [{:keys [db-name]} (<-DatabaseConfig db)]]
          (.createDatabase db-cat db-name)))

      db-cat)))

(defmethod ig/halt-key! :xtdb/db-catalog [_ db-cat]
  (util/close db-cat))

(defn <-node ^xtdb.database.DatabaseCatalog [node]
  (:db-cat node))

(defn primary-db ^xtdb.database.Database [node]
  ;; HACK a temporary util to just pull the primary DB out of the node
  ;; chances are the non-test callers of this will need to know which database they're interested in.
  (.getPrimary (<-node node)))

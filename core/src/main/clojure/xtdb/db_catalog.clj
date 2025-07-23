(ns xtdb.db-catalog
  (:require [integrant.core :as ig]
            [xtdb.util :as util])
  (:import [java.util HashMap]
           xtdb.api.Xtdb$Config
           [xtdb.database Database DatabaseCatalog]))

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

(defn- db-system [db-name base]
  (let [^Xtdb$Config conf (get-in base [:config :config])
        indexer-conf (.getIndexer conf)
        opts {:base base, :db-name db-name, :part-idx 0}]
    (-> {::allocator opts
         :xtdb/block-catalog opts
         :xtdb/table-catalog opts
         :xtdb/trie-catalog opts
         :xtdb.metadata/metadata-manager opts
         :xtdb/log (assoc opts :factory (.getLog conf))
         :xtdb/buffer-pool (assoc opts :factory (.getStorage conf))
         :xtdb.indexer/live-index (assoc opts :indexer-conf indexer-conf)

         ::for-query opts

         :xtdb.indexer/for-db opts
         :xtdb.compactor/for-db opts
         :xtdb.log/processor (assoc opts :indexer-conf indexer-conf)}
        (doto ig/load-namespaces))))

(defmethod ig/init-key :xtdb/db-catalog [_ {:keys [base]}]
  (let [!dbs (HashMap.)
        sys (try
              (-> (db-system "xtdb" base)
                  ig/prep
                  ig/init)
              (catch clojure.lang.ExceptionInfo e
                (try
                  (ig/halt! (:system (ex-data e)))
                  (catch Throwable t
                    (let [^Throwable e (or (ex-cause e) e)]
                      (throw (doto e (.addSuppressed t))))))

                (throw (ex-cause e))))

        primary (try
                  (-> ^Database (::for-query sys)
                      (.withComponents (:xtdb.log/processor sys)
                                       (:xtdb.compactor/for-db sys)))
                  (catch Throwable t
                    (ig/halt! sys)
                    (throw t)))]

    (.put !dbs "xtdb" [{:db primary, :sys sys}])

    (reify DatabaseCatalog
      (getPrimary [_] primary)

      (getDatabaseNames [_] (set (keys !dbs)))

      (databaseOrNull [_ db-name] (mapv :db (.get !dbs db-name)))

      (close [_]
        (doseq [[_ dbs] !dbs
                {:keys [sys]} dbs]
          (ig/halt! sys))))))

(defmethod ig/halt-key! :xtdb/db-catalog [_ db-cat]
  (util/close db-cat))

(defn <-node ^xtdb.database.DatabaseCatalog [node]
  (:db-cat node))

(defn primary-db ^xtdb.database.Database [node]
  ;; HACK a temporary util to just pull the primary DB out of the node
  ;; chances are the non-test callers of this will need to know which database they're interested in.
  (.getPrimary (<-node node)))

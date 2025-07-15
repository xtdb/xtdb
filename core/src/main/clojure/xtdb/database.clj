(ns xtdb.database
  (:require [integrant.core :as ig]
            [xtdb.util :as util])
  (:import xtdb.api.Xtdb$Config
           [xtdb.database Database]))

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
             nil nil))

(defmethod ig/prep-key :xtdb/database [_ _]
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
        opts {:base base, :db-name db-name}]
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

(defmethod ig/init-key :xtdb/database [_ {:keys [base]}]
  (try
    (let [sys (-> (db-system "xtdb" base)
                  ig/prep
                  ig/init)]

      {:db (-> ^Database (::for-query sys)
               (.withComponents (:xtdb.log/processor sys)
                                (:xtdb.compactor/for-db sys)))
       :sys sys})
    (catch clojure.lang.ExceptionInfo e
      (try
        (ig/halt! (:system (ex-data e)))
        (catch Throwable t
          (let [^Throwable e (or (ex-cause e) e)]
            (throw (doto e (.addSuppressed t))))))
      (throw (ex-cause e)))))

(defmethod ig/resolve-key :xtdb/database [_ {:keys [db]}]
  db)

(defmethod ig/halt-key! :xtdb/database [_ {:keys [sys]}]
  (ig/halt! sys))

(defn <-node ^xtdb.database.Database [node]
  (:db node))

(ns xtdb.indexer.replica-log
  (:require [integrant.core :as ig]
            [xtdb.util :as util])
  (:import [xtdb.database ReplicaIndexer]))

;; Nested integrant sub-system that owns the replica indexer's state and services.
;; The parent db-system injects shared resources (allocator, buffer-pool, db-storage)
;; via child-opts, which override the ig/ref defaults in each component's expand-key.

(defn- replica-system [{:keys [indexer-conf mode tx-source-conf db-catalog] :as opts}]
  (let [child-opts (-> (dissoc opts :indexer-conf :mode :tx-source-conf :db-catalog)
                       ;; compactor::for-db uses :storage, others use :db-storage
                       (assoc :storage (:db-storage opts)))]
    (-> {:xtdb/block-catalog child-opts
         :xtdb/table-catalog child-opts
         :xtdb/trie-catalog child-opts
         :xtdb.indexer/live-index (assoc child-opts :indexer-conf indexer-conf)
         :xtdb.indexer/crash-logger child-opts
         :xtdb.db-catalog/state child-opts
         :xtdb.tx-source/for-db (assoc child-opts :tx-source-conf tx-source-conf)
         :xtdb.indexer/for-db child-opts
         :xtdb.compactor/for-db (assoc child-opts :mode mode)
         :xtdb.log/processor (cond-> (assoc child-opts :indexer-conf indexer-conf :mode mode :tx-source-conf tx-source-conf)
                               db-catalog (assoc :db-catalog db-catalog))}
        (doto ig/load-namespaces))))

(defmethod ig/expand-key :xtdb.indexer/replica-log [k opts]
  {k (into {:allocator (ig/ref :xtdb.db-catalog/allocator)
            :buffer-pool (ig/ref :xtdb/buffer-pool)
            :db-storage (ig/ref :xtdb.db-catalog/storage)}
           opts)})

(defmethod ig/init-key :xtdb.indexer/replica-log [_ opts]
  (let [sys (-> (replica-system opts)
                ig/expand
                ig/init)]
    {:replica-indexer (ReplicaIndexer. (:processor (:xtdb.log/processor sys))
                                       (:xtdb.compactor/for-db sys)
                                       (:xtdb.db-catalog/state sys)
                                       (:xtdb.tx-source/for-db sys))
     :sys sys}))

(defmethod ig/halt-key! :xtdb.indexer/replica-log [_ {:keys [sys]}]
  (ig/halt! sys))

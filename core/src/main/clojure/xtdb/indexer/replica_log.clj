(ns xtdb.indexer.replica-log
  (:require [integrant.core :as ig]
            [xtdb.util :as util])
  (:import [xtdb.database ReplicaIndexer]))

;; Nested integrant sub-system that owns the replica indexer's state and services.
;; The parent db-system injects shared resources (allocator, buffer-pool, db-storage)
;; via child-opts, which override the ig/ref defaults in each component's expand-key.

(defn- replica-system [{:keys [indexer-conf mode tx-source-conf db-catalog db-state] :as opts}]
  (let [child-opts (-> (dissoc opts :indexer-conf :mode :tx-source-conf :db-catalog :db-state)
                       ;; compactor::for-db uses :storage, others use :db-storage
                       (assoc :storage (:db-storage opts)
                              :db-state db-state
                              ;; compactor::for-db uses :state not :db-state
                              :state db-state))]
    (-> {:xtdb.indexer/live-index (assoc child-opts :indexer-conf indexer-conf)
         :xtdb.indexer/crash-logger child-opts
         :xtdb.tx-source/for-db (assoc child-opts :tx-source-conf tx-source-conf)
         :xtdb.indexer/for-db child-opts
         :xtdb.compactor/for-db (assoc child-opts :mode mode)
         :xtdb.log/processor (cond-> (assoc child-opts :indexer-conf indexer-conf :mode mode :tx-source-conf tx-source-conf)
                               db-catalog (assoc :db-catalog db-catalog))
         ::replica-indexer child-opts}
        (doto ig/load-namespaces))))

(defmethod ig/expand-key ::replica-indexer [k opts]
  {k (into {:log-proc (ig/ref :xtdb.log/processor)
            :compactor (ig/ref :xtdb.compactor/for-db)
            :live-index (ig/ref :xtdb.indexer/live-index)
            :tx-source (ig/ref :xtdb.tx-source/for-db)}
           opts)})

(defmethod ig/init-key ::replica-indexer [_ {:keys [log-proc compactor db-state live-index tx-source]}]
  (ReplicaIndexer. log-proc compactor db-state live-index tx-source))

(defmethod ig/expand-key :xtdb.indexer/replica-log [k opts]
  {k (into {:allocator (ig/ref :xtdb.db-catalog/allocator)
            :buffer-pool (ig/ref :xtdb/buffer-pool)
            :db-storage (ig/ref :xtdb.db-catalog/storage)}
           opts)})

(defmethod ig/init-key :xtdb.indexer/replica-log [_ opts]
  (-> (replica-system opts) ig/expand ig/init))

(defmethod ig/resolve-key :xtdb.indexer/replica-log [_ {::keys [replica-indexer]}]
  replica-indexer)

(defmethod ig/halt-key! :xtdb.indexer/replica-log [_ sys]
  (ig/halt! sys))

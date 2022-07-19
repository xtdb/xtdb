(ns core2.snapshot
  (:require core2.api
            [core2.indexer :as idx]
            core2.ingester
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import core2.api.TransactionInstant
           [core2.indexer TransactionIndexer]
           core2.ingester.Ingester
           java.time.Duration
           [java.util.concurrent CompletableFuture TimeUnit]))

(deftype Snapshot [metadata-mgr temporal-mgr buffer-pool
                   ^TransactionIndexer indexer
                   ^TransactionInstant tx])

(definterface ISnapshotFactory
  (^java.util.concurrent.CompletableFuture #_<Snapshot> snapshot [^core2.api.TransactionInstant tx]))

(defmethod ig/prep-key ::snapshot-factory [_ opts]
  (merge {:indexer (ig/ref ::idx/indexer)
          :ingester (ig/ref :core2/ingester)
          :metadata-mgr (ig/ref :core2.metadata/metadata-manager)
          :temporal-mgr (ig/ref :core2.temporal/temporal-manager)
          :buffer-pool (ig/ref :core2.buffer-pool/buffer-pool)}
         opts))

(defmethod ig/init-key ::snapshot-factory [_ {:keys [^TransactionIndexer indexer, ^Ingester ingester, metadata-mgr temporal-mgr buffer-pool]}]
  (reify
    ISnapshotFactory
    (snapshot [_ tx]
      (-> (if tx
            (.awaitTxAsync ingester tx)
            (CompletableFuture/completedFuture (.latestCompletedTx ingester)))
          (util/then-apply (fn [tx]
                             (Snapshot. metadata-mgr temporal-mgr buffer-pool indexer tx)))))))

(defn snapshot-async ^java.util.concurrent.CompletableFuture [^ISnapshotFactory snapshot-factory, tx]
  (-> (if-not (instance? CompletableFuture tx)
        (CompletableFuture/completedFuture tx)
        tx)
      (util/then-compose (fn [tx]
                           (.snapshot snapshot-factory tx)))))

(defn snapshot
  ([^ISnapshotFactory snapshot-factory]
   (snapshot snapshot-factory nil))

  ([^ISnapshotFactory snapshot-factory, tx]
   (snapshot snapshot-factory tx nil))

  ([^ISnapshotFactory snapshot-factory, tx, ^Duration timeout]
   @(-> (snapshot-async snapshot-factory tx)
        (cond-> timeout (.orTimeout (.toMillis timeout) TimeUnit/MILLISECONDS)))))

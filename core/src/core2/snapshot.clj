(ns core2.snapshot
  (:require core2.api
            core2.ingester
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import core2.api.TransactionInstant
           core2.ingester.Ingester
           java.time.Duration
           [java.util.concurrent CompletableFuture TimeUnit]))

(deftype Snapshot [metadata-mgr temporal-mgr watermark-mgr buffer-pool
                   ^TransactionInstant tx])

(definterface ISnapshotFactory
  (^java.util.concurrent.CompletableFuture #_<Snapshot> snapshot [^core2.api.TransactionInstant tx]))

(defmethod ig/prep-key ::snapshot-factory [_ opts]
  (merge {:ingester (ig/ref :core2/ingester)
          :metadata-mgr (ig/ref :core2.metadata/metadata-manager)
          :temporal-mgr (ig/ref :core2.temporal/temporal-manager)
          :watermark-mgr (ig/ref :core2.watermark/watermark-manager)
          :buffer-pool (ig/ref :core2.buffer-pool/buffer-pool)}
         opts))

(defmethod ig/init-key ::snapshot-factory [_ {:keys [^Ingester ingester, metadata-mgr temporal-mgr watermark-mgr buffer-pool]}]
  (reify
    ISnapshotFactory
    (snapshot [_ tx]
      (-> (if tx
            (.awaitTxAsync ingester tx)
            (CompletableFuture/completedFuture (.latestCompletedTx ingester)))
          (util/then-apply (fn [tx]
                             (assert watermark-mgr)
                             (Snapshot. metadata-mgr temporal-mgr watermark-mgr buffer-pool tx)))))))

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

(ns core2.ingester
  (:require core2.api
            [core2.await :as await]
            core2.indexer
            core2.log
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import core2.indexer.TransactionIndexer
           [core2.log Log LogSubscriber]
           java.lang.AutoCloseable
           java.util.concurrent.PriorityBlockingQueue))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface Ingester
  (^core2.api.TransactionInstant latestCompletedTx [])
  (^java.util.concurrent.CompletableFuture #_<TransactionInstant> awaitTxAsync [^core2.api.TransactionInstant tx]))

(defmethod ig/prep-key :core2/ingester [_ opts]
  (-> (merge {:log (ig/ref :core2/log)
              :indexer (ig/ref :core2.indexer/indexer)}
             opts)
      (util/maybe-update :poll-sleep-duration util/->duration)))

(defmethod ig/init-key :core2/ingester [_ {:keys [^Log log ^TransactionIndexer indexer]}]
  (let [!cancel-hook (promise)
        awaiters (PriorityBlockingQueue.)
        !ingester-error (atom nil)]
    (.subscribe log
                (:tx-id (.latestCompletedTx indexer))
                (reify LogSubscriber
                  (onSubscribe [_ cancel-hook]
                    (deliver !cancel-hook cancel-hook))

                  (acceptRecord [_ record]
                    (if (Thread/interrupted)
                      (throw (InterruptedException.))
                      (try
                        (when-let [tx-key (.indexTx indexer (.tx record) (.record record))]
                          (await/notify-tx tx-key awaiters))

                        (catch Throwable e
                          (reset! !ingester-error e)
                          (await/notify-ex e awaiters)
                          (throw e)))))))

    (reify
      Ingester
      (latestCompletedTx [_] (.latestCompletedTx indexer))

      (awaitTxAsync [this tx]
        (await/await-tx-async tx
                              #(or (some-> @!ingester-error throw)
                                   (.latestCompletedTx this))
                              awaiters))

      AutoCloseable
      (close [_]
        (util/try-close @!cancel-hook)))))

(defmethod ig/halt-key! :core2/ingester [_ ingester]
  (util/try-close ingester))

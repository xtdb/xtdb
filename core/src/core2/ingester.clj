(ns core2.ingester
  (:require core2.api
            core2.indexer
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import core2.api.TransactionInstant
           core2.indexer.TransactionIndexer
           [core2.log Log LogSubscriber]
           java.lang.AutoCloseable))

(defmethod ig/prep-key :core2/ingester [_ opts]
  (-> (merge {:log (ig/ref :core2/log)
              :indexer (ig/ref :core2.indexer/indexer)}
             opts)
      (util/maybe-update :poll-sleep-duration util/->duration)))

(defmethod ig/init-key :core2/ingester [_ {:keys [^Log log ^TransactionIndexer indexer]}]
  (let [!cancel-hook (promise)]
    (.subscribe log
                (some-> ^TransactionInstant (.latestCompletedTx indexer) (.tx-id))
                (reify LogSubscriber
                  (onSubscribe [_ cancel-hook]
                    (deliver !cancel-hook cancel-hook))

                  (acceptRecord [_ record]
                    (try
                      (if (Thread/interrupted)
                        (throw (InterruptedException.))
                        (.indexTx indexer (.tx record) (.record record)))
                      (catch Throwable t
                        (.printStackTrace t)
                        (throw t))))))
    @!cancel-hook))

(defmethod ig/halt-key! :core2/ingester [_ ^AutoCloseable cancel-hook]
  (.close cancel-hook))

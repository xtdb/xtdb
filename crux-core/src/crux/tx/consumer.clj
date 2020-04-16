(ns ^:no-doc crux.tx.consumer
  (:require [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.node :as n]
            [crux.tx :as tx]
            [crux.tx.event :as txe]
            [crux.codec :as c])
  (:import java.io.Closeable
           java.time.Duration))

(defn- index-tx-log [{:keys [!error], ::n/keys [tx-log indexer document-store]} {::keys [^Duration poll-sleep-duration]}]
  (log/info "Started tx-consumer")
  (try
    (while true
      (let [consumed-txs? (with-open [tx-log (db/open-tx-log tx-log (::tx/tx-id (db/latest-completed-tx indexer)))]
                            (let [tx-log (iterator-seq tx-log)
                                  consumed-txs? (not (empty? tx-log))]
                              (doseq [tx-log (partition-all 1000 tx-log)]
                                (doseq [doc-hashes (->> tx-log
                                                        (mapcat ::txe/tx-events)
                                                        (mapcat tx/tx-event->doc-hashes)
                                                        (map c/new-id)
                                                        (partition-all 100))]
                                  (db/index-docs indexer (db/fetch-docs document-store doc-hashes))
                                  (when (Thread/interrupted)
                                    (throw (InterruptedException.))))

                                (doseq [{:keys [::txe/tx-events] :as tx} tx-log
                                        :let [tx (select-keys tx [::tx/tx-time ::tx/tx-id])]]
                                  (db/index-tx indexer tx tx-events)
                                  (when (Thread/interrupted)
                                    (throw (InterruptedException.)))))
                              consumed-txs?))]
        (when (Thread/interrupted)
          (throw (InterruptedException.)))
        (when-not consumed-txs?
          (Thread/sleep (.toMillis poll-sleep-duration)))))

    (catch InterruptedException e)
    (catch Throwable e
      (reset! !error e)
      (log/error e "Error consuming transactions")))

  (log/info "Shut down tx-consumer"))

(defrecord TxConsumer [^Thread executor-thread, !error]
  db/TxConsumer
  (consumer-error [_] @!error)

  Closeable
  (close [_]
    (.interrupt executor-thread)
    (.join executor-thread)))

(def tx-consumer
  {:start-fn (fn [deps args]
               (let [!error (atom nil)]
                 (->TxConsumer (doto (Thread. #(index-tx-log (assoc deps :!error !error) args))
                                 (.setName "crux-tx-consumer")
                                 (.start))
                               !error)))
   :deps [::n/indexer ::n/document-store ::n/tx-log]
   :args {::poll-sleep-duration {:default (Duration/ofMillis 100)
                                 :doc "How long to sleep between polling for new transactions"
                                 :crux.config/type :crux.config/duration}}})

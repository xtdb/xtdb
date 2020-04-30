(ns ^:no-doc crux.tx.consumer
  (:require [clojure.tools.logging :as log]
            [crux.io :as cio]
            [crux.db :as db]
            [crux.lru :as lru]
            [crux.node :as n]
            [crux.tx :as tx]
            [crux.tx.event :as txe]
            [crux.codec :as c])
  (:import java.io.Closeable
           [java.util.concurrent Executors ExecutorService TimeUnit]
           java.time.Duration))

(defn- index-tx-log [{:keys [!error tx-log indexer document-store] :as tx-consumer} {::keys [^Duration poll-sleep-duration]}]
  (log/info "Started tx-consumer")
  (try
    (while true
      (let [consumed-txs? (when-let [tx-stream (try
                                                 (db/open-tx-log tx-log (::tx/tx-id (db/latest-completed-tx indexer)))
                                                 (catch InterruptedException e (throw e))
                                                 (catch Exception e
                                                   (log/warn e "Error reading TxLog, will retry")))]
                            (try
                              (let [tx-log-entries (iterator-seq tx-stream)
                                    consumed-txs? (not (empty? tx-log-entries))]
                                (doseq [tx-log-entry (partition-all 1000 tx-log-entries)]
                                  (doseq [doc-hashes (->> tx-log-entry
                                                          (mapcat ::txe/tx-events)
                                                          (mapcat tx/tx-event->doc-hashes)
                                                          (map c/new-id)
                                                          (partition-all 100))
                                          :let [docs (db/fetch-docs document-store doc-hashes)]]
                                    (tx/index-docs tx-consumer docs)
                                    (when (Thread/interrupted)
                                      (throw (InterruptedException.))))

                                  (doseq [{:keys [::txe/tx-events] :as tx} tx-log-entry
                                          :let [tx (select-keys tx [::tx/tx-time ::tx/tx-id])]]
                                    (when-let [{:keys [tombstones]} (tx/index-tx tx-consumer tx tx-events)]
                                      (when (seq tombstones)
                                        (db/submit-docs document-store tombstones)))

                                    (when (Thread/interrupted)
                                      (throw (InterruptedException.)))))
                                consumed-txs?)
                              (finally
                                (.close ^Closeable tx-stream))))]
        (when (Thread/interrupted)
          (throw (InterruptedException.)))
        (when-not consumed-txs?
          (Thread/sleep (.toMillis poll-sleep-duration)))))

    (catch InterruptedException e)
    (catch Throwable e
      (reset! !error e)
      (log/error e "Error consuming transactions")))

  (log/info "Shut down tx-consumer"))

(defrecord TxConsumer [^Thread executor-thread ^ExecutorService stats-executor !error indexer document-store tx-log object-store bus]
  db/TxConsumer
  (consumer-error [_] @!error)

  Closeable
  (close [_]
    (when executor-thread
      (.interrupt executor-thread)
      (.join executor-thread))
    (when stats-executor
      (doto stats-executor
        (.shutdown)
        (.awaitTermination 60000 TimeUnit/MILLISECONDS)))))

(def tx-consumer
  {:start-fn (fn [{::n/keys [indexer document-store tx-log object-store bus query-engine]} args]
               (let [stats-executor (Executors/newSingleThreadExecutor (cio/thread-factory "crux.tx.update-stats-thread"))
                     tx-consumer (map->TxConsumer
                                  {:!error (atom nil)
                                   :indexer indexer
                                   :document-store document-store
                                   :tx-log tx-log
                                   :object-store object-store
                                   :bus bus
                                   :query-engine query-engine
                                   :stats-executor stats-executor})]
                 (assoc tx-consumer
                        :executor-thread
                        (doto (Thread. #(index-tx-log tx-consumer args))
                          (.setName "crux-tx-consumer")
                          (.start)))))
   :deps [::n/indexer ::n/document-store ::n/tx-log ::n/object-store ::n/bus ::n/query-engine]
   :args {::poll-sleep-duration {:default (Duration/ofMillis 100)
                                 :doc "How long to sleep between polling for new transactions"
                                 :crux.config/type :crux.config/duration}}})

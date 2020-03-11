(ns crux.compaction
  (:require [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.node :as n]
            [crux.tx :as tx]
            [crux.kv :as kv]
            [crux.index :as idx])
  (:import java.io.Closeable
           java.time.Duration
           crux.codec.EntityTx))

(defn compact [object-store snapshot eid valid-time tx-time]
  (with-open [i (kv/new-iterator snapshot)]
    ;; These are candidates for killing:
    (doseq [^EntityTx entity-tx (->> (idx/entity-history-seq-descending i eid valid-time tx-time)
                                     (rest))]
      (log/info "Pruning" entity-tx)
      (db/delete-objects object-store [(.content-hash entity-tx)]))))

;; Spiked out plumbing:

(defn- index-tx-log [{:keys [!error] ::n/keys [tx-log indexer document-store]} {::keys [^Duration poll-sleep-duration]}]
  (log/info "Started compactor")
  (try
    (while true
      (let [consumed-txs? (with-open [tx-log (db/open-tx-log tx-log (::tx/tx-id (db/latest-completed-tx indexer)))]
                            (let [tx-log (iterator-seq tx-log)
                                  consumed-txs? (not (empty? tx-log))]
                              (doseq [{:keys [crux.tx.event/tx-events] :as tx} tx-log
                                      :let [tx (select-keys tx [::tx/tx-time ::tx/tx-id])]]

                                ;; What to do here?

                                (when (Thread/interrupted)
                                  (throw (InterruptedException.))))
                              consumed-txs?))]
        (when (Thread/interrupted)
          (throw (InterruptedException.)))
        (when-not consumed-txs?
          (Thread/sleep (.toMillis poll-sleep-duration)))))
    (catch InterruptedException e)
    (catch Exception e
      (reset! !error e)
      (log/error e "Error compacting")))
  (log/info "Shut down tx-compactor"))

(defrecord Compactor [^Thread executor-thread, !error]
  db/TxConsumer
  (consumer-error [_] @!error)
  Closeable
  (close [_]
    (.interrupt executor-thread)
    (.join executor-thread)))

(def compactor
  {:start-fn (fn [deps args]
               (let [!error (atom nil)]
                 (->Compactor (doto (Thread. #(index-tx-log (assoc deps :!error !error) args))
                                (.setName "crux-tx-consumer")
                                (.start))
                              !error)))
   :deps [::n/indexer ::n/document-store ::n/tx-log]
   :args {::poll-sleep-duration {:default (Duration/ofMillis 100)
                                 :doc "How long to sleep between running compaction"
                                 :crux.config/type :crux.config/duration}}})

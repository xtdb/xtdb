(ns crux.tx.consumer
  (:require [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.codec :as c]
            [crux.io :as cio])
  (:import java.util.Date
           java.io.Closeable))

(defprotocol Offsets
  (read-offsets [this])
  (store-offsets [this offsets]))

(defrecord IndexedOffsets [indexer k]
  Offsets
  (read-offsets [this]
    (db/read-index-meta indexer k))
  (store-offsets [this offsets]
    (log/debug "Consumer state:" k (cio/pr-edn-str offsets))
    (db/store-index-meta indexer k offsets)))

(defprotocol Queue
  (next-events [this])
  (mark-processed [this records]))

(defn consume
  [{:keys [queue index-fn]}]
  (let [records (next-events queue)]
    (index-fn records)
    (mark-processed queue records)
    (count records)))

(defn consume-and-block
  [{:keys [queue pending-records-state accept-fn index-fn]}]
  (let [_ (when (empty? @pending-records-state)
            (reset! pending-records-state (next-events queue)))
        records (->> @pending-records-state
                     (take-while accept-fn)
                     (vec))]
    (index-fn records)
    (mark-processed queue records)
    (swap! pending-records-state (comp vec (partial drop (count records))))
    (when (seq @pending-records-state)
      (log/debug "Blocked processing" (count pending-records-state) "records"))
    (count records)))

(defn start-consumer
  ^java.io.Closeable
  [{:keys [idle-sleep-ms queue accept-fn index-fn]}]
  (let [running? (atom true)
        worker-thread
        (doto
            (Thread. ^Runnable (fn []
                                 (try
                                   (while @running?
                                     (let [opts {:queue queue
                                                 :index-fn index-fn}
                                           idle? (if accept-fn
                                                   (consume-and-block (merge opts {:pending-records-state (atom [])
                                                                                   :accept-fn accept-fn}))
                                                   (consume opts))]
                                       (when (and idle-sleep-ms idle?)
                                         (Thread/sleep idle-sleep-ms))))
                                   (catch Throwable t
                                     (log/fatal t "Event log consumer threw exception, consumption has stopped:"))))
                     "crux.tx.consumer-thread")
            (.start))]
    (reify Closeable
      (close [_]
        (reset! running? false)
        (.join worker-thread)))))

(deftype Message [body topic ^long message-id ^Date message-time key headers])

(defprotocol OffsetBasedQueue
  (next-events-from-offset [this offset]))

(defn offsets-based-queue [indexer q]
  (let [offsets (map->IndexedOffsets {:indexer indexer :k :crux.tx-log/consumer-state})]
    (when-not (read-offsets offsets)
      (store-offsets offsets {:crux.tx/event-log {:next-offset 0}}))
    (reify Queue
      (next-events [this]
        (let [next-offset (get-in (read-offsets offsets) [:crux.tx/event-log :next-offset])]
          (next-events-from-offset q next-offset)))
      (mark-processed [this records]
        (when-let [^Message last-msg (last records)]
          (let [next-offset (inc (long (.message-id last-msg)))
                consumer-state {:crux.tx/event-log {:next-offset next-offset}}]
            (store-offsets offsets consumer-state)))))))

(defn index-records [indexer records]
  (let [{doc-msgs :docs, tx-msgs :txs} (->> records (group-by #(:crux.tx/sub-topic (.headers ^Message %))))]
    (when (seq doc-msgs)
      (db/index-docs indexer (->> doc-msgs
                                  (into {} (map (fn [^Message m]
                                                  [(c/new-id (.key m)) (.body m)]))))))
    (doseq [^Message tx-msg tx-msgs]
      (let [tx {:crux.tx/tx-time (.message-time tx-msg)
                :crux.tx/tx-id (.message-id tx-msg)}]
        (db/index-tx indexer tx (.body tx-msg))))
    (not (empty? records))))

(defn start-indexing-consumer
  ^java.io.Closeable
  [{:keys [indexer queue]}]
  (start-consumer {:queue queue
                   :index-fn (partial index-records indexer)
                   :idle-sleep-ms 10}))

(ns crux.tx.polling
  (:require [crux.db :as db]
            [crux.io :as cio]
            [clojure.tools.logging :as log]
            [crux.tx.consumer :as consumer]
            [crux.codec :as c])
  (:import java.util.Date
           crux.tx.consumer.Message
           java.io.Closeable))

(defn- polling-consumer [running? indexer object-store event-log-consumer {:keys [idle-sleep-ms]
                                                                           :or {idle-sleep-ms 10}}]
  (when-not (db/read-index-meta indexer :crux.tx-log/consumer-state)
    (db/store-index-meta
     indexer
     :crux.tx-log/consumer-state {:crux.tx/event-log {:next-offset 0}}))
  (while @running?
    (let [idle? (with-open [context (consumer/new-event-log-context event-log-consumer)]
                  (let [next-offset (get-in (db/read-index-meta indexer :crux.tx-log/consumer-state) [:crux.tx/event-log :next-offset])
                        msgs (consumer/next-events event-log-consumer context next-offset)
                        {doc-msgs :docs, tx-msgs :txs} (->> msgs (group-by #(:crux.tx/sub-topic (.headers ^Message %))))]

                    (when (seq doc-msgs)
                      (db/put-objects object-store (->> doc-msgs
                                                        (into {} (map (fn [^Message m]
                                                                        [(c/new-id (.key m)) (.body m)]))))))

                    (doseq [^Message tx-msg tx-msgs]
                      (let [tx {:crux.tx/tx-time (.message-time tx-msg)
                                :crux.tx/tx-id (.message-id tx-msg)}]
                        (db/index-tx indexer tx (.body tx-msg))))

                    (if-let [^Message last-msg (last msgs)]
                      (let [next-offset (inc (long (.message-id last-msg)))
                            consumer-state {:crux.tx/event-log {:next-offset next-offset}}]
                        (log/debug "Event log consumer state:" (cio/pr-edn-str consumer-state))
                        (db/store-index-meta indexer :crux.tx-log/consumer-state consumer-state)
                        false)

                      true)))]
      (when idle?
        (Thread/sleep idle-sleep-ms)))))

(defn start-event-log-consumer ^java.io.Closeable [indexer object-store event-log-consumer]
  (let [running? (atom true)
        worker-thread (doto (Thread. #(try
                                        (polling-consumer running? indexer object-store event-log-consumer {})
                                        (catch Throwable t
                                          (log/fatal t "Event log consumer threw exception, consumption has stopped:")))
                                     "crux.tx.event-log-consumer-thread")
                        (.start))]
    (reify Closeable
      (close [_]
        (reset! running? false)
        (.join worker-thread)))))

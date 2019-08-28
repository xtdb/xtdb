(ns crux.tx.polling
  (:require [crux.db :as db]
            [clojure.tools.logging :as log]
            [crux.tx.consumer :as consumer])
  (:import java.util.Date
           crux.tx.consumer.Message
           java.io.Closeable))

(defn- polling-consumer [running? indexer event-log-consumer {:keys [idle-sleep-ms]
                                                              :or {idle-sleep-ms 10}}]
  (when-not (db/read-index-meta indexer :crux.tx-log/consumer-state)
    (db/store-index-meta
     indexer
     :crux.tx-log/consumer-state {:crux.tx/event-log {:lag 0
                                                      :next-offset 0
                                                      :time nil}}))
  (while @running?
    (let [idle? (with-open [context (consumer/new-event-log-context event-log-consumer)]
                  (let [next-offset (get-in (db/read-index-meta indexer :crux.tx-log/consumer-state) [:crux.tx/event-log :next-offset])]
                    (if-let [^Message last-message (reduce (fn [last-message ^Message m]
                                                             (case (get (.headers m) :crux.tx/sub-topic)
                                                               :docs
                                                               (db/index-doc indexer (.key m) (.body m))
                                                               :txs
                                                               (db/index-tx indexer
                                                                            (.body m)
                                                                            (.message-time m)
                                                                            (.message-id m)))
                                                             m)
                                                           nil
                                                           (consumer/next-events event-log-consumer context next-offset))]
                      (let [end-offset (consumer/end-offset event-log-consumer)
                            next-offset (inc (long (.message-id last-message)))
                            lag (- end-offset next-offset)
                            _ (when (pos? lag)
                                (log/debug "Falling behind" ::event-log "at:" next-offset "end:" end-offset))
                            consumer-state {:crux.tx/event-log
                                            {:lag lag
                                             :next-offset next-offset
                                             :time (.message-time last-message)}}]
                        (log/debug "Event log consumer state:" (pr-str consumer-state))
                        (db/store-index-meta indexer :crux.tx-log/consumer-state consumer-state)
                        false)
                      true)))]
      (when idle?
        (Thread/sleep idle-sleep-ms)))))

(defn start-event-log-consumer ^java.io.Closeable [indexer event-log-consumer]
  (let [running? (atom true)
        worker-thread (doto (Thread. #(try
                                        (polling-consumer running? indexer event-log-consumer {})
                                        (catch Throwable t
                                          (log/fatal t "Event log consumer threw exception, consumption has stopped:")))
                                     "crux.tx.event-log-consumer-thread")
                        (.start))]
    (reify Closeable
      (close [_]
        (reset! running? false)
        (.join worker-thread)))))

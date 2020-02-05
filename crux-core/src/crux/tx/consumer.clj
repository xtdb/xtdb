(ns crux.tx.consumer
  (:import java.util.Date
           java.io.Closeable))

(deftype Message [body topic ^long message-id ^Date message-time key headers])

(defprotocol PolledEventLog
  (new-event-log-context ^java.io.Closeable [this])

  (next-events [this context next-offset])

  (end-offset [this]))

(defn start-indexing-consumer
  ^java.io.Closeable
  [{:keys [index-fn]}]
  (let [running? (atom true)
        worker-thread
        (doto
            (Thread. ^Runnable (partial index-fn running?) "crux.tx.event-log-consumer-thread")
            (.start))]
    (reify Closeable
      (close [_]
        (reset! running? false)
        (.join worker-thread)))))

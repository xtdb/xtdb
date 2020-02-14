(ns crux.tx.consumer
  (:require [clojure.tools.logging :as log])
  (:import java.util.Date
           java.io.Closeable))

(deftype Message [body topic ^long message-id ^Date message-time key headers])

(defprotocol PolledEventLog
  (next-events [this next-offset]))

(defn start-indexing-consumer
  ^java.io.Closeable
  [{:keys [idle-sleep-ms index-fn]}]
  (let [running? (atom true)
        worker-thread
        (doto
            (Thread. ^Runnable (fn []
                                 (try
                                   (while @running?
                                     (let [idle? (index-fn)]
                                       (when (and idle-sleep-ms idle?)
                                         (Thread/sleep idle-sleep-ms))))
                                   (catch Throwable t
                                     (log/fatal t "Event log consumer threw exception, consumption has stopped:"))))
                     "crux.tx.event-log-consumer-thread")
            (.start))]
    (reify Closeable
      (close [_]
        (reset! running? false)
        (.join worker-thread)))))

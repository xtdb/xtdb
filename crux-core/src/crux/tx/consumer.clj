(ns crux.tx.consumer
  (:import java.util.Date))

(deftype Message [body topic ^long message-id ^Date message-time key headers])

(defprotocol PolledEventLog
  (new-event-log-context ^java.io.Closeable [this])

  (next-events [this context next-offset])

  (end-offset [this]))

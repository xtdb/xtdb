(ns ^:no-doc crux.jdbc.sqlite
  (:require [crux.jdbc :as j]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc])
  (:import [java.time LocalDateTime ZoneId]
           java.time.format.DateTimeFormatter
           java.util.Date
           java.util.function.Supplier))

(def ^:private ^ThreadLocal sqlite-df-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss.SSS")))))

(defmethod j/->date :sqlite [d _]
  (assert d)
  (-> (LocalDateTime/parse d (.get sqlite-df-tl))
      (.atZone (ZoneId/of "UTC"))
      (.toInstant)
      (Date/from)))

(defn ->dialect [_]
  (reify j/Dialect
    (db-type [_] :sqlite)

    (setup-schema! [_ pool]
      (doto pool
        (jdbc/execute! ["
CREATE TABLE IF NOT EXISTS tx_events (
  event_offset INTEGER PRIMARY KEY,
  event_key VARCHAR,
  tx_time DATETIME DEFAULT(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
  topic VARCHAR NOT NULL,
  v BINARY NOT NULL,
  compacted INTEGER NOT NULL)"])

        (jdbc/execute! ["DROP INDEX IF EXISTS tx_events_event_key_idx"])
        (jdbc/execute! ["CREATE INDEX IF NOT EXISTS tx_events_event_key_idx_2 ON tx_events(event_key)"])))))

(defmethod j/doc-exists-sql :sqlite [_ doc-id]
  ["SELECT EVENT_OFFSET from tx_events WHERE EVENT_KEY = ? AND COMPACTED = 0" doc-id])

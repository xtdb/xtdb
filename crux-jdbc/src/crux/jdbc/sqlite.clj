(ns ^:no-doc crux.jdbc.sqlite
  (:require [crux.jdbc :as j]
            [next.jdbc :as jdbc])
  (:import java.util.function.Supplier
           (java.time LocalDateTime ZoneId)
           (java.time.format DateTimeFormatter)
           (java.util Date)))

(defmethod j/setup-schema! :sqlite [_ ds]
  (jdbc/execute! ds ["create table if not exists tx_events (
  event_offset integer PRIMARY KEY, event_key VARCHAR,
  tx_time datetime DEFAULT(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
  topic VARCHAR NOT NULL,
  v BINARY NOT NULL,
  compacted INTEGER NOT NULL)"])
  (jdbc/execute! ds ["create index if not exists tx_events_event_key_idx on tx_events(compacted, event_key)"]))

(def ^:private ^ThreadLocal sqlite-df-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss.SSS")))))

(defmethod j/->date :sqlite [dbtype d]
  (assert d)
  (-> (LocalDateTime/parse d (.get sqlite-df-tl))
      (.atZone (ZoneId/of "UTC"))
      (.toInstant)
      (Date/from)))

(ns crux.jdbc.h2
  (:require [crux.jdbc :as j]
            [next.jdbc :as jdbc]))

(defmethod j/setup-schema! :h2 [_ ds]
  (jdbc/execute! ds ["create table if not exists tx_events (
  event_offset int auto_increment PRIMARY KEY, event_key VARCHAR,
  tx_time datetime default CURRENT_TIMESTAMP, topic VARCHAR NOT NULL,
  v BINARY NOT NULL,
  compacted INTEGER NOT NULL)"])
  (jdbc/execute! ds ["create index if not exists tx_events_event_key_idx on tx_events(compacted, event_key)"]))

(defmethod j/prep-for-tests! :h2 [_ ds]
  (jdbc/execute! ds ["DROP ALL OBJECTS"]))

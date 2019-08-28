(ns crux.jdbc.psql
  (:require [crux.jdbc :as j]
            [next.jdbc :as jdbc]))

(defmethod j/setup-schema! :psql [_ ds]
  (jdbc/execute! ds ["create table if not exists tx_events (
  event_offset serial PRIMARY KEY, event_key VARCHAR,
  tx_time timestamp default CURRENT_TIMESTAMP, topic VARCHAR NOT NULL,
  v bytea NOT NULL,
  compacted INTEGER NOT NULL)"])
  (jdbc/execute! ds ["create index if not exists tx_events_event_key_idx on tx_events(compacted, event_key)"]))

(defmethod j/->pool-options :psql [_ {:keys [username user] :as options}]
  (assoc options :username (or username user)))

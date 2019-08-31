(ns crux.jdbc.mysql
  (:require [crux.jdbc :as j]
            [next.jdbc :as jdbc]))

(defmethod j/setup-schema! :mysql [_ ds]
  (jdbc/execute! ds ["create table if not exists tx_events (
  event_offset int auto_increment PRIMARY KEY, event_key VARCHAR(255),
  tx_time datetime(3) default CURRENT_TIMESTAMP, topic VARCHAR(255) NOT NULL,
  v BLOB NOT NULL, compacted INTEGER NOT NULL)"])
  (jdbc/execute! ds ["create index if not exists tx_events_event_key_idx on tx_events(compacted, event_key)"]))

(defmethod j/->pool-options :mysql [_ {:keys [username user] :as options}]
  (assoc options :username (or username user)))

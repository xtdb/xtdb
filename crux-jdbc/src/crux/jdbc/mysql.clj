(ns ^:no-doc crux.jdbc.mysql
  (:require [crux.jdbc :as j]
            [next.jdbc :as jdbc]))

(defmethod j/setup-schema! :mysql [_ ds]
  (jdbc/execute! ds ["create table if not exists tx_events (
  event_offset int auto_increment PRIMARY KEY, event_key VARCHAR(255),
  tx_time datetime(3) default CURRENT_TIMESTAMP(3), topic VARCHAR(255) NOT NULL,
  v BLOB NOT NULL, compacted INTEGER NOT NULL)"])
  (when (= 0
           (:IdxPresent
            (first (jdbc/execute! ds ["SELECT COUNT(1) IdxPresent FROM INFORMATION_SCHEMA.STATISTICS
                                      WHERE table_schema=DATABASE() AND table_name='tx_events' AND index_name='tx_events_event_key_idx'"]))))
    (jdbc/execute! ds ["create index tx_events_event_key_idx on tx_events(compacted, event_key)"])))

(defmethod j/->pool-options :mysql [_ {:keys [username user] :as options}]
  (assoc options :username (or username user)))

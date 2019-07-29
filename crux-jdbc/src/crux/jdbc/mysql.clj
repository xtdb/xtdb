(ns crux.jdbc.mysql
  (:require [crux.jdbc :as j]
            [next.jdbc :as jdbc]))

(defmethod j/setup-schema! :mysql [_ ds]
  (jdbc/execute! ds ["create table if not exists tx_events (
  event_offset int auto_increment PRIMARY KEY, event_key VARCHAR(255),
  tx_time datetime(3) default CURRENT_TIMESTAMP, topic VARCHAR(255) NOT NULL,
  v BLOB NOT NULL)"]))

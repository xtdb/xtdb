(ns ^:no-doc crux.jdbc.h2
  (:require [crux.jdbc :as j]
            [next.jdbc :as jdbc]
            [crux.system :as sys]))

(defn ->data-source {::sys/deps {:open-data-source `j/->open-data-source}
                     ::sys/args {:db-file {:required? true
                                           :spec ::sys/path}}}
  [{:keys [open-data-source db-file]}]
  (doto (open-data-source {:dbname (str db-file)
                           :dbtype "h2"})

    (jdbc/execute! ["create table if not exists tx_events (
  event_offset int auto_increment PRIMARY KEY, event_key VARCHAR,
  tx_time datetime default CURRENT_TIMESTAMP, topic VARCHAR NOT NULL,
  v BINARY NOT NULL,
  compacted INTEGER NOT NULL)"])

    (jdbc/execute! ["create index if not exists tx_events_event_key_idx on tx_events(compacted, event_key)"])))

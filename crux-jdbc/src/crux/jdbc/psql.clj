(ns ^:no-doc crux.jdbc.psql
  (:require [crux.jdbc :as j]
            [next.jdbc :as jdbc]
            [crux.system :as sys]
            [clojure.spec.alpha :as s]))

(defn ->data-source {::sys/deps {:open-data-source `j/->open-data-source}
                     ::sys/args {:db-spec {:spec (s/map-of keyword? any?)}}}
  [{:keys [open-data-source db-spec] :as opts}]
  (doto (open-data-source (merge {:dbtype "postgresql"
                                  :username (:user db-spec)}
                                 db-spec))
    (jdbc/execute! ["create table if not exists tx_events (
  event_offset serial PRIMARY KEY, event_key VARCHAR,
  tx_time timestamp default CURRENT_TIMESTAMP, topic VARCHAR NOT NULL,
  v bytea NOT NULL,
  compacted INTEGER NOT NULL)"])
    (jdbc/execute! ["create index if not exists tx_events_event_key_idx on tx_events(compacted, event_key)"])))

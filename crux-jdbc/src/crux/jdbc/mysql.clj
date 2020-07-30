(ns ^:no-doc crux.jdbc.mysql
  (:require [crux.jdbc :as j]
            [next.jdbc :as jdbc]
            [crux.system :as sys]
            [clojure.spec.alpha :as s]))

(defn- setup-schema! [ds]
  (jdbc/execute! ds ["create table if not exists tx_events (
  event_offset int auto_increment PRIMARY KEY, event_key VARCHAR(255),
  tx_time datetime(3) default CURRENT_TIMESTAMP(3), topic VARCHAR(255) NOT NULL,
  v LONGBLOB NOT NULL, compacted INTEGER NOT NULL)"])

  (when (zero? (-> (jdbc/execute! ds ["SELECT COUNT(1) IdxPresent FROM INFORMATION_SCHEMA.STATISTICS
                                         WHERE table_schema=DATABASE() AND table_name='tx_events' AND index_name='tx_events_event_key_idx'"])
                   first
                   :IdxPresent))
    (jdbc/execute! ds ["create index tx_events_event_key_idx on tx_events(compacted, event_key)"])))

(defn ->data-source {::sys/deps {:open-data-source `j/->open-data-source}
                     ::sys/args {:db-spec {:spec (s/map-of keyword? any?)}}}
  [{:keys [open-data-source db-spec]}]
  (doto (open-data-source (merge {:dbtype "mysql"
                                  :username (:user db-spec)}
                                 db-spec))
    (setup-schema!)))

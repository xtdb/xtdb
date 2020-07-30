(ns ^:no-doc crux.jdbc.mssql
  (:require [crux.jdbc :as j]
            [next.jdbc :as jdbc]
            [clojure.spec.alpha :as s]
            [crux.system :as sys]))

(defn- schema-exists? [ds]
  (seq (jdbc/execute-one! ds ["SELECT * FROM sysobjects WHERE name='tx_events' and xtype='U'"])))

(defn- setup-schema! [ds]
  (when-not (schema-exists? ds)
    (jdbc/execute! ds ["create table tx_events (
  event_offset int NOT NULL IDENTITY PRIMARY KEY, event_key VARCHAR(1000),
  tx_time datetime NOT NULL default GETDATE(), topic VARCHAR(255) NOT NULL,
  v VARBINARY(max) NOT NULL, compacted INTEGER NOT NULL)"])
    (jdbc/execute! ds ["create index tx_events_event_key_idx on tx_events(compacted, event_key)"])))

(defn ->data-source {::sys/deps {:open-data-source `j/->open-data-source}
                     ::sys/args {:db-spec {:spec (s/map-of keyword? any?)}}}
  [{:keys [open-data-source db-spec]}]
  (doto (open-data-source (doto (merge {:dbtype "mssql"
                                        :username (:user db-spec)}
                                       db-spec)
                            (prn :mssql-opts)))
    (setup-schema!)))

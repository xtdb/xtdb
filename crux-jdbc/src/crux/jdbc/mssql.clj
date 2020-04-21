(ns ^:no-doc crux.jdbc.mssql
  (:require [crux.jdbc :as j]
            [next.jdbc :as jdbc]))

(defn- schema-exists? [ds]
  (seq (jdbc/execute-one! ds ["SELECT * FROM sysobjects WHERE name='tx_events' and xtype='U'"])))

(defmethod j/setup-schema! :mssql [_ ds]
  (when-not (schema-exists? ds)
    (jdbc/execute! ds ["create table tx_events (
  event_offset int NOT NULL IDENTITY PRIMARY KEY, event_key VARCHAR(1000),
  tx_time datetime NOT NULL default GETDATE(), topic VARCHAR(255) NOT NULL,
  v VARBINARY(max) NOT NULL, compacted INTEGER NOT NULL)"])
    (jdbc/execute! ds ["create index tx_events_event_key_idx on tx_events(compacted, event_key)"])))

(defmethod j/prep-for-tests! :mssql [_ ds]
  (jdbc/execute! ds ["DROP TABLE IF EXISTS tx_events;"]))

(defmethod j/->pool-options :mssql [_ {:keys [username user] :as options}]
  (assoc options :username (or username user)))

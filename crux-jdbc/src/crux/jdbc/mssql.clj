(ns ^:no-doc crux.jdbc.mssql
  (:require [crux.jdbc :as j]
            [next.jdbc :as jdbc]))

(defn- schema-exists? [ds]
  (seq (jdbc/execute-one! ds ["SELECT * FROM sysobjects WHERE name='tx_events' and xtype='U'"])))

(defn ->dialect [_]
  (reify j/Dialect
    (db-type [_] :mssql)

    (setup-schema! [_ ds]
     (when-not (schema-exists? ds)
       (jdbc/execute! ds ["
CREATE TABLE tx_events (
  event_offset INT NOT NULL IDENTITY PRIMARY KEY,
  event_key VARCHAR(1000),
  tx_time DATETIME NOT NULL default GETDATE(),
  topic VARCHAR(255) NOT NULL,
  v VARBINARY(max) NOT NULL,
  compacted INTEGER NOT NULL)"])

       (jdbc/execute! ds ["CREATE INDEX tx_events_event_key_idx ON tx_events(compacted, event_key)"])))))

(ns ^:no-doc crux.jdbc.mssql
  (:require [clojure.tools.logging :as log]
            [crux.jdbc :as j]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc.result-set :as jdbcr])
  (:import java.util.Date
           microsoft.sql.DateTimeOffset))

(defn- check-tx-time-col [ds]
  (when-not (= "datetimeoffset"
               (-> (jdbc/execute-one! ds
                                      ["SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'tx_events' AND COLUMN_NAME = 'tx_time'"]
                                      {:builder-fn jdbcr/as-unqualified-lower-maps})
                   :data_type))
    (log/warn (str "`tx_time` column not in UTC format. "
                   "See https://github.com/juxt/crux/releases/tag/20.09-1.12.1 for more details."))))

(defn ->dialect [_]
  (reify j/Dialect
    (db-type [_] :mssql)

    (setup-schema! [_ ds]
      (doto ds
        (jdbc/execute! ["
IF NOT EXISTS (select * from sys.tables where name='tx_events')
CREATE TABLE tx_events (
  event_offset INT NOT NULL IDENTITY PRIMARY KEY,
  event_key VARCHAR(1000),
  tx_time DATETIMEOFFSET NOT NULL default SYSDATETIMEOFFSET(),
  topic VARCHAR(255) NOT NULL,
  v VARBINARY(max) NOT NULL,
  compacted INTEGER NOT NULL)"])

        (jdbc/execute! ["
IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = object_id('dbo.tx_events') AND NAME ='tx_events_event_key_idx')
DROP INDEX tx_events.tx_events_event_key_idx"])

        (jdbc/execute! ["
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = object_id('dbo.tx_events') AND NAME ='tx_events_event_key_idx_2')
CREATE INDEX tx_events_event_key_idx_2 ON tx_events(event_key)"])

        (check-tx-time-col)))))

(defmethod j/->date :mssql [^DateTimeOffset d _]
  (Date/from (.toInstant (.getOffsetDateTime d))))

(defmethod j/doc-exists-sql :mssql [_ doc-id]
  ["SELECT EVENT_OFFSET from tx_events WITH (rowlock,updlock) WHERE EVENT_KEY = ? AND COMPACTED = 0" doc-id])

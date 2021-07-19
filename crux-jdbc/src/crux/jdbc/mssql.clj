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
  (reify
    j/Dialect
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

        (check-tx-time-col)))

    j/Docs2Dialect
    (setup-docs2-schema! [_ pool {:keys [table-name]}]
      (doto pool
        (jdbc/execute! [(format "
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='%s')
CREATE TABLE %s (
  doc_id VARCHAR(255) NOT NULL PRIMARY KEY,
  doc VARBINARY(max) NOT NULL)"
                                table-name table-name)])))

    (doc-upsert-sql+param-groups [_ docs {:keys [table-name]}]
      (into [(format "
MERGE %s docs
USING (VALUES (?, ?)) AS new_doc (doc_id, doc)
ON (docs.doc_id = new_doc.doc_id)
WHEN MATCHED
  THEN UPDATE SET doc = new_doc.doc
WHEN NOT MATCHED BY TARGET
  THEN INSERT (doc_id, doc) VALUES (new_doc.doc_id, new_doc.doc);"
                     table-name)]
            docs))))

(defmethod j/->date :mssql [^DateTimeOffset d _]
  (Date/from (.toInstant (.getOffsetDateTime d))))

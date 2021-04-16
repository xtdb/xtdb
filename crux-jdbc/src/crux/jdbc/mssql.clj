(ns ^:no-doc crux.jdbc.mssql
  (:require [crux.jdbc :as j]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as jdbcr]
            [clojure.tools.logging :as log])
  (:import microsoft.sql.DateTimeOffset
           java.util.Date))

(defn- schema-exists? [ds]
  (seq (jdbc/execute-one! ds ["SELECT * FROM sysobjects WHERE name='tx_events' and xtype='U'"])))

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
     (when-not (schema-exists? ds)
       (doto ds
         (jdbc/execute! ["
CREATE TABLE tx_events (
  event_offset INT NOT NULL IDENTITY PRIMARY KEY,
  event_key VARCHAR(1000),
  tx_time DATETIMEOFFSET NOT NULL default SYSDATETIMEOFFSET(),
  topic VARCHAR(255) NOT NULL,
  v VARBINARY(max) NOT NULL,
  compacted INTEGER NOT NULL)"])

         (jdbc/execute! ["CREATE INDEX tx_events_event_key_idx ON tx_events(event_key)"])

         (check-tx-time-col))))))

(defmethod j/->date :mssql [^DateTimeOffset d _]
  (Date/from (.toInstant (.getOffsetDateTime d))))

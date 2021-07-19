(ns ^:no-doc crux.jdbc.mysql
  (:require [clojure.tools.logging :as log]
            [crux.jdbc :as j]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc.result-set :as jdbcr]))

(defn- check-tx-time-col [pool]
  (when-not (= "timestamp"
               (-> (jdbc/execute-one! pool
                                      ["SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'tx_events' AND COLUMN_NAME = 'tx_time'"]
                                      {:builder-fn jdbcr/as-unqualified-lower-maps})
                   :data_type))
    (log/warn (str "`tx_time` column not in UTC format. "
                   "See https://github.com/juxt/crux/releases/tag/20.09-1.12.1 for more details."))))

(defn- idx-exists? [ds idx-name]
  (pos? (-> (jdbc/execute! ds ["
SELECT COUNT(1) IdxPresent
FROM INFORMATION_SCHEMA.STATISTICS
WHERE table_schema=DATABASE()
  AND table_name='tx_events'
  AND index_name=?"
                               idx-name])
            first
            :IdxPresent)))

(defn ->dialect [_]
  (reify
    j/Dialect
    (db-type [_] :mysql)
    (setup-schema! [_ ds]
      (jdbc/execute! ds ["
CREATE TABLE IF NOT EXISTS tx_events (
  event_offset INT AUTO_INCREMENT PRIMARY KEY,
  event_key VARCHAR(255),
  tx_time TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
  topic VARCHAR(255) NOT NULL,
  v LONGBLOB NOT NULL,
  compacted INTEGER NOT NULL)"])

      (when (idx-exists? ds "tx_events_event_key_idx")
        (jdbc/execute! ds ["DROP INDEX tx_events_event_key_idx ON tx_events"]))

      (when-not (idx-exists? ds "tx_events_event_key_idx_2")
        (jdbc/execute! ds ["CREATE INDEX tx_events_event_key_idx_2 ON tx_events(event_key)"]))

      (check-tx-time-col ds))

    j/Docs2Dialect
    (setup-docs2-schema! [_ pool {:keys [table-name]}]
      (doto pool
        (jdbc/execute! [(format "
CREATE TABLE IF NOT EXISTS %s (
  doc_id VARCHAR(255) NOT NULL PRIMARY KEY,
  doc LONGBLOB NOT NULL)"
                                table-name)])))

    (doc-upsert-sql+param-groups [_ docs {:keys [table-name]}]
      (into [(format "
INSERT INTO %s (doc_id, doc) VALUES (?, ?) AS new
ON DUPLICATE KEY UPDATE doc = new.doc
"
                     table-name)]
            docs))))

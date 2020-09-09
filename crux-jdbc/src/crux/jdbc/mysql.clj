(ns ^:no-doc crux.jdbc.mysql
  (:require [crux.jdbc :as j]
            [next.jdbc :as jdbc]))

(defn ->dialect [_]
  (reify j/Dialect
    (db-type [_] :mysql)
    (setup-schema! [_ ds]
      (jdbc/execute! ds ["
CREATE TABLE IF NOT EXISTS tx_events (
  event_offset INT AUTO_INCREMENT PRIMARY KEY,
  event_key VARCHAR(255),
  tx_time DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
  topic VARCHAR(255) NOT NULL,
  v LONGBLOB NOT NULL,
  compacted INTEGER NOT NULL)"])

      (when (zero? (-> (jdbc/execute! ds ["
SELECT COUNT(1) IdxPresent
FROM INFORMATION_SCHEMA.STATISTICS
WHERE table_schema=DATABASE() AND table_name='tx_events' AND index_name='tx_events_event_key_idx'"])
                       first
                       :IdxPresent))

        (jdbc/execute! ds ["CREATE INDEX tx_events_event_key_idx ON tx_events(compacted, event_key)"])))))

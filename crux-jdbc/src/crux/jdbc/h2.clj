(ns ^:no-doc crux.jdbc.h2
  (:require [crux.jdbc :as j]
            [next.jdbc :as jdbc]))

(defn ->dialect [_]
  (reify j/Dialect
    (db-type [_] :h2)

    (setup-schema! [_ pool]
      (doto pool
        (jdbc/execute! ["
CREATE TABLE IF NOT EXISTS tx_events (
  event_offset INT AUTO_INCREMENT PRIMARY KEY,
  event_key VARCHAR,
  tx_time DATETIME DEFAULT CURRENT_TIMESTAMP,
  topic VARCHAR NOT NULL,
  v BINARY NOT NULL,
  compacted INTEGER NOT NULL)"])

        (jdbc/execute! ["CREATE INDEX IF NOT EXISTS tx_events_event_key_idx ON tx_events(compacted, event_key)"])))))

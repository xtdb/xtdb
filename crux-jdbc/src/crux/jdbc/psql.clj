(ns ^:no-doc crux.jdbc.psql
  (:require [crux.jdbc :as j]
            [next.jdbc :as jdbc]
            [crux.system :as sys]
            [clojure.spec.alpha :as s]))

(defn ->dialect [_]
  (reify j/Dialect
    (db-type [_] :postgresql)

    (setup-schema! [_ pool]
      (doto pool
        (jdbc/execute! ["
CREATE TABLE IF NOT EXISTS tx_events (
  event_offset SERIAL PRIMARY KEY,
  event_key VARCHAR,
  tx_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  topic VARCHAR NOT NULL,
  v BYTEA NOT NULL,
  compacted INTEGER NOT NULL)"])

        (jdbc/execute! ["CREATE INDEX IF NOT EXISTS tx_events_event_key_idx ON tx_events(compacted, event_key)"])))))

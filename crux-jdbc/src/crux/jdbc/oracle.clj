(ns ^:no-doc crux.jdbc.oracle
  (:require [crux.jdbc :as j]
            [next.jdbc :as jdbc]
            [taoensso.nippy :as nippy])
  (:import [oracle.sql BLOB TIMESTAMP]))

(defn- schema-exists? [pool]
  (-> (jdbc/execute-one! pool ["SELECT COUNT(*) FROM user_tables where table_name = 'TX_EVENTS'"])
      first val pos?))

(defn ->dialect [_]
  (reify j/Dialect
    (db-type [_] :oracle)

    (setup-schema! [_ pool]
     (when-not (schema-exists? pool)
       (jdbc/execute! pool ["
CREATE TABLE tx_events (
  event_offset SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_key VARCHAR2(255),
  tx_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  topic VARCHAR2(255) NOT NULL,
  v BLOB NOT NULL,
  compacted INTEGER NOT NULL)"])

       (jdbc/execute! pool ["CREATE INDEX tx_events_event_key_idx ON tx_events(compacted, event_key)"])))))

(defmethod j/->date :oracle [^TIMESTAMP d _]
  (assert d)
  (.dateValue d))

;; TODO readAllBytes doesn't exists in JDK8
(defmethod j/<-blob :oracle [^BLOB v _]
  (-> v .getBinaryStream .readAllBytes nippy/thaw))

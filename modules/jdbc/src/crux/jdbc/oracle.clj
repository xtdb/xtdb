(ns ^:no-doc crux.jdbc.oracle
  (:require [crux.jdbc :as j]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc]
            [juxt.clojars-mirrors.nippy.v3v1v1.taoensso.nippy :as nippy])
  (:import [oracle.sql BLOB TIMESTAMP]))

(defn- table-exists? [pool tbl-name]
  (-> (jdbc/execute-one! pool ["SELECT COUNT(*) FROM user_tables where table_name = ?" tbl-name])
      first val pos?))

(defn- idx-exists? [pool idx-name]
  (-> (jdbc/execute-one! pool ["SELECT COUNT(*) FROM user_indexes where index_name = ?" idx-name])
      first val pos?))

(defn ->dialect [_]
  (reify j/Dialect
    (db-type [_] :oracle)

    (setup-schema! [_ pool]
      (when-not (table-exists? pool "TX_EVENTS")
        (jdbc/execute! pool ["
CREATE TABLE tx_events (
  event_offset SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_key VARCHAR2(255),
  tx_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  topic VARCHAR2(255) NOT NULL,
  v BLOB NOT NULL,
  compacted INTEGER NOT NULL)"]))

      (when (idx-exists? pool "TX_EVENTS_EVENT_KEY_IDX")
        (jdbc/execute! pool ["DROP INDEX TX_EVENTS_EVENT_KEY_IDX"]))

      (when-not (idx-exists? pool "TX_EVENTS_EVENT_KEY_IDX_2")
        (jdbc/execute! pool ["CREATE INDEX TX_EVENTS_EVENT_KEY_IDX_2 ON tx_events(event_key)"])))))

(defmethod j/->date :oracle [^TIMESTAMP d _]
  (assert d)
  (.dateValue d))

;; TODO readAllBytes doesn't exists in JDK8
(defmethod j/<-blob :oracle [^BLOB v _]
  (-> v .getBinaryStream .readAllBytes nippy/thaw))

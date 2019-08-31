(ns crux.jdbc.oracle
  (:require [crux.jdbc :as j]
            [next.jdbc :as jdbc]
            [clojure.spec.alpha :as s]
            [taoensso.nippy :as nippy])
  (:import [oracle.sql TIMESTAMP BLOB]))

(defmethod j/setup-schema! :oracle [_ ds]
  (jdbc/execute! ds ["create table tx_events (
  event_offset SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, event_key VARCHAR2(255),
  tx_time timestamp default CURRENT_TIMESTAMP, topic VARCHAR2(255) NOT NULL,
  v BLOB NOT NULL, compacted INTEGER NOT NULL)"])
  (jdbc/execute! ds ["create index tx_events_event_key_idx on tx_events(compacted, event_key)"]))

(defmethod j/->date :oracle [dbtype ^TIMESTAMP d]
  (assert d)
  (.dateValue d))

(defmethod j/->v :oracle [_ ^BLOB v]
  (-> v .getBinaryStream .readAllBytes nippy/thaw))

(defmethod j/prep-for-tests! :oracle [_ ds]
  (when (< 0 (val (first (jdbc/execute-one! ds ["SELECT COUNT(*) FROM user_tables where table_name = 'TX_EVENTS'"]))))
    (jdbc/execute! ds ["DROP TABLE tx_events"])))

(defmethod j/->pool-options :oracle [_ {:keys [username user] :as options}]
  (assoc options :username (or username user)))

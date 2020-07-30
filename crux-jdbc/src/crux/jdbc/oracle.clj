(ns ^:no-doc crux.jdbc.oracle
  (:require [crux.jdbc :as j]
            [next.jdbc :as jdbc]
            [clojure.spec.alpha :as s]
            [taoensso.nippy :as nippy]
            [crux.system :as sys])
  (:import [oracle.sql TIMESTAMP BLOB]))

(defn- schema-exists? [ds]
  (pos? (val (first (jdbc/execute-one! ds ["SELECT COUNT(*) FROM user_tables where table_name = 'TX_EVENTS'"])))))

(defn- setup-schema! [ds]
  (when-not (schema-exists? ds)
    (jdbc/execute! ds ["create table tx_events (
  event_offset SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, event_key VARCHAR2(255),
  tx_time timestamp default CURRENT_TIMESTAMP, topic VARCHAR2(255) NOT NULL,
  v BLOB NOT NULL, compacted INTEGER NOT NULL)"])
    (jdbc/execute! ds ["create index tx_events_event_key_idx on tx_events(compacted, event_key)"])))

(defmethod j/->date :oracle [dbtype ^TIMESTAMP d]
  (assert d)
  (.dateValue d))

(defmethod j/->v :oracle [_ ^BLOB v]
  (-> v .getBinaryStream .readAllBytes nippy/thaw))

(defn ->data-source {::sys/deps {:open-data-source `j/->open-data-source}
                     ::sys/args {:username {:spec ::sys/string}
                                 :user {:spec ::sys/string}}}
  [{:keys [open-data-source username user]}]
  (doto (open-data-source {:dbtype "oracle"
                           :username (or username user)})
    (setup-schema!)))

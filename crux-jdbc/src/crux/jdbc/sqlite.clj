(ns crux.jdbc.sqlite
  (:require [crux.jdbc :as j]
            [next.jdbc :as jdbc])
  (:import java.text.SimpleDateFormat))

(defmethod j/setup-schema! :sqlite [_ ds]
  (jdbc/execute! ds ["create table if not exists tx_events (
  event_offset integer PRIMARY KEY, event_key VARCHAR,
  tx_time datetime DEFAULT(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')), topic VARCHAR NOT NULL,
  v BINARY NOT NULL)"]))

(def ^SimpleDateFormat sqlite-df (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss.SSS"))

(defmethod j/->date :sqlite [dbtype d]
  (assert d)
  (.parse sqlite-df ^String d))

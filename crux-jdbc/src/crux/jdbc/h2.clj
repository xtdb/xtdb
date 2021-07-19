(ns ^:no-doc crux.jdbc.h2
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [crux.jdbc :as j]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc.result-set :as jdbcr])
  (:import [java.time LocalDate LocalTime OffsetDateTime ZoneOffset]
           java.util.Date
           org.h2.api.TimestampWithTimeZone))

(defn- check-tx-time-col [pool]
  (when-not (->> (jdbc/execute! pool ["SHOW COLUMNS FROM tx_events"] {:builder-fn jdbcr/as-unqualified-lower-maps})
                 (filter (comp #{"tx_time"} str/lower-case :field))
                 first :type
                 (re-find #"WITH TIME ZONE"))
    (log/warn (str "`tx_time` column not in UTC format. "
                   "See https://github.com/juxt/crux/releases/tag/20.09-1.12.1 for more details."))))

(defn ->dialect [_]
  (reify
    j/Dialect
    (db-type [_] :h2)

    (setup-schema! [_ pool]
      (doto pool
        (jdbc/execute! ["
CREATE TABLE IF NOT EXISTS tx_events (
  event_offset INT AUTO_INCREMENT PRIMARY KEY,
  event_key VARCHAR,
  tx_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  topic VARCHAR NOT NULL,
  v BINARY NOT NULL,
  compacted INTEGER NOT NULL)"])

        (jdbc/execute! ["DROP INDEX IF EXISTS tx_events_event_key_idx"])
        (jdbc/execute! ["CREATE INDEX IF NOT EXISTS tx_events_event_key_idx_2 ON tx_events(event_key)"])
        (check-tx-time-col)))

    j/Docs2Dialect
    (setup-docs2-schema! [_ pool {:keys [table-name]}]
      (doto pool
        (jdbc/execute! [(format "
CREATE TABLE IF NOT EXISTS %s (
  doc_id VARCHAR NOT NULL PRIMARY KEY,
  doc BINARY NOT NULL)"
                                table-name)])))

    (doc-upsert-sql+param-groups [_ docs {:keys [table-name]}]
      (into [(format "MERGE INTO %s (doc_id, doc) KEY (doc_id) VALUES (?, ?)"
                     table-name)]
            docs))))

(defmethod j/->date :h2 [^TimestampWithTimeZone d _]
  (-> (OffsetDateTime/of (LocalDate/of (.getYear d) (.getMonth d) (.getDay d))
                         (LocalTime/ofNanoOfDay (.getNanosSinceMidnight d))
                         (ZoneOffset/ofTotalSeconds (.getTimeZoneOffsetSeconds d)))
      .toInstant
      Date/from))

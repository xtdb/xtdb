(ns ^:no-doc xtdb.jdbc.h2
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.jdbc :as j]
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
                   "See https://github.com/xtdb/xtdb/releases/tag/20.09-1.12.1 for more details."))))

(defn ->dialect [_]
  (reify j/Dialect
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
        (check-tx-time-col)))))

(defmethod j/->date :h2 [^TimestampWithTimeZone d _]
  (-> (OffsetDateTime/of (LocalDate/of (.getYear d) (.getMonth d) (.getDay d))
                         (LocalTime/ofNanoOfDay (.getNanosSinceMidnight d))
                         (ZoneOffset/ofTotalSeconds (.getTimeZoneOffsetSeconds d)))
      .toInstant
      Date/from))

(ns xtdb.flight-sql
  "Configuration support for FlightSQL server module."
  (:require [xtdb.node :as xtn])
  (:import [xtdb.api FlightSql$Factory Xtdb$Config]))

(defmethod xtn/apply-config! ::server [^Xtdb$Config config, _ {:keys [host port]}]
  (.module config (cond-> (FlightSql$Factory.)
                    (some? host) (.host host)
                    (some? port) (.port port))))

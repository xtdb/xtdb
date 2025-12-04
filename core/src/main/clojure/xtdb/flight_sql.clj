(ns xtdb.flight-sql
  "FlightSQL server integrant component."
  (:require [integrant.core :as ig]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import [xtdb.api FlightSql FlightSqlConfig Xtdb$Config]))

(defmethod xtn/apply-config! :flight-sql [^Xtdb$Config config _ {:keys [host port]}]
  (cond-> (.getFlightSql config)
    (some? host) (.host host)
    (some? port) (.port port)))

(defmethod ig/expand-key ::server [k config]
  {k {:node (ig/ref :xtdb/node)
      :config config}})

(defmethod ig/init-key ::server [_ {:keys [node config]}]
  (FlightSql/open node config))

(defmethod ig/halt-key! ::server [_ server]
  (util/try-close server))

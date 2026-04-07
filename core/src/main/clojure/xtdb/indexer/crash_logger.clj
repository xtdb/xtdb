(ns xtdb.indexer.crash-logger
  (:require [clojure.pprint :as pp]
            [xtdb.error :as err])
  (:import (xtdb.arrow RelationReader VectorReader)
           (xtdb.error Anomaly$Caller Interrupted)
           (xtdb.indexer CrashLogger LiveIndex OpenTx$Table)
           (xtdb.table TableRef)))

(defn crash-log! [^CrashLogger crash-logger, ex, {:keys [^TableRef table] :as data},
                  {:keys [^LiveIndex live-idx, ^OpenTx$Table open-tx-table, ^RelationReader query-rel, ^VectorReader tx-ops-rdr]}]
  (.writeCrashLog crash-logger
                  (with-out-str (pp/pprint (assoc data :ex ex)))
                  table live-idx open-tx-table query-rel tx-ops-rdr))

(defmacro with-crash-log [crash-logger msg data state & body]
  `(let [data# ~data]
     (try
       (err/wrap-anomaly data#
         ~@body)
       (catch Interrupted e# (throw e#))
       (catch Anomaly$Caller e# (throw e#))
       (catch Throwable e#
         (try
           (crash-log! ~crash-logger e# data# ~state)
           (catch Throwable t#
             (.addSuppressed e# t#)))

         (throw (ex-info ~msg data# e#))))))

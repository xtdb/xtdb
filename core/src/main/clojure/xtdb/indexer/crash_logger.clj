(ns xtdb.indexer.crash-logger
  (:require [clojure.pprint :as pp]
            [integrant.core :as ig]
            [xtdb.error :as err])
  (:import (xtdb.arrow RelationReader VectorReader)
           (xtdb.error Anomaly$Caller Interrupted)
           (xtdb.indexer CrashLogger LiveIndex LiveTable$Tx)
           (xtdb.table TableRef)))

(defn crash-log! [^CrashLogger crash-logger, ex, {:keys [^TableRef table] :as data},
                  {:keys [^LiveIndex live-idx, ^LiveTable$Tx live-table-tx, ^RelationReader query-rel, ^VectorReader tx-ops-rdr]}]
  (.writeCrashLog crash-logger
                  (with-out-str (pp/pprint (assoc data :ex ex)))
                  table live-idx live-table-tx query-rel tx-ops-rdr))

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

(defmethod ig/expand-key :xtdb.indexer/crash-logger [k {:keys [base]}]
  {k {:base base
      :allocator (ig/ref :xtdb.db-catalog/allocator)
      :buffer-pool (ig/ref :xtdb/buffer-pool)}})

(defmethod ig/init-key :xtdb.indexer/crash-logger [_ {{:keys [config]} :base
                                                       :keys [allocator buffer-pool]}]
  (CrashLogger. allocator buffer-pool (:node-id config)))

(defmethod ig/halt-key! :xtdb.indexer/crash-logger [_ _])

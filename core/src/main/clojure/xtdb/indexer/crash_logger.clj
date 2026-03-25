(ns xtdb.indexer.crash-logger
  (:require [clojure.pprint :as pp]
            [integrant.core :as ig]
            [xtdb.error :as err])
  (:import (xtdb.arrow RelationReader VectorReader)
           xtdb.database.DatabaseStorage
           (xtdb.error Anomaly$Caller Interrupted)
           xtdb.NodeBase
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

(defmethod ig/expand-key :xtdb.indexer/crash-logger [k opts]
  {k (into {:allocator (ig/ref :xtdb.db-catalog/allocator)
            :storage (ig/ref :xtdb.db-catalog/storage)}
           opts)})

(defmethod ig/init-key :xtdb.indexer/crash-logger [_ {:keys [allocator ^NodeBase base ^DatabaseStorage storage]}]
  (CrashLogger. allocator (.getBufferPool storage) (.getNodeId (.getConfig base))))

(defmethod ig/halt-key! :xtdb.indexer/crash-logger [_ _])

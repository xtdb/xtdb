(ns xtdb.tx-sink 
  (:require [integrant.core :as ig]
            [xtdb.log :as log]
            [xtdb.node :as xtn])
  (:import (xtdb.api TxSinkConfig Xtdb$Config)
           (xtdb.indexer Indexer$TxSink)))

(defmethod xtn/apply-config! :tx-sink [^Xtdb$Config config _ {:keys [output-log format]}]
  (.txSink config
           (cond-> (TxSinkConfig.)
             (some? output-log) (.outputLog (log/->log-factory (first output-log) (second output-log)))
             (some? format) (.format (str (symbol format))))))

(defmethod ig/expand-key :xtdb/tx-sink [k {:keys [tx-sink-conf]}]
  {k {:tx-sink-conf tx-sink-conf}})

(defmethod ig/init-key :xtdb/tx-sink [_ {:keys [^TxSinkConfig tx-sink-conf]}]
  (when tx-sink-conf
    (reify Indexer$TxSink
      (onCommit [_ _tx-key _live-idx-tx]))))

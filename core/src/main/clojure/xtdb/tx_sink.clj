(ns xtdb.tx-sink 
  (:require [integrant.core :as ig])
  (:import (xtdb.indexer Indexer$TxSink)))

(defmethod ig/init-key :xtdb/tx-sink [_ _]
  #_
  (reify Indexer$TxSink
    (onCommit [_ tx-key _live-idx-tx])))

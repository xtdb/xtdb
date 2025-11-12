(ns xtdb.tx-sink.main
  (:require [xtdb.node :as xtn]
            xtdb.node.impl)
  (:import xtdb.api.Xtdb))

(defn open! ^Xtdb [node-opts]
  (let [config (doto (xtn/->config node-opts)
                 (-> (.getCompactor) (.threads 0))
                 (.setServer nil)
                 (some-> (.getTxSink) (.enable true)))]
    (.open config)))

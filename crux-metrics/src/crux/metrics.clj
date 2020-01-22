(ns crux.metrics
  (:require [crux.metrics.bus :as m-bus]))

(def state {::state {:start-fn (fn [{:crux.node/keys [bus indexer]} _]
                                 (m-bus/assign-ingest bus indexer))}})

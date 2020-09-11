(ns crux.corda.service
  (:require [crux.api :as crux]))

(defn process-tx [node service-hub corda-tx-id]
  (prn "process tx" node service-hub corda-tx-id))

(defn start-node [service-hub]
  (crux/start-node {:crux.node/topology '[crux.standalone/topology]}))

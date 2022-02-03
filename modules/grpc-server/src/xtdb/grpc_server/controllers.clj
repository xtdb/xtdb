(ns xtdb.grpc-server.controllers
  (:require [xtdb.api :as xt]
            [xtdb.grpc-server.adapters.submit-tx :as adapters.submit]
            [xtdb.grpc-server.adapters.status :as adapters.status])
  (:gen-class))

(defn status [node]
  (-> (xt/status node)
      (adapters.status/edn->grpc)))

(defmacro dbg [x] `(let [x# ~x] (println "\n\n\n" '~x "=\n" x# "\n\n\n") x#))

(defn submit-tx [node tx-ops]
  (-> tx-ops
       (adapters.submit/grpc->edn)
       (xt/submit-tx node)
       (dbg)
       #_(adapters.submit/edn->grpc)))
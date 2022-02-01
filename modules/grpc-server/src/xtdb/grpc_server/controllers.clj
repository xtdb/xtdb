(ns xtdb.grpc-server.controllers
  (:require [xtdb.api :as xt]
            [com.grpc.xtdb.GrpcApi.server :as grpc-api]
            [com.grpc.xtdb :as grpc]
            [xtdb.grpc-server.adapters.status :as adapters.status])
  (:gen-class))

(defn status [node]
  (println (str "\n\n\n" "controller" "\n\n\n"))
  (-> (xt/status node)
      (adapters.status/edn->grpc)))
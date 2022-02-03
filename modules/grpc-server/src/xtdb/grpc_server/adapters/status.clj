(ns xtdb.grpc-server.adapters.status
  (:require [xtdb.grpc-server.utils :as utils]
            [com.grpc.xtdb :refer [new-StatusResponse]])
  (:gen-class))

(defn edn->grpc [edn]
  (->
   {:version (:xtdb.version/version  edn)
    :kv-store (:xtdb.kv/kv-store edn)
    :estimate-num-keys (:xtdb.kv/estimate-num-keys edn)
    :index-version (:xtdb.index/index-version edn)}
   (utils/nil->default :size (:xtdb.kv/size edn) 0)
   (utils/assoc-some :revision (:xtdb.version/revision edn))
   (utils/assoc-some :consumer-state (:xtdb.tx-log/consumer-state edn))
   (new-StatusResponse)))
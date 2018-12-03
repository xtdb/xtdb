(ns crux.status
  (:require [clojure.tools.logging :as log]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kafka :as k]
            [crux.kv-store :as ks]))

(defn status-map [kv bootstrap-servers]
  {:crux.zk/zk-active? (k/zk-active? bootstrap-servers)
   :crux.kv-store/kv-backend (ks/kv-name kv)
   :crux.kv-store/estimate-num-keys (ks/count-keys kv)
   :crux.kv-store/size (some-> (ks/db-dir kv) (cio/folder-size))
   :crux.tx-log/tx-time (idx/read-meta kv :crux.tx-log/tx-time)})

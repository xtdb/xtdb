(ns crux.status
  (:require [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kafka :as k]
            [crux.kv :as kv]))

;; TODO: Rethink this, creates explicit dependencies on various
;; subsystems, better if they could report back status themselves.
(defn status-map [{:keys [kv-store indexer consumer-config]} {:keys [tx-topic] :as options}]
  (merge
   {:crux.zk/zk-active? (k/zk-active? consumer-config)
    :crux.kv/kv-backend (kv/kv-name kv-store)
    :crux.kv/estimate-num-keys (kv/count-keys kv-store)
    :crux.kv/size (some-> (kv/db-dir kv-store) (cio/folder-size))}
   (k/consumer-status indexer consumer-config tx-topic)))

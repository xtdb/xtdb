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
   (kv/kv-status kv-store)
   (idx/index-status kv-store)
   (k/zk-status consumer-config)
   (k/consumer-status indexer)))

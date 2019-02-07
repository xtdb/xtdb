(ns crux.status
  (:require [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.tx :as tx]))

;; TODO: Rethink this, creates explicit dependencies on various
;; subsystems, better if they could report back status themselves.
(defn status-map [{:keys [kv-store indexer consumer-config]} options]
  (merge
   (kv/kv-status kv-store)
   (idx/index-status kv-store)
   (tx/consumer-status indexer)
   (when-let [zk-status (try
                          (requiring-resolve 'crux.kafka/zk-status)
                          (catch Throwable t
                            (log/debug t "Could not require crux.kafka.")))]
     (zk-status consumer-config))))

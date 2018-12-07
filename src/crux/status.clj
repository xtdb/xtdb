(ns crux.status
  (:require [clojure.tools.logging :as log]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kafka :as k]
            [crux.kv :as kv]))

;; TODO: Rethink this, creates explicit dependencies on various
;; subsystems, better if they could report back status themselves.
(defn status-map [kv bootstrap-servers]
  {:crux.zk/zk-active? (if bootstrap-servers
                         (k/zk-active? bootstrap-servers)
                         false)
   :crux.kv/kv-backend (kv/kv-name kv)
   :crux.kv/estimate-num-keys (kv/count-keys kv)
   :crux.kv/size (some-> (kv/db-dir kv) (cio/folder-size))
   :crux.tx-log/tx-time (idx/read-meta kv :crux.tx-log/tx-time)})

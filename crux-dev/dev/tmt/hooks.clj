(ns tmt.hooks
  (:require [crux.api :as api]
            [crux.db :as db]))

(def node (api/start-node {:crux.node/topology :crux.standalone/topology
                           :crux.node/kv-store "crux.kv.memdb/kv"
                           :crux.kv/db-dir "data/db-dir-1"
                           :crux.standalone/event-log-dir "data/eventlog-1"
                           :crux.standalone/event-log-kv-store "crux.kv.memdb/kv"}))

(db/add-tx-hook! (:indexer node) (fn [in] (fn [out] (tap> (str "tx" {:in in :out out})))))
(db/add-doc-hook! (:indexer node) (fn [in] (fn [out] (tap> (str "doc" {:in in :out out})))))

(api/submit-tx node [[:crux.tx/put {:crux.db/id :foo}]])

(ns crux.java
  (:require [crux.api :as api]
            [clojure.tools.logging :as log])
  (:import (crux.api ICruxAPI ICruxDatasource
                     ICruxIngestAPI ICruxAsyncIngestAPI)
           (java.io Closeable)))

(defn- conform-tx-op [tx-op]
  (->> tx-op
       (mapv (fn [el]
               (cond
                 (map? el) el
                 (instance? java.util.Map el) (into {} el)
                 :else el)))))

(defrecord CruxDatasource [data-source]
  ICruxDatasource
  (entity [_ eid] (api/entity data-source eid))
  (entity [_ snapshot eid] (api/entity data-source snapshot eid))
  (entityTx [_ eid] (api/entity-tx data-source eid))

  (newSnapshot [_] (api/new-snapshot data-source))

  (q [_ query] (api/q data-source query))
  (q [_ snapshot query] (api/q data-source snapshot query))

  (historyAscending [_ snapshot eid] (api/history-ascending data-source snapshot eid))
  (historyDescending [_ snapshot eid] (api/history-descending data-source snapshot eid))

  (validTime [_] (api/valid-time data-source))
  (transactionTime [_] (api/transaction-time data-source)))

(defrecord CruxNode [node]
  ICruxAPI
  (db [_] (->CruxDatasource (api/db node)))
  (db [_ valid-time] (->CruxDatasource (api/db node valid-time)))
  (db [_ valid-time transaction-time] (->CruxDatasource (api/db node valid-time transaction-time)))

  (document [_ content-hash] (api/document node content-hash))
  (documents [_ content-hash-set] (api/documents node content-hash-set))

  (history [_ eid] (api/history node eid))

  (historyRange [_ eid valid-time-start transaction-time-start valid-time-end transaction-time-end]
    (api/history-range node eid valid-time-start transaction-time-start valid-time-end transaction-time-end))

  (status [_] (api/status node))

  (hasTxCommitted [_ submitted-tx] (api/tx-committed? node submitted-tx))

  (sync [_ timeout] (api/sync node timeout))

  (sync [_ tx-time timeout]
    (defonce warn-on-deprecated-sync
      (log/warn "(sync tx-time <timeout?>) is deprecated, replace with either (await-tx-time tx-time <timeout?>) or, preferably, (await-tx tx <timeout?>)"))
    (api/await-tx-time node tx-time timeout))

  (awaitTx [_ submitted-tx timeout] (api/await-tx node submitted-tx timeout))
  (awaitTxTime [_ tx-time timeout] (api/await-tx-time node tx-time timeout))

  (latestCompletedTx [_] (api/latest-completed-tx node))
  (latestSubmittedTx [_] (api/latest-submitted-tx node))

  (attributeStats [_] (api/attribute-stats node))

  ICruxIngestAPI
  (submitTx [_ tx-ops] (api/submit-tx node (mapv conform-tx-op tx-ops)))
  (openTxLog [_ from-tx-id with-ops?] (api/open-tx-log node from-tx-id with-ops?))

  Closeable
  (close [_] (.close ^Closeable node)))

(defn start-node [options]
  (->CruxNode (api/start-node options)))

(defrecord CruxAsyncIngestClient [aic]
  ICruxAsyncIngestAPI
  (submitTxAsync [_ tx-ops] (api/submit-tx-async aic (mapv conform-tx-op tx-ops)))

  Closeable
  (close [_] (.close ^Closeable aic)))

(defn start-remote-api-client [url]
  (->CruxNode (api/new-api-client url)))

(defn start-async-ingest-client [opts]
  (->CruxAsyncIngestClient (api/new-ingest-client opts)))

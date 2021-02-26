(ns crux.api.java
  (:refer-clojure :exclude [sync])
  (:require [crux.api :as api])
  (:import clojure.lang.IDeref
           [crux.api CruxDocument DBBasis ICruxAPI ICruxAsyncIngestAPI ICruxDatasource TransactionInstant]
           crux.api.tx.Transaction
           java.time.Duration
           [java.util Date List Map]
           java.util.concurrent.CompletableFuture
           java.util.function.Supplier))

(defrecord JCruxDatasource [^java.io.Closeable datasource]
  ICruxDatasource
  (entity [_ eid] (CruxDocument/factory (api/entity datasource eid)))
  (entityTx [_ eid] (api/entity-tx datasource eid))
  (query [_ query args] (api/q* datasource query args))
  (openQuery [_ query args] (api/open-q* datasource query args))
  (pull [_ projection eid] (api/pull datasource projection eid))
  (^java.util.List pullMany [_ projection ^Iterable eids] (api/pull-many datasource projection eids))
  (^java.util.List pullMany [_ projection ^"[Ljava.lang.Object;" eids] (api/pull-many datasource projection (seq eids)))
  (^java.util.List entityHistory [_ eid ^crux.api.HistoryOptions opts] (api/entity-history datasource eid (.getSortOrderKey opts) (.toMap opts)))
  (^crux.api.ICursor openEntityHistory [_ eid ^crux.api.HistoryOptions opts] (api/open-entity-history datasource eid (.getSortOrderKey opts) (.toMap opts)))
  (validTime [_] (api/valid-time datasource))
  (transactionTime [_] (api/transaction-time datasource))
  (dbBasis [_] (DBBasis/factory (api/db-basis datasource)))
  (^ICruxDatasource withTx [_ ^Transaction tx] (->JCruxDatasource (api/with-tx datasource (.toVector tx))))
  (^ICruxDatasource withTx [_ ^List tx-ops] (->JCruxDatasource (api/with-tx datasource tx-ops)))
  (close [_] (.close datasource)))

(defrecord JCruxNode [^java.io.Closeable node]
  ICruxAPI
  (^ICruxDatasource db [_] (->JCruxDatasource (api/db node)))
  (^ICruxDatasource db [_ ^Date valid-time] (->JCruxDatasource (api/db node valid-time)))
  (^ICruxDatasource db [_ ^Date valid-time ^Date tx-time] (->JCruxDatasource (api/db node valid-time tx-time)))
  (^ICruxDatasource db [_ ^DBBasis basis] (->JCruxDatasource (api/db node (.toMap basis))))
  (^ICruxDatasource db [_ ^TransactionInstant tx-instant] (->JCruxDatasource (api/db node (.toMap tx-instant))))
  (^ICruxDatasource db [_ ^Map db-basis] (->JCruxDatasource (api/db node db-basis)))

  (^ICruxDatasource openDB [_] (->JCruxDatasource (api/open-db node)))
  (^ICruxDatasource openDB [_ ^Date valid-time] (->JCruxDatasource (api/open-db node valid-time)))
  (^ICruxDatasource openDB [_ ^Date valid-time ^Date tx-time] (->JCruxDatasource (api/open-db node valid-time tx-time)))
  (^ICruxDatasource openDB [_ ^DBBasis basis] (->JCruxDatasource (api/open-db node (.toMap basis))))
  (^ICruxDatasource openDB [_ ^Map basis] (->JCruxDatasource (api/open-db node basis)))
  (^ICruxDatasource openDB [_ ^TransactionInstant tx-instant] (->JCruxDatasource (api/open-db node (.toMap tx-instant))))

  (status [_] (api/status node))
  (attributeStats [_] (api/attribute-stats node))

  (^Map submitTx [_ ^List tx] (api/submit-tx node tx))
  (^TransactionInstant submitTx [_ ^Transaction tx] (TransactionInstant/factory ^Map (api/submit-tx node (.toVector tx))))

  (^boolean hasTxCommitted [_ ^Map transaction] (api/tx-committed? node transaction))
  (^boolean hasTxCommitted [_ ^TransactionInstant transaction] (api/tx-committed? node (.toMap transaction)))

  (openTxLog [_ after-tx-id with-ops?] (api/open-tx-log node after-tx-id with-ops?))
  (sync [_ timeout] (api/sync node timeout))

  (awaitTxTime [_ tx-time timeout] (api/await-tx-time node tx-time timeout))
  (^TransactionInstant awaitTx [_ ^TransactionInstant submitted-tx ^Duration timeout] (TransactionInstant/factory ^Map (api/await-tx node (.toMap submitted-tx) timeout)))
  (^Map awaitTx [_ ^Map submitted-tx ^Duration timeout] (api/await-tx node submitted-tx timeout))

  (listen [_ event-opts consumer] (api/listen node event-opts #(.accept consumer %)))
  (latestCompletedTx [_] (TransactionInstant/factory ^Map (api/latest-completed-tx node)))
  (latestSubmittedTx [_] (TransactionInstant/factory ^Map (api/latest-submitted-tx node)))
  (activeQueries [_] (api/active-queries node))
  (recentQueries [_] (api/recent-queries node))
  (slowestQueries [_] (api/slowest-queries node))
  (close [_] (.close node)))

(defrecord JCruxIngestClient [^java.io.Closeable client]
  ICruxAsyncIngestAPI
  (^TransactionInstant submitTx [_ ^Transaction tx] (TransactionInstant/factory ^Map (api/submit-tx client (.toVector tx))))
  (^Map submitTx [_ ^List tx] (api/submit-tx client tx))

  (openTxLog [_ after-tx-id with-ops?] (api/open-tx-log client after-tx-id with-ops?))

  (^CompletableFuture submitTxAsync [_ ^Transaction tx]
   (CompletableFuture/supplyAsync
    (reify Supplier
      (get [_]
        (TransactionInstant/factory ^Map @(api/submit-tx-async client (.toVector tx)))))))

  (^IDeref submitTxAsync [_ ^List tx-ops] (api/submit-tx-async client tx-ops))

  (close [_] (.close client)))

(ns crux.api.java
  (:refer-clojure :exclude [sync])
  (:require [crux.api :as xt])
  (:import clojure.lang.IDeref
           [crux.api CruxDocument DBBasis ICruxAPI ICruxAsyncIngestAPI ICruxDatasource TransactionInstant]
           crux.api.tx.Transaction
           java.time.Duration
           [java.util Date List Map]
           java.util.concurrent.CompletableFuture
           java.util.function.Supplier))

(defrecord JCruxDatasource [^java.io.Closeable datasource]
  ICruxDatasource
  (entity [_ eid] (CruxDocument/factory (xt/entity datasource eid)))
  (entityTx [_ eid] (xt/entity-tx datasource eid))
  (query [_ query args] (xt/q* datasource query args))
  (openQuery [_ query args] (xt/open-q* datasource query args))
  (pull [_ projection eid] (xt/pull datasource projection eid))
  (^java.util.List pullMany [_ projection ^Iterable eids] (xt/pull-many datasource projection eids))
  (^java.util.List pullMany [_ projection ^"[Ljava.lang.Object;" eids] (xt/pull-many datasource projection (seq eids)))
  (^java.util.List entityHistory [_ eid ^crux.api.HistoryOptions opts] (xt/entity-history datasource eid (.getSortOrderKey opts) (.toMap opts)))
  (^crux.api.ICursor openEntityHistory [_ eid ^crux.api.HistoryOptions opts] (xt/open-entity-history datasource eid (.getSortOrderKey opts) (.toMap opts)))
  (validTime [_] (xt/valid-time datasource))
  (transactionTime [_] (xt/transaction-time datasource))
  (dbBasis [_] (DBBasis/factory (xt/db-basis datasource)))
  (^ICruxDatasource withTx [_ ^Transaction tx] (->JCruxDatasource (xt/with-tx datasource (.toVector tx))))
  (^ICruxDatasource withTx [_ ^List tx-ops] (->JCruxDatasource (xt/with-tx datasource tx-ops)))
  (close [_] (.close datasource)))

(defrecord JCruxNode [^java.io.Closeable node]
  ICruxAPI
  (^ICruxDatasource db [_] (->JCruxDatasource (xt/db node)))
  (^ICruxDatasource db [_ ^Date valid-time] (->JCruxDatasource (xt/db node valid-time)))
  (^ICruxDatasource db [_ ^Date valid-time ^Date tx-time] (->JCruxDatasource (xt/db node valid-time tx-time)))
  (^ICruxDatasource db [_ ^DBBasis basis] (->JCruxDatasource (xt/db node (.toMap basis))))
  (^ICruxDatasource db [_ ^TransactionInstant tx-instant] (->JCruxDatasource (xt/db node (.toMap tx-instant))))
  (^ICruxDatasource db [_ ^Map db-basis] (->JCruxDatasource (xt/db node db-basis)))

  (^ICruxDatasource openDB [_] (->JCruxDatasource (xt/open-db node)))
  (^ICruxDatasource openDB [_ ^Date valid-time] (->JCruxDatasource (xt/open-db node valid-time)))
  (^ICruxDatasource openDB [_ ^Date valid-time ^Date tx-time] (->JCruxDatasource (xt/open-db node valid-time tx-time)))
  (^ICruxDatasource openDB [_ ^DBBasis basis] (->JCruxDatasource (xt/open-db node (.toMap basis))))
  (^ICruxDatasource openDB [_ ^Map basis] (->JCruxDatasource (xt/open-db node basis)))
  (^ICruxDatasource openDB [_ ^TransactionInstant tx-instant] (->JCruxDatasource (xt/open-db node (.toMap tx-instant))))

  (status [_] (xt/status node))
  (attributeStats [_] (xt/attribute-stats node))

  (^Map submitTx [_ ^List tx] (xt/submit-tx node tx))
  (^TransactionInstant submitTx [_ ^Transaction tx] (TransactionInstant/factory ^Map (xt/submit-tx node (.toVector tx))))

  (^boolean hasTxCommitted [_ ^Map transaction] (xt/tx-committed? node transaction))
  (^boolean hasTxCommitted [_ ^TransactionInstant transaction] (xt/tx-committed? node (.toMap transaction)))

  (openTxLog [_ after-tx-id with-ops?] (xt/open-tx-log node after-tx-id with-ops?))
  (sync [_ timeout] (xt/sync node timeout))

  (awaitTxTime [_ tx-time timeout] (xt/await-tx-time node tx-time timeout))
  (^TransactionInstant awaitTx [_ ^TransactionInstant submitted-tx ^Duration timeout] (TransactionInstant/factory ^Map (xt/await-tx node (.toMap submitted-tx) timeout)))
  (^Map awaitTx [_ ^Map submitted-tx ^Duration timeout] (xt/await-tx node submitted-tx timeout))

  (listen [_ event-opts consumer] (xt/listen node event-opts #(.accept consumer %)))
  (latestCompletedTx [_] (TransactionInstant/factory ^Map (xt/latest-completed-tx node)))
  (latestSubmittedTx [_] (TransactionInstant/factory ^Map (xt/latest-submitted-tx node)))
  (activeQueries [_] (xt/active-queries node))
  (recentQueries [_] (xt/recent-queries node))
  (slowestQueries [_] (xt/slowest-queries node))
  (close [_] (.close node)))

(defrecord JCruxIngestClient [^java.io.Closeable client]
  ICruxAsyncIngestAPI
  (^TransactionInstant submitTx [_ ^Transaction tx] (TransactionInstant/factory ^Map (xt/submit-tx client (.toVector tx))))
  (^Map submitTx [_ ^List tx] (xt/submit-tx client tx))

  (openTxLog [_ after-tx-id with-ops?] (xt/open-tx-log client after-tx-id with-ops?))

  (^CompletableFuture submitTxAsync [_ ^Transaction tx]
   (CompletableFuture/supplyAsync
    (reify Supplier
      (get [_]
        (TransactionInstant/factory ^Map @(xt/submit-tx-async client (.toVector tx)))))))

  (^IDeref submitTxAsync [_ ^List tx-ops] (xt/submit-tx-async client tx-ops))

  (close [_] (.close client)))

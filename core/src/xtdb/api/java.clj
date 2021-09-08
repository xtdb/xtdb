(ns xtdb.api.java
  (:refer-clojure :exclude [sync])
  (:require [xtdb.api :as xt])
  (:import clojure.lang.IDeref
           [xtdb.api XtdbDocument DBBasis IXtdb IXtdbSubmitClient IXtdbDatasource TransactionInstant]
           xtdb.api.tx.Transaction
           java.time.Duration
           [java.util Date List Map]
           java.util.concurrent.CompletableFuture
           java.util.function.Supplier))

(defrecord JXtdbDatasource [^java.io.Closeable datasource]
  IXtdbDatasource
  (entity [_ eid] (XtdbDocument/factory (xt/entity datasource eid)))
  (entityTx [_ eid] (xt/entity-tx datasource eid))
  (query [_ query args] (xt/q* datasource query args))
  (openQuery [_ query args] (xt/open-q* datasource query args))
  (pull [_ projection eid] (xt/pull datasource projection eid))
  (^java.util.List pullMany [_ projection ^Iterable eids] (xt/pull-many datasource projection eids))
  (^java.util.List pullMany [_ projection ^"[Ljava.lang.Object;" eids] (xt/pull-many datasource projection (seq eids)))
  (^java.util.List entityHistory [_ eid ^xtdb.api.HistoryOptions opts] (xt/entity-history datasource eid (.getSortOrderKey opts) (.toMap opts)))
  (^xtdb.api.ICursor openEntityHistory [_ eid ^xtdb.api.HistoryOptions opts] (xt/open-entity-history datasource eid (.getSortOrderKey opts) (.toMap opts)))
  (validTime [_] (xt/valid-time datasource))
  (transactionTime [_] (xt/transaction-time datasource))
  (dbBasis [_] (DBBasis/factory (xt/db-basis datasource)))
  (^IXtdbDatasource withTx [_ ^Transaction tx] (->JXtdbDatasource (xt/with-tx datasource (.toVector tx))))
  (^IXtdbDatasource withTx [_ ^List tx-ops] (->JXtdbDatasource (xt/with-tx datasource tx-ops)))
  (close [_] (.close datasource)))

(defrecord JXtdbNode [^java.io.Closeable node]
  IXtdb
  (^IXtdbDatasource db [_] (->JXtdbDatasource (xt/db node)))
  (^IXtdbDatasource db [_ ^Date valid-time] (->JXtdbDatasource (xt/db node valid-time)))
  (^IXtdbDatasource db [_ ^Date valid-time ^Date tx-time] (->JXtdbDatasource (xt/db node valid-time tx-time)))
  (^IXtdbDatasource db [_ ^DBBasis basis] (->JXtdbDatasource (xt/db node (.toMap basis))))
  (^IXtdbDatasource db [_ ^TransactionInstant tx-instant] (->JXtdbDatasource (xt/db node (.toMap tx-instant))))
  (^IXtdbDatasource db [_ ^Map db-basis] (->JXtdbDatasource (xt/db node db-basis)))

  (^IXtdbDatasource openDB [_] (->JXtdbDatasource (xt/open-db node)))
  (^IXtdbDatasource openDB [_ ^Date valid-time] (->JXtdbDatasource (xt/open-db node valid-time)))
  (^IXtdbDatasource openDB [_ ^Date valid-time ^Date tx-time] (->JXtdbDatasource (xt/open-db node valid-time tx-time)))
  (^IXtdbDatasource openDB [_ ^DBBasis basis] (->JXtdbDatasource (xt/open-db node (.toMap basis))))
  (^IXtdbDatasource openDB [_ ^Map basis] (->JXtdbDatasource (xt/open-db node basis)))
  (^IXtdbDatasource openDB [_ ^TransactionInstant tx-instant] (->JXtdbDatasource (xt/open-db node (.toMap tx-instant))))

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

(defrecord JXtdbSubmitClient [^java.io.Closeable client]
  IXtdbSubmitClient
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

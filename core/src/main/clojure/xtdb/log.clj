(ns xtdb.log
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.api :as xt]
            [xtdb.db-catalog :as db]
            [xtdb.error :as err]
            [xtdb.node :as xtn]
            [xtdb.table :as table]
            [xtdb.time :as time]
            [xtdb.tx-ops :as tx-ops]
            [xtdb.util :as util])
  (:import (java.time Duration Instant)
           (java.util HashMap List)
           java.util.concurrent.TimeUnit
           org.apache.arrow.memory.BufferAllocator
           (xtdb.api IndexerConfig TransactionKey Xtdb$Config)
           (xtdb.api.log Log Log$Cluster$Factory Log$Factory Log$Message$Tx Log$MessageMetadata)
           (xtdb.arrow Relation Vector)
           xtdb.catalog.BlockCatalog
           (xtdb.database Database Database$Catalog)
           xtdb.indexer.LogProcessor
           xtdb.table.TableRef
           (xtdb.tx TxOp$DeleteDocs TxOp$EraseDocs TxOp$PatchDocs TxOp$PutDocs TxOp$Sql TxOpts TxWriter)
           (xtdb.tx_ops DeleteDocs EraseDocs PatchDocs PutDocs PutRel Sql SqlByteArgs)
           (xtdb.util MsgIdUtil)))

(set! *unchecked-math* :warn-on-boxed)

(def ^:private forbidden-schemas #{"xt" "information_schema" "pg_catalog"})

(defn forbidden-table? [^TableRef table]
  (contains? forbidden-schemas (.getSchemaName table)))

(defn forbidden-table-ex [table]
  (err/incorrect :xtdb/forbidden-table (format "Cannot write to table: %s" (table/ref->schema+table table))
                 {:table table}))

(defprotocol OpenTxOp
  (open-tx-op [tx-op al opts]))

(extend-protocol OpenTxOp
  Sql
  (open-tx-op [{:keys [sql arg-rows]} al _opts]
    (TxOp$Sql. sql (when (seq arg-rows)
                     (Relation/openFromRows al
                                            (for [arg-row arg-rows]
                                              (into {}
                                                    (map-indexed (fn [idx v] [(str "?_" idx) v]))
                                                    arg-row))))))

  SqlByteArgs
  (open-tx-op [{:keys [sql arg-bytes]} al _opts]
    (TxOp$Sql. sql (when arg-bytes
                     (Relation/openFromArrowStream al arg-bytes))))

  PutDocs
  (open-tx-op [{:keys [table-name docs valid-from valid-to]} al opts]
    (doseq [doc docs]
      (when-not (or (:xt/id doc) (get doc "_id"))
        (throw (err/incorrect :missing-id "missing '_id'" {:doc doc}))))

    (TxOp$PutDocs. (or (namespace table-name) "public") (name table-name)
                   (some-> valid-from (time/->instant opts)) (some-> valid-to (time/->instant opts))
                   (Relation/openFromRows al docs)))

  PutRel
  (open-tx-op [{:keys [table-name ^bytes rel-bytes]} ^BufferAllocator al, _opts]
    ;; doesn't (yet?) support valid-from/to
    (util/with-open [ldr (Relation/streamLoader al rel-bytes)]
      (util/with-close-on-catch [rel (Relation. al (.getSchema ldr))]
        (or (.loadNextPage ldr rel)
            (throw (AssertionError. "No data in PutRel rel-bytes")))
        (TxOp$PutDocs. (or (namespace table-name) "public") (name table-name)
                       nil nil
                       rel))))

  PatchDocs
  (open-tx-op [{:keys [table-name docs valid-from valid-to]} al opts]
    (TxOp$PatchDocs. (or (namespace table-name) "public") (name table-name)
                     (some-> valid-from (time/->instant opts)) (some-> valid-to (time/->instant opts))
                     (Relation/openFromRows al docs)))

  DeleteDocs
  (open-tx-op [{:keys [table-name ^List doc-ids valid-from valid-to]} ^BufferAllocator al, opts]
    (TxOp$DeleteDocs. (or (namespace table-name) "public") (name table-name)
                      (some-> valid-from (time/->instant opts)) (some-> valid-to (time/->instant opts))
                      (Vector/fromList al "_id" doc-ids)))

  EraseDocs
  (open-tx-op [{:keys [table-name ^List doc-ids]} ^BufferAllocator al, _opts]
    (TxOp$EraseDocs. (or (namespace table-name) "public") (name table-name)
                     (Vector/fromList al "_id" doc-ids))))

(defn serialize-tx-ops ^bytes [^BufferAllocator allocator tx-ops
                               {:keys [^Instant system-time, default-tz user-metadata], {:keys [user]} :authn, :as opts}]
  (util/with-open [ops (util/safe-mapv #(open-tx-op % allocator opts) tx-ops)]
    (TxWriter/serializeTxOps ops allocator (TxOpts. default-tz (time/->instant system-time) user user-metadata))))

(defmulti ->log-cluster-factory
  (fn [k _opts]
    (when-let [ns (namespace k)]
      (doseq [k [(symbol ns)
                 (symbol (str ns "." (name k)))]]

        (try
          (require k)
          (catch Throwable _))))
    k)
  :default ::default)

(defmethod ->log-cluster-factory ::default [k _]
  (throw (err/incorrect :xtdb/unknown-log-cluster-type (format "Unknown log cluster type: %s" k)
                        {:log-cluster-type k})))

(defmethod ->log-cluster-factory :kafka [_ opts] (->log-cluster-factory :xtdb.kafka/cluster opts))

(defmethod xtn/apply-config! ::clusters [^Xtdb$Config config _ clusters]
  (doseq [[cluster-alias [tag opts]] clusters]
    (.logCluster config (str (symbol cluster-alias)) (->log-cluster-factory tag opts)))
  config)

(defmethod ig/init-key ::clusters [_ clusters]
  (util/with-close-on-catch [!clusters (HashMap.)]
    (doseq [[cluster-alias ^Log$Cluster$Factory factory] clusters]
      (.put !clusters
            (str (symbol cluster-alias))
            (.open factory)))
    (into {} !clusters)))

(defmulti ->log-factory
  (fn [k _opts]
    (when-let [ns (namespace k)]
      (doseq [k [(symbol ns)
                 (symbol (str ns "." (name k)))]]

        (try
          (require k)
          (catch Throwable _))))
    k)
  :default ::default)

(defmethod ->log-factory ::default [k _]
  (throw (err/incorrect :xtdb/unknown-log-type (format "Unknown log type: %s" k)
                        {:log-type k})))

(defmethod ->log-factory ::in-memory [_ {:keys [instant-src epoch]}]
  (cond-> (Log/getInMemoryLog)
    instant-src (.instantSource instant-src)
    epoch (.epoch epoch)))

(defmethod ->log-factory ::local [_ {:keys [path instant-src epoch instant-source-for-non-tx-msgs?]}]
  (cond-> (Log/localLog (util/->path path))
    instant-src (.instantSource instant-src)
    epoch (.epoch epoch)
    instant-source-for-non-tx-msgs? (.useInstantSourceForNonTx)))

(defmethod ->log-factory :in-memory [_ opts] (->log-factory ::in-memory opts))
(defmethod ->log-factory :local [_ opts] (->log-factory ::local opts))
(defmethod ->log-factory :kafka [_ opts] (->log-factory :xtdb.kafka/kafka opts))

(defmethod xtn/apply-config! :xtdb/log [^Xtdb$Config config _ [tag opts]]
  (.log config (->log-factory tag opts)))

(defmethod ig/expand-key :xtdb/log [k {:keys [base factory]}]
  {k {:base base
      :block-cat (ig/ref :xtdb/block-catalog)
      :factory factory}})

(def out-of-sync-log-message
  "Node failed to start due to an invalid transaction log state (%s) that does not correspond with the latest indexed transaction (epoch=%s and offset=%s).

   Please see https://docs.xtdb.com/ops/backup-and-restore/out-of-sync-log.html for more information and next steps.")

(defn ->out-of-sync-exception [latest-completed-offset ^long latest-submitted-offset ^long epoch]
  (let [log-state-str (if (= -1 latest-submitted-offset)
                        "the log is empty"
                        (format "epoch=%s, offset=%s" epoch latest-submitted-offset))]
    (IllegalStateException.
     (format out-of-sync-log-message log-state-str epoch latest-completed-offset))))

(defn validate-offsets [^Log log ^TransactionKey latest-completed-tx]
  (when latest-completed-tx
    (let [latest-completed-tx-id (.getTxId latest-completed-tx)
          latest-completed-offset (MsgIdUtil/msgIdToOffset latest-completed-tx-id)
          latest-completed-epoch (MsgIdUtil/msgIdToEpoch latest-completed-tx-id)
          epoch (.getEpoch log)
          latest-submitted-offset (.getLatestSubmittedOffset log)]
      (if (= latest-completed-epoch epoch)
        (cond
          (< latest-submitted-offset latest-completed-offset)
          (throw (->out-of-sync-exception latest-completed-offset latest-submitted-offset epoch)))
        (log/info "Starting node with a log that has a different epoch than the latest completed tx (This is expected if you are starting a new epoch) - Skipping offset validation.")))))

(defmethod ig/init-key :xtdb/log [_ {:keys [^BlockCatalog block-cat, ^Log$Factory factory]
                                     {:keys [log-clusters]} :base}]
  (doto (.openLog factory log-clusters)
    (validate-offsets (.getLatestCompletedTx block-cat))))

(defmethod ig/halt-key! :xtdb/log [_ ^Log log]
  (util/close log))

(defn- ->TxOps [tx-ops]
  (->> tx-ops
       (mapv (fn [tx-op]
               (cond-> tx-op
                 (not (record? tx-op)) tx-ops/parse-tx-op)))))

(defn submit-tx ^long
  [{:keys [^BufferAllocator allocator, ^Database$Catalog db-cat, default-tz]} tx-ops {:keys [default-db, system-time] :as opts}]

  (let [^Database db (or (.databaseOrNull db-cat default-db)
                         (throw (err/incorrect :xtdb/unknown-db (format "Unknown database: %s" default-db)
                                               {:db-name default-db})))
        log (.getLog db)
        default-tz (:default-tz opts default-tz)]
    (util/rethrowing-cause
      (let [^Log$MessageMetadata message-meta @(.appendMessage log
                                                               (Log$Message$Tx. (serialize-tx-ops allocator (->TxOps tx-ops)
                                                                                                  (-> (select-keys opts [:authn :default-db])
                                                                                                      (assoc :default-tz (:default-tz opts default-tz)
                                                                                                             :system-time (some-> system-time time/expect-instant))))))]
        (MsgIdUtil/offsetToMsgId (.getEpoch log) (.getLogOffset message-meta))))))

(defmethod ig/expand-key :xtdb.log/processor [k {:keys [base ^IndexerConfig indexer-conf]}]
  {k {:base base
      :allocator (ig/ref :xtdb.db-catalog/allocator)
      :db (ig/ref :xtdb.db-catalog/for-query)
      :indexer (ig/ref :xtdb.indexer/for-db)
      :compactor (ig/ref :xtdb.compactor/for-db)
      :block-flush-duration (.getFlushDuration indexer-conf)
      :skip-txs (.getSkipTxs indexer-conf)
      :enabled? (.getEnabled indexer-conf)}})

(defmethod ig/init-key :xtdb.log/processor [_ {{:keys [meter-registry db-catalog]} :base
                                               :keys [allocator db indexer compactor block-flush-duration skip-txs enabled?]}]
  (when enabled?
    (LogProcessor. allocator meter-registry db-catalog db indexer compactor block-flush-duration (set skip-txs))))

(defmethod ig/halt-key! :xtdb.log/processor [_ ^LogProcessor log-processor]
  (util/close log-processor))

(defn await-db
  ([db msg-id] (await-db db msg-id nil))
  ([^Database db, ^long msg-id, ^Duration timeout]
   @(cond-> (.awaitAsync (.getLogProcessor db) msg-id)
      timeout (.orTimeout (.toMillis timeout) TimeUnit/MILLISECONDS))))

(defn sync-db
  ([db] (sync-db db nil))
  ([^Database db, ^Duration timeout]
   (let [msg-id (.getLatestSubmittedMsgId (.getLogProcessor db))]
     (await-db db msg-id timeout))))

(defn await-node
  ([node token] (await-node node token nil))
  ([node token timeout] (.awaitAll (db/<-node node) token timeout)))

(defn sync-node
  ([node] (sync-node node nil))
  ([node timeout] (.syncAll (db/<-node node) timeout)))

(defn send-flush-block-msg! [^Database db]
  (.sendFlushBlockMessage db))

(defn send-attach-db! ^long [^Database primary-db, db-name, db-config]
  (MsgIdUtil/offsetToMsgId (.getEpoch (.getLog primary-db))
                           (.getLogOffset (.sendAttachDbMessage primary-db db-name db-config))))

(defn send-detach-db! ^long [^Database primary-db, db-name]
  (MsgIdUtil/offsetToMsgId (.getEpoch (.getLog primary-db))
                           (.getLogOffset (.sendDetachDbMessage primary-db db-name))))

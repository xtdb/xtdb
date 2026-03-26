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
           (xtdb.api TransactionKey Xtdb$Config)
           (xtdb.api.log Log Log$Cluster$Factory Log$Factory)
           (xtdb.arrow Relation Vector)
           (xtdb.database Database DatabaseStorage Database$Catalog Database$Config)
           xtdb.table.TableRef
           (xtdb.tx TxOp$DeleteDocs TxOp$EraseDocs TxOp$PatchDocs TxOp$PutDocs TxOp$Sql TxOpts)
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
        default-tz (:default-tz opts default-tz)]
    (util/rethrowing-cause
      (util/with-open [ops (util/safe-mapv #(open-tx-op % allocator opts) (->TxOps tx-ops))]
        (.getTxId (.submitTxBlocking db ops (TxOpts. default-tz (some-> system-time time/expect-instant)
                                                     (get-in opts [:authn :user]) (:user-metadata opts))))))))


(defn await-tx [^Database db, ^long tx-id, ^Duration timeout]
  (.awaitTxBlocking db tx-id timeout))

(defn await-source [^Database db, ^long msg-id, ^Duration timeout]
  (.awaitSourceBlocking db msg-id timeout))

(defn sync-db [^Database db, ^Duration timeout]
  (let [msg-id (.getLatestSubmittedMsgId (.getSourceLog db))]
    (await-source db msg-id timeout)))

(defn await-node [node token timeout]
  (.awaitAll (db/<-node node) token timeout))

(defn sync-node [node timeout]
  (.syncAll (db/<-node node) timeout))

(defn send-flush-block-msg! [^Database db]
  (.sendFlushBlockMessage db))

(defn send-attach-db! ^long [^Database primary-db, db-name, db-config]
  (.getMsgId (.sendAttachDbMessage primary-db db-name db-config)))

(defn send-detach-db! ^long [^Database primary-db, db-name]
  (.getMsgId (.sendDetachDbMessage primary-db db-name)))

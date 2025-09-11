(ns xtdb.log
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.api :as xt]
            [xtdb.db-catalog :as db]
            [xtdb.error :as err]
            [xtdb.node :as xtn]
            [xtdb.sql :as sql]
            [xtdb.table :as table]
            [xtdb.time :as time]
            [xtdb.tx-ops :as tx-ops]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (java.time Duration Instant)
           (java.util ArrayList HashMap)
           java.util.concurrent.TimeUnit
           org.apache.arrow.memory.BufferAllocator
           (org.apache.arrow.vector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo ArrowType$Union FieldType Schema)
           org.apache.arrow.vector.types.UnionMode
           (xtdb.api IndexerConfig TransactionKey Xtdb$Config)
           (xtdb.api.log Log Log$Cluster$Factory Log$Factory Log$Message$Tx Log$MessageMetadata)
           (xtdb.api.tx TxOp TxOp$Sql)
           (xtdb.arrow Relation VectorWriter)
           xtdb.catalog.BlockCatalog
           (xtdb.database Database Database$Catalog)
           xtdb.indexer.LogProcessor
           xtdb.table.TableRef
           (xtdb.tx_ops DeleteDocs EraseDocs PatchDocs PutDocs SqlByteArgs)
           (xtdb.util MsgIdUtil)))

(set! *unchecked-math* :warn-on-boxed)

(def ^:private ^org.apache.arrow.vector.types.pojo.Field tx-ops-field
  (types/->field "tx-ops" (ArrowType$Union. UnionMode/Dense nil) false))

(def ^:private ^org.apache.arrow.vector.types.pojo.Schema tx-schema
  (Schema. [(types/->field "tx-ops" #xt.arrow/type :list false (types/field-with-name tx-ops-field "$data$"))

            (types/col-type->field "system-time" types/nullable-temporal-type)
            (types/col-type->field "default-tz" :utf8)
            (types/->field "user" #xt.arrow/type :utf8 true)]))

(def ^:private forbidden-schemas #{"xt" "information_schema" "pg_catalog"})

(defn forbidden-table? [^TableRef table]
  (contains? forbidden-schemas (.getSchemaName table)))

(defn forbidden-table-ex [table]
  (err/incorrect :xtdb/forbidden-table (format "Cannot write to table: %s" (table/ref->schema+table table))
                 {:table table}))

(defn encode-sql-args [^BufferAllocator allocator, arg-rows]
  (if (apply not= (map count arg-rows))
    (throw (err/illegal-arg :sql/arg-rows-different-lengths
                            {::err/message "All SQL arg-rows must have the same number of columns"
                             :arg-rows arg-rows}))
    (let [param-count (count (first arg-rows))
          vecs (ArrayList. param-count)]
      (try
        (dotimes [col-idx param-count]
          (.add vecs
                (vw/open-vec allocator (symbol (str "?_" col-idx))
                             (mapv #(nth % col-idx nil) arg-rows))))

        (let [root (doto (VectorSchemaRoot. vecs) (.setRowCount (count arg-rows)))]
          (util/build-arrow-ipc-byte-buffer root :stream
            (fn [write-page!]
              (write-page!))))

        (finally
          (run! util/try-close vecs))))))

(defn- ->sql-writer [^VectorWriter op-writer, ^BufferAllocator allocator]
  (let [sql-writer (.vectorFor op-writer "sql" (FieldType/notNullable #xt.arrow/type :struct))
        query-writer (.vectorFor sql-writer "query" (FieldType/notNullable #xt.arrow/type :utf8))
        args-writer (.vectorFor sql-writer "args" (FieldType/nullable #xt.arrow/type :varbinary))]
    (fn write-sql! [^TxOp$Sql op]
      (let [sql (.sql op)]
        (.writeObject query-writer sql)

        (when-let [arg-rows (not-empty (.argRows op))]
          (.writeObject args-writer (encode-sql-args allocator arg-rows))))

      (.endStruct sql-writer))))

(defn- ->sql-byte-args-writer [^VectorWriter op-writer]
  (let [sql-writer (.vectorFor op-writer "sql" (FieldType/notNullable #xt.arrow/type :struct))
        query-writer (.vectorFor sql-writer "query" (FieldType/notNullable #xt.arrow/type :utf8))
        args-writer (.vectorFor sql-writer "args" (FieldType/nullable #xt.arrow/type :varbinary))]
    (fn write-sql! [{:keys [sql arg-bytes]}]
      (.writeObject query-writer sql)

      (when arg-bytes
        (.writeObject args-writer arg-bytes))

      (.endStruct sql-writer))))

(defn- ->docs-op-writer [db-name, ^VectorWriter op-writer]
  (let [iids-writer (.vectorFor op-writer "iids" (FieldType/notNullable #xt.arrow/type :list))
        iid-writer (some-> iids-writer
                           (.getListElements (FieldType/notNullable #xt.arrow/type [:fixed-size-binary 16])))
        doc-writer (.vectorFor op-writer "documents" (FieldType/notNullable #xt.arrow/type :union))
        valid-from-writer (.vectorFor op-writer "_valid_from" types/nullable-temporal-field-type)
        valid-to-writer (.vectorFor op-writer "_valid_to" types/nullable-temporal-field-type)
        table-doc-writers (HashMap.)]
    (fn write-put! [{:keys [table-name docs valid-from valid-to]} opts]
      (let [table (table/->ref db-name table-name)]
        (when (forbidden-table? table) (throw (forbidden-table-ex table)))

        (let [^VectorWriter table-doc-writer
              (.computeIfAbsent table-doc-writers table
                                (fn [table]
                                  (doto (.vectorFor doc-writer (str (table/ref->schema+table table)) (FieldType/notNullable #xt.arrow/type :list))
                                    (.getListElements (FieldType/notNullable #xt.arrow/type :struct)))))]

          (.writeObject table-doc-writer docs)

          (doseq [doc docs
                  :let [eid (val (or (->> doc
                                          (some (fn [e]
                                                  (let [k (key e)]
                                                    (when (.equals "_id" (cond-> k
                                                                           (keyword? k) util/->normal-form-str))
                                                      e)))))
                                     (throw (err/illegal-arg :missing-id {:doc doc}))))]]
            (.writeBytes iid-writer (util/->iid eid)))
          (.endList iids-writer))

        (.writeObject valid-from-writer (time/->instant valid-from opts))
        (.writeObject valid-to-writer (time/->instant valid-to opts))

        (.endStruct op-writer)))))

(defn- ->put-writer [db-name, ^VectorWriter op-writer]
  (->docs-op-writer db-name (.vectorFor op-writer "put-docs" (FieldType/notNullable #xt.arrow/type :struct))))

(defn- ->patch-writer [db-name, ^VectorWriter op-writer]
  (->docs-op-writer db-name (.vectorFor op-writer "patch-docs" (FieldType/notNullable #xt.arrow/type :struct))))

(defn- ->delete-writer [db-name, ^VectorWriter op-writer]
  (let [delete-writer (.vectorFor op-writer "delete-docs" (FieldType/notNullable #xt.arrow/type :struct))
        table-writer (.vectorFor delete-writer "table" (FieldType/notNullable #xt.arrow/type :utf8))
        iids-writer (.vectorFor delete-writer "iids" (FieldType/notNullable #xt.arrow/type :list))
        iid-writer (some-> iids-writer
                           (.getListElements (FieldType/notNullable #xt.arrow/type [:fixed-size-binary 16])))
        valid-from-writer (.vectorFor delete-writer "_valid_from" types/nullable-temporal-field-type)
        valid-to-writer (.vectorFor delete-writer "_valid_to" types/nullable-temporal-field-type)]
    (fn write-delete! [{:keys [table-name doc-ids valid-from valid-to]}]
      (let [table (table/->ref db-name table-name)]
        (when (forbidden-table? table) (throw (forbidden-table-ex table)))
        (when (seq doc-ids)
          (.writeObject table-writer (str (table/ref->schema+table table)))

          (doseq [doc-id doc-ids]
            (.writeObject iid-writer (util/->iid doc-id)))
          (.endList iids-writer)

          (.writeObject valid-from-writer valid-from)
          (.writeObject valid-to-writer valid-to)

          (.endStruct delete-writer))))))

(defn- ->erase-writer [db-name, ^VectorWriter op-writer]
  (let [erase-writer (.vectorFor op-writer "erase-docs" (FieldType/notNullable #xt.arrow/type :struct))
        table-writer (.vectorFor erase-writer "table" (FieldType/notNullable #xt.arrow/type :utf8))
        iids-writer (.vectorFor erase-writer "iids" (FieldType/notNullable #xt.arrow/type :list))
        iid-writer (some-> iids-writer
                           (.getListElements (FieldType/notNullable #xt.arrow/type [:fixed-size-binary 16])))]
    (fn [{:keys [table-name doc-ids]}]
      (let [table (table/->ref db-name table-name)]
        (when (forbidden-table? table) (throw (forbidden-table-ex table)))
        (when (seq doc-ids)
          (.writeObject table-writer (str (table/ref->schema+table table)))

          (doseq [doc-id doc-ids]
            (.writeObject iid-writer (util/->iid doc-id)))
          (.endList iids-writer)

          (.endStruct erase-writer))))))

(defn write-tx-ops! [^BufferAllocator allocator, ^VectorWriter op-writer, tx-ops, {:keys [default-db default-tz]}]
  (let [!write-sql! (delay (->sql-writer op-writer allocator))
        !write-sql-byte-args! (delay (->sql-byte-args-writer op-writer))
        !write-put! (delay (->put-writer default-db op-writer))
        !write-patch! (delay (->patch-writer default-db op-writer))
        !write-delete! (delay (->delete-writer default-db op-writer))
        !write-erase! (delay (->erase-writer default-db op-writer))]

    (doseq [tx-op tx-ops]
      (condp instance? tx-op
        TxOp$Sql (let [^TxOp$Sql tx-op tx-op]
                   (if-let [put-docs-ops (sql/sql->static-ops (.sql tx-op) (.argRows tx-op))]
                     (doseq [op put-docs-ops]
                       (@!write-put! op {:default-tz default-tz}))

                     (@!write-sql! tx-op)))

        SqlByteArgs (@!write-sql-byte-args! tx-op)
        PutDocs (@!write-put! tx-op {:default-tz default-tz})
        PatchDocs (@!write-patch! tx-op {:default-tz default-tz})
        DeleteDocs (@!write-delete! tx-op)
        EraseDocs (@!write-erase! tx-op)
        (throw (err/illegal-arg :invalid-tx-op {:tx-op tx-op}))))))

(defn serialize-tx-ops ^bytes [^BufferAllocator allocator tx-ops
                               {:keys [^Instant system-time, default-tz], {:keys [user]} :authn, :as opts}]
  (with-open [rel (Relation/open allocator tx-schema)]
    (let [ops-list-writer (.get rel "tx-ops")

          default-tz-writer (.get rel "default-tz")
          user-writer (.get rel "user")]

      (when system-time
        (.writeObject (.get rel "system-time") (-> (time/->zdt system-time)
                                                   (.withZoneSameInstant time/utc))))

      (when user
        (.writeObject user-writer user))

      (when default-tz
        (.writeObject default-tz-writer (str default-tz)))

      (write-tx-ops! allocator (.getListElements ops-list-writer) tx-ops opts)
      (.endList ops-list-writer)

      (.endRow rel)

      (.getAsArrowStream rel))))

(defmethod xtn/apply-config! ::clusters [^Xtdb$Config config _ clusters]
  (doseq [[cluster-alias [tag opts]] clusters]
    (xtn/apply-config! config
                       (case tag
                         :kafka :xtdb.kafka/cluster
                         tag)
                       [cluster-alias opts]))
  config)

(defmethod ig/init-key ::clusters [_ clusters]
  (util/with-close-on-catch [!clusters (HashMap.)]
    (doseq [[cluster-alias ^Log$Cluster$Factory factory] clusters]
      (.put !clusters
            (str (symbol cluster-alias))
            (.open factory)))
    (into {} !clusters)))

(defmethod xtn/apply-config! ::in-memory [^Xtdb$Config config _ {:keys [instant-src epoch]}]
  (.log config
        (cond-> (Log/getInMemoryLog)
          instant-src (.instantSource instant-src)
          epoch (.epoch epoch))))

(defmethod xtn/apply-config! ::local [^Xtdb$Config config _ {:keys [path instant-src epoch instant-source-for-non-tx-msgs?]}]
  (.log config
        (cond-> (Log/localLog (util/->path path))
          instant-src (.instantSource instant-src)
          epoch (.epoch epoch)
          instant-source-for-non-tx-msgs? (.useInstantSourceForNonTx))))

(defmethod xtn/apply-config! :xtdb/log [^Xtdb$Config config _ [tag opts]]
  (xtn/apply-config! config
                     (case tag
                       :in-memory ::in-memory
                       :local ::local
                       :kafka :xtdb/kafka)
                     opts))

(defmethod ig/prep-key :xtdb/log [_ {:keys [base factory]}]
  {:base base
   :block-cat (ig/ref :xtdb/block-catalog)
   :factory factory})

(def out-of-sync-log-message
  "Node failed to start due to an invalid transaction log state (%s) that does not correspond with the latest indexed transaction (epoch=%s and offset=%s).

   Please see https://docs.xtdb.com/ops/ops/backup-and-restore/out-of-sync-log for more information and next steps.")

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
                 (not (instance? TxOp tx-op)) tx-ops/parse-tx-op)))))

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

(defmethod ig/prep-key :xtdb.log/processor [_ {:keys [base ^IndexerConfig indexer-conf]}]
  {:base base
   :allocator (ig/ref :xtdb.db-catalog/allocator)
   :db (ig/ref :xtdb.db-catalog/for-query)
   :indexer (ig/ref :xtdb.indexer/for-db)
   :compactor (ig/ref :xtdb.compactor/for-db)
   :block-flush-duration (.getFlushDuration indexer-conf)
   :skip-txs (.getSkipTxs indexer-conf)})

(defmethod ig/init-key :xtdb.log/processor [_ {{:keys [meter-registry db-catalog]} :base
                                               :keys [allocator db indexer compactor block-flush-duration skip-txs] :as deps}]
  (when deps
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

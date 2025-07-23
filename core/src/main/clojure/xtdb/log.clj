(ns xtdb.log
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.api :as xt]
            [xtdb.basis :as basis]
            [xtdb.db-catalog :as db]
            [xtdb.error :as err]
            [xtdb.node :as xtn]
            [xtdb.protocols :as xtp]
            [xtdb.sql :as sql]
            [xtdb.time :as time]
            [xtdb.tx-ops :as tx-ops]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (java.time Duration Instant)
           (java.util ArrayList HashMap)
           [java.util.concurrent TimeUnit]
           org.apache.arrow.memory.BufferAllocator
           (org.apache.arrow.vector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo ArrowType$Union FieldType Schema)
           org.apache.arrow.vector.types.UnionMode
           (xtdb.api IndexerConfig TransactionKey Xtdb$Config)
           (xtdb.api.log Log Log$Factory Log$Message$Tx Log$MessageMetadata)
           (xtdb.api.tx TxOp TxOp$Sql)
           (xtdb.arrow Relation VectorWriter)
           xtdb.catalog.BlockCatalog
           (xtdb.database Database DatabaseCatalog)
           xtdb.indexer.LogProcessor
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

(def ^:private forbidden-tables #{"xt/" "information_schema/" "pg_catalog/"})

(defn forbidden-table? [table-name]
  (when-not (= table-name "xt/tx_fns")
    (some (fn [s] (str/starts-with? table-name s)) forbidden-tables)))

(defn forbidden-table-ex [table-name]
  (err/illegal-arg :xtdb/forbidden-table
                   {::err/message (format "Cannot write to table: %s" table-name)
                    :table-name table-name}))

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

(defn- ->docs-op-writer [^VectorWriter op-writer]
  (let [iids-writer (.vectorFor op-writer "iids" (FieldType/notNullable #xt.arrow/type :list))
        iid-writer (some-> iids-writer
                           (.getListElements (FieldType/notNullable #xt.arrow/type [:fixed-size-binary 16])))
        doc-writer (.vectorFor op-writer "documents" (FieldType/notNullable #xt.arrow/type :union))
        valid-from-writer (.vectorFor op-writer "_valid_from" types/nullable-temporal-field-type)
        valid-to-writer (.vectorFor op-writer "_valid_to" types/nullable-temporal-field-type)
        table-doc-writers (HashMap.)]
    (fn write-put! [{:keys [table-name docs valid-from valid-to]} opts]
      (let [table-name (str (symbol (util/with-default-schema table-name)))]
        (when (forbidden-table? table-name) (throw (forbidden-table-ex table-name)))

        (let [^VectorWriter table-doc-writer
              (.computeIfAbsent table-doc-writers table-name
                                (fn [table]
                                  (doto (.vectorFor doc-writer table (FieldType/notNullable #xt.arrow/type :list))
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

(defn- ->put-writer [^VectorWriter op-writer]
  (->docs-op-writer (.vectorFor op-writer "put-docs" (FieldType/notNullable #xt.arrow/type :struct))))

(defn- ->patch-writer [^VectorWriter op-writer]
  (->docs-op-writer (.vectorFor op-writer "patch-docs" (FieldType/notNullable #xt.arrow/type :struct))))

(defn- ->delete-writer [^VectorWriter op-writer]
  (let [delete-writer (.vectorFor op-writer "delete-docs" (FieldType/notNullable #xt.arrow/type :struct))
        table-writer (.vectorFor delete-writer "table" (FieldType/notNullable #xt.arrow/type :utf8))
        iids-writer (.vectorFor delete-writer "iids" (FieldType/notNullable #xt.arrow/type :list))
        iid-writer (some-> iids-writer
                           (.getListElements (FieldType/notNullable #xt.arrow/type [:fixed-size-binary 16])))
        valid-from-writer (.vectorFor delete-writer "_valid_from" types/nullable-temporal-field-type)
        valid-to-writer (.vectorFor delete-writer "_valid_to" types/nullable-temporal-field-type)]
    (fn write-delete! [{:keys [table-name doc-ids valid-from valid-to]}]
      (let [table-name (str (symbol (util/with-default-schema table-name)))]
        (when (forbidden-table? table-name) (throw (forbidden-table-ex table-name)))
        (when (seq doc-ids)
          (.writeObject table-writer table-name)

          (doseq [doc-id doc-ids]
            (.writeObject iid-writer (util/->iid doc-id)))
          (.endList iids-writer)

          (.writeObject valid-from-writer valid-from)
          (.writeObject valid-to-writer valid-to)

          (.endStruct delete-writer))))))

(defn- ->erase-writer [^VectorWriter op-writer]
  (let [erase-writer (.vectorFor op-writer "erase-docs" (FieldType/notNullable #xt.arrow/type :struct))
        table-writer (.vectorFor erase-writer "table" (FieldType/notNullable #xt.arrow/type :utf8))
        iids-writer (.vectorFor erase-writer "iids" (FieldType/notNullable #xt.arrow/type :list))
        iid-writer (some-> iids-writer
                           (.getListElements (FieldType/notNullable #xt.arrow/type [:fixed-size-binary 16])))]
    (fn [{:keys [table-name doc-ids]}]
      (let [table-name (str (symbol (util/with-default-schema table-name)))]
        (when (forbidden-table? table-name) (throw (forbidden-table-ex table-name)))
        (when (seq doc-ids)
          (.writeObject table-writer table-name)

          (doseq [doc-id doc-ids]
            (.writeObject iid-writer (util/->iid doc-id)))
          (.endList iids-writer)

          (.endStruct erase-writer))))))

(defn write-tx-ops! [^BufferAllocator allocator, ^VectorWriter op-writer, tx-ops, {:keys [default-tz]}]
  (let [!write-sql! (delay (->sql-writer op-writer allocator))
        !write-sql-byte-args! (delay (->sql-byte-args-writer op-writer))
        !write-put! (delay (->put-writer op-writer))
        !write-patch! (delay (->patch-writer op-writer))
        !write-delete! (delay (->delete-writer op-writer))
        !write-erase! (delay (->erase-writer op-writer))]

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

(defn serialize-tx-ops ^java.nio.ByteBuffer [^BufferAllocator allocator tx-ops {:keys [^Instant system-time, default-tz]
                                                                                {:keys [user]} :authn :as opts}]
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

(defmethod xtn/apply-config! ::memory-log [^Xtdb$Config config _ {:keys [instant-src epoch]}]
  (doto config
    (.setLog (cond-> (Log/getInMemoryLog)
               instant-src (.instantSource instant-src)
               epoch (.epoch epoch)))))

(defmethod xtn/apply-config! ::local-directory-log [^Xtdb$Config config _
                                                    {:keys [path instant-src epoch instant-source-for-non-tx-msgs?]}]
  (doto config
    (.setLog (cond-> (Log/localLog (util/->path path))
               instant-src (.instantSource instant-src)
               epoch (.epoch epoch)
               instant-source-for-non-tx-msgs? (.useInstantSourceForNonTx)))))

(defmethod xtn/apply-config! :xtdb/log [config _ [tag opts]]
  (xtn/apply-config! config
                     (case tag
                       :in-memory ::memory-log
                       :local ::local-directory-log
                       :kafka :xtdb.kafka/log)
                     opts))

(defmethod ig/prep-key :xtdb/log [_ {:keys [factory]}]
  {:block-cat (ig/ref :xtdb/block-catalog)
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

(defmethod ig/init-key :xtdb/log [_ {:keys [^BlockCatalog block-cat, ^Log$Factory factory]}]
  (doto (.openLog factory)
    (validate-offsets (.getLatestCompletedTx block-cat))))

(defmethod ig/halt-key! :xtdb/log [_ ^Log log]
  (util/close log))

(defn- ->TxOps [tx-ops]
  (->> tx-ops
       (mapv (fn [tx-op]
               (cond-> tx-op
                 (not (instance? TxOp tx-op)) tx-ops/parse-tx-op)))))

(defn submit-tx ^long
  [{:keys [^BufferAllocator allocator, ^DatabaseCatalog db-cat, default-tz]} tx-ops {:keys [system-time] :as opts}]

  (let [db (.getPrimary db-cat) ; TODO multi-db
        log (.getLog db)
        default-tz (:default-tz opts default-tz)]
    (util/rethrowing-cause
      (let [^Log$MessageMetadata message-meta @(.appendMessage log
                                                               (Log$Message$Tx. (serialize-tx-ops allocator (->TxOps tx-ops)
                                                                                                  (-> (select-keys opts [:authn])
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

(defmethod ig/init-key :xtdb.log/processor [_ {{:keys [meter-registry]} :base
                                               :keys [allocator db indexer compactor block-flush-duration skip-txs] :as deps}]
  (when deps
    (LogProcessor. allocator meter-registry db indexer compactor block-flush-duration (set skip-txs))))

(defmethod ig/halt-key! :xtdb.log/processor [_ ^LogProcessor log-processor]
  (util/close log-processor))

(defn await-db
  ;; TODO this should probably be await-node given it's taking a multi-db token
  ([^Database db]
   (-> @(.awaitAsync (.getLogProcessor db))
       (util/rethrowing-cause)))

  ([^Database db, token]
   (when token
     (-> @(.awaitAsync (.getLogProcessor db) (-> (basis/<-tx-basis-str token)
                                                 (get-in ["xtdb" 0])))
         (util/rethrowing-cause))))

  ([^Database db, token ^Duration timeout]
   (when token
     (-> @(cond-> (.awaitAsync (.getLogProcessor db) (-> (basis/<-tx-basis-str token)
                                                         (get-in ["xtdb" 0])))
            timeout (.orTimeout (.toMillis timeout) TimeUnit/MILLISECONDS))
         (util/rethrowing-cause)))))

(defn sync-node
  ([node] (await-db (db/primary-db node)))
  ([node timeout] (await-db (db/primary-db node) (xtp/await-token node) timeout)))

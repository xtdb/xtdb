(ns xtdb.log
  (:require [clojure.string :as str]
            [integrant.core :as ig]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            [xtdb.node :as xtn]
            xtdb.protocols
            [xtdb.sql.plan :as plan]
            [xtdb.time :as time]
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
           (xtdb.api Xtdb$Config)
           (xtdb.api.log Log Log$Factory Log$Message$Tx)
           (xtdb.api.tx TxOp$Sql)
           (xtdb.arrow Relation Vector VectorWriter)
           xtdb.indexer.LogProcessor
           (xtdb.tx_ops Abort AssertExists AssertNotExists Call Delete DeleteDocs Erase EraseDocs Insert PatchDocs PutDocs SqlByteArgs Update XtqlAndArgs)
           xtdb.types.ClojureForm))

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

(defn- ->xtql+args-writer [^VectorWriter op-writer, ^BufferAllocator allocator]
  (let [xtql-writer (.legWriter op-writer "xtql" (FieldType/notNullable #xt.arrow/type :struct))
        xtql-op-writer (.keyWriter xtql-writer "op" (FieldType/notNullable #xt.arrow/type :transit))
        args-writer (.keyWriter xtql-writer "args" (FieldType/nullable #xt.arrow/type :varbinary))]
    (fn write-xtql+args! [{:keys [op arg-rows]}]
      (.writeObject xtql-op-writer (ClojureForm. op))

      (when arg-rows
        (util/with-open [args-wtr (vw/->vec-writer allocator "args" (FieldType/notNullable #xt.arrow/type :struct))]
          (doseq [arg-row arg-rows]
            (.writeObject args-wtr arg-row))

          (.syncValueCount args-wtr)

          (.writeBytes args-writer
                       (util/build-arrow-ipc-byte-buffer (VectorSchemaRoot. ^Iterable (seq (.getVector args-wtr))) :stream
                         (fn [write-batch!]
                           (write-batch!))))))

      (.endStruct xtql-writer))))

(defn- ->xtql-writer [^VectorWriter op-writer]
  (let [xtql-writer (.legWriter op-writer "xtql" (FieldType/notNullable #xt.arrow/type :struct))
        xtql-op-writer (.keyWriter xtql-writer "op" (FieldType/notNullable #xt.arrow/type :transit))]

    ;; create this even if it's not required here
    (.keyWriter xtql-writer "args" (FieldType/nullable #xt.arrow/type :varbinary))

    (fn write-xtql! [op]
      (.writeObject xtql-op-writer (ClojureForm. op))
      (.endStruct xtql-writer))))

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
                                            (fn [write-batch!]
                                              (write-batch!))))

        (finally
          (run! util/try-close vecs))))))

(defn- ->sql-writer [^VectorWriter op-writer, ^BufferAllocator allocator]
  (let [sql-writer (.legWriter op-writer "sql" (FieldType/notNullable #xt.arrow/type :struct))
        query-writer (.keyWriter sql-writer "query" (FieldType/notNullable #xt.arrow/type :utf8))
        args-writer (.keyWriter sql-writer "args" (FieldType/nullable #xt.arrow/type :varbinary))]
    (fn write-sql! [^TxOp$Sql op]
      (let [sql (.sql op)]
        (.writeObject query-writer sql)

        (when-let [arg-rows (not-empty (.argRows op))]
          (.writeObject args-writer (encode-sql-args allocator arg-rows))))

      (.endStruct sql-writer))))

(defn- ->sql-byte-args-writer [^VectorWriter op-writer]
  (let [sql-writer (.legWriter op-writer "sql" (FieldType/notNullable #xt.arrow/type :struct))
        query-writer (.keyWriter sql-writer "query" (FieldType/notNullable #xt.arrow/type :utf8))
        args-writer (.keyWriter sql-writer "args" (FieldType/nullable #xt.arrow/type :varbinary))]
    (fn write-sql! [{:keys [sql arg-bytes]}]
      (.writeObject query-writer sql)

      (when arg-bytes
        (.writeObject args-writer arg-bytes))

      (.endStruct sql-writer))))

(defn- ->docs-op-writer [^VectorWriter op-writer]
  (let [iids-writer (.keyWriter op-writer "iids" (FieldType/notNullable #xt.arrow/type :list))
        iid-writer (some-> iids-writer
                           (.elementWriter (FieldType/notNullable #xt.arrow/type [:fixed-size-binary 16])))
        doc-writer (.keyWriter op-writer "documents" (FieldType/notNullable #xt.arrow/type :union))
        valid-from-writer (.keyWriter op-writer "_valid_from" types/nullable-temporal-field-type)
        valid-to-writer (.keyWriter op-writer "_valid_to" types/nullable-temporal-field-type)
        table-doc-writers (HashMap.)]
    (fn write-put! [{:keys [table-name docs valid-from valid-to]}]
      (let [table-name (str (symbol (util/with-default-schema table-name)))]
        (when (forbidden-table? table-name) (throw (forbidden-table-ex table-name)))

        (let [^VectorWriter table-doc-writer
              (.computeIfAbsent table-doc-writers table-name
                                (fn [table]
                                  (doto (.legWriter doc-writer table (FieldType/notNullable #xt.arrow/type :list))
                                    (.elementWriter (FieldType/notNullable #xt.arrow/type :struct)))))]

          (.writeObject table-doc-writer docs)

          (doseq [doc docs
                  :let [eid (val (or (->> doc
                                          (some (fn [e]
                                                  (when (.equals "_id" (util/->normal-form-str (key e)))
                                                    e))))
                                     (throw (err/illegal-arg :missing-id {:doc doc}))))]]
            (.writeBytes iid-writer (util/->iid eid)))
          (.endList iids-writer))

        (.writeObject valid-from-writer valid-from)
        (.writeObject valid-to-writer valid-to)

        (.endStruct op-writer)))))

(defn- ->put-writer [^VectorWriter op-writer]
  (->docs-op-writer (.legWriter op-writer "put-docs" (FieldType/notNullable #xt.arrow/type :struct))))

(defn- ->patch-writer [^VectorWriter op-writer]
  (->docs-op-writer (.legWriter op-writer "patch-docs" (FieldType/notNullable #xt.arrow/type :struct))))

(defn- ->delete-writer [^VectorWriter op-writer]
  (let [delete-writer (.legWriter op-writer "delete-docs" (FieldType/notNullable #xt.arrow/type :struct))
        table-writer (.keyWriter delete-writer "table" (FieldType/notNullable #xt.arrow/type :utf8))
        iids-writer (.keyWriter delete-writer "iids" (FieldType/notNullable #xt.arrow/type :list))
        iid-writer (some-> iids-writer
                           (.elementWriter (FieldType/notNullable #xt.arrow/type [:fixed-size-binary 16])))
        valid-from-writer (.keyWriter delete-writer "_valid_from" types/nullable-temporal-field-type)
        valid-to-writer (.keyWriter delete-writer "_valid_to" types/nullable-temporal-field-type)]
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
  (let [erase-writer (.legWriter op-writer "erase-docs" (FieldType/notNullable #xt.arrow/type :struct))
        table-writer (.keyWriter erase-writer "table" (FieldType/notNullable #xt.arrow/type :utf8))
        iids-writer (.keyWriter erase-writer "iids" (FieldType/notNullable #xt.arrow/type :list))
        iid-writer (some-> iids-writer
                           (.elementWriter (FieldType/notNullable #xt.arrow/type [:fixed-size-binary 16])))]
    (fn [{:keys [table-name doc-ids]}]
      (let [table-name (str (symbol (util/with-default-schema table-name)))]
        (when (forbidden-table? table-name) (throw (forbidden-table-ex table-name)))
        (when (seq doc-ids)
          (.writeObject table-writer table-name)

          (doseq [doc-id doc-ids]
            (.writeObject iid-writer (util/->iid doc-id)))
          (.endList iids-writer)

          (.endStruct erase-writer))))))

(defn- ->call-writer [^VectorWriter op-writer]
  (let [call-writer (.legWriter op-writer "call" (FieldType/notNullable #xt.arrow/type :struct))
        fn-iid-writer (.keyWriter call-writer "fn-iid" (FieldType/notNullable #xt.arrow/type [:fixed-size-binary 16]))
        args-list-writer (.keyWriter call-writer "args" (FieldType/notNullable #xt.arrow/type :transit))]
    (fn write-call! [{:keys [fn-id args]}]
      (.writeObject fn-iid-writer (util/->iid fn-id))

      (let [clj-form (xt/->ClojureForm (vec args))]
        (.writeObject args-list-writer clj-form))

      (.endStruct call-writer))))

(defn- ->abort-writer [^VectorWriter op-writer]
  (let [abort-writer (.legWriter op-writer "abort" (FieldType/nullable #xt.arrow/type :null))]
    (fn [_op]
      (.writeNull abort-writer))))

(defn open-tx-ops-rel ^xtdb.arrow.Relation [^BufferAllocator allocator]
  (Relation. [(Vector/fromField allocator tx-ops-field)]))

(defn write-tx-ops! [^BufferAllocator allocator, ^VectorWriter op-writer, tx-ops, {:keys [default-tz]}]
  (let [!write-xtql+args! (delay (->xtql+args-writer op-writer allocator))
        !write-xtql! (delay (->xtql-writer op-writer))
        !write-sql! (delay (->sql-writer op-writer allocator))
        !write-sql-byte-args! (delay (->sql-byte-args-writer op-writer))
        !write-put! (delay (->put-writer op-writer))
        !write-patch! (delay (->patch-writer op-writer))
        !write-delete! (delay (->delete-writer op-writer))
        !write-erase! (delay (->erase-writer op-writer))
        !write-call! (delay (->call-writer op-writer))
        !write-abort! (delay (->abort-writer op-writer))]

    (doseq [tx-op tx-ops]
      (condp instance? tx-op
        XtqlAndArgs (@!write-xtql+args! tx-op)
        Insert (@!write-xtql! tx-op)
        Update (@!write-xtql! tx-op)
        Delete (@!write-xtql! tx-op)
        Erase (@!write-xtql! tx-op)
        AssertExists (@!write-xtql! tx-op)
        AssertNotExists (@!write-xtql! tx-op)

        TxOp$Sql (let [^TxOp$Sql tx-op tx-op]
                   (if-let [put-docs-ops (plan/sql->static-ops (.sql tx-op) (.argRows tx-op) {:default-tz default-tz})]
                     (doseq [op put-docs-ops]
                       (@!write-put! op))

                     (@!write-sql! tx-op)))

        SqlByteArgs (@!write-sql-byte-args! tx-op)
        PutDocs (@!write-put! tx-op)
        PatchDocs (@!write-patch! tx-op)
        DeleteDocs (@!write-delete! tx-op)
        EraseDocs (@!write-erase! tx-op)
        Call (@!write-call! tx-op)
        Abort (@!write-abort! tx-op)
        (throw (err/illegal-arg :invalid-tx-op {:tx-op tx-op}))))))

(defn serialize-tx-ops ^java.nio.ByteBuffer [^BufferAllocator allocator tx-ops {:keys [^Instant system-time, default-tz]
                                                                                {:keys [user]} :authn :as opts}]
  (with-open [rel (Relation. allocator tx-schema)]
    (let [ops-list-writer (.get rel "tx-ops")

          default-tz-writer (.get rel "default-tz")
          user-writer (.get rel "user")]

      (when system-time
        (.writeObject (.get rel "system-time") (time/->zdt system-time)))

      (when user
        (.writeObject user-writer user))

      (when default-tz
        (.writeObject default-tz-writer (str default-tz)))

      (write-tx-ops! allocator (.elementWriter ops-list-writer) tx-ops opts)
      (.endList ops-list-writer)

      (.endRow rel)

      (.getAsArrowStream rel))))

(defmethod xtn/apply-config! ::memory-log [^Xtdb$Config config _ {:keys [instant-src]}]
  (doto config
    (.setLog (cond-> (Log/getInMemoryLog)
                   instant-src (.instantSource instant-src)))))

(defmethod xtn/apply-config! ::local-directory-log [^Xtdb$Config config _ {:keys [path instant-src]}]
  (doto config
    (.setLog (cond-> (Log/localLog (util/->path path))
                   instant-src (.instantSource instant-src)))))

(defmethod xtn/apply-config! :xtdb/log [config _ [tag opts]]
  (xtn/apply-config! config
                     (case tag
                       :in-memory ::memory-log
                       :local ::local-directory-log
                       :kafka :xtdb.kafka/log)
                     opts))

(defmethod ig/init-key :xtdb/log [_ ^Log$Factory factory]
  (.openLog factory))

(defmethod ig/halt-key! :xtdb/log [_ ^Log log]
  (util/close log))

(defn submit-tx& ^java.util.concurrent.CompletableFuture
  [{:keys [^BufferAllocator allocator, ^Log log, default-tz]} tx-ops {:keys [system-time] :as opts}]

  (.appendMessage log
                  (Log$Message$Tx. (serialize-tx-ops allocator tx-ops
                                                     (-> (select-keys opts [:authn])
                                                         (assoc :default-tz (:default-tz opts default-tz)
                                                                :system-time (some-> system-time time/expect-instant)))))))

(defmethod ig/prep-key :xtdb.log/processor [_ opts]
  (when opts
    (into {:allocator (ig/ref :xtdb/allocator)
           :indexer (ig/ref :xtdb/indexer)
           :live-idx (ig/ref :xtdb.indexer/live-index)
           :log (ig/ref :xtdb/log)
           :trie-catalog (ig/ref :xtdb/trie-catalog)
           :metrics-registry (ig/ref :xtdb.metrics/registry)
           :chunk-flush-duration #xt/duration "PT4H"}
          opts)))

(defmethod ig/init-key :xtdb.log/processor [_ {:keys [allocator indexer log live-idx trie-catalog metrics-registry chunk-flush-duration] :as deps}]
  (when deps
    (LogProcessor. allocator indexer live-idx log trie-catalog metrics-registry chunk-flush-duration)))

(defmethod ig/halt-key! :xtdb.log/processor [_ ^LogProcessor log-processor]
  (util/close log-processor))

(defn await-tx
  (^java.util.concurrent.CompletableFuture [{:keys [^LogProcessor log-processor]}]
   (-> @(.awaitAsync log-processor)
       (util/rethrowing-cause)))

  (^java.util.concurrent.CompletableFuture [{:keys [^LogProcessor log-processor]} ^long tx-id]
   (-> @(.awaitAsync log-processor tx-id)
       (util/rethrowing-cause)))

  (^java.util.concurrent.CompletableFuture [{:keys [^LogProcessor log-processor]} ^long tx-id ^Duration timeout]
   (-> @(cond-> (.awaitAsync log-processor tx-id)
          timeout (.orTimeout (.toMillis timeout) TimeUnit/MILLISECONDS))
       (util/rethrowing-cause))))

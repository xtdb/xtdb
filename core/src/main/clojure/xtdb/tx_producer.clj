(ns xtdb.tx-producer
  (:require [clojure.spec.alpha :as s]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.error :as err]
            xtdb.log
            [xtdb.sql :as sql]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector :as vec]
            [xtdb.vector.writer :as vw])
  (:import (java.time Instant ZoneId)
           (java.lang AutoCloseable)
           (java.util ArrayList HashMap List)
           org.apache.arrow.memory.BufferAllocator
           (org.apache.arrow.vector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo ArrowType$Union Schema)
           org.apache.arrow.vector.types.UnionMode
           (xtdb.log Log LogRecord)
           (xtdb.tx Ops Ops$Abort Ops$Call Ops$Delete Ops$Evict Ops$Put Ops$Sql)
           xtdb.vector.IValueWriter))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ITxProducer
  (submitTx
    ^java.util.concurrent.CompletableFuture #_<TransactionInstant> [^java.util.List txOps, ^java.util.Map opts]))

(def eid? (some-fn uuid? integer? string? keyword?))

(def table? keyword?)

(defmulti parse-tx-op first :default ::default)

(defmethod parse-tx-op ::default [tx-op]
  (throw (err/illegal-arg :xtdb.tx/invalid-tx-op
                          {:tx-op tx-op
                           :op (first tx-op)})))

(defn expect-sql [sql tx-op]
  (when-not (string? sql)
    (throw (err/illegal-arg :xtdb.tx/expected-sql
                            {::err/message "Expected SQL query",
                             :tx-op tx-op
                             :sql sql}))))

(defmethod parse-tx-op :sql [[_ sql+params :as tx-op]]
  (when-not sql+params
    (throw (err/illegal-arg :xtdb.tx/expected-sql+params
                            {::err/message "expected SQL query or [sql & params]", :tx-op tx-op})))

  (cond
    (string? sql+params) (Ops/sql sql+params)

    (vector? sql+params)
    (let [[sql & params] sql+params]
      (expect-sql sql tx-op)
      (Ops/sql sql params))

    :else
    (throw (err/illegal-arg :xtdb.tx/invalid-tx-op
                            {::err/message "unexpected value in :sql - expecting vector or string", :tx-op tx-op}))))

(defmethod parse-tx-op :sql-batch [[_ sql+params :as tx-op]]
  (when-not (vector? sql+params)
    (throw (err/illegal-arg :xtdb.tx/expected-sql+params
                            {::err/message "expected [sql & param-groups]", :tx-op tx-op})))

  (let [[sql & params] sql+params]
    (expect-sql sql tx-op)
    (when-not (sequential? params)
      (throw (err/illegal-arg :xtdb.tx/expected-param-seqs
                              {::err/message "expected seqs of params"
                               :tx-op tx-op
                               :params params})))

    (when-let [non-seq (some (complement sequential?) params)]
      (throw (err/illegal-arg :xtdb.tx/expected-param-seqs
                              {::err/message "expected seqs of params"
                               :tx-op tx-op
                               :non-seq non-seq})))
    (Ops/sqlBatch ^String sql, ^List (vec params))))

(defn expect-table-name [table-name tx-op]
  (when-not (table? table-name)
    (throw (err/illegal-arg :xtdb.tx/invalid-table
                            {::err/message "expected table name", :tx-op tx-op :table table-name})))

  table-name)

(defn expect-eid [eid tx-op]
  (when-not (eid? eid)
    (throw (err/illegal-arg :xtdb.tx/invalid-eid
                            {::err/message "expected entity id", :tx-op tx-op :eid eid})))

  eid)

(defn expect-instant [instant temporal-opts tx-op]
  (when-not (s/valid? ::util/datetime-value instant)
    (throw (err/illegal-arg :xtdb.tx/invalid-date-time
                            {::err/message "expected date-time"
                             :tx-op tx-op
                             :temporal-opts temporal-opts})))

  (util/->instant instant))

(defn expect-temporal-opts [temporal-opts tx-op]
  (when-not (map? temporal-opts)
    (throw (err/illegal-arg :xtdb.tx/invalid-temporal-opts
                            {::err/message "expected map of temporal opts"
                             :tx-op tx-op
                             :temporal-opts temporal-opts})))

  (when-let [for-valid-time (:for-valid-time temporal-opts)]
    (when-not (vector? for-valid-time)
      (throw (err/illegal-arg :xtdb.tx/invalid-temporal-opts
                              {::err/message "expected vector for `:for-valid-time`"
                               :tx-op tx-op
                               :for-valid-time for-valid-time})))

    (let [[tag & args] for-valid-time]
      (case tag
        :in {:valid-from (some-> (first args) (expect-instant temporal-opts tx-op))
             :valid-to (some-> (second args) (expect-instant temporal-opts tx-op))}
        :from {:valid-from (some-> (first args) (expect-instant temporal-opts tx-op))}
        :to {:valid-to (some-> (first args) (expect-instant temporal-opts tx-op))}
        (throw (err/illegal-arg :xtdb.tx/invalid-temporal-opts
                                {::err/message "invalid tag for `:for-valid-time`, expected one of `#{:in :from :to}`"
                                 :tx-op tx-op
                                 :for-valid-time for-valid-time
                                 :tag tag}))))))

(defn expect-doc [doc tx-op]
  (when-not (map? doc)
    (throw (err/illegal-arg :xtdb.tx/expected-doc
                            {::err/message "expected doc map", :doc doc, :tx-op tx-op})))
  (let [eid (:xt/id doc)]
    (when-not (eid? eid)
      (throw (err/illegal-arg :xtdb.tx/invalid-eid
                              {::err/message "expected xt/id", :tx-op tx-op :doc doc, :xt/id eid}))))

  doc)

(defmethod parse-tx-op :put [[_ table-name doc temporal-opts :as tx-op]]
  (expect-table-name table-name tx-op)
  (expect-doc doc tx-op)

  (let [{:keys [^Instant valid-from, ^Instant valid-to]} (some-> temporal-opts (expect-temporal-opts tx-op))]
    (-> (Ops/put table-name doc)
        (.validFrom valid-from)
        (.validTo valid-to))))

(defn expect-fn-id [fn-id tx-op]
  (when-not (eid? fn-id)
    (throw (err/illegal-arg :xtdb.tx/invalid-fn-id {::err/message "expected fn-id", :tx-op tx-op :fn-id fn-id}))))

(defmethod parse-tx-op :put-fn [[_ fn-id tx-fn temporal-opts :as tx-op]]
  (expect-fn-id fn-id tx-op)

  (when-not tx-fn
    (throw (err/illegal-arg :xtdb.tx/invalid-tx-fn {::err/message "expected tx-fn", :tx-op tx-op :tx-fn tx-fn})))

  (let [{:keys [^Instant valid-from, ^Instant valid-to]} (some-> temporal-opts (expect-temporal-opts tx-op))]
    (-> (Ops/putFn fn-id tx-fn)
        (.validFrom valid-from)
        (.validTo valid-to))))

(defmethod parse-tx-op :delete [[_ table-name eid temporal-opts :as tx-op]]
  (expect-table-name table-name tx-op)
  (expect-eid eid tx-op)

  (let [{:keys [^Instant valid-from, ^Instant valid-to]} (some-> temporal-opts (expect-temporal-opts tx-op))]
    (-> (Ops/delete table-name eid)
        (.validFrom valid-from)
        (.validTo valid-to))))

(defmethod parse-tx-op :evict [[_ table-name eid :as tx-op]]
  (expect-table-name table-name tx-op)
  (expect-eid eid tx-op)

  (Ops/evict table-name eid))

(defmethod parse-tx-op :call [[_ fn-id & args :as tx-op]]
  (expect-fn-id fn-id tx-op)
  (Ops/call fn-id (into-array Object args)))

;; required for C1 importer
(defmethod parse-tx-op :abort [_] Ops/ABORT)

(defmulti tx-op-spec first)

(defmethod tx-op-spec :sql-batch [_]
  (s/cat :op #{:sql-batch}
         :sql+params (s/and vector?
                            (s/cat :sql string?,
                                   :param-groups (s/? (s/alt :rows (s/* (s/coll-of any? :kind sequential?))
                                                             :bytes #(= :varbinary (vw/value->col-type %))))))))

(def ^:private ^org.apache.arrow.vector.types.pojo.Field tx-ops-field
  (types/->field "tx-ops" (ArrowType$Union. UnionMode/Dense (int-array (range 6))) false
                 (types/col-type->field 'sql [:struct {'query :utf8
                                                       'params [:union #{:null :varbinary}]}])


                 (types/->field "put" types/struct-type false
                                (types/->field "document" types/dense-union-type false)
                                (types/col-type->field 'xt$valid_from types/nullable-temporal-type)
                                (types/col-type->field 'xt$valid_to types/nullable-temporal-type))

                 (types/->field "delete" types/struct-type false
                                (types/col-type->field 'table :utf8)
                                (types/->field "xt$id" types/dense-union-type false)
                                (types/col-type->field 'xt$valid_from types/nullable-temporal-type)
                                (types/col-type->field 'xt$valid_to types/nullable-temporal-type))

                 (types/->field "evict" types/struct-type false
                                (types/col-type->field '_table [:union #{:null :utf8}])
                                (types/->field "xt$id" types/dense-union-type false))

                 (types/->field "call" types/struct-type false
                                (types/->field "fn-id" types/dense-union-type false)
                                (types/->field "args" types/list-type false
                                               (types/->field "arg" types/dense-union-type false)))

                 ;; C1 importer
                 (types/col-type->field 'abort :null)))

(def ^:private ^org.apache.arrow.vector.types.pojo.Schema tx-schema
  (Schema. [(types/->field "tx-ops" types/list-type false tx-ops-field)

            (types/col-type->field "system-time" types/nullable-temporal-type)
            (types/col-type->field "default-tz" :utf8)
            (types/col-type->field "all-application-time?" :bool)]))

(defn encode-params [^BufferAllocator allocator, query, param-rows]
  (let [plan (sql/compile-query query)
        {:keys [^long param-count]} (meta plan)

        vecs (ArrayList. param-count)]
    (try
      ;; TODO check param count in each row, handle error
      (dotimes [col-idx param-count]
        (.add vecs
              (vw/open-vec allocator (symbol (str "?_" col-idx))
                           (mapv #(nth % col-idx) param-rows))))

      (let [root (doto (VectorSchemaRoot. vecs) (.setRowCount (count param-rows)))]
        (util/build-arrow-ipc-byte-buffer root :stream
          (fn [write-batch!]
            (write-batch!))))

      (finally
        (run! util/try-close vecs)))))

(defn- ->sql-writer [^IValueWriter op-writer, ^BufferAllocator allocator]
  (let [sql-writer (.writerForTypeId op-writer 0)
        query-writer (.structKeyWriter sql-writer "query")
        params-writer (.structKeyWriter sql-writer "params")]
    (fn write-sql! [^Ops$Sql op]
      (let [sql (.sql op)]
        (.startStruct sql-writer)
        (vw/write-value! sql query-writer)

        (when-let [param-bytes (.paramGroupBytes op)]
          (vw/write-value! param-bytes params-writer))

        (when-let [param-rows (.paramGroupRows op)]
          (vw/write-value! (encode-params allocator sql param-rows) params-writer)))

      (.endStruct sql-writer))))

(defn- ->put-writer [^IValueWriter op-writer]
  (let [put-writer (.writerForTypeId op-writer 1)
        doc-writer (.structKeyWriter put-writer "document")
        valid-from-writer (.structKeyWriter put-writer "xt$valid_from")
        valid-to-writer (.structKeyWriter put-writer "xt$valid_to")
        table-doc-writers (HashMap.)]
    (fn write-put! [^Ops$Put op]
      (.startStruct put-writer)
      (let [table-doc-writer (.computeIfAbsent table-doc-writers (util/kw->normal-form-kw (.tableName op))
                                               (util/->jfn
                                                 (fn [table]
                                                   (let [type-id (.registerNewType doc-writer (types/col-type->field (name table) [:struct {}]))]
                                                     (.writerForTypeId doc-writer type-id)))))]
        (vw/write-value! (->> (.doc op)
                              (into {} (map (juxt (comp util/kw->normal-form-kw key)
                                                  val))))
                         table-doc-writer))

      (vw/write-value! (.validFrom op) valid-from-writer)
      (vw/write-value! (.validTo op) valid-to-writer)

      (.endStruct put-writer))))

(defn- ->delete-writer [^IValueWriter op-writer]
  (let [delete-writer (.writerForTypeId op-writer 2)
        table-writer (.structKeyWriter delete-writer "table")
        id-writer (.structKeyWriter delete-writer "xt$id")
        valid-from-writer (.structKeyWriter delete-writer "xt$valid_from")
        valid-to-writer (.structKeyWriter delete-writer "xt$valid_to")]
    (fn write-delete! [^Ops$Delete op]
      (.startStruct delete-writer)

      (vw/write-value! (name (util/kw->normal-form-kw (.tableName op))) table-writer)

      (let [eid (.entityId op)]
        (vw/write-value! eid (.writerForType id-writer (vw/value->col-type eid))))

      (vw/write-value! (.validFrom op) valid-from-writer)
      (vw/write-value! (.validTo op) valid-to-writer)

      (.endStruct delete-writer))))

(defn- ->evict-writer [^IValueWriter op-writer]
  (let [evict-writer (.writerForTypeId op-writer 3)
        table-writer (.structKeyWriter evict-writer "_table")
        id-writer (.structKeyWriter evict-writer "xt$id")]
    (fn [^Ops$Evict op]
      (.startStruct evict-writer)
      (vw/write-value! (name (.tableName op)) table-writer)

      (let [eid (.entityId op)]
        (vw/write-value! eid (.writerForType id-writer (vw/value->col-type eid))))

      (.endStruct evict-writer))))

(defn- ->call-writer [^IValueWriter op-writer]
  (let [call-writer (.writerForTypeId op-writer 4)
        fn-id-writer (.structKeyWriter call-writer "fn-id")
        args-list-writer (.structKeyWriter call-writer "args")]
    (fn write-call! [^Ops$Call op]
      (.startStruct call-writer)

      (let [fn-id (.fnId op)]
        (vw/write-value! fn-id (.writerForType fn-id-writer (vw/value->col-type fn-id))))

      (vw/write-value! (vec (.args op)) args-list-writer)

      (.endStruct call-writer))))

(defn- ->abort-writer [^IValueWriter op-writer]
  (let [abort-writer (.writerForTypeId op-writer 5)]
    (fn [^Ops$Abort _op]
      (.writeNull abort-writer nil))))

(defn open-tx-ops-vec ^org.apache.arrow.vector.ValueVector [^BufferAllocator allocator]
  (.createVector tx-ops-field allocator))

(defn write-tx-ops! [^BufferAllocator allocator, ^IValueWriter op-writer, tx-ops]
  (let [write-sql! (->sql-writer op-writer allocator)
        write-put! (->put-writer op-writer)
        write-delete! (->delete-writer op-writer)
        write-evict! (->evict-writer op-writer)
        write-call! (->call-writer op-writer)
        write-abort! (->abort-writer op-writer)]

    (doseq [tx-op tx-ops
            :let [tx-op (cond-> tx-op
                          (vector? tx-op) parse-tx-op)]]
      (condp instance? tx-op
        Ops$Sql (write-sql! tx-op)
        Ops$Put (write-put! tx-op)
        Ops$Delete (write-delete! tx-op)
        Ops$Evict (write-evict! tx-op)
        Ops$Call (write-call! tx-op)
        Ops$Abort (write-abort! tx-op)
        (throw (err/illegal-arg :invalid-tx-op {:tx-op tx-op}))))))

(defn serialize-tx-ops ^java.nio.ByteBuffer [^BufferAllocator allocator tx-ops {:keys [^Instant system-time, default-tz, default-all-valid-time?]}]
  (with-open [root (VectorSchemaRoot/create tx-schema allocator)]
    (let [ops-list-writer (vw/->writer (.getVector root "tx-ops"))

          default-tz-writer (vw/->writer (.getVector root "default-tz"))
          app-time-behaviour-writer (vw/->writer (.getVector root "all-application-time?"))]

      (when system-time
        (vw/write-value! system-time (vw/->writer (.getVector root "system-time"))))

      (vw/write-value! (str default-tz) default-tz-writer)
      (vw/write-value! (boolean default-all-valid-time?) app-time-behaviour-writer)

      (.startList ops-list-writer)
      (write-tx-ops! allocator (.listElementWriter ops-list-writer) tx-ops)
      (.endList ops-list-writer)

      (.setRowCount root 1)
      (.syncSchema root)

      (util/root->arrow-ipc-byte-buffer root :stream))))

(defn validate-opts [tx-opts]
  (when (contains? tx-opts :system-time)
    (let [system-time (:system-time tx-opts)]
      (when-not (inst? system-time)
        (throw (err/illegal-arg
                 :xtdb.api/invalid-system-time
                 {:xtdb.error/message (format "system-time must be an inst, supplied value: %s" system-time)}))))))

(deftype TxProducer [^BufferAllocator allocator, ^Log log, ^ZoneId default-tz]
  ITxProducer
  (submitTx [_ tx-ops opts]
    (validate-opts opts)
    (let [{:keys [system-time] :as opts} (-> (into {:default-tz default-tz} opts)
                                             (util/maybe-update :system-time util/->instant))]
      (-> (.appendRecord log (serialize-tx-ops allocator tx-ops opts))
          (util/then-apply
            (fn [^LogRecord result]
              (cond-> (.tx result)
                system-time (assoc :system-time system-time)))))))
  AutoCloseable
  (close [_] (.close allocator)))

(defmethod ig/prep-key ::tx-producer [_ opts]
  (merge {:log (ig/ref :xtdb/log)
          :allocator (ig/ref :xtdb/allocator)
          :default-tz (ig/ref :xtdb/default-tz)}
         opts))

(defmethod ig/init-key ::tx-producer [_ {:keys [log allocator default-tz]}]
  (TxProducer. (util/->child-allocator allocator "tx-producer") log default-tz))

(defmethod ig/halt-key! ::tx-producer [_ tx-producer]
  (util/close tx-producer))

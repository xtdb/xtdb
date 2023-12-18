(ns xtdb.tx-producer
  (:require [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            xtdb.log
            [xtdb.sql :as sql]
            [xtdb.time :as time]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import clojure.lang.Keyword
           (java.lang AutoCloseable)
           (java.time Instant ZoneId)
           (java.util ArrayList HashMap)
           org.apache.arrow.memory.BufferAllocator
           (org.apache.arrow.vector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo ArrowType$Union FieldType Schema)
           org.apache.arrow.vector.types.UnionMode
           (xtdb.api TransactionKey)
           (xtdb.log Log LogRecord)
           (xtdb.tx Abort Call Delete Erase Put Sql Xtql)
           xtdb.types.ClojureForm
           xtdb.vector.IVectorWriter))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ITxProducer
  (submitTx
    ^java.util.concurrent.CompletableFuture #_<TransactionKey> [^java.util.List txOps, ^java.util.Map opts]))

(def ^:private ^org.apache.arrow.vector.types.pojo.Field tx-ops-field
  (types/->field "tx-ops" (ArrowType$Union. UnionMode/Dense nil) false))

(def ^:private ^org.apache.arrow.vector.types.pojo.Schema tx-schema
  (Schema. [(types/->field "tx-ops" #xt.arrow/type :list false tx-ops-field)

            (types/col-type->field "system-time" types/nullable-temporal-type)
            (types/col-type->field "default-tz" :utf8)
            (types/col-type->field "all-application-time?" :bool)]))

(defn- ->xtql-writer [^IVectorWriter op-writer, ^BufferAllocator allocator]
  (let [xtql-writer (.legWriter op-writer :xtql (FieldType/notNullable #xt.arrow/type :struct))
        query-writer (.structKeyWriter xtql-writer "query" (FieldType/notNullable #xt.arrow/type :transit))
        params-writer (.structKeyWriter xtql-writer "params" (FieldType/nullable #xt.arrow/type :varbinary))]
    (fn write-xtql! [^Xtql op]
      (.startStruct xtql-writer)
      (vw/write-value! (ClojureForm. (.query op)) query-writer)

      (when-let [arg-rows (.args op)]
        (util/with-open [param-wtr (vw/->vec-writer allocator "params" (FieldType/notNullable #xt.arrow/type :struct))]
          (doseq [arg-row arg-rows]
            (vw/write-value! arg-row param-wtr))

          (.syncValueCount param-wtr)

          (.writeBytes params-writer
                       (util/build-arrow-ipc-byte-buffer (VectorSchemaRoot. ^Iterable (seq (.getVector param-wtr)))
                                                         :stream
                                                         (fn [write-batch!]
                                                           (write-batch!))))))

      (.endStruct xtql-writer))))

(defn encode-sql-params [^BufferAllocator allocator, query, param-rows]
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

(defn- ->sql-writer [^IVectorWriter op-writer, ^BufferAllocator allocator]
  (let [sql-writer (.legWriter op-writer :sql (FieldType/notNullable #xt.arrow/type :struct))
        query-writer (.structKeyWriter sql-writer "query" (FieldType/notNullable #xt.arrow/type :utf8))
        params-writer (.structKeyWriter sql-writer "params" (FieldType/nullable #xt.arrow/type :varbinary))]
    (fn write-sql! [^Sql op]
      (let [sql (.sql op)]
        (.startStruct sql-writer)
        (vw/write-value! sql query-writer)

        (when-let [arg-bytes (.argBytes op)]
          (vw/write-value! arg-bytes params-writer))

        (when-let [arg-rows (.argRows op)]
          (vw/write-value! (encode-sql-params allocator sql arg-rows) params-writer)))

      (.endStruct sql-writer))))

(defn- ->put-writer [^IVectorWriter op-writer]
  (let [put-writer (.legWriter op-writer :put (FieldType/notNullable #xt.arrow/type :struct))
        doc-writer (.structKeyWriter put-writer "document" (FieldType/notNullable #xt.arrow/type :union))
        valid-from-writer (.structKeyWriter put-writer "xt$valid_from" types/nullable-temporal-field-type)
        valid-to-writer (.structKeyWriter put-writer "xt$valid_to" types/nullable-temporal-field-type)
        table-doc-writers (HashMap.)]
    (fn write-put! [^Put op]
      (.startStruct put-writer)
      (let [table-doc-writer (.computeIfAbsent table-doc-writers (util/kw->normal-form-kw (.tableName op))
                                               (util/->jfn
                                                 (fn [^Keyword table]
                                                   (.legWriter doc-writer table (FieldType/notNullable #xt.arrow/type :struct)))))]
        (vw/write-value! (->> (.doc op)
                              (into {} (map (juxt (comp util/str->normal-form-str str symbol key)
                                                  val))))
                         table-doc-writer))

      (vw/write-value! (.validFrom op) valid-from-writer)
      (vw/write-value! (.validTo op) valid-to-writer)

      (.endStruct put-writer))))

(defn- ->delete-writer [^IVectorWriter op-writer]
  (let [delete-writer (.legWriter op-writer :delete (FieldType/notNullable #xt.arrow/type :struct))
        table-writer (.structKeyWriter delete-writer "table" (FieldType/notNullable #xt.arrow/type :utf8))
        id-writer (.structKeyWriter delete-writer "xt$id" (FieldType/notNullable #xt.arrow/type :union))
        valid-from-writer (.structKeyWriter delete-writer "xt$valid_from" types/nullable-temporal-field-type)
        valid-to-writer (.structKeyWriter delete-writer "xt$valid_to" types/nullable-temporal-field-type)]
    (fn write-delete! [^Delete op]
      (.startStruct delete-writer)

      (vw/write-value! (str (symbol (util/kw->normal-form-kw (.tableName op)))) table-writer)

      (let [eid (.entityId op)]
        (vw/write-value! eid (.legWriter id-writer (vw/value->arrow-type eid))))

      (vw/write-value! (.validFrom op) valid-from-writer)
      (vw/write-value! (.validTo op) valid-to-writer)

      (.endStruct delete-writer))))

(defn- ->erase-writer [^IVectorWriter op-writer]
  (let [erase-writer (.legWriter op-writer :erase (FieldType/notNullable #xt.arrow/type :struct))
        table-writer (.structKeyWriter erase-writer "table" (FieldType/notNullable #xt.arrow/type :utf8))
        id-writer (.structKeyWriter erase-writer "xt$id" (FieldType/notNullable #xt.arrow/type :union))]
    (fn [^Erase op]
      (.startStruct erase-writer)
      (vw/write-value! (str (symbol (.tableName op))) table-writer)

      (let [eid (.entityId op)]
        (vw/write-value! eid (.legWriter id-writer (vw/value->arrow-type eid))))

      (.endStruct erase-writer))))

(defn- ->call-writer [^IVectorWriter op-writer]
  (let [call-writer (.legWriter op-writer :call (FieldType/notNullable #xt.arrow/type :struct))
        fn-id-writer (.structKeyWriter call-writer "fn-id" (FieldType/notNullable #xt.arrow/type :union))
        args-list-writer (.structKeyWriter call-writer "args" (FieldType/notNullable #xt.arrow/type :transit))]
    (fn write-call! [^Call op]
      (.startStruct call-writer)

      (let [fn-id (.fnId op)]
        (vw/write-value! fn-id (.legWriter fn-id-writer (vw/value->arrow-type fn-id))))

      (let [clj-form (xt/->ClojureForm (vec (.args op)))]
        (vw/write-value! clj-form (.legWriter args-list-writer (vw/value->arrow-type clj-form))))

      (.endStruct call-writer))))

(defn- ->abort-writer [^IVectorWriter op-writer]
  (let [abort-writer (.legWriter op-writer :abort (FieldType/nullable #xt.arrow/type :null))]
    (fn [^Abort _op]
      (.writeNull abort-writer))))

(defn open-tx-ops-vec ^org.apache.arrow.vector.ValueVector [^BufferAllocator allocator]
  (.createVector tx-ops-field allocator))

(defn write-tx-ops! [^BufferAllocator allocator, ^IVectorWriter op-writer, tx-ops]
  (let [!write-xtql! (delay (->xtql-writer op-writer allocator))
        !write-sql! (delay (->sql-writer op-writer allocator))
        !write-put! (delay (->put-writer op-writer))
        !write-delete! (delay (->delete-writer op-writer))
        !write-erase! (delay (->erase-writer op-writer))
        !write-call! (delay (->call-writer op-writer))
        !write-abort! (delay (->abort-writer op-writer))]

    (doseq [tx-op tx-ops]
      (condp instance? tx-op
        Xtql (@!write-xtql! tx-op)
        Sql (@!write-sql! tx-op)
        Put (@!write-put! tx-op)
        Delete (@!write-delete! tx-op)
        Erase (@!write-erase! tx-op)
        Call (@!write-call! tx-op)
        Abort (@!write-abort! tx-op)
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
      (write-tx-ops! allocator
                     (.listElementWriter ops-list-writer)
                     tx-ops)
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
                                             (util/maybe-update :system-time time/->instant))]
      (-> (.appendRecord log (serialize-tx-ops allocator tx-ops opts))
          (util/then-apply
            (fn [^LogRecord result]
              (cond-> ^TransactionKey (.tx result)
                system-time (.withSystemTime system-time)))))))
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

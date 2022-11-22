(ns core2.tx-producer
  (:require [clojure.spec.alpha :as s]
            [core2.error :as err]
            core2.log
            [core2.sql :as sql]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.writer :as vw]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import (core2.log Log LogRecord)
           core2.vector.IDenseUnionWriter
           java.io.ByteArrayOutputStream
           (java.time Instant ZoneId)
           java.util.ArrayList
           org.apache.arrow.memory.BufferAllocator
           (org.apache.arrow.vector TimeStampMicroTZVector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo ArrowType$Union Schema)
           org.apache.arrow.vector.types.UnionMode))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface ITxProducer
  (submitTx
    ^java.util.concurrent.CompletableFuture #_<TransactionInstant> [^java.util.List txOps])
  (submitTx
    ^java.util.concurrent.CompletableFuture #_<TransactionInstant> [^java.util.List txOps, ^java.util.Map opts]))

(s/def ::id any?)
(s/def ::doc (s/keys :req-un [::id]))
(s/def ::app-time-start inst?)
(s/def ::app-time-end inst?)

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(s/def ::app-time-as-of-now? boolean)

(defmulti tx-op-spec first)

(defmethod tx-op-spec :put [_]
  (s/cat :op #{:put}
         :doc ::doc
         :app-time-opts (s/? (s/keys :opt-un [::app-time-start ::app-time-end]))))

(defmethod tx-op-spec :delete [_]
  (s/cat :op #{:delete}
         :table (s/? string?)
         :id ::id
         :app-time-opts (s/? (s/keys :opt-un [::app-time-start ::app-time-end]))))

(defmethod tx-op-spec :evict [_]
  ;; eventually this could have app-time/sys start/end?
  (s/cat :op #{:evict}
         :table (s/? string?)
         :id ::id))

(defmethod tx-op-spec :sql [_]
  (s/cat :op #{:sql}
         :query string?
         ;; TODO FSQL might benefit from being able to just chuck the VSR/VSR bytes in here.
         :param-rows (s/? (s/coll-of (s/coll-of any? :kind sequential?) :kind sequential?))))

(s/def ::tx-op
  (s/and vector? (s/multi-spec tx-op-spec (fn [v _] v))))

(s/def ::tx-ops (s/coll-of ::tx-op :kind sequential?))

(defn conform-tx-ops [tx-ops]
  (let [parsed-tx-ops (s/conform ::tx-ops tx-ops)]
    (when (s/invalid? parsed-tx-ops)
      (throw (err/illegal-arg :core2/invalid-tx-ops
                              {::err/message (str "Invalid tx ops: " (s/explain-str ::tx-ops tx-ops))
                               :tx-ops tx-ops
                               :explain-data (s/explain-data ::tx-ops tx-ops)})))

    parsed-tx-ops))

(def ^:private nullable-inst-type [:union #{:null [:timestamp-tz :micro "UTC"]}])

(def ^:private ^org.apache.arrow.vector.types.pojo.Schema tx-schema
  (Schema. [(types/->field "tx-ops" types/list-type false
                           (types/->field "tx-ops" (ArrowType$Union. UnionMode/Dense (int-array (range 6))) false
                                          (types/->field "put" types/struct-type false
                                                         (types/->field "document" types/dense-union-type false)
                                                         (types/col-type->field 'application_time_start nullable-inst-type)
                                                         (types/col-type->field 'application_time_end nullable-inst-type))
                                          (types/->field "delete" types/struct-type false
                                                         (types/col-type->field '_table [:union #{:null :utf8}])
                                                         (types/->field "id" types/dense-union-type false)
                                                         (types/col-type->field 'application_time_start nullable-inst-type)
                                                         (types/col-type->field 'application_time_end nullable-inst-type))
                                          (types/->field "evict" types/struct-type false
                                                         (types/col-type->field '_table [:union #{:null :utf8}])
                                                         (types/->field "id" types/dense-union-type false))

                                          (types/col-type->field 'sql [:struct {:query :utf8
                                                                                :params [:union #{:null :varbinary}]}])))

            (types/col-type->field "system-time" nullable-inst-type)
            (types/col-type->field "default-tz" :utf8)
            (types/col-type->field "application-time-as-of-now?" :bool)]))

(defn- ->put-writer [^IDenseUnionWriter tx-ops-writer]
  (let [put-writer (.asStruct (.writerForTypeId tx-ops-writer 0))
        doc-writer (.asDenseUnion (.writerForName put-writer "document"))
        app-time-start-writer (.writerForName put-writer "application_time_start")
        app-time-end-writer (.writerForName put-writer "application_time_end")]
    (fn write-put! [{:keys [doc], {:keys [app-time-start app-time-end]} :app-time-opts}]
      (let [doc (into {:_table "xt_docs"} doc)]
        (.startValue put-writer)
        (doto (.writerForType doc-writer (types/value->col-type doc))
          (.startValue)
          (->> (types/write-value! doc))
          (.endValue))

        (types/write-value! app-time-start app-time-start-writer)
        (types/write-value! app-time-end app-time-end-writer)

        (.endValue put-writer)))))

(defn- ->delete-writer [^IDenseUnionWriter tx-ops-writer]
  (let [delete-writer (.asStruct (.writerForTypeId tx-ops-writer 1))
        table-writer (.writerForName delete-writer "_table")
        id-writer (.asDenseUnion (.writerForName delete-writer "id"))
        app-time-start-writer (.writerForName delete-writer "application_time_start")
        app-time-end-writer (.writerForName delete-writer "application_time_end")]
    (fn write-delete! [{:keys [id table], {:keys [app-time-start app-time-end]} :app-time-opts}]
      (.startValue delete-writer)

      (some-> table (types/write-value! table-writer))

      (doto (-> id-writer
                (.writerForType (types/value->col-type id)))
        (.startValue)
        (->> (types/write-value! id))
        (.endValue))

      (types/write-value! app-time-start app-time-start-writer)
      (types/write-value! app-time-end app-time-end-writer)

      (.endValue delete-writer))))

(defn- ->evict-writer [^IDenseUnionWriter tx-ops-writer]
  (let [evict-writer (.asStruct (.writerForTypeId tx-ops-writer 2))
        table-writer (.writerForName evict-writer "_table")
        id-writer (.asDenseUnion (.writerForName evict-writer "id"))]
    (fn [{:keys [table id]}]
      (.startValue evict-writer)
      (some-> table (types/write-value! table-writer))
      (doto (-> id-writer
                (.writerForType (types/value->col-type id)))
        (.startValue)
        (->> (types/write-value! id))
        (.endValue))
      (.endValue evict-writer))))

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

(defn- ->sql-writer [^IDenseUnionWriter tx-ops-writer, ^BufferAllocator allocator]
  (let [sql-writer (.asStruct (.writerForTypeId tx-ops-writer 3))
        query-writer (.writerForName sql-writer "query")
        params-writer (.writerForName sql-writer "params")]
    (fn write-sql! [{:keys [query param-rows]}]
      (.startValue sql-writer)

      (types/write-value! query query-writer)
      (when param-rows
        (types/write-value! (encode-params allocator query param-rows) params-writer))

      (.endValue sql-writer))))

(defn serialize-tx-ops ^java.nio.ByteBuffer [^BufferAllocator allocator tx-ops {:keys [^Instant sys-time, default-tz, app-time-as-of-now?]}]
  (let [tx-ops (conform-tx-ops tx-ops)
        op-count (count tx-ops)]
    (with-open [root (VectorSchemaRoot/create tx-schema allocator)]
      (let [ops-list-writer (.asList (vw/vec->writer (.getVector root "tx-ops")))
            default-tz-writer (vw/vec->writer (.getVector root "default-tz"))
            app-time-behaviour-writer (vw/vec->writer (.getVector root "application-time-as-of-now?"))
            tx-ops-writer (.asDenseUnion (.getDataWriter ops-list-writer))

            write-put! (->put-writer tx-ops-writer)
            write-delete! (->delete-writer tx-ops-writer)
            write-evict! (->evict-writer tx-ops-writer)
            write-sql! (->sql-writer tx-ops-writer allocator)]

        (when sys-time
          (doto ^TimeStampMicroTZVector (.getVector root "system-time")
            (.setSafe 0 (util/instant->micros sys-time))))

        (types/write-value! (str default-tz) default-tz-writer)
        (types/write-value! (boolean app-time-as-of-now?) app-time-behaviour-writer)

        (.startValue ops-list-writer)

        (dotimes [tx-op-n op-count]
          (.startValue tx-ops-writer)

          (let [tx-op (nth tx-ops tx-op-n)]
            (case (:op tx-op)
              :put (write-put! tx-op)
              :delete (write-delete! tx-op)
              :evict (write-evict! tx-op)
              :sql (write-sql! tx-op)))

          (.endValue tx-ops-writer))

        (.endValue ops-list-writer)

        (.setRowCount root 1)
        (.syncSchema root)

        (util/root->arrow-ipc-byte-buffer root :stream)))))

(deftype TxProducer [^BufferAllocator allocator, ^Log log, ^ZoneId default-tz]
  ITxProducer
  (submitTx [this tx-ops]
    (.submitTx this tx-ops {}))

  (submitTx [_ tx-ops opts]
    (let [{:keys [sys-time] :as opts} (-> (into {:default-tz default-tz} opts)
                                          (util/maybe-update :sys-time util/->instant))]
      (-> (.appendRecord log (serialize-tx-ops allocator tx-ops opts))
          (util/then-apply
            (fn [^LogRecord result]
              (cond-> (.tx result)
                sys-time (assoc :sys-time sys-time))))))))

(defmethod ig/prep-key ::tx-producer [_ opts]
  (merge {:log (ig/ref :core2/log)
          :allocator (ig/ref :core2/allocator)
          :default-tz (ig/ref :core2/default-tz)}
         opts))

(defmethod ig/init-key ::tx-producer [_ {:keys [log allocator default-tz]}]
  (TxProducer. allocator log default-tz))

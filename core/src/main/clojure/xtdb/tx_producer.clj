(ns xtdb.tx-producer
  (:require [clojure.spec.alpha :as s]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.core.sql :as sql]
            [xtdb.error :as err]
            xtdb.log
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (java.time Instant ZoneId)
           (java.util ArrayList HashMap)
           org.apache.arrow.memory.BufferAllocator
           (org.apache.arrow.vector TimeStampMicroTZVector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo ArrowType$Union Schema)
           org.apache.arrow.vector.types.UnionMode
           (xtdb.log Log LogRecord)
           (xtdb.vector IDenseUnionWriter IVectorWriter)))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ITxProducer
  (submitTx
    ^java.util.concurrent.CompletableFuture #_<TransactionInstant> [^java.util.List txOps, ^java.util.Map opts]))

(s/def :xt/id any?)
(s/def ::doc (s/and (s/keys :req [:xt/id])
                    (s/conformer #(update-keys % util/kw->normal-form-kw) #(update-keys % util/normal-form-kw->datalog-form-kw))))
(s/def ::table (s/and keyword?
                      (s/conformer (comp util/symbol->normal-form-symbol symbol) (comp util/normal-form-kw->datalog-form-kw keyword))))
(s/def ::app-time-start (s/nilable ::util/datetime-value))
(s/def ::app-time-end (s/nilable ::util/datetime-value))

(s/def ::for-app-time (s/cat :in #{:in}
                             :app-time-start ::app-time-start
                             :app-time-end (s/? ::app-time-end)))

(s/def ::temporal-opts (s/and (s/keys :opt-un [::for-app-time])
                              (s/conformer #(:for-app-time %) #(hash-map :for-app-time %))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(s/def ::default-all-app-time? boolean)

(defmulti tx-op-spec first)

(defmethod tx-op-spec :sql [_]
  (s/cat :op #{:sql}
         :query string?
         :params (s/? (s/or :rows (s/coll-of (s/coll-of any? :kind sequential?) :kind sequential?)
                            :bytes #(= :varbinary (types/value->col-type %))))))

(defmethod tx-op-spec :put [_]
  (s/cat :op #{:put}
         :table ::table
         :doc ::doc
         :app-time-opts (s/? ::temporal-opts)))

(defmethod tx-op-spec :delete [_]
  (s/cat :op #{:delete}
         :table ::table
         :id :xt/id
         :app-time-opts (s/? ::temporal-opts)))

(defmethod tx-op-spec :evict [_]
  ;; eventually this could have app-time/sys start/end?
  (s/cat :op #{:evict}
         :table ::table
         :id :xt/id))

;; required for C1 importer
(defmethod tx-op-spec :abort [_]
  (s/cat :op #{:abort}))

(defmethod tx-op-spec :call [_]
  (s/cat :op #{:call}
         :fn-id :xt/id
         :args (s/* any?)))

(s/def ::tx-op
  (s/and vector? (s/multi-spec tx-op-spec (fn [v _] v))))

(s/def ::tx-ops (s/coll-of ::tx-op :kind sequential?))

(defn conform-tx-ops [tx-ops]
  (let [parsed-tx-ops (s/conform ::tx-ops tx-ops)]
    (when (s/invalid? parsed-tx-ops)
      (throw (err/illegal-arg :xtdb/invalid-tx-ops
                              {::err/message (str "Invalid tx ops: " (s/explain-str ::tx-ops tx-ops))
                               :tx-ops tx-ops
                               :explain-data (s/explain-data ::tx-ops tx-ops)})))

    parsed-tx-ops))

(def ^:private nullable-inst-type [:union #{:null [:timestamp-tz :micro "UTC"]}])

(def ^:private ^org.apache.arrow.vector.types.pojo.Field tx-ops-field
  (types/->field "tx-ops" (ArrowType$Union. UnionMode/Dense (int-array (range 6))) false
                 (types/col-type->field 'sql [:struct {'query :utf8
                                                       'params [:union #{:null :varbinary}]}])


                 (types/->field "put" types/struct-type false
                                (types/->field "document" types/dense-union-type false)
                                (types/col-type->field 'xt$valid_from nullable-inst-type)
                                (types/col-type->field 'xt$valid_to nullable-inst-type))

                 (types/->field "delete" types/struct-type false
                                (types/col-type->field 'table :utf8)
                                (types/->field "xt$id" types/dense-union-type false)
                                (types/col-type->field 'xt$valid_from nullable-inst-type)
                                (types/col-type->field 'xt$valid_to nullable-inst-type))

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
  (Schema. [(types/->field "tx-ops" types/list-type false
                           tx-ops-field)

            (types/col-type->field "system-time" nullable-inst-type)
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

(defn- ->sql-writer [^IDenseUnionWriter tx-ops-writer, ^BufferAllocator allocator]
  (let [sql-writer (.asStruct (.writerForTypeId tx-ops-writer 0))
        query-writer (.writerForName sql-writer "query")
        params-writer (.writerForName sql-writer "params")]
    (fn write-sql! [{:keys [query params]}]
      (.startValue sql-writer)

      (types/write-value! query query-writer)

      (when params
        (zmatch params
                [:rows param-rows] (types/write-value! (encode-params allocator query param-rows) params-writer)
                [:bytes param-bytes] (types/write-value! param-bytes params-writer)))

      (.endValue sql-writer))))

(defn- ->put-writer [^IDenseUnionWriter tx-ops-writer]
  (let [put-writer (.asStruct (.writerForTypeId tx-ops-writer 1))
        doc-writer (.asDenseUnion (.writerForName put-writer "document"))
        app-time-start-writer (.writerForName put-writer "xt$valid_from")
        app-time-end-writer (.writerForName put-writer "xt$valid_to")
        table-doc-writers (HashMap.)]
    (fn write-put! [{:keys [doc table], {:keys [app-time-start app-time-end]} :app-time-opts}]
      (.startValue put-writer)

      (.startValue doc-writer)
      (let [^IVectorWriter
            table-doc-writer (.computeIfAbsent table-doc-writers table
                                               (util/->jfn
                                                 (fn [table]
                                                   (let [type-id (.registerNewType doc-writer (types/col-type->field (name table) [:struct {}]))]
                                                     (.writerForTypeId doc-writer type-id)))))]
        (.startValue table-doc-writer)
        (types/write-value! doc table-doc-writer)
        (.endValue table-doc-writer))

      (types/write-value! app-time-start app-time-start-writer)
      (types/write-value! app-time-end app-time-end-writer)

      (.endValue put-writer))))

(defn- ->delete-writer [^IDenseUnionWriter tx-ops-writer]
  (let [delete-writer (.asStruct (.writerForTypeId tx-ops-writer 2))
        table-writer (.writerForName delete-writer "table")
        id-writer (.asDenseUnion (.writerForName delete-writer "xt$id"))
        app-time-start-writer (.writerForName delete-writer "xt$valid_from")
        app-time-end-writer (.writerForName delete-writer "xt$valid_to")]
    (fn write-delete! [{:keys [id table],
                        {:keys [app-time-start app-time-end]} :app-time-opts}]
      (.startValue delete-writer)

      (types/write-value! (name table) table-writer)

      (doto (-> id-writer
                (.writerForType (types/value->col-type id)))
        (.startValue)
        (->> (types/write-value! id))
        (.endValue))

      (types/write-value! app-time-start app-time-start-writer)
      (types/write-value! app-time-end app-time-end-writer)

      (.endValue delete-writer))))

(defn- ->evict-writer [^IDenseUnionWriter tx-ops-writer]
  (let [evict-writer (.asStruct (.writerForTypeId tx-ops-writer 3))
        table-writer (.writerForName evict-writer "_table")
        id-writer (.asDenseUnion (.writerForName evict-writer "xt$id"))]
    (fn [{:keys [id table]}]
      (.startValue evict-writer)
      (some-> (name table) (types/write-value! table-writer))
      (doto (-> id-writer
                (.writerForType (types/value->col-type id)))
        (.startValue)
        (->> (types/write-value! id))
        (.endValue))
      (.endValue evict-writer))))

(defn- ->call-writer [^IDenseUnionWriter tx-ops-writer]
  (let [call-writer (.asStruct (.writerForTypeId tx-ops-writer 4))
        fn-id-writer (.asDenseUnion (.writerForName call-writer "fn-id"))
        args-list-writer (.asList (.writerForName call-writer "args"))]
    (fn write-call! [{:keys [fn-id args]}]
      (.startValue call-writer)
      (doto (-> fn-id-writer
                (.writerForType (types/value->col-type fn-id)))
        (.startValue)
        (->> (types/write-value! fn-id))
        (.endValue))

      (types/write-value! (vec args) args-list-writer)

      (.endValue call-writer))))

(defn- ->abort-writer [^IDenseUnionWriter tx-ops-writer]
  (let [abort-writer (.writerForTypeId tx-ops-writer 5)]
    (fn [_]
      (.startValue abort-writer)
      (.endValue abort-writer))))

(defn open-tx-ops-vec ^org.apache.arrow.vector.ValueVector [^BufferAllocator allocator]
  (.createVector tx-ops-field allocator))

(defn write-tx-ops! [^BufferAllocator allocator, ^IDenseUnionWriter tx-ops-writer, tx-ops]
  (let [tx-ops (conform-tx-ops tx-ops)
        op-count (count tx-ops)

        write-sql! (->sql-writer tx-ops-writer allocator)
        write-put! (->put-writer tx-ops-writer)
        write-delete! (->delete-writer tx-ops-writer)
        write-evict! (->evict-writer tx-ops-writer)
        write-call! (->call-writer tx-ops-writer)
        write-abort! (->abort-writer tx-ops-writer)]

    (dotimes [tx-op-n op-count]
      (.startValue tx-ops-writer)

      (let [tx-op (nth tx-ops tx-op-n)]
        (case (:op tx-op)
          :sql (write-sql! tx-op)
          :put (write-put! tx-op)
          :delete (write-delete! tx-op)
          :evict (write-evict! tx-op)
          :call (write-call! tx-op)
          :abort (write-abort! tx-op)))

      (.endValue tx-ops-writer))))

(defn serialize-tx-ops ^java.nio.ByteBuffer [^BufferAllocator allocator tx-ops {:keys [^Instant sys-time, default-tz, default-all-app-time?]}]
  (with-open [root (VectorSchemaRoot/create tx-schema allocator)]
    (let [ops-list-writer (.asList (vw/vec->writer (.getVector root "tx-ops")))
          tx-ops-writer (.asDenseUnion (.getDataWriter ops-list-writer))

          default-tz-writer (vw/vec->writer (.getVector root "default-tz"))
          app-time-behaviour-writer (vw/vec->writer (.getVector root "all-application-time?"))]

      (when sys-time
        (doto ^TimeStampMicroTZVector (.getVector root "system-time")
          (.setSafe 0 (util/instant->micros sys-time))))

      (types/write-value! (str default-tz) default-tz-writer)
      (types/write-value! (boolean default-all-app-time?) app-time-behaviour-writer)

      (.startValue ops-list-writer)

      (write-tx-ops! allocator tx-ops-writer tx-ops)

      (.endValue ops-list-writer)

      (.setRowCount root 1)
      (.syncSchema root)

      (util/root->arrow-ipc-byte-buffer root :stream))))

(deftype TxProducer [^BufferAllocator allocator, ^Log log, ^ZoneId default-tz]
  ITxProducer
  (submitTx [_ tx-ops opts]
    (let [{:keys [sys-time] :as opts} (-> (into {:default-tz default-tz} opts)
                                          (util/maybe-update :sys-time util/->instant))]
      (-> (.appendRecord log (serialize-tx-ops allocator tx-ops opts))
          (util/then-apply
            (fn [^LogRecord result]
              (cond-> (.tx result)
                sys-time (assoc :sys-time sys-time))))))))

(defmethod ig/prep-key ::tx-producer [_ opts]
  (merge {:log (ig/ref :xtdb/log)
          :allocator (ig/ref :xtdb/allocator)
          :default-tz (ig/ref :xtdb/default-tz)}
         opts))

(defmethod ig/init-key ::tx-producer [_ {:keys [log allocator default-tz]}]
  (TxProducer. allocator log default-tz))

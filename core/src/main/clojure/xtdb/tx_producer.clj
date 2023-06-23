(ns xtdb.tx-producer
  (:require [clojure.spec.alpha :as s]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.api.protocols :as xtp]
            [xtdb.error :as err]
            xtdb.log
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.sql :as sql]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector :as vec]
            [xtdb.vector.writer :as vw])
  (:import (java.time Instant ZoneId)
           (java.util ArrayList HashMap)
           org.apache.arrow.memory.BufferAllocator
           (org.apache.arrow.vector VectorSchemaRoot)
           org.apache.arrow.vector.types.UnionMode
           (org.apache.arrow.vector.types.pojo ArrowType$Union Schema)
           (xtdb.log Log LogRecord)
           xtdb.vector.IValueWriter))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ITxProducer
  (submitTx
    ^java.util.concurrent.CompletableFuture #_<TransactionInstant> [^java.util.List txOps, ^java.util.Map opts]))

(s/def :xt/id
  (some-fn uuid? integer? string? keyword?))

(s/def ::doc
  (s/and (s/keys :req [:xt/id])
         (s/conformer #(update-keys % util/kw->normal-form-kw) #(update-keys % util/normal-form-kw->datalog-form-kw))))

(s/def ::table
  (s/and keyword?
         (s/conformer (comp util/symbol->normal-form-symbol symbol) (comp util/normal-form-kw->datalog-form-kw keyword))))

(s/def ::valid-time-start (s/nilable ::util/datetime-value))
(s/def ::valid-time-end (s/nilable ::util/datetime-value))

(s/def ::for-valid-time
  (s/and (s/or :in (s/cat :in #{:in}
                          :valid-time-start ::valid-time-start
                          :valid-time-end (s/? ::valid-time-end))
               :from (s/cat :from #{:from}
                            :valid-time-start ::valid-time-start)
               :to (s/cat :to #{:to}
                          :valid-time-end ::valid-time-end))

         (s/conformer val (fn [x] [(some #{:in :from :to} (vals x)) x]))))

(s/def ::temporal-opts
  (s/and (s/keys :opt-un [::for-valid-time])
         (s/conformer #(:for-valid-time %) #(hash-map :for-valid-time %))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(s/def ::default-all-valid-time? boolean)

(defmulti tx-op-spec first)

(defmethod tx-op-spec :sql [_]
  (s/cat :op #{:sql}

         :sql+params
         (-> (s/or :sql string?
                   :sql+params (s/and vector? (s/cat :sql string?, :param-group (s/* any?))))
             (s/and (s/conformer (fn [[tag arg]]
                                   (case tag
                                     :sql+params (let [{:keys [sql param-group]} arg]
                                                   {:sql sql
                                                    :param-groups (when (seq param-group)
                                                                    [:rows [(vec param-group)]])})
                                     :sql {:sql arg}))
                                 (fn [{:keys [sql param-groups]}]
                                   [:sql+params {:sql sql,
                                                 :param-group (first param-groups)}]))))))

(defmethod tx-op-spec :sql-batch [_]
  (s/cat :op #{:sql-batch}
         :sql+params (s/and vector?
                            (s/cat :sql string?,
                                   :param-groups (s/? (s/alt :rows (s/* (s/coll-of any? :kind sequential?))
                                                             :bytes #(= :varbinary (vw/value->col-type %))))))))

(defmethod tx-op-spec :put [_]
  (s/cat :op #{:put}
         :table ::table
         :doc ::doc
         :app-time-opts (s/? ::temporal-opts)))

(defmethod tx-op-spec :put-fn [_]
  (s/cat :op #{:put-fn}
         :id :xt/id
         :tx-fn some?
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
  (s/and vector? (s/multi-spec tx-op-spec :op)))

(s/def ::tx-ops (s/coll-of ::tx-op :kind sequential?))

(defn conform-tx-ops [tx-ops]
  (let [parsed-tx-ops (s/conform ::tx-ops tx-ops)]
    (when (s/invalid? parsed-tx-ops)
      (throw (err/illegal-arg :xtdb/invalid-tx-ops
                              {::err/message (str "Invalid tx ops: " (s/explain-str ::tx-ops tx-ops))
                               :tx-ops tx-ops
                               :explain-data (s/explain-data ::tx-ops tx-ops)})))

    parsed-tx-ops))

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
    (fn write-sql! [{{:keys [sql param-groups]} :sql+params}]
      (.startStruct sql-writer)
      (vw/write-value! sql query-writer)

      (when param-groups
        (zmatch param-groups
          [:rows param-rows] (vw/write-value! (encode-params allocator sql param-rows) params-writer)
          [:bytes param-bytes] (vw/write-value! param-bytes params-writer)))

      (.endStruct sql-writer))))

(defn- ->put-writer [^IValueWriter op-writer]
  (let [put-writer (.writerForTypeId op-writer 1)
        doc-writer (.structKeyWriter put-writer "document")
        valid-time-start-writer (.structKeyWriter put-writer "xt$valid_from")
        valid-time-end-writer (.structKeyWriter put-writer "xt$valid_to")
        table-doc-writers (HashMap.)]
    (fn write-put! [{:keys [doc table], {:keys [valid-time-start valid-time-end]} :app-time-opts}]
      (.startStruct put-writer)
      (let [table-doc-writer (.computeIfAbsent table-doc-writers table
                                               (util/->jfn
                                                 (fn [table]
                                                   (let [type-id (.registerNewType doc-writer (types/col-type->field (name table) [:struct {}]))]
                                                     (.writerForTypeId doc-writer type-id)))))]
        (vw/write-value! doc table-doc-writer))

      (vw/write-value! valid-time-start valid-time-start-writer)
      (vw/write-value! valid-time-end valid-time-end-writer)

      (.endStruct put-writer))))

(defn- ->delete-writer [^IValueWriter op-writer]
  (let [delete-writer (.writerForTypeId op-writer 2)
        table-writer (.structKeyWriter delete-writer "table")
        id-writer (.structKeyWriter delete-writer "xt$id")
        valid-time-start-writer (.structKeyWriter delete-writer "xt$valid_from")
        valid-time-end-writer (.structKeyWriter delete-writer "xt$valid_to")]
    (fn write-delete! [{:keys [id table], {:keys [valid-time-start valid-time-end]} :app-time-opts}]
      (.startStruct delete-writer)

      (vw/write-value! (name table) table-writer)
      (vw/write-value! id (.writerForType id-writer (vw/value->col-type id)))
      (vw/write-value! valid-time-start valid-time-start-writer)
      (vw/write-value! valid-time-end valid-time-end-writer)

      (.endStruct delete-writer))))

(defn- ->evict-writer [^IValueWriter op-writer]
  (let [evict-writer (.writerForTypeId op-writer 3)
        table-writer (.structKeyWriter evict-writer "_table")
        id-writer (.structKeyWriter evict-writer "xt$id")]
    (fn [{:keys [id table]}]
      (.startStruct evict-writer)
      (some-> (name table) (vw/write-value! table-writer))
      (vw/write-value! id (.writerForType id-writer (vw/value->col-type id)))
      (.endStruct evict-writer))))

(defn- ->call-writer [^IValueWriter op-writer]
  (let [call-writer (.writerForTypeId op-writer 4)
        fn-id-writer (.structKeyWriter call-writer "fn-id")
        args-list-writer (.structKeyWriter call-writer "args")]
    (fn write-call! [{:keys [fn-id args]}]
      (.startStruct call-writer)

      (vw/write-value! fn-id (.writerForType fn-id-writer (vw/value->col-type fn-id)))
      (vw/write-value! (vec args) args-list-writer)

      (.endStruct call-writer))))

(defn- ->abort-writer [^IValueWriter op-writer]
  (let [abort-writer (.writerForTypeId op-writer 5)]
    (fn [_]
      (.writeNull abort-writer nil))))

(defn open-tx-ops-vec ^org.apache.arrow.vector.ValueVector [^BufferAllocator allocator]
  (.createVector tx-ops-field allocator))

(defn write-tx-ops! [^BufferAllocator allocator, ^IValueWriter op-writer, tx-ops]
  (let [tx-ops (conform-tx-ops tx-ops)
        op-count (count tx-ops)

        write-sql! (->sql-writer op-writer allocator)
        write-put! (->put-writer op-writer)
        write-delete! (->delete-writer op-writer)
        write-evict! (->evict-writer op-writer)
        write-call! (->call-writer op-writer)
        write-abort! (->abort-writer op-writer)]

    (dotimes [tx-op-n op-count]
      (let [tx-op (nth tx-ops tx-op-n)]
        (case (:op tx-op)
          :sql (write-sql! tx-op)
          :sql-batch (write-sql! tx-op)
          :put (write-put! tx-op)
          :put-fn (let [{:keys [id tx-fn app-time-opts]} tx-op]
                    (write-put! {:table :xt$tx_fns
                                 :doc {:xt$id id, :xt$fn (xtp/->ClojureForm tx-fn)}
                                 :app-time-opts app-time-opts}))
          :delete (write-delete! tx-op)
          :evict (write-evict! tx-op)
          :call (write-call! tx-op)
          :abort (write-abort! tx-op))))))

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
                system-time (assoc :system-time system-time))))))))

(defmethod ig/prep-key ::tx-producer [_ opts]
  (merge {:log (ig/ref :xtdb/log)
          :allocator (ig/ref :xtdb/allocator)
          :default-tz (ig/ref :xtdb/default-tz)}
         opts))

(defmethod ig/init-key ::tx-producer [_ {:keys [log allocator default-tz]}]
  (TxProducer. allocator log default-tz))

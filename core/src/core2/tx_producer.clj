(ns core2.tx-producer
  (:require [clojure.spec.alpha :as s]
            [core2.error :as err]
            core2.log
            [core2.types :as t]
            [core2.util :as util]
            [core2.vector.writer :as vw]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import [core2.log Log LogRecord]
           [core2.types LegType$StructLegType]
           core2.vector.IVectorWriter
           java.time.Instant
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector TimeStampMicroTZVector VectorSchemaRoot]
           [org.apache.arrow.vector.types.pojo ArrowType$Union Schema]
           org.apache.arrow.vector.types.UnionMode))

(definterface ITxProducer
  (submitTx
    ^java.util.concurrent.CompletableFuture #_<TransactionInstant> [^java.util.List txOps]))

(s/def ::id any?)
(s/def ::doc (s/keys :req-un [::_id]))
(s/def ::_valid-time-start inst?)
(s/def ::_valid-time-end inst?)

(defmulti tx-op-spec first)

(defmethod tx-op-spec :put [_]
  (s/cat :op #{:put}
         :doc ::doc
         :vt-opts (s/? (s/keys :opt-un [::_valid-time-start ::_valid-time-end]))))

(defmethod tx-op-spec :delete [_]
  (s/cat :op #{:delete}
         :_id ::id
         :vt-opts (s/? (s/keys :opt-un [::_valid-time-start ::_valid-time-end]))))

(defmethod tx-op-spec :evict [_]
  ;; eventually this could have vt/tt start/end?
  (s/cat :op #{:evict}
         :_id ::id))

(s/def ::tx-op
  (s/and vector? (s/multi-spec tx-op-spec (fn [v _] v))))

(s/def ::tx-ops (s/coll-of ::tx-op :kind sequential?))

(defn- conform-tx-ops [tx-ops]
  (let [parsed-tx-ops (s/conform ::tx-ops tx-ops)]
    (when (s/invalid? parsed-tx-ops)
      (throw (err/illegal-arg :invalid-tx-ops
                              {::err/message (s/explain ::tx-ops tx-ops)
                               :tx-ops tx-ops
                               :explain-data (s/explain-data ::tx-ops tx-ops)})))
    parsed-tx-ops))

(def ^:private ^org.apache.arrow.vector.types.pojo.Field valid-time-start-field
  (t/->field "_valid-time-start" t/timestamp-micro-tz-type true))

(def ^:private ^org.apache.arrow.vector.types.pojo.Field valid-time-end-field
  (t/->field "_valid-time-end" t/timestamp-micro-tz-type true))

(def ^:private ^org.apache.arrow.vector.types.pojo.Schema tx-schema
  (Schema. [(t/->field "tx-ops" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                       (t/->field "put" t/struct-type false
                                  (t/->field "document" t/dense-union-type false)
                                  valid-time-start-field
                                  valid-time-end-field)
                       (t/->field "delete" t/struct-type false
                                  (t/->field "_id" t/dense-union-type false)
                                  valid-time-start-field
                                  valid-time-end-field)
                       (t/->field "evict" t/struct-type false
                                  (t/->field "_id" t/dense-union-type false)))]))

(defn serialize-tx-ops ^java.nio.ByteBuffer [tx-ops ^BufferAllocator allocator]
  (let [tx-ops (conform-tx-ops tx-ops)]
    (with-open [root (VectorSchemaRoot/create tx-schema allocator)]
      (let [tx-ops-writer (.asDenseUnion (vw/vec->writer (.getVector root "tx-ops")))

            put-writer (.asStruct (.writerForTypeId tx-ops-writer 0))
            put-doc-writer (.asDenseUnion (.writerForName put-writer "document"))
            put-vt-start-writer (.writerForName put-writer "_valid-time-start")
            put-vt-end-writer (.writerForName put-writer "_valid-time-end")

            delete-writer (.asStruct (.writerForTypeId tx-ops-writer 1))
            delete-id-writer (.asDenseUnion (.writerForName delete-writer "_id"))
            delete-vt-start-writer (.writerForName delete-writer "_valid-time-start")
            delete-vt-end-writer (.writerForName delete-writer "_valid-time-end")

            evict-writer (.asStruct (.writerForTypeId tx-ops-writer 2))
            evict-id-writer (.asDenseUnion (.writerForName evict-writer "_id"))]

        (dotimes [tx-op-n (count tx-ops)]
          (.startValue tx-ops-writer)

          (let [{:keys [op vt-opts] :as tx-op} (nth tx-ops tx-op-n)]
            (case op
              :put (let [put-idx (.startValue put-writer)]
                     (let [{:keys [doc]} tx-op
                           put-doc-leg-writer (-> (.writerForType put-doc-writer (LegType$StructLegType. (into #{} (map name) (keys doc))))
                                                  (doto (.startValue))
                                                  (.asStruct))]
                       (doseq [[k v] doc]
                         (doto (-> (.writerForName put-doc-leg-writer (name k))
                                   (.asDenseUnion)
                                   (.writerForType (t/value->leg-type v)))
                           (.startValue)
                           (->> (t/write-value! v))
                           (.endValue)))

                       (.endValue put-doc-leg-writer))

                     (when-let [^Instant vt-start (:_valid-time-start vt-opts)]
                       (.set ^TimeStampMicroTZVector (.getVector put-vt-start-writer)
                             put-idx (util/instant->micros (util/->instant vt-start))))

                     (when-let [^Instant vt-end (:_valid-time-end vt-opts)]
                       (.set ^TimeStampMicroTZVector (.getVector put-vt-end-writer)
                             put-idx (util/instant->micros (util/->instant vt-end))))

                     (.endValue put-writer))

              :delete (let [delete-idx (.startValue delete-writer)]
                        (let [id (:_id tx-op)]
                          (doto (-> delete-id-writer
                                    (.writerForType (t/value->leg-type id)))
                            (.startValue)
                            (->> (t/write-value! id))
                            (.endValue)))

                        (when-let [^Instant vt-start (:_valid-time-start vt-opts)]
                          (.set ^TimeStampMicroTZVector (.getVector delete-vt-start-writer)
                                delete-idx (util/instant->micros (util/->instant vt-start))))

                        (when-let [^Instant vt-end (:_valid-time-end vt-opts)]
                          (.set ^TimeStampMicroTZVector (.getVector delete-vt-end-writer)
                                delete-idx (util/instant->micros (util/->instant vt-end)))))

              :evict (do
                       (.startValue evict-writer)
                       (let [id (:_id tx-op)]
                         (doto (-> evict-id-writer
                                   (.writerForType (t/value->leg-type id)))
                           (.startValue)
                           (->> (t/write-value! id))
                           (.endValue)))
                       (.endValue evict-writer))))

          (.endValue tx-ops-writer))

        (util/set-vector-schema-root-row-count root (count tx-ops))
        (.syncSchema root)

        (util/root->arrow-ipc-byte-buffer root :stream)))))

(deftype TxProducer [^Log log, ^BufferAllocator allocator]
  ITxProducer
  (submitTx [_ tx-ops]
    (-> (.appendRecord log (serialize-tx-ops tx-ops allocator))
        (util/then-apply
          (fn [^LogRecord result]
            (.tx result))))))

(defmethod ig/prep-key ::tx-producer [_ opts]
  (merge {:log (ig/ref :core2/log)
          :allocator (ig/ref :core2/allocator)}
         opts))

(defmethod ig/init-key ::tx-producer [_ {:keys [log allocator]}]
  (TxProducer. log allocator))

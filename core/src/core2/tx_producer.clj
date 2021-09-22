(ns core2.tx-producer
  (:require [clojure.spec.alpha :as s]
            [core2.error :as err]
            core2.log
            [core2.relation :as rel]
            [core2.types :as t]
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import [core2.log Log LogRecord]
           core2.relation.IColumnWriter
           java.util.Date
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector TimeStampMilliVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex DenseUnionVector StructVector]
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
  (t/->field "_valid-time-start" (t/->arrow-type :timestamp-milli) true))

(def ^:private ^org.apache.arrow.vector.types.pojo.Field valid-time-end-field
  (t/->field "_valid-time-end" (t/->arrow-type :timestamp-milli) true))

(def ^:private ^org.apache.arrow.vector.types.pojo.Schema tx-schema
  (Schema. [(t/->field "tx-ops" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                       (t/->field "put" t/struct-type false
                                  (t/->field "document" t/struct-type false)
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
      (let [^DenseUnionVector tx-ops-duv (.getVector root "tx-ops")
            tx-ops-writer (rel/vec->writer tx-ops-duv)

            put-writer (.writerForTypeId tx-ops-writer 0)
            ^StructVector put-vec (.getVector put-writer)
            ^StructVector put-doc-vec (.getChild put-vec "document" StructVector)
            put-doc-writer (rel/vec->writer put-doc-vec)
            put-vt-start-writer (rel/vec->writer (.getChild put-vec "_valid-time-start" TimeStampMilliVector))
            put-vt-end-writer (rel/vec->writer (.getChild put-vec "_valid-time-end" TimeStampMilliVector))

            delete-writer (.writerForTypeId tx-ops-writer 1)
            ^StructVector delete-vec (.getVector delete-writer)
            delete-id-writer (rel/vec->writer (.getChild delete-vec "_id" DenseUnionVector))
            delete-vt-start-writer (rel/vec->writer (.getChild delete-vec "_valid-time-start" TimeStampMilliVector))
            delete-vt-end-writer (rel/vec->writer (.getChild delete-vec "_valid-time-end" TimeStampMilliVector))

            evict-writer (.writerForTypeId tx-ops-writer 2)
            ^StructVector evict-vec (.getVector evict-writer)
            evict-id-writer (rel/vec->writer (.getChild evict-vec "_id" DenseUnionVector))]

        (dotimes [tx-op-n (count tx-ops)]
          (let [{:keys [op vt-opts] :as tx-op} (nth tx-ops tx-op-n)]
            (case op
              :put (let [put-idx (.appendIndex put-writer)]
                     (.setIndexDefined put-vec put-idx)
                     (.setIndexDefined put-doc-vec put-idx)

                     (let [{:keys [doc]} tx-op]
                       (doseq [[k v] doc]
                         (let [^IColumnWriter writer (-> (.writerForName put-doc-writer (name k))
                                                         (.writerForType (t/class->arrow-type (type v))))
                               dest-idx (.appendIndex writer put-idx)]
                           (t/set-safe! (.getVector writer) dest-idx v))))

                     (when-let [^Date vt-start (:_valid-time-start vt-opts)]
                       (.set ^TimeStampMilliVector (.getVector put-vt-start-writer)
                             put-idx (.getTime vt-start)))

                     (when-let [^Date vt-end (:_valid-time-end vt-opts)]
                       (.set ^TimeStampMilliVector (.getVector put-vt-end-writer)
                             put-idx (.getTime vt-end))))

              :delete (let [delete-idx (.appendIndex delete-writer)]
                        (.setIndexDefined delete-vec delete-idx)

                        (let [id (:_id tx-op)
                              ^IColumnWriter writer (-> delete-id-writer
                                                        (.writerForType (t/class->arrow-type (type id))))
                              dest-idx (.appendIndex writer delete-idx)]
                          (t/set-safe! (.getVector writer) dest-idx id))

                        (when-let [^Date vt-start (:_valid-time-start vt-opts)]
                          (.set ^TimeStampMilliVector (.getVector delete-vt-start-writer)
                                delete-idx (.getTime vt-start)))

                        (when-let [^Date vt-end (:_valid-time-end vt-opts)]
                          (.set ^TimeStampMilliVector (.getVector delete-vt-end-writer)
                                delete-idx (.getTime vt-end))))

              :evict (let [evict-idx (.appendIndex evict-writer)]
                       (.setIndexDefined evict-vec evict-idx)

                       (let [id (:_id tx-op)
                             ^IColumnWriter writer (-> evict-id-writer
                                                       (.writerForType (t/class->arrow-type (type id))))
                             dest-idx (.appendIndex writer evict-idx)]
                         (t/set-safe! (.getVector writer) dest-idx id))))))

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

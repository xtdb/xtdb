(ns core2.tx-producer
  (:require [clojure.spec.alpha :as s]
            [core2.error :as err]
            core2.log
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.writer :as vw]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import [core2.log Log LogRecord]
           (java.time Clock Instant)
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector TimeStampMicroTZVector VectorSchemaRoot]
           [org.apache.arrow.vector.types.pojo ArrowType$Union Schema]
           org.apache.arrow.vector.types.UnionMode))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface ITxProducer
  (submitTx
    ^java.util.concurrent.CompletableFuture #_<TransactionInstant> [^java.util.List txOps])
  (submitTx
    ^java.util.concurrent.CompletableFuture #_<TransactionInstant> [^java.util.List txOps, ^java.util.Map opts]))

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
  (types/col-type->field "_valid-time-start" [:timestamp-tz :micro "UTC"]))

(def ^:private ^org.apache.arrow.vector.types.pojo.Field valid-time-end-field
  (types/col-type->field "_valid-time-end" [:timestamp-tz :micro "UTC"]))

(def ^:private ^org.apache.arrow.vector.types.pojo.Schema tx-schema
  (Schema. [(types/->field "tx-ops" types/list-type false
                           (types/->field "tx-ops" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                                          (types/->field "put" types/struct-type false
                                                         (types/->field "document" types/dense-union-type false)
                                                         valid-time-start-field
                                                         valid-time-end-field)
                                          (types/->field "delete" types/struct-type false
                                                         (types/->field "_id" types/dense-union-type false)
                                                         valid-time-start-field
                                                         valid-time-end-field)
                                          (types/->field "evict" types/struct-type false
                                                         (types/->field "_id" types/dense-union-type false))))
            (types/col-type->field "tx-time" [:union #{:null [:timestamp-tz :micro "UTC"]}])]))

(defn serialize-tx-ops ^java.nio.ByteBuffer [^BufferAllocator allocator tx-ops {:keys [^Instant tx-time, ^Instant default-valid-time]}]
  (let [tx-ops (conform-tx-ops tx-ops)
        op-count (count tx-ops)]
    (with-open [root (VectorSchemaRoot/create tx-schema allocator)]
      (let [ops-list-writer (.asList (vw/vec->writer (.getVector root "tx-ops")))
            tx-ops-writer (.asDenseUnion (.getDataWriter ops-list-writer))
            ^TimeStampMicroTZVector tx-time-vec (.getVector root "tx-time")

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

        (when tx-time
          (.setSafe tx-time-vec 0 (util/instant->micros tx-time)))

        (.startValue ops-list-writer)

        (dotimes [tx-op-n op-count]
          (.startValue tx-ops-writer)

          (let [{:keys [op vt-opts] :as tx-op} (nth tx-ops tx-op-n)]
            (case op
              :put (let [put-idx (.startValue put-writer)]
                     (let [{:keys [doc]} tx-op]
                       (doto (.writerForType put-doc-writer (types/value->col-type doc))
                         (.startValue)
                         (->> (types/write-value! doc))
                         (.endValue)))

                     (let [^Instant vt-start (or (some-> (:_valid-time-start vt-opts) util/->instant)
                                                 default-valid-time)]
                       (.set ^TimeStampMicroTZVector (.getVector put-vt-start-writer)
                             put-idx (util/instant->micros vt-start)))

                     (let [^Instant vt-end (or (some-> (:_valid-time-end vt-opts) util/->instant)
                                               util/end-of-time)]
                       (.set ^TimeStampMicroTZVector (.getVector put-vt-end-writer)
                             put-idx (util/instant->micros vt-end)))

                     (.endValue put-writer))

              :delete (let [delete-idx (.startValue delete-writer)]
                        (let [id (:_id tx-op)]
                          (doto (-> delete-id-writer
                                    (.writerForType (types/value->col-type id)))
                            (.startValue)
                            (->> (types/write-value! id))
                            (.endValue)))

                        (let [^Instant vt-start (or (some-> (:_valid-time-start vt-opts) util/->instant)
                                                    default-valid-time)]
                          (.set ^TimeStampMicroTZVector (.getVector delete-vt-start-writer)
                                delete-idx (util/instant->micros vt-start)))

                        (let [^Instant vt-end (or (some-> (:_valid-time-end vt-opts) util/->instant)
                                                  util/end-of-time)]
                          (.set ^TimeStampMicroTZVector (.getVector delete-vt-end-writer)
                                delete-idx (util/instant->micros vt-end))))

              :evict (do
                       (.startValue evict-writer)
                       (let [id (:_id tx-op)]
                         (doto (-> evict-id-writer
                                   (.writerForType (types/value->col-type id)))
                           (.startValue)
                           (->> (types/write-value! id))
                           (.endValue)))
                       (.endValue evict-writer))))

          (.endValue tx-ops-writer))

        (.endValue ops-list-writer)

        (.setRowCount root 1)
        (.syncSchema root)

        (util/root->arrow-ipc-byte-buffer root :stream)))))

(deftype TxProducer [^BufferAllocator allocator, ^Log log, ^Clock clock]
  ITxProducer
  (submitTx [this tx-ops]
    (.submitTx this tx-ops {}))

  (submitTx [_ tx-ops opts]
    (let [{:keys [tx-time] :as opts} (-> opts
                                         (assoc :default-valid-time (.instant clock))
                                         (some-> (update :tx-time util/->instant)))]
      (-> (.appendRecord log (serialize-tx-ops allocator tx-ops opts))
          (util/then-apply
            (fn [^LogRecord result]
              (cond-> (.tx result)
                tx-time (assoc :tx-time tx-time))))))))

(defmethod ig/prep-key ::tx-producer [_ opts]
  (merge {:clock (ig/ref :core2/clock)
          :log (ig/ref :core2/log)
          :allocator (ig/ref :core2/allocator)}
         opts))

(defmethod ig/init-key ::tx-producer [_ {:keys [clock log allocator]}]
  (TxProducer. allocator log clock))

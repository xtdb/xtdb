(ns core2.tx-producer
  (:require [clojure.set :as set]
            core2.log
            [core2.system :as sys]
            [core2.types :as t]
            [core2.util :as util])
  (:import core2.DenseUnionUtil
           [core2.log LogRecord LogWriter]
           [java.util LinkedHashMap LinkedHashSet Set]
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector TimeStampVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex DenseUnionVector StructVector]
           org.apache.arrow.vector.types.UnionMode
           [org.apache.arrow.vector.types.pojo ArrowType ArrowType$Union Schema]))

(definterface ITxProducer
  (submitTx
    ^java.util.concurrent.CompletableFuture #_<TransactionInstant> [^java.util.List txOps]))

(defn- ->doc-k-types [tx-ops]
  (let [doc-k-types (LinkedHashMap.)]
    (doseq [{:keys [op] :as tx-op} tx-ops
            [k v] (case op
                    :put (:doc tx-op)
                    :delete (select-keys tx-op [:_id]))]
      (let [^Set field-types (.computeIfAbsent doc-k-types k (util/->jfn (fn [_] (LinkedHashSet.))))]
        (.add field-types (t/class->arrow-type (type v)))))

    doc-k-types))

(defn- validate-tx-ops [tx-ops]
  (doseq [{:keys [op _valid-time-start _valid-time-end] :as tx-op} tx-ops]
    (case op
      :put (assert (contains? (:doc tx-op) :_id))
      :delete (assert (and (contains? tx-op :_id)
                           (set/subset? (set (keys tx-op)) #{:op :_id :_valid-time-start :_valid-time-end}))))

    (when _valid-time-start
      (assert (inst? _valid-time-start)))
    (when _valid-time-end
      (assert (inst? _valid-time-end)))))

(defn- ->doc-field [k v-types]
  (let [v-types (sort-by t/arrow-type->type-id v-types)]
    (apply t/->field
           (name k)
           (ArrowType$Union. UnionMode/Dense
                             (int-array (map t/arrow-type->type-id v-types)))
           false
           (for [^ArrowType v-type v-types]
             (t/->field (str "type-" (t/arrow-type->type-id v-type)) v-type false)))))

(def ^:private ^org.apache.arrow.vector.types.pojo.Field valid-time-start-field
  (t/->field "_valid-time-start" (t/->arrow-type :timestamp-milli) true))

(def ^:private ^org.apache.arrow.vector.types.pojo.Field valid-time-end-field
  (t/->field "_valid-time-end" (t/->arrow-type :timestamp-milli) true))

(defn serialize-tx-ops ^java.nio.ByteBuffer [tx-ops ^BufferAllocator allocator]
  (validate-tx-ops tx-ops)
  (let [tx-ops (vec tx-ops)
        put-k-types (->doc-k-types tx-ops)
        document-field (apply t/->field "document" t/struct-type false
                              (for [[k v-types] put-k-types]
                                (->doc-field k v-types)))
        delete-id-field (->doc-field :_id (:_id put-k-types))
        tx-schema (Schema. [(t/->field "tx-ops" (ArrowType$Union. UnionMode/Dense (int-array [0 1])) false
                                       (t/->field "put" t/struct-type false
                                                  document-field
                                                  valid-time-start-field
                                                  valid-time-end-field)
                                       (t/->field "delete" t/struct-type false
                                                  delete-id-field
                                                  valid-time-start-field
                                                  valid-time-end-field))])]
    (with-open [root (VectorSchemaRoot/create tx-schema allocator)]
      (let [^DenseUnionVector tx-ops-duv (.getVector root "tx-ops")]

        (dotimes [tx-op-n (count tx-ops)]
          (let [{:keys [op _valid-time-start _valid-time-end] :as tx-op} (nth tx-ops tx-op-n)
                op-type-id (case op :put 0, :delete 1)
                ^StructVector op-vec (.getStruct tx-ops-duv op-type-id)
                tx-op-offset (DenseUnionUtil/writeTypeId tx-ops-duv tx-op-n op-type-id)
                valid-time-start-vec (.getChild op-vec "_valid-time-start" TimeStampVector)
                valid-time-end-vec (.getChild op-vec "_valid-time-end" TimeStampVector)]
            (case op
              :put (let [^StructVector document-vec (.getChild op-vec "document" StructVector)]
                     (.setIndexDefined op-vec tx-op-offset)
                     (.setIndexDefined document-vec tx-op-offset)

                     (let [{:keys [doc]} tx-op]
                       (doseq [[k v] doc
                               :let [^DenseUnionVector value-duv (.getChild document-vec (name k) DenseUnionVector)]]
                         (if (some? v)
                           (let [type-id (t/arrow-type->type-id (t/class->arrow-type (type v)))
                                 value-offset (DenseUnionUtil/writeTypeId value-duv tx-op-offset type-id)]
                             (t/set-safe! (.getVectorByType value-duv type-id) value-offset v))

                           (util/set-value-count value-duv (inc (.getValueCount value-duv)))))))

              :delete (let [id (:_id tx-op)
                            ^DenseUnionVector id-duv (.getChild op-vec "_id" DenseUnionVector)]
                        (.setIndexDefined op-vec tx-op-offset)

                        (if (some? id)
                          (let [type-id (t/arrow-type->type-id (t/class->arrow-type (type id)))
                                value-offset (DenseUnionUtil/writeTypeId id-duv tx-op-offset type-id)]
                            (t/set-safe! (.getVectorByType id-duv type-id) value-offset id))

                          (util/set-value-count id-duv (inc (.getValueCount id-duv))))))

            (if _valid-time-start
              (t/set-safe! valid-time-start-vec tx-op-offset _valid-time-start)
              (doto valid-time-start-vec
                (util/set-value-count (inc tx-op-offset))
                (t/set-null! tx-op-offset)))

            (if _valid-time-end
              (t/set-safe! valid-time-end-vec tx-op-offset _valid-time-end)
              (doto valid-time-end-vec
                (util/set-value-count (inc tx-op-offset))
                (t/set-null! tx-op-offset)))))

        (util/set-vector-schema-root-row-count root (count tx-ops))

        (util/root->arrow-ipc-byte-buffer root :stream)))))

(deftype TxProducer [^LogWriter log, ^BufferAllocator allocator]
  ITxProducer
  (submitTx [_ tx-ops]
    (-> (.appendRecord log (serialize-tx-ops tx-ops allocator))
        (util/then-apply
          (fn [^LogRecord result]
            (.tx result))))))

(defn ->tx-producer {::sys/deps {:log :core2/log
                                 :allocator :core2/allocator}}
  [{:keys [log allocator]}]
  (TxProducer. log allocator))

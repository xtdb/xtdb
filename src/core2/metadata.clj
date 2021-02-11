(ns core2.metadata
  (:require [core2.types :as t]
            [core2.util :as util])
  (:import core2.types.ReadWrite
           java.util.Comparator
           [org.apache.arrow.vector BigIntVector FieldVector TinyIntVector VarCharVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector.types Types Types$MinorType]
           org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.util.Text))

(set! *unchecked-math* :warn-on-boxed)

(def ^org.apache.arrow.vector.types.pojo.Schema metadata-schema
  (Schema. [(t/->field "file" (.getType Types$MinorType/VARCHAR) true)
            (t/->field "column" (.getType Types$MinorType/VARCHAR) true)
            (t/->field "field" (.getType Types$MinorType/VARCHAR) true)
            (t/->field "type-id" (.getType Types$MinorType/TINYINT) false)
            (t/->primitive-dense-union-field "min")
            (t/->primitive-dense-union-field "max")
            (t/->field "count" (.getType Types$MinorType/BIGINT) true)]))

(definterface MinMax
  (^void writeMinMax [^org.apache.arrow.vector.FieldVector src-vec
                      ^org.apache.arrow.vector.FieldVector min-vec, ^int min-vec-idx
                      ^org.apache.arrow.vector.FieldVector max-vec, ^int max-vec-idx]))

(deftype MinMaxImpl [^ReadWrite rw, ^Comparator comparator
                     curr-val min-val max-val]
  MinMax
  (writeMinMax [_this src-vec min-vec min-vec-idx max-vec max-vec-idx]
    (dotimes [src-idx (.getValueCount src-vec)]
      (.read rw src-vec src-idx curr-val)
      (when (or (not (.isSet rw min-val)) (neg? (.compare comparator curr-val min-val)))
        (.read rw src-vec src-idx min-val))

      (when (or (not (.isSet rw max-val)) (pos? (.compare comparator curr-val max-val)))
        (.read rw src-vec src-idx max-val)))

    (.write rw min-vec min-vec-idx min-val)
    (.write rw max-vec max-vec-idx max-val)))

(defn- ->min-max-impl [^ReadWrite rw, ^Comparator comparator]
  (MinMaxImpl. rw comparator (.newHolder rw) (.newHolder rw) (.newHolder rw)))

(defn- ->min-max [^FieldVector field-vec]
  (condp = (Types/getMinorTypeForArrowType (.getType (.getField field-vec)))
    Types$MinorType/BIGINT (->min-max-impl t/bigint-rw t/bigint-comp)
    Types$MinorType/FLOAT8 (->min-max-impl t/float8-rw t/float8-comp)
    Types$MinorType/BIT (->min-max-impl t/bit-rw t/bit-comp)
    Types$MinorType/TIMESTAMPMILLI (->min-max-impl t/timestamp-milli-rw t/timestamp-milli-comp)
    Types$MinorType/NULL (->min-max-impl t/null-rw (Comparator/nullsFirst (Comparator/naturalOrder)))
    Types$MinorType/VARBINARY (->min-max-impl t/varbinary-rw t/varbinary-comp)
    Types$MinorType/VARCHAR (->min-max-impl t/varchar-rw t/varchar-comp)))

(defn- write-min-max [^FieldVector field-vec, ^VectorSchemaRoot metadata-root, idx]
  (let [^byte type-id (t/arrow-type->type-id (.getType (.getField field-vec)))

        ^DenseUnionVector min-vec (.getVector metadata-root "min")
        min-offset (util/write-type-id min-vec idx type-id)

        ^DenseUnionVector max-vec (.getVector metadata-root "max")
        max-offset (util/write-type-id max-vec idx type-id)]
    (when (pos? (.getValueCount field-vec))
      (.writeMinMax ^MinMax (->min-max field-vec)
                    field-vec
                    (.getVectorByType min-vec type-id) min-offset
                    (.getVectorByType max-vec type-id) max-offset))))

(defn write-col-meta [^VectorSchemaRoot metadata-root, ^VectorSchemaRoot live-root
                      ^String col-name ^String file-name]
  (letfn [(write-vec-meta [^FieldVector field-vec ^String field-name]
            (when (pos? (.getValueCount field-vec))
              (let [idx (.getRowCount metadata-root)
                    ^byte type-id (t/arrow-type->type-id (.getType (.getField field-vec)))]

                (doto ^VarCharVector (.getVector metadata-root "column")
                  (.setSafe idx (Text. col-name)))
                (doto ^VarCharVector (.getVector metadata-root "field")
                  (.setSafe idx (Text. field-name)))
                (doto ^VarCharVector (.getVector metadata-root "file")
                  (.setSafe idx (Text. file-name)))
                (doto ^TinyIntVector (.getVector metadata-root "type-id")
                  (.setSafe idx type-id))
                (doto ^BigIntVector (.getVector metadata-root "count")
                  (.setSafe idx (.getValueCount field-vec)))

                (write-min-max field-vec metadata-root idx)

                (.setRowCount metadata-root (inc idx)))))]

    (doseq [^FieldVector field-vec (.getFieldVectors live-root)
            :let [field-name (.getName (.getField field-vec))]]
      (if (instance? DenseUnionVector field-vec)
        (doseq [child-vec (.getChildrenFromFields ^DenseUnionVector field-vec)]
          (write-vec-meta child-vec field-name))
        (write-vec-meta field-vec field-name)))))

(defn chunk->metadata ^java.nio.ByteBuffer [live-roots allocator ^long chunk-idx]
  (with-open [metadata-root (VectorSchemaRoot/create metadata-schema allocator)]
    (doseq [[^String col-name, ^VectorSchemaRoot live-root] live-roots]
      (let [obj-key (format "chunk-%08x-%s.arrow" chunk-idx col-name)]
        (write-col-meta metadata-root live-root col-name obj-key)))

    (util/root->arrow-ipc-byte-buffer metadata-root :file)))

(defn- field-idx [^VectorSchemaRoot metadata, ^String column-name, ^String field-name]
  (let [column-vec (.getVector metadata "column")
        field-vec (.getVector metadata "field")]
    (reduce (fn [_ ^long idx]
              (when (and (= (str (.getObject column-vec idx))
                            column-name)
                         (= (str (.getObject field-vec idx))
                            field-name))
                (reduced idx)))
            nil
            (range (.getRowCount metadata)))))

(defn max-value
  ([^VectorSchemaRoot metadata, ^String field-name]
   (max-value metadata field-name field-name))

  ([^VectorSchemaRoot metadata, ^String column-name, ^String field-name]
   (when-let [field-idx (field-idx metadata column-name field-name)]
     ;; TODO boxing
     (.getObject (.getVector metadata "max") field-idx))))

(defn chunk-file [^VectorSchemaRoot metadata, ^String field-name]
  (when-let [field-idx (field-idx metadata field-name field-name)]
    (str (.getObject (.getVector metadata "file") field-idx))))

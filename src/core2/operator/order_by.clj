(ns core2.operator.order-by
  (:require [core2.util :as util])
  (:import clojure.lang.Keyword
           [core2 DenseUnionUtil ICursor]
           [java.util ArrayList Collections Comparator List]
           java.util.function.Consumer
           [org.apache.arrow.algorithm.sort DefaultVectorComparators VectorValueComparator]
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector TimeStampMilliVector ValueVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           org.apache.arrow.vector.types.pojo.Field))

(set! *unchecked-math* :warn-on-boxed)

(deftype OrderSpec [^String col-name, ^Keyword direction])

(defn ->order-spec [col-name direction]
  (OrderSpec. col-name direction))

(defn- accumulate-roots ^org.apache.arrow.vector.VectorSchemaRoot [^ICursor in-cursor, ^BufferAllocator allocator]
  (let [!acc-root (atom nil)]
    (.forEachRemaining in-cursor
                       (reify Consumer
                         (accept [_ in-root]
                           (let [^VectorSchemaRoot in-root in-root
                                 ^VectorSchemaRoot
                                 acc-root (swap! !acc-root (fn [^VectorSchemaRoot acc-root]
                                                             (if acc-root
                                                               (do
                                                                 (assert (= (.getSchema acc-root) (.getSchema in-root)))
                                                                 acc-root)
                                                               (VectorSchemaRoot/create (.getSchema in-root) allocator))))
                                 schema (.getSchema acc-root)
                                 acc-row-count (.getRowCount acc-root)
                                 in-row-count (.getRowCount in-root)]

                             (doseq [^Field field (.getFields schema)]
                               (let [acc-vec (.getVector acc-root field)
                                     in-vec (.getVector in-root field)]
                                 (dotimes [idx in-row-count]
                                   (if (and (instance? DenseUnionVector acc-vec)
                                            (instance? DenseUnionVector in-vec))
                                     (DenseUnionUtil/copyIdxSafe in-vec idx acc-vec (+ acc-row-count idx))
                                     (.copyFromSafe acc-vec idx (+ acc-row-count idx) in-vec)))))

                             (util/set-vector-schema-root-row-count acc-root (+ acc-row-count in-row-count))))))
    @!acc-root))

(defn- ->arrow-comparator ^org.apache.arrow.algorithm.sort.VectorValueComparator [^ValueVector v]
  (if (instance? TimeStampMilliVector v)
    (proxy [VectorValueComparator] []
      (compareNotNull [idx]
        (Long/compare (.get ^TimeStampMilliVector v idx)
                      (.get ^TimeStampMilliVector v idx))))
    (doto (DefaultVectorComparators/createDefaultComparator v)
      (.attachVector v))))

(defn order-root ^java.util.List [^VectorSchemaRoot root, ^List #_<OrderSpec> order-specs]
  (let [idxs (ArrayList. ^List (range (.getRowCount root)))
        comparator (reduce (fn [^Comparator acc ^OrderSpec order-spec]
                             (let [^String column-name (.col-name order-spec)
                                   direction (.direction order-spec)
                                   in-vec (util/maybe-single-child-dense-union (.getVector root column-name))
                                   arrow-comparator (->arrow-comparator in-vec)
                                   ^Comparator comparator (cond-> (reify Comparator
                                                                    (compare [_ left-idx right-idx]
                                                                      (.compare arrow-comparator left-idx right-idx)))
                                                            (= :desc direction) (.reversed))]
                               (if acc
                                 (.thenComparing acc comparator)
                                 comparator)))
                           nil
                           order-specs)]
    (Collections/sort idxs comparator)
    idxs))

(deftype OrderByCursor [^BufferAllocator allocator
                        ^ICursor in-cursor
                        ^List #_<OrderSpec> order-specs
                        ^:unsynchronized-mutable ^VectorSchemaRoot out-root]
  ICursor
  (tryAdvance [this c]
    (when out-root
      (.close out-root))

    (if-not out-root
      (if-let [acc-root (accumulate-roots in-cursor allocator)]
        (with-open [acc-root acc-root]
          (let [sorted-idxs (order-root acc-root order-specs)
                out-root (VectorSchemaRoot/create (.getSchema acc-root) allocator)]

            (set! (.out-root this) out-root)

            (if (pos? (.getRowCount acc-root))
              (do (dotimes [n (util/root-field-count acc-root)]
                    (let [in-vec (.getVector acc-root n)
                          out-vec (.getVector out-root n)]
                      (util/set-value-count out-vec (.getValueCount in-vec))
                      (dotimes [idx (.size sorted-idxs)]
                        (if (and (instance? DenseUnionVector in-vec)
                                 (instance? DenseUnionVector out-vec))
                          (DenseUnionUtil/copyIdxSafe in-vec (.get sorted-idxs idx) out-vec idx)
                          (.copyFrom out-vec (.get sorted-idxs idx) idx in-vec)))))
                  (util/set-vector-schema-root-row-count out-root (.getRowCount acc-root))
                  (.accept c out-root)
                  true)
              false)))
        false)
      false))

  (close [_]
    (util/try-close out-root)
    (util/try-close in-cursor)))

(defn ->order-by-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor in-cursor, ^List #_<OrderSpec> order-specs]
  (OrderByCursor. allocator in-cursor order-specs nil))

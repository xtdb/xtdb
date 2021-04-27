(ns core2.operator.table
  (:require [core2.types :as ty])
  (:import [core2 DenseUnionUtil ICursor]
           [java.util ArrayList List]
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.complex.DenseUnionVector
           org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.VectorSchemaRoot))

(set! *unchecked-math* :warn-on-boxed)

(deftype TableCursor [^BufferAllocator allocator
                      ^List rows
                      ^:unsynchronized-mutable done?]
  ICursor
  (tryAdvance [this c]
    (if (or done? (.isEmpty rows))
      false
      (do (set! (.done? this) true)
          (with-open [out-root (VectorSchemaRoot/create (Schema.
                                                         (for [k (keys (first rows))]
                                                           (ty/->primitive-dense-union-field (name k))))
                                                        allocator)]
            (let [row-count (.size rows)]
              (dotimes [n row-count]
                (let [row (.get rows n)]
                  (doseq [[k v] row]
                    (let [type-id (ty/arrow-type->type-id (ty/->arrow-type (class v)))
                          ^DenseUnionVector duv (.getVector out-root (name k))
                          offset (DenseUnionUtil/writeTypeId duv n type-id)]
                      (when (some? v)
                        (ty/set-safe! (.getVectorByType duv type-id) offset v))))))
              (.setRowCount out-root row-count)
              (.accept c out-root)
              true)))))

  (close [_]
    (.clear rows)))

(defn ->table-cursor ^core2.ICursor [^BufferAllocator allocator,
                                     ^List rows]
  (assert (or (empty? rows) (= 1 (count (distinct (map keys rows))))))
  (TableCursor. allocator (ArrayList. rows) false))

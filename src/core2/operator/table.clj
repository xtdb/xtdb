(ns core2.operator.table
  (:require [core2.types :as ty]
            [core2.error :as err])
  (:import [core2 DenseUnionUtil IChunkCursor]
           [java.util ArrayList List]
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.complex.DenseUnionVector
           org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.VectorSchemaRoot))

(set! *unchecked-math* :warn-on-boxed)

(deftype TableCursor [^BufferAllocator allocator
                      ^Schema schema
                      ^List rows
                      ^:unsynchronized-mutable done?]
  IChunkCursor
  (getSchema [_] schema)

  (tryAdvance [this c]
    (if (or done? (.isEmpty rows))
      false
      (do (set! (.done? this) true)
          (with-open [out-root (VectorSchemaRoot/create schema allocator)]
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

(defn ->table-cursor ^core2.IChunkCursor [^BufferAllocator allocator,
                                          ^List rows]
  (when-not (or (empty? rows) (= 1 (count (distinct (map keys rows)))))
    (throw (err/illegal-arg :mismatched-keys-in-table
                            {::err/message "Mismatched keys in table"
                             :key-sets (into #{} (map keys) rows)})))

  (TableCursor. allocator
                (Schema. (for [k (keys (first rows))]
                           (ty/->primitive-dense-union-field (name k))))
                (ArrayList. rows) false))

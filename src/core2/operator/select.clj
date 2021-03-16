(ns core2.operator.select
  (:require [core2.util :as util])
  (:import core2.ICursor
           [core2.select IVectorSchemaRootPredicate IVectorSchemaRootSelector]
           java.util.function.Consumer
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.types.pojo.Field
           org.apache.arrow.vector.VectorSchemaRoot
           org.roaringbitmap.RoaringBitmap))

(deftype SelectCursor [^BufferAllocator allocator
                       ^ICursor in-cursor
                       ^IVectorSchemaRootSelector selector
                       ^:unsynchronized-mutable ^VectorSchemaRoot out-root]
  ICursor
  (tryAdvance [this c]
    (when out-root
      (.close out-root)
      (set! (.out-root this) nil))

    (while (and (nil? out-root)
                (.tryAdvance in-cursor
                             (reify Consumer
                               (accept [_ in-root]
                                 (let [^VectorSchemaRoot in-root in-root
                                       schema (.getSchema in-root)
                                       idx-bitmap (.select selector in-root)
                                       selected-count (.getCardinality idx-bitmap)]

                                   (when (pos? selected-count)
                                     (let [^VectorSchemaRoot out-root (VectorSchemaRoot/create schema allocator)]
                                       (set! (.out-root this) out-root)

                                       (doseq [^Field field (.getFields schema)]
                                         (util/project-vec (.getVector in-root field)
                                                           (.stream idx-bitmap)
                                                           (.getCardinality idx-bitmap)
                                                           (.getVector out-root field)))

                                       (util/set-vector-schema-root-row-count out-root selected-count)))))))))

    (if out-root
      (do
        (.accept c out-root)
        true)
      false))

  (close [_]
    (util/try-close out-root)
    (util/try-close in-cursor)))

(defn pred->selector ^core2.select.IVectorSchemaRootSelector [^IVectorSchemaRootPredicate pred]
  (reify IVectorSchemaRootSelector
    (select [_ in-root]
      (let [idx-bitmap (RoaringBitmap.)]

        (dotimes [idx (.getRowCount in-root)]
          (when (.test pred in-root idx)
            (.add idx-bitmap idx)))

        idx-bitmap))))

(defn ->select-cursor ^core2.ICursor [^BufferAllocator allocator,
                                      ^ICursor in-cursor,
                                      ^IVectorSchemaRootSelector selector]
  (SelectCursor. allocator in-cursor selector nil))

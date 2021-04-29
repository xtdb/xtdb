(ns core2.operator.select
  (:require [core2.util :as util])
  (:import core2.IChunkCursor
           core2.select.IVectorSchemaRootSelector
           java.util.function.Consumer
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.types.pojo.Field
           org.apache.arrow.vector.VectorSchemaRoot))

(set! *unchecked-math* :warn-on-boxed)

(deftype SelectCursor [^BufferAllocator allocator
                       ^VectorSchemaRoot out-root
                       ^IChunkCursor in-cursor
                       ^IVectorSchemaRootSelector selector]
  IChunkCursor
  (getSchema [_] (.getSchema out-root))

  (tryAdvance [_ c]
    (.clear out-root)

    (while (and (zero? (.getRowCount out-root))
                (.tryAdvance in-cursor
                             (reify Consumer
                               (accept [_ in-root]
                                 (let [^VectorSchemaRoot in-root in-root
                                       idx-bitmap (.select selector in-root)
                                       selected-count (.getCardinality idx-bitmap)]
                                   (when (pos? selected-count)
                                     (doseq [^Field field (.getFields (.getSchema in-root))]
                                       (util/project-vec (.getVector in-root field)
                                                         (.stream idx-bitmap)
                                                         (.getCardinality idx-bitmap)
                                                         (.getVector out-root field)))

                                     (util/set-vector-schema-root-row-count out-root selected-count))))))))

    (if (pos? (.getRowCount out-root))
      (do
        (.accept c out-root)
        true)
      false))

  (close [_]
    (util/try-close out-root)
    (util/try-close in-cursor)))

(defn ->select-cursor ^core2.IChunkCursor [^BufferAllocator allocator,
                                           ^IChunkCursor in-cursor,
                                           ^IVectorSchemaRootSelector selector]
  (SelectCursor. allocator (VectorSchemaRoot/create (.getSchema in-cursor) allocator)
                 in-cursor selector))

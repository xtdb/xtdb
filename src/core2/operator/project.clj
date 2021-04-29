(ns core2.operator.project
  (:require [core2.util :as util])
  (:import core2.IChunkCursor
           java.util.List
           java.util.function.Consumer
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.VectorSchemaRoot
           org.apache.arrow.vector.types.pojo.Schema))

(set! *unchecked-math* :warn-on-boxed)

(definterface ProjectionSpec
  (^org.apache.arrow.vector.types.pojo.Field getField [^org.apache.arrow.vector.types.pojo.Schema inSchema])

  (^void project [^org.apache.arrow.vector.VectorSchemaRoot inRoot
                  ^org.apache.arrow.vector.FieldVector outVector]))

(deftype IdentityProjectionSpec [^String col-name]
  ProjectionSpec
  (getField [_ in-schema]
    (.findField in-schema col-name))

  (project [_ in-root out-vec]
    (doto (.makeTransferPair (.getVector in-root col-name) out-vec)
      (.splitAndTransfer 0 (.getRowCount in-root)))))

(defn ->identity-projection-spec ^core2.operator.project.ProjectionSpec [^String col-name]
  (IdentityProjectionSpec. col-name))

(deftype ProjectCursor [^BufferAllocator allocator
                        ^Schema out-schema
                        ^VectorSchemaRoot out-root
                        ^IChunkCursor in-cursor
                        ^List #_<ProjectionSpec> projection-specs]
  IChunkCursor
  (getSchema [_] out-schema)

  (tryAdvance [_ c]
    (.clear out-root)

    (if (.tryAdvance in-cursor
                     (reify Consumer
                       (accept [_ in-root]
                         (dorun
                          (map-indexed (fn [idx ^ProjectionSpec projection-spec]
                                         (.project projection-spec in-root (.getVector out-root ^int idx)))
                                       projection-specs))
                         (util/set-vector-schema-root-row-count out-root (.getRowCount ^VectorSchemaRoot in-root)))))
      (do
        (.accept c out-root)
        true)
      false))

  (close [_]
    (util/try-close out-root)
    (util/try-close in-cursor)))

(defn ->project-cursor ^core2.IChunkCursor [^BufferAllocator allocator, ^IChunkCursor in-cursor, ^List #_<ProjectionSpec> projection-specs]
  (let [in-schema (.getSchema in-cursor)
        out-schema (Schema. (for [^ProjectionSpec spec projection-specs]
                              (.getField spec in-schema)))]
    (ProjectCursor. allocator out-schema
                    (VectorSchemaRoot/create out-schema allocator)
                    in-cursor projection-specs)))

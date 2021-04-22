(ns core2.operator.project
  (:require [core2.util :as util])
  (:import core2.ICursor
           java.util.List
           java.util.function.Consumer
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.VectorSchemaRoot))

(set! *unchecked-math* :warn-on-boxed)

(definterface ProjectionSpec
  (^org.apache.arrow.vector.ValueVector project [^org.apache.arrow.vector.VectorSchemaRoot inRoot
                                                 ^org.apache.arrow.memory.BufferAllocator allocator]))

(deftype IdentityProjectionSpec [^String col-name]
  ProjectionSpec
  (project [_ in-root allocator]
    (let [in-vec (.getVector in-root col-name)]
      (-> (.getTransferPair in-vec allocator)
          (doto (.splitAndTransfer 0 (.getValueCount in-vec)))
          (.getTo)))))

(defn ->identity-projection-spec ^core2.operator.project.ProjectionSpec [^String col-name]
  (IdentityProjectionSpec. col-name))

(deftype ProjectCursor [^BufferAllocator allocator
                        ^ICursor in-cursor
                        ^List #_<ProjectionSpec> projection-specs
                        ^:unsynchronized-mutable ^VectorSchemaRoot out-root]
  ICursor
  (tryAdvance [this c]
    (when out-root
      (.close out-root))

    (if (.tryAdvance in-cursor
                     (reify Consumer
                       (accept [_ in-root]
                         (let [^Iterable out-vecs (for [^ProjectionSpec projection-spec projection-specs]
                                                    (.project projection-spec in-root allocator))]
                           (set! (.out-root this) (VectorSchemaRoot. out-vecs))))))
      (do
        (.accept c out-root)
        true)
      false))

  (close [_]
    (util/try-close out-root)
    (util/try-close in-cursor)))

(defn ->project-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor in-cursor, ^List #_<ProjectionSpec> projection-specs]
  (ProjectCursor. allocator in-cursor projection-specs nil))

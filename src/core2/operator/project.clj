(ns core2.operator.project
  (:require [core2.util :as util]
            [core2.vector :as vec])
  (:import java.util.List
           java.util.function.Consumer
           org.apache.arrow.memory.BufferAllocator
           core2.ICursor
           java.util.LinkedHashMap
           java.util.Map))

(set! *unchecked-math* :warn-on-boxed)

(definterface ProjectionSpec
  (^core2.vector.IReadColumn project [^org.apache.arrow.memory.BufferAllocator allocator
                                      ^core2.vector.IReadRelation readRelation]))

(deftype IdentityProjectionSpec [^String col-name]
  ProjectionSpec
  (project [_ _allocator in-rel]
    (.readColumn in-rel col-name)))

(defn ->identity-projection-spec ^core2.operator.project.ProjectionSpec [^String col-name]
  (IdentityProjectionSpec. col-name))

(deftype ProjectCursor [^BufferAllocator allocator
                        ^ICursor in-cursor
                        ^List #_<ProjectionSpec> projection-specs]
  ICursor
  (tryAdvance [_ c]
    (.tryAdvance in-cursor
                 (reify Consumer
                   (accept [_ read-rel]
                     (let [^Map out-cols (LinkedHashMap.)]
                       (try
                         (doseq [^ProjectionSpec projection-spec projection-specs]
                           (let [out-col (.project projection-spec allocator read-rel)]
                             (.put out-cols (.getName out-col) out-col)))
                         (.accept c (vec/->read-relation out-cols))
                         (finally
                           (run! util/try-close (vals out-cols)))))))))

  (close [_]
    (util/try-close in-cursor)))

(defn ->project-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor in-cursor, ^List #_<ProjectionSpec> projection-specs]
  (ProjectCursor. allocator in-cursor projection-specs))

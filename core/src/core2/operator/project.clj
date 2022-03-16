(ns core2.operator.project
  (:require [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import java.util.List
           java.util.function.Consumer
           org.apache.arrow.memory.BufferAllocator
           core2.ICursor
           core2.operator.IProjectionSpec
           java.util.LinkedList
           org.apache.arrow.vector.BigIntVector))

(set! *unchecked-math* :warn-on-boxed)

(deftype IdentityProjectionSpec [^String col-name]
  )

(defn ->identity-projection-spec ^core2.operator.IProjectionSpec [^String col-name]
  (reify IProjectionSpec
    (getColumnName [_] col-name)
    (project [_ _allocator in-rel]
      (.vectorForName in-rel col-name))))

(defn ->row-number-projection-spec ^core2.operator.IProjectionSpec [^String col-name]
  (let [row-num (long-array [1])]
    (reify IProjectionSpec
      (getColumnName [_] col-name)
      (project [_ allocator in-rel]
        (let [out-vec (BigIntVector. col-name allocator)
              start-row-num (aget row-num 0)
              row-count (.rowCount in-rel)]
          (try
            (.setValueCount out-vec row-count)
            (dotimes [idx row-count]
              (.set out-vec idx (+ idx start-row-num)))
            (aset row-num 0 (+ start-row-num row-count))
            (iv/->direct-vec out-vec)
            (catch Throwable e
              (.close out-vec)
              (throw e))))))))

(deftype ProjectCursor [^BufferAllocator allocator
                        ^ICursor in-cursor
                        ^List #_<IProjectionSpec> projection-specs]
  ICursor
  (tryAdvance [_ c]
    (.tryAdvance in-cursor
                 (reify Consumer
                   (accept [_ read-rel]
                     (let [out-cols (LinkedList.)]
                       (try
                         (doseq [^IProjectionSpec projection-spec projection-specs]
                           (let [out-col (.project projection-spec allocator read-rel)]
                             (.add out-cols out-col)))

                         (.accept c (iv/->indirect-rel out-cols))

                         (finally
                           (run! util/try-close out-cols))))))))

  (close [_]
    (util/try-close in-cursor)))

(defn ->project-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor in-cursor, ^List #_<IProjectionSpec> projection-specs]
  (ProjectCursor. allocator in-cursor projection-specs))

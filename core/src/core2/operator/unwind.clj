(ns core2.operator.unwind
  (:require [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw])
  (:import core2.ICursor
           [core2.vector IIndirectVector IIndirectRelation IVectorWriter]
           java.util.function.Consumer
           java.util.stream.IntStream
           java.util.LinkedList
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector.complex BaseListVector DenseUnionVector FixedSizeListVector ListVector]))

(set! *unchecked-math* :warn-on-boxed)

(defn- get-data-vector ^org.apache.arrow.vector.ValueVector [^BaseListVector v]
  (if (instance? FixedSizeListVector v)
    (.getDataVector ^FixedSizeListVector v)
    (.getDataVector ^ListVector v)))

(defn- unwind-list-element ^long [^BaseListVector in-vec ^IVectorWriter out-writer ^long idx]
  (let [data-vector (get-data-vector in-vec)
        row-copier (.rowCopier out-writer data-vector)
        element-start-idx (.getElementStartIndex in-vec idx)
        elements-at-idx (- (.getElementEndIndex in-vec idx) element-start-idx)]
    (dotimes [n elements-at-idx]
      (.startValue out-writer)
      (.copyRow row-copier (+ element-start-idx n))
      (.endValue out-writer))
    elements-at-idx))

(deftype UnwindCursor [^BufferAllocator allocator
                       ^ICursor in-cursor
                       ^String column-name]
  ICursor
  (tryAdvance [this c]
    (let [!advanced (atom false)]
      (while (and (.tryAdvance in-cursor
                               (reify Consumer
                                 (accept [_ in-rel]
                                   (let [^IIndirectRelation in-rel in-rel
                                         out-cols (LinkedList.)
                                         from-col (.vectorForName in-rel column-name)
                                         idxs (IntStream/builder)
                                         in-vec (.getVector from-col)]
                                     (with-open [out-vec (DenseUnionVector/empty column-name allocator)]
                                       (let [out-writer (vw/vec->writer out-vec)]
                                         (cond
                                           (instance? DenseUnionVector in-vec)
                                           (let [^DenseUnionVector in-vec in-vec]
                                             (dotimes [n (.getValueCount from-col)]
                                               (let [idx (.getIndex from-col n)
                                                     inner-vec (.getVectorByType in-vec (.getTypeId in-vec idx))]
                                                 (when (instance? BaseListVector inner-vec)
                                                   (dotimes [m (unwind-list-element inner-vec out-writer (.getOffset in-vec idx))]
                                                     (.add idxs n))))))

                                           (instance? BaseListVector in-vec)
                                           (dotimes [n (.getValueCount from-col)]
                                             (dotimes [m (unwind-list-element in-vec out-writer (.getIndex from-col n))]
                                               (.add idxs n)))))

                                       (let [idxs (.toArray (.build idxs))]
                                         (when (pos? (alength idxs))
                                           (doseq [^IIndirectVector in-col in-rel]
                                             (if (= column-name (.getName in-col))
                                               (.add out-cols (iv/->direct-vec out-vec))
                                               (.add out-cols (.select in-col idxs))))

                                           (.accept c (iv/->indirect-rel out-cols))
                                           (reset! !advanced true))))))))
                  (not @!advanced)))
      @!advanced))

  (close [_]
    (.close in-cursor)))

(defn ->unwind-cursor ^core2.ICursor [allocator ^ICursor in-cursor column-name]
  (UnwindCursor. allocator in-cursor column-name))

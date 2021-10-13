(ns core2.operator.select
  (:require [core2.coalesce :as coalesce]
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import core2.ICursor
           core2.vector.IIndirectRelation
           java.util.function.Consumer
           org.apache.arrow.memory.BufferAllocator))

(set! *unchecked-math* :warn-on-boxed)

(definterface IRelationSelector
  (^org.roaringbitmap.RoaringBitmap select [^core2.vector.IIndirectRelation in-rel]))

(definterface IColumnSelector
  (^org.roaringbitmap.RoaringBitmap select [^core2.vector.IIndirectVector in-col]))

(deftype SelectCursor [^ICursor in-cursor
                       ^IRelationSelector selector]
  ICursor
  (tryAdvance [_ c]
    (let [!advanced (atom false)]
      (while (and (.tryAdvance in-cursor
                               (reify Consumer
                                 (accept [_ in-rel]
                                   (let [^IIndirectRelation in-rel in-rel]
                                     (when-let [idxs (.select selector in-rel)]
                                       (when-not (.isEmpty idxs)
                                         (.accept c (iv/select in-rel (.toArray idxs)))
                                         (reset! !advanced true)))))))
                  (not @!advanced)))
      @!advanced))

  (close [_]
    (util/try-close in-cursor)))

(defn ->select-cursor ^core2.ICursor [^BufferAllocator allocator, ^ICursor in-cursor, ^IRelationSelector selector]
  (-> (SelectCursor. in-cursor selector)
      (coalesce/->coalescing-cursor allocator)))

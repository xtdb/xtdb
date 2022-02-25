(ns core2.operator.max-1-row
  (:require [core2.vector.indirect :as iv])
  (:import (core2 ICursor)
           (core2.vector IIndirectRelation)
           (java.util Set)
           (java.util.function Consumer)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector NullVector)))

(deftype Max1RowCursor [^BufferAllocator allocator
                        ^Set col-names
                        ^ICursor in-cursor
                        ^:unsynchronized-mutable ^long sent-rows
                        ^:unsynchronized-mutable ^boolean sent-null-row?]
  ICursor
  (tryAdvance [this c]
    (or (.tryAdvance in-cursor
                     (reify Consumer
                       (accept [_ in-rel]
                         (let [^IIndirectRelation in-rel in-rel
                               seen-rows (+ sent-rows (.rowCount in-rel))]
                           (if (> seen-rows 1)
                             (throw (RuntimeException. "cardinality violation"))
                             (do
                               (set! (.sent-rows this) seen-rows)
                               (.accept c in-rel)))))))
        (boolean
         (when (and (zero? sent-rows) (not sent-null-row?))
           (set! (.sent-null-row? this) true)

           (.accept c (iv/->indirect-rel (for [col-name col-names]
                                           (iv/->direct-vec (doto (NullVector. (str col-name))
                                                              (.setValueCount 1))))
                                         1))
           true))))

  (close [_]
    (.close in-cursor)))

(defn ->max-1-row-cursor [^BufferAllocator allocator, col-names, ^ICursor in-cursor]
  (Max1RowCursor. allocator col-names in-cursor 0 false))

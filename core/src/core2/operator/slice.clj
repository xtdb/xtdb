(ns core2.operator.slice
  (:import core2.ICursor
           core2.vector.IIndirectRelation
           java.util.function.Consumer
           java.util.stream.IntStream)
  (:require [core2.vector.indirect :as iv]))

(set! *unchecked-math* :warn-on-boxed)

(defn offset+length [^long offset, ^long limit,
                     ^long idx, ^long row-count]
  (let [rel-offset (max (- offset idx) 0)
        consumed (max (- idx offset) 0)
        rel-length (min (- limit consumed)
                         (- row-count rel-offset))]
    (when (pos? rel-length)
      [rel-offset rel-length])))

(deftype SliceCursor [^ICursor in-cursor
                      ^long offset
                      ^long limit
                      ^:unsynchronized-mutable ^long idx]
  ICursor
  (tryAdvance [this c]
    (let [!advanced? (atom false)]
      (while (and (not @!advanced?)
                  (< (- idx offset) limit)
                  (.tryAdvance in-cursor
                               (reify Consumer
                                 (accept [_ in-rel]
                                   (let [^IIndirectRelation in-rel in-rel
                                         row-count (.rowCount in-rel)
                                         old-idx (.idx this)]

                                     (set! (.-idx this) (+ old-idx row-count))

                                     (when-let [[^long rel-offset, ^long rel-length] (offset+length offset limit old-idx row-count)]
                                       (.accept c (iv/select in-rel (.toArray (IntStream/range rel-offset (+ rel-offset rel-length)))))
                                       (reset! !advanced? true))))))))
      @!advanced?))

  (close [_]
    (.close in-cursor)))

(defn ->slice-cursor ^core2.ICursor [^ICursor in-cursor offset limit]
  (SliceCursor. in-cursor (or offset 0) (or limit Long/MAX_VALUE) 0))

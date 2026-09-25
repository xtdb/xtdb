(ns xtdb.operator.top
  (:import java.util.stream.IntStream
           (xtdb.api ICursor)
           xtdb.arrow.RelationReader))

(set! *unchecked-math* :warn-on-boxed)

(defn offset+length [^long skip, ^long limit,
                     ^long idx, ^long row-count]
  (let [rel-offset (max (- skip idx) 0)
        consumed (max (- idx skip) 0)
        rel-length (min (- limit consumed)
                         (- row-count rel-offset))]
    (when (pos? rel-length)
      [rel-offset rel-length])))

(deftype TopCursor [^ICursor in-cursor
                    ^long skip
                    ^long limit
                    ^:unsynchronized-mutable ^long idx]
  ICursor
  (getCursorType [_] "top")
  (getChildCursors [_] [in-cursor])

  (tryAdvance [this c]
    (let [advanced? (boolean-array 1)]
      (while (and (not (aget advanced? 0))
                  (< (- idx skip) limit)
                  (.tryAdvance in-cursor
                               (fn [^RelationReader in-rel]
                                 (let [row-count (.getRowCount in-rel)
                                       old-idx (.idx this)]

                                   (set! (.-idx this) (+ old-idx row-count))

                                   (when-let [[^long rel-offset, ^long rel-length] (offset+length skip limit old-idx row-count)]
                                     (.accept c (.select in-rel (.toArray (IntStream/range rel-offset (+ rel-offset rel-length)))))
                                     (aset advanced? 0 true)))))))
      (aget advanced? 0)))

  (close [_]
    (.close in-cursor)))

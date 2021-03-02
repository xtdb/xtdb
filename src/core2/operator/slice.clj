(ns core2.operator.slice
  (:require [core2.util :as util])
  (:import core2.ICursor
           java.util.function.Consumer
           org.apache.arrow.vector.VectorSchemaRoot))

(defn offset+length [^long offset, ^long limit,
                     ^long idx, ^long row-count]
  (let [root-offset (max (- offset idx) 0)
        consumed (max (- idx offset) 0)
        root-length (min (- limit consumed)
                         (- row-count root-offset))]
    (when (pos? root-length)
      [root-offset root-length])))

(deftype SliceCursor [^ICursor in-cursor
                      ^long offset
                      ^long limit
                      ^:unsynchronized-mutable ^long idx
                      ^:unsynchronized-mutable ^VectorSchemaRoot out-root]
  ICursor
  (tryAdvance [this c]
    (when out-root
      (.close out-root)
      (set! (.out-root this) nil))

    (while (and (nil? out-root)
                (< (- idx offset) limit)
                (.tryAdvance in-cursor
                             (reify Consumer
                               (accept [_ root]
                                 (let [^VectorSchemaRoot root root
                                       row-count (.getRowCount root)
                                       old-idx (.idx this)]

                                   (set! (.-idx this) (+ old-idx row-count))

                                   (when-let [[root-offset root-length] (offset+length offset limit old-idx row-count)]
                                     (set! (.out-root this) (util/slice-root root root-offset root-length)))))))))

    (if out-root
      (do
        (.accept c out-root)
        true)
      false))

  (close [_]
    (when out-root
      (.close out-root))

    (.close in-cursor)))

(defn ->slice-cursor [in-cursor offset limit]
  (SliceCursor. in-cursor (or offset 0) (or limit Long/MAX_VALUE) 0 nil))

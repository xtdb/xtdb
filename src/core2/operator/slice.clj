(ns core2.operator.slice
  (:import core2.IChunkCursor
           java.util.function.Consumer
           [org.apache.arrow.vector ValueVector VectorSchemaRoot])
  (:require [core2.util :as util]))

(set! *unchecked-math* :warn-on-boxed)

(defn offset+length [^long offset, ^long limit,
                     ^long idx, ^long row-count]
  (let [root-offset (max (- offset idx) 0)
        consumed (max (- idx offset) 0)
        root-length (min (- limit consumed)
                         (- row-count root-offset))]
    (when (pos? root-length)
      [root-offset root-length])))

(deftype SliceCursor [^VectorSchemaRoot out-root
                      ^IChunkCursor in-cursor
                      ^long offset
                      ^long limit
                      ^:unsynchronized-mutable ^long idx]
  IChunkCursor
  (getSchema [_] (.getSchema in-cursor))

  (tryAdvance [this c]
    (.clear out-root)

    (while (and (zero? (.getRowCount out-root))
                (< (- idx offset) limit)
                (.tryAdvance in-cursor
                             (reify Consumer
                               (accept [_ in-root]
                                 (let [^VectorSchemaRoot in-root in-root
                                       row-count (.getRowCount in-root)
                                       old-idx (.idx this)]

                                   (set! (.-idx this) (+ old-idx row-count))

                                   (when-let [[root-offset root-length] (offset+length offset limit old-idx row-count)]
                                     (util/slice-root-to in-root root-offset root-length out-root))))))))

    (if (pos? (.getRowCount out-root))
      (do
        (.accept c out-root)
        true)
      false))

  (close [_]
    (when out-root
      (.close out-root))

    (.close in-cursor)))

(defn ->slice-cursor ^core2.IChunkCursor [allocator ^IChunkCursor in-cursor offset limit]
  (SliceCursor. (VectorSchemaRoot/create (.getSchema in-cursor) allocator)
                in-cursor (or offset 0) (or limit Long/MAX_VALUE) 0))

(ns core2.blocks
  (:require [core2.util :as util])
  (:import core2.ICursor
           java.util.Iterator
           [org.apache.arrow.vector VectorSchemaRoot]))

(deftype SliceCursor [^VectorSchemaRoot root
                      ^Iterator row-counts
                      ^:unsynchronized-mutable ^int start-idx
                      ^:unsynchronized-mutable ^VectorSchemaRoot current-slice]
  ICursor
  (tryAdvance [this c]
    (when current-slice
      (.close current-slice)
      (set! (.current-slice this) nil))

    (if-not (.hasNext row-counts)
      false
      (let [^long len (.next row-counts)
            ^VectorSchemaRoot sliced-root (util/slice-root root start-idx len)]

        (set! (.current-slice this) sliced-root)
        (set! (.start-idx this) (+ start-idx len))

        (.accept c sliced-root)
        true)))

  (close [_]
    (when current-slice
      (.close current-slice))))

(defn ->slices ^core2.ICursor [^VectorSchemaRoot root, ^Iterable row-counts]
  (SliceCursor. root (.iterator row-counts) 0 nil))

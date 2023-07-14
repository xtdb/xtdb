(ns xtdb.blocks
  (:require [xtdb.util :as util])
  (:import [java.util Arrays Iterator]
           [org.apache.arrow.vector VectorSchemaRoot]
           xtdb.ICursor))

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

(defn ->slices ^xtdb.ICursor [^VectorSchemaRoot root, ^ints row-counts]
  (SliceCursor. root (.iterator (Arrays/stream row-counts)) 0 nil))

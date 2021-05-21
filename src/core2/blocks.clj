(ns core2.blocks
  (:require [core2.types :as t]
            [core2.util :as util])
  (:import core2.ICursor
           [org.apache.arrow.vector BigIntVector VectorSchemaRoot]))

(deftype SliceCursor [^VectorSchemaRoot root
                      ^long max-rows-per-block
                      ^:unsynchronized-mutable ^long start-row-id
                      ^:unsynchronized-mutable ^int start-idx
                      ^:unsynchronized-mutable ^VectorSchemaRoot current-slice]
  ICursor
  (tryAdvance [this c]
    (when current-slice
      (.close current-slice)
      (set! (.current-slice this) nil))

    (let [row-count (.getRowCount root)]
      (if-not (< start-idx row-count)
        false
        (let [^BigIntVector row-id-vec (.getVector root t/row-id-field)
              target-row-id (+ start-row-id max-rows-per-block)
              ^long len (loop [len 0]
                          (let [idx (+ start-idx len)]
                            (if (or (>= idx row-count)
                                    (>= (.get row-id-vec idx) target-row-id))
                              len
                              (recur (inc len)))))

              ^VectorSchemaRoot sliced-root (util/slice-root root start-idx len)]

          (set! (.current-slice this) sliced-root)
          (.accept c sliced-root)

          (set! (.start-row-id this) (+ start-row-id max-rows-per-block))
          (set! (.start-idx this) (+ start-idx len))
          true))))

  (close [_]
    (when current-slice
      (.close current-slice))))

(defn ->slices ^core2.ICursor [^VectorSchemaRoot root, ^long start-row-id, ^long max-rows-per-block]
  (SliceCursor. root max-rows-per-block start-row-id 0 nil))

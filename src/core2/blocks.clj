(ns core2.blocks
  (:require [core2.types :as t]
            [core2.util :as util])
  (:import core2.ICursor
           java.util.Iterator
           [org.apache.arrow.vector BigIntVector VectorSchemaRoot]))

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

(defn row-id-aligned-blocks [^VectorSchemaRoot root, ^long start-row-id, ^long max-rows-per-block]
  (let [row-count (.getRowCount root)
        ^BigIntVector row-id-vec (.getVector root t/row-id-field)]
    (letfn [(count-seq [start-row-id start-idx]
              (if-not (< start-idx row-count)
                []
                (let [target-row-id (+ start-row-id max-rows-per-block)
                      ^long len (loop [len 0]
                                  (let [idx (+ start-idx len)]
                                    (if (or (>= idx row-count)
                                            (>= (.get row-id-vec idx) target-row-id))
                                      len
                                      (recur (inc len)))))]

                  (cons len
                        (count-seq (+ start-row-id max-rows-per-block)
                                   (+ start-idx len))))))]
      (count-seq start-row-id 0))))

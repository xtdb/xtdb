(ns core2.operator.table
  (:require [core2.expression :as expr]
            [core2.types :as ty]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw])
  (:import (core2 ICursor)
           (core2.vector IIndirectRelation)
           (java.util LinkedList List)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector.complex DenseUnionVector)))

(set! *unchecked-math* :warn-on-boxed)

(deftype TableCursor [^BufferAllocator allocator
                      ^long row-count
                      cols
                      ^:unsynchronized-mutable done?]
  ICursor
  (tryAdvance [this c]
    (if (or done? (nil? cols))
      false
      (do
        (set! (.done? this) true)

        (let [out-cols (LinkedList.)]
          (try
            (doseq [[k vs] cols]
              (let [out-vec (DenseUnionVector/empty (name k) allocator)
                    out-writer (.asDenseUnion (vw/vec->writer out-vec))]
                (.add out-cols (iv/->direct-vec out-vec))
                (dorun
                 (map-indexed (fn [idx v]
                                (util/set-value-count out-vec idx)

                                (.startValue out-writer)
                                (doto (.writerForType out-writer (ty/value->leg-type v))
                                  (.startValue)
                                  (->> (ty/write-value! v))
                                  (.endValue))
                                (.endValue out-writer))

                              vs))))

            (catch Exception e
              (run! util/try-close out-cols)
              (throw e)))

          (with-open [^IIndirectRelation out-rel (iv/->indirect-rel out-cols row-count)]
            (.accept c out-rel)
            true)))))

  (close [_]))

(defn ->table-cursor ^core2.ICursor [^BufferAllocator allocator, col-names, ^List rows, params]
  (TableCursor. allocator
                (count rows)
                (when (seq rows)
                  (->> (for [col-name col-names
                             :let [col-k (keyword col-name)
                                   col-s (symbol col-name)]]
                         [col-name (vec (for [row rows
                                              :let [v (get row col-k (get row col-s))]]
                                          (expr/eval-scalar-value allocator v #{} params)))])
                       (into {})))
                false))

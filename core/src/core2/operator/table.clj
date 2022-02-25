(ns core2.operator.table
  (:require [core2.error :as err]
            [core2.types :as ty]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw])
  (:import (core2 ICursor)
           (java.util ArrayList LinkedList List Set)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector.complex DenseUnionVector)))

(set! *unchecked-math* :warn-on-boxed)

(deftype TableCursor [^BufferAllocator allocator
                      ^Set col-names
                      ^List rows
                      ^:unsynchronized-mutable done?]
  ICursor
  (getColumnNames [_] col-names)

  (tryAdvance [this c]
    (if (or done? (.isEmpty rows))
      false
      (do
        (set! (.done? this) true)

        (let [out-cols (LinkedList.)]
          (try
            (doseq [k (keys (first rows))]
              (let [out-vec (DenseUnionVector/empty (name k) allocator)
                    out-writer (.asDenseUnion (vw/vec->writer out-vec))]
                (.add out-cols (iv/->direct-vec out-vec))
                (dorun
                 (map-indexed (fn [idx row]
                                (util/set-value-count out-vec idx)

                                (.startValue out-writer)
                                (let [v (get row k)]
                                  (doto (.writerForType out-writer (ty/value->leg-type v))
                                    (.startValue)
                                    (->> (ty/write-value! v))
                                    (.endValue)))
                                (.endValue out-writer))

                              rows))))
            (catch Exception e
              (run! util/try-close out-cols)
              (throw e)))

          (with-open [out-rel (iv/->indirect-rel out-cols)]
            (.accept c out-rel)
            true)))))

  (close [_]))

(defn ->table-cursor ^core2.ICursor [^BufferAllocator allocator, ^List rows, {:keys [explicit-col-names]}]
  (let [col-names (or explicit-col-names
                      (into #{} (map symbol) (keys (first rows))))]
    (when-not (every? #(= col-names (into #{} (map symbol) (keys %))) rows)
      (throw (err/illegal-arg :mismatched-keys-in-table
                              {::err/message "Mismatched keys in table"
                               :expected col-names
                               :key-sets (into #{} (map keys) rows)})))

    (TableCursor. allocator col-names (ArrayList. rows) false)))

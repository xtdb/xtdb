(ns core2.operator.order-by
  (:require [core2.util :as util])
  (:import clojure.lang.Keyword
           core2.ICursor
           java.util.function.Consumer
           java.util.List
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector IntVector VectorSchemaRoot]
           org.apache.arrow.vector.types.pojo.Field))

(deftype OrderSpec [^String col-name, ^Keyword direction])

(definterface IdxComparator
  (^int compareIdx [^int leftIdx, ^int rightIdx]))

(defn ->order-spec [col-name direction]
  (OrderSpec. col-name direction))

(defn order-root [^VectorSchemaRoot root, ^IntVector sorted-idxs, ^List #_<OrderSpec> order-specs]
  )

(defn- accumulate-roots ^org.apache.arrow.vector.VectorSchemaRoot [^ICursor in-cursor, ^BufferAllocator allocator]
  (let [!acc-root (atom nil)]
    (.forEachRemaining in-cursor
                       (reify Consumer
                         (accept [_ in-root]
                           (let [^VectorSchemaRoot in-root in-root
                                 ^VectorSchemaRoot
                                 acc-root (swap! !acc-root (fn [^VectorSchemaRoot acc-root]
                                                             (if acc-root
                                                               (do
                                                                 (assert (= (.getSchema acc-root) (.getSchema in-root)))
                                                                 acc-root)
                                                               (VectorSchemaRoot/create (.getSchema in-root) allocator))))
                                 schema (.getSchema acc-root)
                                 acc-row-count (.getRowCount acc-root)
                                 in-row-count (.getRowCount in-root)]

                             (doseq [^Field field (.getFields schema)]
                               (let [acc-vec (.getVector acc-root field)
                                     in-vec (.getVector in-root field)]
                                 (dotimes [idx in-row-count]
                                   (.copyFromSafe acc-vec idx (+ acc-row-count idx) in-vec))))

                             (util/set-vector-schema-root-row-count acc-root (+ acc-row-count in-row-count))))))
    @!acc-root))

(deftype OrderByCursor [^BufferAllocator allocator
                        ^ICursor in-cursor
                        ^List #_<OrderSpec> order-specs
                        ^:unsynchronized-mutable ^VectorSchemaRoot out-root
                        ^:unsynchronized-mutable ^IntVector sorted-idxs
                        ^:unsynchronized-mutable ^int out-idx
                        ^:unsynchronized-mutable ^VectorSchemaRoot current-slice
                        ^int out-block-size]
  ICursor
  (tryAdvance [this c]
    (when-not sorted-idxs
      (let [out-root (accumulate-roots in-cursor allocator)
            sorted-idxs (doto (IntVector. "sorted-idxs" allocator)
                          (.allocateNew (.getRowCount out-root)))]
        ;; TODO
        #_
        (order-root out-root sorted-idxs order-specs)

        (set! (.out-root this) out-root)
        (set! (.sorted-idxs this) sorted-idxs)))

    (when current-slice
      (.close current-slice)
      (set! (.current-slice this) nil))

    (let [row-count (.getRowCount out-root)]
      (if (< out-idx row-count)
        (let [len (min (- row-count out-idx) out-block-size)
              slice (util/slice-root out-root out-idx len)]
          (set! (.-current-slice this) slice)
          (set! (.-out-idx this) (+ out-idx len))
          (.accept c slice)
          true)
        false)))

  (close [_]
    (some-> current-slice .close)
    (some-> out-root .close)
    (some-> sorted-idxs .close)
    (.close in-cursor)))

(defn ->order-by-cursor
  (^core2.ICursor [^BufferAllocator allocator, ^ICursor in-cursor, ^List #_<OrderSpec> order-specs]
   (->order-by-cursor allocator in-cursor order-specs {}))

  (^core2.ICursor [^BufferAllocator allocator, ^ICursor in-cursor, ^List #_<OrderSpec> order-specs
                   {:keys [out-block-size], :or {out-block-size 10000}}]
   (OrderByCursor. allocator in-cursor order-specs nil nil 0 nil out-block-size)))

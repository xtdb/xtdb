(ns core2.temporal.kd-tree
  (:require [core2.types :as t]
            [core2.util :as util]
            [clojure.tools.logging :as log])
  (:import [java.io Closeable]
           java.nio.file.Path
           java.nio.channels.FileChannel$MapMode
           [java.util ArrayDeque ArrayList Arrays Collection Comparator Date Deque HashMap
            IdentityHashMap LinkedHashMap List Map Map$Entry Spliterator Spliterator$OfInt Spliterator$OfLong Spliterators]
           [java.util.function Consumer Function LongConsumer LongFunction LongPredicate LongUnaryOperator]
           [java.util.stream LongStream StreamSupport]
           [org.apache.arrow.memory ArrowBuf BufferAllocator ReferenceManager RootAllocator]
           [org.apache.arrow.vector BitVectorHelper BigIntVector BufferLayout IntVector TinyIntVector
            VectorLoader VectorSchemaRoot]
           [org.apache.arrow.vector.complex FixedSizeListVector StructVector]
           [org.apache.arrow.vector.types.pojo ArrowType$FixedSizeList Field Schema]
           [org.apache.arrow.vector.types Types$MinorType]
           org.apache.arrow.vector.ipc.ArrowFileWriter
           [org.apache.arrow.vector.ipc.message ArrowBuffer ArrowFooter ArrowRecordBatch]
           org.apache.arrow.compression.CommonsCompressionFactory
           org.roaringbitmap.longlong.Roaring64Bitmap))

;; TODO:

;; Remove the single VSR-based tree? It's currently needed to store
;; deletions which the in-place builder doesn't support. This can be
;; fixed by supporting Node trees directly in write-points-in-place
;; and store the sign in the axis-delete-flag.

(set! *unchecked-math* :warn-on-boxed)

(definterface IKdTreePointAccess
  (^java.util.List getPoint [^long idx])
  (^longs getArrayPoint [^long idx])
  (^long getCoordinate [^long idx ^int axis])
  (^void setCoordinate [^long idx ^int axis ^long value])
  (^void swapPoint [^long from-idx ^long to-idx])
  (^boolean isDeleted [^long idx]))

(defprotocol KdTree
  (kd-tree-insert [_ allocator point])
  (kd-tree-delete [_ allocator point])
  (kd-tree-range-search [_ min-range max-range])
  (kd-tree-depth-first [_])
  (kd-tree-depth [_])
  (kd-tree-retain [_ allocator])
  (kd-tree-point-access [_])
  (kd-tree-size [_])
  (kd-tree-value-count [_])
  (kd-tree-dimensions [_]))

(deftype Node [^FixedSizeListVector point-vec ^int point-idx ^byte axis left right ^boolean deleted?]
  Closeable
  (close [_]
    (util/try-close point-vec)))

(defn next-axis ^long [^long axis ^long k]
  (let [next-axis (inc axis)]
    (if (= k next-axis)
      0
      next-axis)))

(defmacro ^:private in-range-access? [min-range access idx max-range]
  `(let [k# (alength ~min-range)]
     (loop [n# (int 0)]
       (if (= n# k#)
         true
         (let [x# (.getCoordinate ~access ~idx n#)]
           (if (and (<= (aget ~min-range n#) x#)
                    (<= x# (aget ~max-range n#)))
             (recur (inc n#))
             false))))))

(def ^:private ^Class longs-class (Class/forName "[J"))

(defn- ->longs ^longs [xs]
  (if (instance? longs-class xs)
    xs
    (long-array xs)))

(declare ->node-kd-tree)

(deftype NilPointAccess []
  IKdTreePointAccess
  (getPoint [_ _]
    (throw (IndexOutOfBoundsException.)))
  (getArrayPoint [_ _]
    (throw (IndexOutOfBoundsException.)))
  (getCoordinate [_ _ _]
    (throw (IndexOutOfBoundsException.)))
  (setCoordinate [_ _ _ _]
    (throw (UnsupportedOperationException.)))
  (swapPoint [_ _ _]
    (throw (UnsupportedOperationException.)))
  (isDeleted [_ _]
    (throw (IndexOutOfBoundsException.))))

(extend-protocol KdTree
  nil
  (kd-tree-insert [_ allocator point]
    (->node-kd-tree allocator [point]))
  (kd-tree-delete [_ allocator point]
    (-> (kd-tree-insert nil allocator point)
        (kd-tree-delete allocator point)))
  (kd-tree-range-search [_ _ _]
    (LongStream/empty))
  (kd-tree-depth-first [_]
    (LongStream/empty))
  (kd-tree-depth [_] 0)
  (kd-tree-retain [_ _])
  (kd-tree-point-access [_]
    (NilPointAccess.))
  (kd-tree-size [_] 0)
  (kd-tree-value-count [_] 0)
  (kd-tree-dimensions [_] 0))

(defn- write-coordinates [^IKdTreePointAccess access ^long idx point]
  (if (instance? longs-class point)
    (dotimes [n (alength ^longs point)]
      (.setCoordinate access idx n (aget ^longs point n)))
    (dotimes [n (count point)]
      (.setCoordinate access idx n (long (nth point n))))))

(defn- write-point ^long [^FixedSizeListVector point-vec ^IKdTreePointAccess access point]
  (let [idx (.getValueCount point-vec)
        list-idx (.startNewValue point-vec idx)]
    (write-coordinates access idx point)
    (.setValueCount point-vec (inc idx))
    idx))

(defn- ->point-field ^org.apache.arrow.vector.types.pojo.Field [^long k]
  (t/->field "point" (ArrowType$FixedSizeList. k) false
             (t/->field "coordinates" (.getType Types$MinorType/BIGINT) false)))

(defn- three-way-partition ^longs [^IKdTreePointAccess access ^long low ^long hi ^long axis]
  (let [pivot (.getCoordinate access (quot (+ low hi) 2) axis)]
    (loop [i (int low)
           j (int low)
           k (inc (int hi))]
      (if (< j k)
        (let [diff (Long/compare (.getCoordinate access j axis) pivot)]
          (cond
            (neg? diff)
            (do (.swapPoint access i j)
                (recur (inc i) (inc j) k))

            (pos? diff)
            (let [k (dec k)]
              (.swapPoint access j k)
              (recur i j k))

            :else
            (recur i (inc j) k)))
        (doto (long-array 2)
          (aset 0 i)
          (aset 1 (dec k)))))))

(defn- log2-ceil ^long [^long n]
  (if (zero? n)
    0
    (inc (- (dec Long/SIZE) (Long/numberOfLeadingZeros (dec n))))))

(defn- left-balanced-median ^long [^long n]
  (case n
    1 1
    2 2
    3 2
    (let [h (log2-ceil (inc n))
          half (bit-shift-left 1 (- h 2))
          last-row (inc (- n (* 2 half)))]
      (+ half (min half last-row)))))

(defn- quick-select ^long [^IKdTreePointAccess access ^long low ^long hi ^long axis]
  (let [k (+ low (dec (left-balanced-median (- hi low))))]
    (loop [low low
           hi (dec hi)]
      (if (< low hi)
        (let [^longs left-right (three-way-partition access low hi axis)
              left (aget left-right 0)
              right (aget left-right 1)]
          (cond
            (< k left)
            (recur low (dec left))

            (> k right)
            (recur (inc right) hi)

            :else
            k))
        low))))

(deftype KdTreeVectorPointAccess [^FixedSizeListVector point-vec ^TinyIntVector axis-delete-flag-vec]
  IKdTreePointAccess
  (getPoint [_ idx]
    (.getObject point-vec idx))

  (getArrayPoint [_ idx]
    (let [^BigIntVector coordinates-vec (.getDataVector point-vec)
          k (.getListSize point-vec)
          point (long-array k)
          element-start-idx (.getElementStartIndex point-vec idx)]
      (dotimes [n k]
        (aset point n (.get coordinates-vec (+ element-start-idx n))))
      point))

  (getCoordinate [_ idx axis]
    (let [^BigIntVector coordinates-vec (.getDataVector point-vec)
          element-start-idx (.getElementStartIndex point-vec idx)]
      (.get coordinates-vec (+ element-start-idx axis))))

  (setCoordinate [_ idx axis value]
    (let [^BigIntVector coordinates-vec (.getDataVector point-vec)
          element-start-idx (.getElementStartIndex point-vec idx)]
      (.setSafe coordinates-vec (+ element-start-idx axis) value)))

  (swapPoint [_ from-idx to-idx]
    (let [^BigIntVector coordinates-vec (.getDataVector point-vec)
          from-idx (.getElementStartIndex point-vec from-idx)
          to-idx (.getElementStartIndex point-vec to-idx)]
      (dotimes [axis (.getListSize point-vec)]
        (let [from-idx (+ from-idx axis)
              to-idx (+ to-idx axis)
              tmp (.get coordinates-vec from-idx)]
          (.set coordinates-vec from-idx (.get coordinates-vec to-idx))
          (.set coordinates-vec to-idx tmp)))))

  (isDeleted [_ idx]
    (if axis-delete-flag-vec
      (neg? (.get axis-delete-flag-vec idx))
      (throw (UnsupportedOperationException.)))))

(defn ->node-kd-tree ^core2.temporal.kd_tree.Node [^BufferAllocator allocator points]
  (when (not-empty points)
    (let [k (count (first points))
          ^FixedSizeListVector point-vec (.createVector ^Field (->point-field k) allocator)
          access (KdTreeVectorPointAccess. point-vec nil)]
      (doseq [point points]
        (write-point point-vec access point))
      (try
        ((fn step [^long start ^long end ^long axis]
           (let [median (quick-select access start end axis)
                 next-axis (next-axis axis k)]
             (when-not (= start median)
               (.swapPoint access start median))
             (Node. point-vec
                    start
                    axis
                    (when (< start median)
                      (step (inc start) (inc median) next-axis))
                    (when (< (inc median) end)
                      (step (inc median) end next-axis))
                    false)))
         0 (.getValueCount point-vec) 0)
        (catch Throwable t
          (util/try-close point-vec)
          (throw t))))))

(defmacro ^:private point-equals-list-element [point access idx k]
  `(loop [n# 0]
     (if (= n# ~k)
       true
       (if (= (.getCoordinate ~access ~idx n#) (aget ~point n#))
         (recur (inc n#))
         false))))

(defn- maybe-split-stack ^java.util.Deque [^Deque stack]
  (let [split-size (quot (.size stack) 2)]
    (when (pos? split-size)
      (let [split-stack (ArrayDeque.)]
        (while (not= split-size (.size split-stack))
          (.add split-stack (.poll stack)))
        split-stack))))

(deftype NodeRangeSearchSpliterator [^IKdTreePointAccess access
                                     ^longs min-range
                                     ^longs max-range
                                     ^Deque stack]
  Spliterator$OfLong
  (^void forEachRemaining [_ ^LongConsumer c]
    (loop []
      (when-let [^Node node (.poll stack)]
        (loop [node node]
          (let [left (.left node)
                right (.right node)
                axis (.axis node)
                point-idx (.point-idx node)
                point-axis (.getCoordinate access point-idx axis)
                min-axis (aget min-range axis)
                max-axis (aget max-range axis)
                min-match? (<= min-axis point-axis)
                max-match? (<= point-axis max-axis)
                visit-left? (and left min-match?)
                visit-right? (and right max-match?)]

            (when (and (or min-match? max-match?)
                       (not (.deleted? node))
                       (in-range-access? min-range access point-idx max-range))
              (.accept c point-idx))

            (cond
              (and visit-left? (not visit-right?))
              (recur left)

              (and visit-right? (not visit-left?))
              (recur right)

              :else
              (do (when visit-right?
                    (.push stack right))

                  (when visit-left?
                    (recur left))))))
        (recur))))

  (^boolean tryAdvance [_ ^LongConsumer c]
    (loop []
      (if-let [^Node node (.poll stack)]
        (let [axis (.axis node)
              point-idx (.point-idx node)
              point-axis (.getCoordinate access point-idx axis)
              left (.left node)
              right (.right node)
              min-axis (aget min-range axis)
              max-axis (aget max-range axis)
              min-match? (<= min-axis point-axis)
              max-match? (<= point-axis max-axis)
              visit-left? (and left min-match?)
              visit-right? (and right max-match?)]

          (when visit-right?
            (.push stack right))

          (when visit-left?
            (.push stack left))

          (if (and (or min-match? max-match?)
                   (not (.deleted? node))
                   (in-range-access? min-range access point-idx max-range))
            (do (.accept c point-idx)
                true)
            (recur)))
        false)))

  (characteristics [_]
    (bit-or Spliterator/DISTINCT Spliterator/IMMUTABLE Spliterator/NONNULL Spliterator/ORDERED))

  (estimateSize [_]
    Long/MAX_VALUE)

  (trySplit [_]
    (when-let [split-stack (maybe-split-stack stack)]
      (NodeRangeSearchSpliterator. access min-range max-range split-stack))))

(deftype NodeDepthFirstSpliterator [^Deque stack]
  Spliterator$OfLong
  (^void forEachRemaining [_ ^LongConsumer c]
   (loop []
     (when-let [^Node node (.poll stack)]
       (loop [node node]
         (let [left (.left node)
               right (.right node)]

           (when-not (.deleted? node)
             (.accept c (.point-idx node)))

           (cond
             (and left (nil? right))
             (recur left)

             (and right (nil? left))
             (recur right)

             :else
             (do (when right
                   (.push stack right))
                 (when left
                   (recur left))))))
       (recur))))

  (^boolean tryAdvance [_ ^LongConsumer c]
   (loop []
     (if-let [^Node node (.poll stack)]
       (do (when-let [right (.right node)]
             (.push stack right))
           (when-let [left (.left node)]
             (.push stack left))

           (if-not (.deleted? node)
             (do (.accept c (.point-idx node))
                 true)
             (recur)))
       false)))

  (characteristics [_]
    (bit-or Spliterator/DISTINCT Spliterator/IMMUTABLE Spliterator/NONNULL Spliterator/ORDERED))

  (estimateSize [_]
    Long/MAX_VALUE)

  (trySplit [_]
    (when-let [split-stack (maybe-split-stack stack)]
      (NodeDepthFirstSpliterator. split-stack))))

(defn- node-kd-tree-build-path [build-path-fns leaf-node]
  (reduce
   (fn [acc build-fn]
     (build-fn acc))
   leaf-node
   build-path-fns))

(defn- node-kd-tree-edit [^Node kd-tree point deleted?]
  (let [point (->longs point)
        ^FixedSizeListVector point-vec (.point-vec kd-tree)
        ^IKdTreePointAccess access (kd-tree-point-access kd-tree)
        k (kd-tree-dimensions kd-tree)]
    (loop [parent-axis (.axis kd-tree)
           node kd-tree
           build-path-fns ()]
      (if-not node
        (let [point-idx (write-point point-vec access point)]
          (node-kd-tree-build-path build-path-fns (Node. point-vec point-idx (next-axis parent-axis k) nil nil deleted?)))
        (let [axis (.axis node)
              idx (.point-idx node)]
          (cond
            (point-equals-list-element point access idx k)
            (node-kd-tree-build-path build-path-fns (Node. point-vec (.point-idx node) (.axis node) (.left node) (.right node) deleted?))

            (< (aget point axis) (.getCoordinate access idx axis))
            (recur (.axis node)
                   (.left node)
                   (cons (fn [left]
                           (Node. point-vec (.point-idx node) (.axis node) left (.right node) (.deleted? node)))
                         build-path-fns))

            :else
            (recur (.axis node)
                   (.right node)
                   (cons (fn [right]
                           (Node. point-vec (.point-idx node) (.axis node) (.left node) right (.deleted? node)))
                         build-path-fns))))))))

(extend-protocol KdTree
  Node
  (kd-tree-insert [kd-tree allocator point]
    (node-kd-tree-edit kd-tree point false))

  (kd-tree-delete [kd-tree allocator point]
    (node-kd-tree-edit kd-tree point true))

  (kd-tree-range-search [kd-tree min-range max-range]
    (let [min-range (->longs min-range)
          max-range (->longs max-range)
          access (kd-tree-point-access kd-tree)
          stack (doto (ArrayDeque.)
                  (.push kd-tree))]
      (StreamSupport/longStream (NodeRangeSearchSpliterator. access min-range max-range stack) false)))

  (kd-tree-depth-first [kd-tree]
    (let [stack (doto (ArrayDeque.)
                  (.push kd-tree))]
      (StreamSupport/longStream (NodeDepthFirstSpliterator. stack) false)))

  (kd-tree-depth [kd-tree]
    (let [stack (doto (ArrayDeque.)
                  (.push [1 kd-tree]))]
      (loop [[depth ^Node node] (.poll stack)
             max-depth 0]
        (if-not node
          max-depth
          (let [depth (long depth)]
            (when-let [left (.left node)]
              (.push stack [(inc depth) left]))

            (when-let [right (.right node)]
              (.push stack [(inc depth) right]))

            (recur (.poll stack)
                   (max depth max-depth)))))))

  (kd-tree-retain [kd-tree allocator]
    (let [^FixedSizeListVector point-vec (.point-vec kd-tree)]
      (Node. (.getTo (doto (.getTransferPair point-vec allocator)
                       (.splitAndTransfer 0 (.getValueCount point-vec))))
             (.point-idx kd-tree)
             (.axis kd-tree)
             (.left kd-tree)
             (.right kd-tree)
             (.deleted? kd-tree))))

  (kd-tree-point-access [kd-tree]
    (KdTreeVectorPointAccess. (.point-vec kd-tree) nil))

  (kd-tree-size [kd-tree]
    (.count ^LongStream (kd-tree-depth-first kd-tree)))

  (kd-tree-value-count [kd-tree]
    (.getValueCount ^FixedSizeListVector (.point-vec kd-tree)))

  (kd-tree-dimensions [kd-tree]
    (.getListSize ^FixedSizeListVector (.point-vec kd-tree))))

(defn kd-tree->seq
  ([kd-tree]
   (kd-tree->seq kd-tree (kd-tree-depth-first kd-tree)))
  ([kd-tree ^LongStream stream]
   (let [^IKdTreePointAccess point-access (kd-tree-point-access kd-tree)]
     (-> stream
         (.mapToObj (reify LongFunction
                      (apply [_ x]
                        (.getPoint point-access x))))
         (.iterator)
         (iterator-seq)))))

(defn rebuild-node-kd-tree ^core2.temporal.kd_tree.Node [^BufferAllocator allocator kd-tree]
  (let [^IKdTreePointAccess point-access (kd-tree-point-access kd-tree)]
    (->node-kd-tree allocator (-> ^LongStream (kd-tree-depth-first kd-tree)
                                  (.mapToObj (reify LongFunction
                                               (apply [_ x]
                                                 (.getArrayPoint point-access x))))
                                  (.toArray)))))

(def ^:private ^:const axis-delete-flag-vec-idx 0)
(def ^:private ^:const split-value-vec-idx 1)
(def ^:private ^:const skip-pointer-vec-idx 2)
(def ^:private ^:const point-vec-idx 3)

(defn- ->column-kd-tree-schema ^org.apache.arrow.vector.types.pojo.Schema [^long k]
  (Schema. [(t/->field "axis-delete-flag" (.getType Types$MinorType/TINYINT) false)
            (t/->field "split-value" (.getType Types$MinorType/BIGINT) false)
            (t/->field "skip-pointer" (.getType Types$MinorType/BIGINT) false)
            (->point-field k)]))

(defn ->column-kd-tree ^org.apache.arrow.vector.VectorSchemaRoot [^BufferAllocator allocator ^Node kd-tree ^long k]
  (let [out-root (VectorSchemaRoot/create (->column-kd-tree-schema k) allocator)
        ^TinyIntVector axis-delete-flag-vec (.getVector out-root "axis-delete-flag")
        ^BigIntVector split-value-vec (.getVector out-root "split-value")
        ^BigIntVector skip-pointer-vec (.getVector out-root "skip-pointer")
        ^FixedSizeListVector point-vec (.getVector out-root "point")
        ^BigIntVector coordinates-vec (.getDataVector point-vec)

        ^FixedSizeListVector in-point-vec (some-> kd-tree (.point-vec))
        ^BigIntVector in-coordinates-vec (some-> in-point-vec (.getDataVector))

        stack (ArrayDeque.)
        node->skip-idx-update (IdentityHashMap.)]
    (when kd-tree
      (.push stack kd-tree))
    (loop [idx (int 0)]
      (if-let [^Node node (.poll stack)]
        (let [deleted? (.deleted? node)
              in-point-idx (.point-idx node)
              in-element-start-idx (.getElementStartIndex in-point-vec in-point-idx)
              axis (.axis node)
              axis-delete-flag (if deleted?
                                 (- (inc axis))
                                 (inc axis))]
          (.setSafe axis-delete-flag-vec idx axis-delete-flag)

          (.copyFromSafe split-value-vec (+ in-element-start-idx axis) idx in-coordinates-vec)
          (.setSafe skip-pointer-vec idx -1)
          (when-let [skip-idx (.remove node->skip-idx-update node)]
            (.setSafe skip-pointer-vec ^long skip-idx idx))
          (.copyFromSafe point-vec in-point-idx idx in-point-vec)

          (when-let [right (.right node)]
            (.put node->skip-idx-update right idx)
            (.push stack right))
          (when-let [left (.left node)]
            (.push stack left))
          (recur (inc idx)))
        (doto out-root
          (.setRowCount idx))))))

(definterface IColumnKdTreeAccess
  (^byte getAxisDeleteFlag [^long idx])
  (^void setAxisDeleteFlag [^long idx ^byte axis-delete-flag])
  (^long getSplitValue [^long idx])
  (^void setSplitValue [^long idx ^long split-value])
  (^long getSkipPointer [^long idx])
  (^void setSkipPointer [^long idx ^long skip-pointer]))

(deftype ColumnStackEntry [^long start ^long end])

(deftype ColumnRangeSearchSpliterator [^IColumnKdTreeAccess column-access
                                       ^IKdTreePointAccess access
                                       ^longs min-range
                                       ^longs max-range
                                       ^Deque stack]
  Spliterator$OfLong
  (^void forEachRemaining [this ^LongConsumer c]
    (loop []
      (when-let [^ColumnStackEntry stack-entry (.poll stack)]
        (loop [idx (.start stack-entry)
               end-idx (.end stack-entry)]
          (when (< idx end-idx)
            (let [axis-delete-flag (int (.getAxisDeleteFlag column-access idx))
                  deleted? (neg? axis-delete-flag)
                  axis (dec (Math/abs axis-delete-flag))
                  axis-value (.getSplitValue column-access idx)
                  min-match? (<= (aget min-range axis) axis-value)
                  max-match? (<= axis-value (aget max-range axis))
                  left-idx (inc idx)
                  right-idx (.getSkipPointer column-access idx)
                  right-idx (if (neg? right-idx)
                              end-idx
                              right-idx)
                  visit-left? (and (not= left-idx right-idx) min-match?)
                  visit-right? (and (not= right-idx end-idx) max-match?)]

              (when (and (or min-match? max-match?)
                         (not deleted?)
                         (in-range-access? min-range access idx max-range))
                (.accept c idx))

              (cond
                (and visit-left? (not visit-right?))
                (recur left-idx right-idx)

                (and visit-right? (not visit-left?))
                (recur right-idx end-idx)

                :else
                (do (when visit-right?
                      (.push stack (ColumnStackEntry. right-idx end-idx)))
                    (when visit-left?
                      (recur left-idx right-idx)))))))
        (recur))))

  (^boolean tryAdvance [this ^LongConsumer c]
    (loop []
      (if-let [^ColumnStackEntry stack-entry (.poll stack)]
        (let [idx (.start stack-entry)
              end-idx (.end stack-entry)]
          (if (< idx end-idx)
            (let [axis-delete-flag (int (.getAxisDeleteFlag column-access idx))
                  deleted? (neg? axis-delete-flag)
                  axis (dec (Math/abs axis-delete-flag))
                  axis-value (.getSplitValue column-access idx)
                  min-match? (<= (aget min-range axis) axis-value)
                  max-match? (<= axis-value (aget max-range axis))
                  left-idx (inc idx)
                  right-idx (.getSkipPointer column-access idx)
                  right-idx (if (neg? right-idx)
                              end-idx
                              right-idx)
                  visit-left? (and (not= left-idx right-idx) min-match?)
                  visit-right? (and (not= right-idx end-idx) max-match?)]

              (cond
                (and visit-left? (not visit-right?))
                (.push stack (ColumnStackEntry. left-idx right-idx))

                (and visit-right? (not visit-left?))
                (.push stack (ColumnStackEntry. right-idx end-idx))

                :else
                (do (when visit-right?
                      (.push stack (ColumnStackEntry. right-idx end-idx)))
                    (when visit-left?
                      (.push stack (ColumnStackEntry. left-idx right-idx)))))

              (if (and (or min-match? max-match?)
                       (not deleted?)
                       (in-range-access? min-range access idx max-range))
                (do (.accept c idx)
                    true)
                (recur)))
            (recur)))
        false)))

  (characteristics [_]
    (bit-or Spliterator/DISTINCT Spliterator/IMMUTABLE Spliterator/NONNULL Spliterator/ORDERED))

  (estimateSize [_]
    Long/MAX_VALUE)

  (trySplit [_]
    (when-let [split-stack (maybe-split-stack stack)]
      (ColumnRangeSearchSpliterator. column-access access min-range max-range split-stack))))

(defn merge-kd-trees [^BufferAllocator allocator kd-tree-to kd-tree-from]
  (let [n (kd-tree-value-count kd-tree-from)
        ^IKdTreePointAccess from-access (kd-tree-point-access kd-tree-from)]
    (loop [idx 0
           acc kd-tree-to]
      (if (= idx n)
        acc
        (let [point (.getArrayPoint from-access idx)]
          (recur (inc idx)
                 (if (.isDeleted from-access idx)
                   (kd-tree-delete acc allocator point)
                   (kd-tree-insert acc allocator point))))))))

(deftype VectorSchemaRootColumnKdTreeAccess [^TinyIntVector axis-delete-flag-vec
                                             ^BigIntVector split-value-vec
                                             ^BigIntVector skip-pointer-vec]
  IColumnKdTreeAccess
  (getAxisDeleteFlag [_ idx]
    (.get axis-delete-flag-vec idx))

  (setAxisDeleteFlag [_ idx axis-delete-flag]
    (.setSafe axis-delete-flag-vec (int idx) axis-delete-flag))

  (getSplitValue [_ idx]
    (.get split-value-vec idx))

  (setSplitValue [_ idx split-value]
    (.setSafe split-value-vec idx split-value))

  (getSkipPointer [_ idx]
    (.get skip-pointer-vec idx))

  (setSkipPointer [_ idx skip-pointer]
    (.setSafe skip-pointer-vec idx skip-pointer)))

(defn- ->root-column-kd-tree-access ^core2.temporal.kd_tree.IColumnKdTreeAccess [^VectorSchemaRoot root]
  (VectorSchemaRootColumnKdTreeAccess. (.getVector root "axis-delete-flag")
                                       (.getVector root "split-value")
                                       (.getVector root "skip-pointer")))

(defn- column-kd-tree-depth-first [kd-tree ^IColumnKdTreeAccess column-access]
  (.filter (LongStream/range 0 (kd-tree-value-count kd-tree))
           (reify LongPredicate
             (test [_ x]
               (pos? (.getAxisDeleteFlag column-access x))))))

(defn- column-kd-tree-range-search [kd-tree ^IColumnKdTreeAccess column-access min-range max-range]
  (let [min-range (->longs min-range)
        max-range (->longs max-range)
        access (kd-tree-point-access kd-tree)
        stack (doto (ArrayDeque.)
                (.push (ColumnStackEntry. 0 (kd-tree-value-count kd-tree))))]
      (StreamSupport/longStream
       (ColumnRangeSearchSpliterator. column-access access min-range max-range stack)
       false)))

(defn- column-kd-tree-depth [kd-tree ^IColumnKdTreeAccess column-access]
  ((fn step [^long idx ^long end-idx]
     (if (< idx end-idx)
       (let [left-idx (inc idx)
             right-idx (.getSkipPointer column-access idx)
             right-idx (if (neg? right-idx)
                         end-idx
                         right-idx)
             visit-left? (not= left-idx right-idx)
             visit-right? (not= right-idx end-idx)]

         (inc (max (long (if visit-right?
                           (step right-idx end-idx)
                           0))
                   (long (if visit-left?
                           (step left-idx right-idx)
                           0)))))
       0)) 0 (kd-tree-value-count kd-tree)))

(extend-protocol KdTree
  VectorSchemaRoot
  (kd-tree-insert [_ allocator point]
    (throw (UnsupportedOperationException.)))

  (kd-tree-delete [_ allocator point]
    (throw (UnsupportedOperationException.)))

  (kd-tree-range-search [kd-tree min-range max-range]
    (column-kd-tree-range-search kd-tree (->root-column-kd-tree-access kd-tree) min-range max-range))

  (kd-tree-depth-first [kd-tree]
    (column-kd-tree-depth-first kd-tree (->root-column-kd-tree-access kd-tree)))

  (kd-tree-depth [kd-tree]
    (column-kd-tree-depth kd-tree (->root-column-kd-tree-access kd-tree)))

  (kd-tree-retain [this allocator]
    (util/slice-root this 0))

  (kd-tree-point-access [kd-tree]
    (KdTreeVectorPointAccess. (.getVector kd-tree point-vec-idx) (.getVector kd-tree axis-delete-flag-vec-idx)))

  (kd-tree-size [kd-tree]
    (.count ^LongStream (kd-tree-depth-first kd-tree)))

  (kd-tree-value-count [kd-tree]
    (.getRowCount kd-tree))

  (kd-tree-dimensions [kd-tree]
    (.getListSize ^FixedSizeListVector (.getVector kd-tree point-vec-idx))))

(defn- write-points-in-place [^VectorSchemaRoot root ^ArrowFileWriter out points ^long batch-size]
  (let [^long k (kd-tree-dimensions root)
        ^IKdTreePointAccess out-access (kd-tree-point-access root)
        column-access (->root-column-kd-tree-access root)
        ^FixedSizeListVector point-vec (.getVector root "point")]
    (if (satisfies? KdTree points)
      (let [^IKdTreePointAccess point-access (kd-tree-point-access points)]
        (.forEach ^LongStream (kd-tree-depth-first points)
                  (reify LongConsumer
                    (accept [_ point-idx]
                      (let [idx (.getRowCount root)]
                        (.startNewValue point-vec idx)
                        (dotimes [n k]
                          (.setCoordinate out-access idx n (.getCoordinate point-access point-idx n)))
                        (.setAxisDeleteFlag column-access idx 0)
                        (.setSplitValue column-access idx 0)
                        (.setSkipPointer column-access idx -1)
                        (.setRowCount root (inc idx))
                        (when (= (.getRowCount root) batch-size)
                          (.writeBatch out)
                          (.clear root)))))))
      (doseq [point points
              :let [idx (.getRowCount root)]]
        (.startNewValue point-vec idx)
        (write-coordinates out-access idx point)
        (.setAxisDeleteFlag column-access idx 0)
        (.setSplitValue column-access idx 0)
        (.setSkipPointer column-access idx -1)
        (.setRowCount root (inc idx))
        (when (= (.getRowCount root) batch-size)
          (.writeBatch out)
          (.clear root))))
    (when (pos? (.getRowCount root))
      (.writeBatch out)
      (.clear root))))

(defn- build-tree-in-place [^IColumnKdTreeAccess kd-tree]
  (let [^IKdTreePointAccess access (kd-tree-point-access kd-tree)
        k (kd-tree-dimensions kd-tree)]

    ((fn step [^long start ^long end ^long axis]
       (let [median (quick-select access start end axis)
             next-axis (next-axis axis k)
             axis-delete-flag (inc axis)]
         (when-not (= start median)
           (.swapPoint access start median))
         (.setAxisDeleteFlag kd-tree start (unchecked-byte axis-delete-flag))
         (.setSplitValue kd-tree start (.getCoordinate access start axis))
         (when (< start median)
           (step (inc start) (inc median) next-axis))
         (if (< (inc median) end)
           (do (.setSkipPointer kd-tree start (inc median))
               (step (inc median) end next-axis))
           (.setSkipPointer kd-tree start -1))
         false))
     0 (kd-tree-value-count kd-tree) 0)))

(definterface IBlockManager
  (^org.apache.arrow.vector.VectorSchemaRoot getRoot [^int block-idx]))

(deftype ArrowBufKdTreePointAccess [^IBlockManager kd-tree ^int batch-shift ^int batch-mask]
  IKdTreePointAccess
  (getPoint [_ idx]
    (let [block-idx (unsigned-bit-shift-right idx batch-shift)
          idx (bit-and idx batch-mask)
          root (.getRoot kd-tree block-idx)
          ^FixedSizeListVector point-vec (.getVector root point-vec-idx)]
      (.getObject point-vec idx)))

  (getArrayPoint [_ idx]
    (let [block-idx (unsigned-bit-shift-right idx batch-shift)
          idx (bit-and idx batch-mask)
          root (.getRoot kd-tree block-idx)
          ^FixedSizeListVector point-vec (.getVector root point-vec-idx)
          ^BigIntVector coordinates-vec (.getDataVector point-vec)
          k (.getListSize point-vec)
          point (long-array k)
          element-start-idx (.getElementStartIndex point-vec idx)]
      (dotimes [n k]
        (aset point n (.get coordinates-vec (+ element-start-idx n))))
      point))

  (getCoordinate [_ idx axis]
    (let [block-idx (unsigned-bit-shift-right idx batch-shift)
          idx (bit-and idx batch-mask)
          root (.getRoot kd-tree block-idx)
          ^FixedSizeListVector point-vec (.getVector root point-vec-idx)]
      (.get ^BigIntVector (.getDataVector point-vec) (+ (.getElementStartIndex point-vec idx) axis))))

  (setCoordinate [_ idx axis value]
    (let [block-idx (unsigned-bit-shift-right idx batch-shift)
          idx (bit-and idx batch-mask)
          root (.getRoot kd-tree block-idx)
          ^FixedSizeListVector point-vec (.getVector root point-vec-idx)]
      (.set ^BigIntVector (.getDataVector point-vec) (+ (.getElementStartIndex point-vec idx) axis) value)))

  (swapPoint [_ from-idx to-idx]
    (let [from-block-idx (unsigned-bit-shift-right from-idx batch-shift)
          from-idx (bit-and from-idx batch-mask)
          to-block-idx (unsigned-bit-shift-right to-idx batch-shift)
          to-idx (bit-and to-idx batch-mask)
          from-root (.getRoot kd-tree from-block-idx)
          to-root (.getRoot kd-tree to-block-idx)
          ^FixedSizeListVector from-point-vec (.getVector from-root point-vec-idx)
          ^FixedSizeListVector to-point-vec (.getVector to-root point-vec-idx)
          ^BigIntVector from-coordinates-vec (.getDataVector from-point-vec)
          ^BigIntVector to-coordinates-vec (.getDataVector to-point-vec)
          from-idx (.getElementStartIndex from-point-vec from-idx)
          to-idx (.getElementStartIndex to-point-vec to-idx)]
      (dotimes [axis (.getListSize from-point-vec)]
        (let [from-idx (+ from-idx axis)
              to-idx (+ to-idx axis)
              tmp (.get from-coordinates-vec from-idx)]
          (.set from-coordinates-vec from-idx (.get to-coordinates-vec to-idx))
          (.set to-coordinates-vec to-idx tmp)))))

  (isDeleted [_ idx]
    (let [block-idx (unsigned-bit-shift-right idx batch-shift)
          idx (bit-and idx batch-mask)
          root (.getRoot kd-tree block-idx)
          ^TinyIntVector axis-delete-flag-vec (.getVector root axis-delete-flag-vec-idx)]
      (neg? (.get axis-delete-flag-vec idx)))))

(defn- ->block-cache [^long cache-size]
  (proxy [LinkedHashMap] [cache-size 0.75 true]
    (removeEldestEntry [entry]
      (if (> (.size ^Map this) cache-size)
        (do (util/try-close (.getValue ^Map$Entry entry))
            true)
        false))))

(deftype ArrowBufKdTree [^ArrowBuf arrow-buf ^ArrowFooter footer ^int batch-shift ^long batch-mask ^long value-count ^int block-cache-size ^Map block-cache
                         ^:unsynchronized-mutable ^int latest-block-idx
                         ^:unsynchronized-mutable ^VectorSchemaRoot latest-block]
  IColumnKdTreeAccess
  (getAxisDeleteFlag [kd-tree idx]
    (let [block-idx (unsigned-bit-shift-right idx batch-shift)
          idx (bit-and idx batch-mask)
          root (.getRoot kd-tree block-idx)
          ^TinyIntVector axis-delete-flag-vec (.getVector root axis-delete-flag-vec-idx)]
      (.get axis-delete-flag-vec idx)))

  (setAxisDeleteFlag [kd-tree idx axis-delete-flag]
    (let [block-idx (unsigned-bit-shift-right idx batch-shift)
          idx (bit-and idx batch-mask)
          root (.getRoot kd-tree block-idx)
          ^TinyIntVector axis-delete-flag-vec (.getVector root axis-delete-flag-vec-idx)]
      (.set axis-delete-flag-vec (int idx) axis-delete-flag)))

  (getSplitValue [kd-tree idx]
    (let [block-idx (unsigned-bit-shift-right idx batch-shift)
          idx (bit-and idx batch-mask)
          root (.getRoot kd-tree block-idx)
          ^BigIntVector split-value-vec (.getVector root split-value-vec-idx)]
      (.get split-value-vec idx)))

  (setSplitValue [kd-tree idx split-value]
    (let [block-idx (unsigned-bit-shift-right idx batch-shift)
          idx (bit-and idx batch-mask)
          root (.getRoot kd-tree block-idx)
          ^BigIntVector split-value-vec (.getVector root split-value-vec-idx)]
      (.set split-value-vec (int idx) split-value)))

  (getSkipPointer [kd-tree idx]
    (let [block-idx (unsigned-bit-shift-right idx batch-shift)
          idx (bit-and idx batch-mask)
          root (.getRoot kd-tree block-idx)
          ^BigIntVector skip-pointer-vec (.getVector root skip-pointer-vec-idx)]
      (.get skip-pointer-vec idx)))

  (setSkipPointer [kd-tree idx skip-pointer]
    (let [block-idx (unsigned-bit-shift-right idx batch-shift)
          idx (bit-and idx batch-mask)
          root (.getRoot kd-tree block-idx)
          ^BigIntVector skip-pointer-vec (.getVector root skip-pointer-vec-idx)]
      (.set skip-pointer-vec (int idx) skip-pointer)))

  IBlockManager
  (getRoot [this block-idx]
    (if (= block-idx latest-block-idx)
      latest-block
      (let [root (.computeIfAbsent block-cache
                                   block-idx
                                   (reify Function
                                     (apply [_ block-idx]
                                       (with-open [arrow-record-batch (util/->arrow-record-batch-view (.get (.getRecordBatches footer) block-idx) arrow-buf)]
                                         (let [root (VectorSchemaRoot/create (.getSchema footer) (.getAllocator (.getReferenceManager arrow-buf)))]
                                           (.load (VectorLoader. root CommonsCompressionFactory/INSTANCE) arrow-record-batch)
                                           root)))))]
        (set! (.latest-block-idx this) block-idx)
        (set! (.latest-block this) root)
        root)))

  KdTree
  (kd-tree-insert [_ allocator point]
    (throw (UnsupportedOperationException.)))

  (kd-tree-delete [_ allocator point]
    (throw (UnsupportedOperationException.)))

  (kd-tree-range-search [kd-tree min-range max-range]
    (column-kd-tree-range-search kd-tree kd-tree min-range max-range))

  (kd-tree-depth-first [kd-tree]
    (column-kd-tree-depth-first kd-tree kd-tree))

  (kd-tree-depth [kd-tree]
    (column-kd-tree-depth kd-tree kd-tree))

  (kd-tree-retain [this allocator]
    (ArrowBufKdTree. (doto arrow-buf
                       (.retain))
                     footer
                     batch-shift
                     batch-mask
                     value-count
                     block-cache-size
                     (->block-cache block-cache-size)
                     -1
                     nil))

  (kd-tree-point-access [kd-tree]
    (ArrowBufKdTreePointAccess. kd-tree batch-shift batch-mask))

  (kd-tree-size [kd-tree]
    value-count)

  (kd-tree-value-count [kd-tree]
    value-count)

  (kd-tree-dimensions [kd-tree]
    (.getListSize ^ArrowType$FixedSizeList (.getType (.findField (.getSchema footer) "point"))))

  Closeable
  (close [_]
    (util/try-close latest-block)
    (.remove block-cache latest-block-idx)
    (doseq [v (vals block-cache)]
      (util/try-close v))
    (.clear block-cache)
    (util/try-close arrow-buf)))

(def ^:const default-block-cache-size 128)

(defn ->arrow-buf-kd-tree
  (^core2.temporal.kd_tree.ArrowBufKdTree [^ArrowBuf arrow-buf]
   (->arrow-buf-kd-tree arrow-buf {}))
  (^core2.temporal.kd_tree.ArrowBufKdTree [^ArrowBuf arrow-buf {:keys [block-cache-size]
                                                                :or {block-cache-size default-block-cache-size}}]
   (let [footer (util/read-arrow-footer arrow-buf)
         batch-sizes (reduce
                      (fn [acc block]
                        (with-open [arrow-record-batch (util/->arrow-record-batch-view block arrow-buf)
                                    root (VectorSchemaRoot/create (.getSchema footer) (.getAllocator (.getReferenceManager arrow-buf)))]
                          (.load (VectorLoader. root CommonsCompressionFactory/INSTANCE) arrow-record-batch)
                          (conj acc (.getLength arrow-record-batch))))
                      []
                      (.getRecordBatches footer))
         value-count (reduce + batch-sizes)
         block-cache (->block-cache block-cache-size)
         batch-size (long (first batch-sizes))
         batch-size (if (= 1 (Long/bitCount batch-size))
                      batch-size
                      (inc Integer/MAX_VALUE))
         batch-mask (dec batch-size)
         batch-shift (Long/bitCount batch-mask)]
     (ArrowBufKdTree. arrow-buf footer batch-shift batch-mask value-count block-cache-size block-cache -1 nil))))

(defn ->mmap-kd-tree ^core2.temporal.kd_tree.ArrowBufKdTree [^BufferAllocator allocator ^Path path]
  (let [nio-buffer (util/->mmap-path path)
        arrow-buf (util/->arrow-buf-view allocator nio-buffer)]
    (->arrow-buf-kd-tree arrow-buf)))

(def ^:private ^:const default-disk-kd-tree-batch-size 1024)

(defn ->disk-kd-tree ^java.nio.file.Path [^BufferAllocator allocator ^Path path points {:keys [k batch-size compress-blocks?]
                                                                                        :or {compress-blocks? false
                                                                                             batch-size default-disk-kd-tree-batch-size}}]
  (assert (= 1 (Long/bitCount batch-size)))
  (util/mkdirs (.getParent path))
  (with-open [root (VectorSchemaRoot/create (->column-kd-tree-schema k) allocator)
              ch (util/->file-channel path util/write-new-file-opts)
              out (ArrowFileWriter. root nil ch)]
    (write-points-in-place root out points batch-size))
  (let [nio-buffer (util/->mmap-path path FileChannel$MapMode/READ_WRITE)]
    (with-open [kd-tree (->arrow-buf-kd-tree (util/->arrow-buf-view allocator nio-buffer))]
      (build-tree-in-place kd-tree)
      (.force nio-buffer))
    (when compress-blocks?
      (util/compress-arrow-ipc-file-blocks path))
    path))

(deftype MergedKdTreePointAccess [^IKdTreePointAccess static-access ^IKdTreePointAccess dynamic-access ^long static-value-count]
  IKdTreePointAccess
  (getPoint [_ idx]
    (if (< idx static-value-count)
      (.getPoint static-access idx)
      (.getPoint dynamic-access (- idx static-value-count))))

  (getArrayPoint [_ idx]
    (if (< idx static-value-count)
      (.getArrayPoint static-access idx)
      (.getArrayPoint dynamic-access (- idx static-value-count))))

  (getCoordinate [_ idx axis]
    (if (< idx static-value-count)
      (.getCoordinate static-access idx axis)
      (.getCoordinate dynamic-access (- idx static-value-count) axis)))

  (setCoordinate [_ _ _ _]
    (throw (UnsupportedOperationException.)))

  (swapPoint [_ _ _]
    (throw (UnsupportedOperationException.))))

(deftype MergedKdTree [static-kd-tree ^:unsynchronized-mutable dynamic-kd-tree ^Roaring64Bitmap static-delete-bitmap ^long static-size ^long static-value-count]
  KdTree
  (kd-tree-insert [this allocator point]
    (set! (.dynamic-kd-tree this) (kd-tree-insert dynamic-kd-tree allocator point))
    this)

  (kd-tree-delete [this allocator point]
    (let [static-delete? (atom false)]
      (.forEach ^LongStream (kd-tree-range-search static-kd-tree point point)
                (reify LongConsumer
                  (accept [_ x]
                    (reset! static-delete? true)
                    (.addLong static-delete-bitmap x))))
      (when (and (not @static-delete?)
                 (pos? (.count ^LongStream (kd-tree-range-search dynamic-kd-tree point point))))
        (set! (.dynamic-kd-tree this) (kd-tree-delete dynamic-kd-tree allocator point))))
    this)

  (kd-tree-range-search [_ min-range max-range]
    (LongStream/concat (.filter ^LongStream (kd-tree-range-search static-kd-tree min-range max-range)
                                (reify LongPredicate
                                  (test [_ x]
                                    (not (.contains static-delete-bitmap x)))))
                       (.map ^LongStream (kd-tree-range-search dynamic-kd-tree min-range max-range)
                             (reify LongUnaryOperator
                               (applyAsLong [_ x]
                                 (+ static-value-count x))))))

  (kd-tree-depth-first [_]
    (LongStream/concat (.filter ^LongStream (kd-tree-depth-first static-kd-tree)
                                (reify LongPredicate
                                  (test [_ x]
                                    (not (.contains static-delete-bitmap x)))))
                       (.map ^LongStream (kd-tree-depth-first dynamic-kd-tree)
                             (reify LongUnaryOperator
                               (applyAsLong [_ x]
                                 (+ static-value-count x))))))

  (kd-tree-depth [_]
    (max (long (kd-tree-depth static-kd-tree))
         (long (kd-tree-depth dynamic-kd-tree))))

  (kd-tree-retain [kd-tree allocator]
    (MergedKdTree. (kd-tree-retain (.static-kd-tree kd-tree) allocator)
                   (kd-tree-retain (.dynamic-kd-tree kd-tree) allocator)
                   (.clone ^Roaring64Bitmap (.static-delete-bitmap kd-tree))
                   (.static-size kd-tree)
                   (.static-value-count kd-tree)))

  (kd-tree-point-access [kd-tree]
    (MergedKdTreePointAccess. (kd-tree-point-access static-kd-tree) (kd-tree-point-access dynamic-kd-tree) static-value-count))

  (kd-tree-size [kd-tree]
    (+ (- static-size (.getLongCardinality static-delete-bitmap))
       (long (kd-tree-size dynamic-kd-tree))))

  (kd-tree-value-count [kd-tree]
    (+ (long (kd-tree-value-count static-kd-tree))
       (long (kd-tree-value-count dynamic-kd-tree))))

  (kd-tree-dimensions [_]
    (kd-tree-dimensions static-kd-tree))

  Closeable
  (close [_]
    (util/try-close static-kd-tree)
    (util/try-close dynamic-kd-tree)
    (.clear static-delete-bitmap)))

(defn ->merged-kd-tree
  (^core2.temporal.kd_tree.MergedKdTree [static-kd-tree]
   (->merged-kd-tree static-kd-tree nil))
  (^core2.temporal.kd_tree.MergedKdTree [static-kd-tree dynamic-kd-tree]
   (MergedKdTree. static-kd-tree dynamic-kd-tree (Roaring64Bitmap.) (kd-tree-size static-kd-tree) (kd-tree-value-count static-kd-tree))))

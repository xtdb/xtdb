(ns core2.temporal.kd-tree
  (:require [core2.types :as t]
            [core2.util :as util]
            [clojure.tools.logging :as log])
  (:import [core2 BitUtil LongStack LRU]
           [core2.temporal IBlockCache IBlockCache$ClockBlockCache IBlockCache$LatestBlockCache SubtreeSpliterator]
           [java.io Closeable]
           java.nio.file.Path
           java.nio.channels.FileChannel$MapMode
           [clojure.lang IFn$LLO IFn$LL Murmur3]
           [java.util ArrayDeque Deque HashMap
            LinkedHashMap List Map Map$Entry PrimitiveIterator$OfLong
            Spliterator Spliterator$OfLong Spliterators]
           [java.util.function BiPredicate Consumer Function IntFunction LongConsumer LongFunction LongPredicate LongSupplier LongUnaryOperator]
           [java.util.stream LongStream StreamSupport]
           [org.apache.arrow.memory ArrowBuf BufferAllocator ReferenceManager RootAllocator]
           [org.apache.arrow.vector BigIntVector VectorLoader VectorSchemaRoot]
           org.apache.arrow.vector.complex.FixedSizeListVector
           [org.apache.arrow.vector.types.pojo ArrowType$FixedSizeList Field Schema]
           [org.apache.arrow.vector.types Types$MinorType]
           org.apache.arrow.vector.ipc.ArrowFileWriter
           [org.apache.arrow.vector.ipc.message ArrowBuffer ArrowFooter ArrowRecordBatch]
           org.apache.arrow.compression.CommonsCompressionFactory
           org.roaringbitmap.longlong.Roaring64Bitmap))

(set! *unchecked-math* :warn-on-boxed)

(definterface IKdTreePointAccess
  (^java.util.List getPoint [^long idx])
  (^longs getArrayPoint [^long idx])
  (^long getCoordinate [^long idx ^int axis])
  (^void setCoordinate [^long idx ^int axis ^long value])
  (^void swapPoint [^long from-idx ^long to-idx])
  (^boolean isDeleted [^long idx])
  (^boolean isInRange [^long idx ^longs min-range ^longs max-range ^int mask]))

(defprotocol KdTree
  (kd-tree-insert [_ allocator point])
  (kd-tree-delete [_ allocator point])
  (kd-tree-range-search [_ min-range max-range])
  (kd-tree-points [_])
  (kd-tree-height [_])
  (kd-tree-retain [_ allocator])
  (kd-tree-point-access [_])
  (kd-tree-size [_])
  (kd-tree-value-count [_])
  (kd-tree-dimensions [_]))

(deftype Node [^FixedSizeListVector point-vec ^int point-idx ^int axis left right]
  Closeable
  (close [_]
    (util/try-close point-vec)))

(defn next-axis
  {:inline (fn [axis k]
             `(let [next-axis# (inc ~axis)]
                (if (= ~k next-axis#)
                  0
                  next-axis#)))}
  ^long [^long axis ^long k]
  (let [next-axis (inc axis)]
    (if (= k next-axis)
      0
      next-axis)))

(def ^:private ^Class longs-class (Class/forName "[J"))

(defn ->longs ^longs [xs]
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
    (throw (IndexOutOfBoundsException.)))
  (isInRange [_ _ _ _ _]
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
  (kd-tree-points [_]
    (LongStream/empty))
  (kd-tree-height [_] -1)
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

(defn write-point ^long [^FixedSizeListVector point-vec ^IKdTreePointAccess access point]
  (let [idx (.getValueCount point-vec)
        list-idx (.startNewValue point-vec idx)]
    (write-coordinates access idx point)
    (.setValueCount point-vec (inc idx))
    idx))

(defn ->point-field ^org.apache.arrow.vector.types.pojo.Field [^long k]
  (t/->field "point" (ArrowType$FixedSizeList. k) false
             (t/->field "coordinates" (.getType Types$MinorType/BIGINT) false)))

(defn- balanced-parent
  {:inline (fn [idx]
             `(unsigned-bit-shift-right ~idx 1))}
  ^long [^long idx]
  (unsigned-bit-shift-right idx 1))

(defn- balanced-left-child
  {:inline (fn [idx]
             `(inc (bit-shift-left ~idx 1)))}
  ^long [^long idx]
  (inc (bit-shift-left idx 1)))

(defn- balanced-right-child
  {:inline (fn [idx]
             `(+ (bit-shift-left ~idx 1) 2))}
  ^long [^long idx]
  (+ (bit-shift-left idx 1) 2))

(defn- balanced-root? [^long idx]
  (zero? idx))

(defn- balanced-leaf? [^long n ^long idx]
  (>= idx (unsigned-bit-shift-right n 1)))

(defn- balanced-inner? [^long n ^long idx]
  (< idx (unsigned-bit-shift-right n 1)))

(defn- balanced-valid?
  {:inline (fn [n idx]
             `(< ~idx ~n))}
  [^long n ^long idx]
  (< idx n))

(defn- balanced-left-child?
  {:inline (fn [n idx]
             `(balanced-valid? ~n (balanced-left-child ~idx)))}
  [^long n ^long idx]
  (balanced-valid? n (balanced-left-child idx)))

(defn- balanced-right-child?
  {:inline (fn [n idx]
             `(balanced-valid? ~n (balanced-right-child ~idx)))}
  [^long n ^long idx]
  (balanced-valid? n (balanced-right-child idx)))

;; height of zero based index, height of root with index 0 is 0.
(defn- balanced-height
  {:inline (fn [idx]
             `(BitUtil/log2 ~idx))}
  ^long [^long idx]
  (BitUtil/log2 idx))

(defn ->subtree-spliterator ^core2.temporal.SubtreeSpliterator [^long n ^long root]
  (SubtreeSpliterator. 0 1 root n))

;; Breadth first kd-tree in-place build based on:

;; "CPU Ray Tracing Large Particle Data using Particle K-D Trees"
;; http://www.sci.utah.edu/publications/Wal2015a/ospParticle.pdf
;; https://github.com/ingowald/ospray-module-pkd

(defn build-breadth-first-tree-in-place
  ([kd-tree] (build-breadth-first-tree-in-place kd-tree false))
  ([kd-tree check?]
   (let [^IKdTreePointAccess access (kd-tree-point-access kd-tree)
         ^long k (kd-tree-dimensions kd-tree)
         ^long n (kd-tree-value-count kd-tree)
         nop (reify LongConsumer
               (accept [_ x]))]

     ((fn step [^long node-idx ^long axis]
        (when (balanced-left-child? n node-idx)
          (if-not (balanced-right-child? n node-idx)
            (let [left-child-idx (balanced-left-child node-idx)]
              (when (> (.getCoordinate access left-child-idx axis)
                       (.getCoordinate access node-idx axis))
                (.swapPoint access node-idx left-child-idx)))
            (loop [^SubtreeSpliterator l (->subtree-spliterator n (balanced-left-child node-idx))
                   ^SubtreeSpliterator r (->subtree-spliterator n (balanced-right-child node-idx))
                   ^SubtreeSpliterator l0 (.clone l)
                   ^SubtreeSpliterator r0 (.clone r)
                   root-pos (.getCoordinate access node-idx axis)]

              (while (and (balanced-valid? n (.getAsLong l))
                          (<= (.getCoordinate access (.getAsLong l) axis) root-pos)
                          (.tryAdvance l nop)))
              (while (and (balanced-valid? n (.getAsLong r))
                          (>= (.getCoordinate access (.getAsLong r) axis) root-pos)
                          (.tryAdvance r nop)))

              (cond
                (and (balanced-valid? n (.getAsLong l))
                     (balanced-valid? n (.getAsLong r)))
                (do (.swapPoint access (.getAsLong l) (.getAsLong r))
                    (.tryAdvance l nop)
                    (.tryAdvance r nop)
                    (recur l
                           r
                           l0
                           r0
                           root-pos))

                (balanced-valid? n (.getAsLong l))
                (let [^SubtreeSpliterator l0 (.clone l)]
                  (while (.tryAdvance l nop)
                    (when (<= (.getCoordinate access (.getAsLong l) axis) root-pos)
                      (.swapPoint access (.getAsLong l) (.getAsLong l0))
                      (.tryAdvance l0 nop)))
                  (.swapPoint access node-idx (.getAsLong l0))
                  (.tryAdvance l0 nop)
                  (recur (.clone l0)
                         (.clone r0)
                         l0
                         r0
                         (.getCoordinate access node-idx axis)))

                (balanced-valid? n (.getAsLong r))
                (let [^SubtreeSpliterator r0 (.clone r)]
                  (while (.tryAdvance r nop)
                    (when (>= (.getCoordinate access (.getAsLong r) axis) root-pos)
                      (.swapPoint access (.getAsLong r) (.getAsLong r0))
                      (.tryAdvance r0 nop)))
                  (.swapPoint access node-idx (.getAsLong r0))
                  (.tryAdvance r0 nop)
                  (recur (.clone l0)
                         (.clone r0)
                         l0
                         r0
                         (.getCoordinate access node-idx axis))))))

          (when check?
            (let [l (Spliterators/iterator ^Spliterator$OfLong (->subtree-spliterator n (balanced-left-child node-idx)))
                  r (Spliterators/iterator ^Spliterator$OfLong (->subtree-spliterator n (balanced-right-child node-idx)))
                  root-pos (.getCoordinate access node-idx axis)]
              (while (.hasNext l)
                (let [l-pos (.getCoordinate access (.nextLong l) axis)]
                  (assert (<= l-pos root-pos) (pr-str '<= l-pos root-pos))))
              (while (.hasNext r)
                (let [r-pos (.getCoordinate access (.nextLong r) axis)]
                  (assert (>= r-pos root-pos) (pr-str '>= r-pos root-pos))))))

          (let [next-axis (next-axis axis k)]
            (.invokePrim ^IFn$LLO step (balanced-left-child node-idx) next-axis)
            (.invokePrim ^IFn$LLO step (balanced-right-child node-idx) next-axis))))
      0 0))))

(deftype KdTreeVectorPointAccess [^FixedSizeListVector point-vec ^int k]
  IKdTreePointAccess
  (getPoint [_ idx]
    (.getObject point-vec idx))

  (getArrayPoint [_ idx]
    (let [^BigIntVector coordinates-vec (.getDataVector point-vec)
          point (long-array k)
          element-start-idx (unchecked-multiply-int idx k)]
      (dotimes [n k]
        (aset point n (.get coordinates-vec (unchecked-add-int element-start-idx n))))
      point))

  (getCoordinate [_ idx axis]
    (let [^BigIntVector coordinates-vec (.getDataVector point-vec)
          element-start-idx (unchecked-multiply-int idx k)]
      (.get coordinates-vec (unchecked-add-int element-start-idx axis))))

  (setCoordinate [_ idx axis value]
    (let [^BigIntVector coordinates-vec (.getDataVector point-vec)
          element-start-idx (unchecked-multiply-int idx k)]
      (.setSafe coordinates-vec (unchecked-add-int element-start-idx axis) value)))

  (swapPoint [_ from-idx to-idx]
    (let [^BigIntVector coordinates-vec (.getDataVector point-vec)
          tmp (.isNull point-vec to-idx)
          _ (if (.isNull point-vec from-idx)
              (.setNull point-vec to-idx)
              (.setNotNull point-vec to-idx))
          _ (if tmp
              (.setNull point-vec from-idx)
              (.setNotNull point-vec from-idx))
          from-idx (unchecked-multiply-int from-idx k)
          to-idx (unchecked-multiply-int to-idx k)]

      (dotimes [axis k]
        (let [from-idx (unchecked-add-int from-idx axis)
              to-idx (unchecked-add-int to-idx axis)
              tmp (.get coordinates-vec from-idx)]
          (.set coordinates-vec from-idx (.get coordinates-vec to-idx))
          (.set coordinates-vec to-idx tmp)))))

  (isDeleted [_ idx]
    (.isNull point-vec idx))

  (isInRange [_ idx min-range max-range mask]
    (let [^BigIntVector coordinates-vec (.getDataVector point-vec)
          element-start-idx (unchecked-multiply-int idx k)]
      (loop [n (int 0)]
        (if (= n k)
          true
          (if (BitUtil/isBitSet mask n)
            (let [x (.get coordinates-vec (unchecked-add-int element-start-idx n))]
              (if (and (<= (aget min-range n) x)
                       (<= x (aget max-range n)))
                (recur (inc n))
                false))
            (recur (inc n))))))))

(defn- reconstruct-node-kd-tree-from-breadth-first-points [^FixedSizeListVector point-vec]
  (let [k (.getListSize point-vec)
        n (.getValueCount point-vec)]
    ((fn step [^long idx ^long axis]
       (let [left-idx (balanced-left-child idx)
             right-idx (balanced-right-child idx)]
         (Node. point-vec
                idx
                axis
                (when (balanced-valid? n left-idx)
                  (.invokePrim ^IFn$LLO step left-idx (next-axis axis k)))
                (when (balanced-valid? n right-idx)
                  (.invokePrim ^IFn$LLO step right-idx (next-axis axis k))))))
     0 0)))

(defn ->node-kd-tree ^core2.temporal.kd_tree.Node [^BufferAllocator allocator points]
  (when (not-empty points)
    (let [k (count (first points))
          ^FixedSizeListVector point-vec (.createVector ^Field (->point-field k) allocator)
          access (KdTreeVectorPointAccess. point-vec k)]
      (doseq [point points]
        (write-point point-vec access point))
      (try
        (let [root (VectorSchemaRoot/of (into-array [point-vec]))]
          (build-breadth-first-tree-in-place root)
          (reconstruct-node-kd-tree-from-breadth-first-points point-vec))
        (catch Throwable t
          (util/try-close point-vec)
          (throw t))))))

(defn- maybe-split-stack ^java.util.Deque [^Deque stack]
  (let [split-size (quot (.size stack) 2)]
    (when (pos? split-size)
      (let [split-stack (ArrayDeque.)]
        (while (not= split-size (.size split-stack))
          (.add split-stack (.poll stack)))
        split-stack))))

(defn- maybe-split-long-stack ^core2.LongStack [^LongStack stack]
  (let [split-size (quot (.size stack) 2)]
    (when (pos? split-size)
      (let [split-array (long-array split-size)
            split-stack (LongStack.)]
        (dotimes [n split-size]
          (aset split-array n (.poll stack)))
        (dotimes [n split-size]
          (.push split-stack (aget split-array (- split-size (inc n)))))
        split-stack))))

(defn range-bitmask ^long [^longs min-range ^longs max-range]
  (let [len (alength min-range)]
    (loop [n 0
           mask 0]
      (if (= n len)
        mask
        (recur (inc n)
               (if (and (= Long/MIN_VALUE (aget min-range n))
                        (= Long/MAX_VALUE (aget max-range n)))
                 mask
                 (bit-or mask (bit-shift-left 1 n))))))))

(deftype NodeRangeSearchSpliterator [^IKdTreePointAccess access
                                     ^longs min-range
                                     ^longs max-range
                                     ^int axis-mask
                                     ^Deque stack]
  Spliterator$OfLong
  (^void forEachRemaining [_ ^LongConsumer c]
   (let [access access
         min-range min-range
         max-range max-range
         stack stack
         axis-mask axis-mask]
     (loop []
       (when-let [^Node node (.poll stack)]
         (loop [node node]
           (let [left (.left node)
                 right (.right node)
                 axis (.axis node)
                 point-idx (.point-idx node)
                 partial-match-axis? (BitUtil/bitNot (BitUtil/isBitSet axis-mask axis))
                 axis-value (if partial-match-axis?
                              0
                              (.getCoordinate access point-idx axis))
                 min-match? (or partial-match-axis? (<= (aget min-range axis) axis-value))
                 max-match? (or partial-match-axis? (<= axis-value (aget max-range axis)))
                 visit-left? (and (boolean left) min-match?)
                 visit-right? (and (boolean right) max-match?)]

             (when (and min-match?
                        max-match?
                        (.isInRange access point-idx min-range max-range axis-mask)
                        (BitUtil/bitNot (.isDeleted access point-idx)))
               (.accept c point-idx))

             (cond
               (and visit-left? (BitUtil/bitNot visit-right?))
               (recur left)

               (and visit-right? (BitUtil/bitNot visit-left?))
               (recur right)

               :else
               (do (when visit-right?
                     (.push stack right))

                   (when visit-left?
                     (recur left))))))
         (recur)))))

  (^boolean tryAdvance [_ ^LongConsumer c]
   (loop []
     (if-let [^Node node (.poll stack)]
       (let [axis (.axis node)
             point-idx (.point-idx node)
             left (.left node)
             right (.right node)
             partial-match-axis? (BitUtil/bitNot (BitUtil/isBitSet axis-mask axis))
             axis-value (if partial-match-axis?
                          0
                          (.getCoordinate access point-idx axis))
             min-match? (or partial-match-axis? (<= (aget min-range axis) axis-value))
             max-match? (or partial-match-axis? (<= axis-value (aget max-range axis)))
             visit-left? (and (boolean left) min-match?)
             visit-right? (and (boolean right) max-match?)]

         (when visit-right?
           (.push stack right))

         (when visit-left?
           (.push stack left))

         (if (and min-match?
                  max-match?
                  (.isInRange access point-idx min-range max-range axis-mask)
                  (BitUtil/bitNot (.isDeleted access point-idx)))
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
      (NodeRangeSearchSpliterator. access min-range max-range axis-mask split-stack))))

(deftype NodeDepthFirstSpliterator [^IKdTreePointAccess access ^Deque stack]
  Spliterator$OfLong
  (^void forEachRemaining [_ ^LongConsumer c]
   (loop []
     (when-let [^Node node (.poll stack)]
       (loop [node node]
         (let [left (.left node)
               right (.right node)]

           (when-not (.isDeleted access (.point-idx node))
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

           (if-not (.isDeleted access (.point-idx node))
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
      (NodeDepthFirstSpliterator. access split-stack))))

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
        ^long k (kd-tree-dimensions kd-tree)
        build-path-fns (ArrayDeque.)]
    (loop [parent-axis (.axis kd-tree)
           node kd-tree]
      (if-not node
        (let [point-idx (write-point point-vec access point)]
          (when deleted?
            (.setNull point-vec point-idx))
          (node-kd-tree-build-path build-path-fns (Node. point-vec point-idx (next-axis parent-axis k) nil nil)))
        (let [axis (.axis node)
              idx (.point-idx node)]
          (cond
            (.isInRange access idx point point -1)
            (if (= (.isDeleted access idx) deleted?)
              (node-kd-tree-build-path build-path-fns (Node. point-vec (.point-idx node) (.axis node) (.left node) (.right node)))
              (let [point-idx (write-point point-vec access point)]
                (when deleted?
                  (.setNull point-vec point-idx))
                (node-kd-tree-build-path build-path-fns (Node. point-vec point-idx (next-axis parent-axis k) (.left node) (.right node)))))

            (< (aget point axis) (.getCoordinate access idx axis))
            (do (.push build-path-fns
                       (fn [left]
                         (Node. point-vec (.point-idx node) (.axis node) left (.right node))))
                (recur (.axis node)
                       (.left node)))

            :else
            (do (.push build-path-fns
                       (fn [right]
                         (Node. point-vec (.point-idx node) (.axis node) (.left node) right)))
                (recur (.axis node)
                       (.right node)))))))))

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
      (StreamSupport/longStream (NodeRangeSearchSpliterator. access min-range max-range (range-bitmask min-range max-range) stack) false)))

  (kd-tree-points [kd-tree]
    (let [stack (doto (ArrayDeque.)
                  (.push kd-tree))]
      (StreamSupport/longStream (NodeDepthFirstSpliterator. (kd-tree-point-access kd-tree) stack) false)))

  (kd-tree-height [kd-tree]
    (let [stack (doto (ArrayDeque.)
                  (.push [0 kd-tree]))]
      (loop [[height ^Node node] (.poll stack)
             max-height 0]
        (if-not node
          max-height
          (let [height (long height)]
            (when-let [left (.left node)]
              (.push stack [(inc height) left]))

            (when-let [right (.right node)]
              (.push stack [(inc height) right]))

            (recur (.poll stack)
                   (max height max-height)))))))

  (kd-tree-retain [kd-tree allocator]
    (let [^FixedSizeListVector point-vec (.point-vec kd-tree)]
      (Node. (.getTo (doto (.getTransferPair point-vec allocator)
                       (.splitAndTransfer 0 (.getValueCount point-vec))))
             (.point-idx kd-tree)
             (.axis kd-tree)
             (.left kd-tree)
             (.right kd-tree))))

  (kd-tree-point-access [kd-tree]
    (KdTreeVectorPointAccess. (.point-vec kd-tree) (kd-tree-dimensions kd-tree)))

  (kd-tree-size [kd-tree]
    (.count ^LongStream (kd-tree-points kd-tree)))

  (kd-tree-value-count [kd-tree]
    (.getValueCount ^FixedSizeListVector (.point-vec kd-tree)))

  (kd-tree-dimensions [kd-tree]
    (.getListSize ^FixedSizeListVector (.point-vec kd-tree))))

(defn kd-tree->seq
  ([kd-tree]
   (kd-tree->seq kd-tree (kd-tree-points kd-tree)))
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
    (->node-kd-tree allocator (-> ^LongStream (kd-tree-points kd-tree)
                                  (.mapToObj (reify LongFunction
                                               (apply [_ x]
                                                 (.getArrayPoint point-access x))))
                                  (.toArray)))))

(def ^:private ^:const point-vec-idx 0)

(defn- ->column-kd-tree-schema ^org.apache.arrow.vector.types.pojo.Schema [^long k]
  (Schema. [(->point-field k)]))

(defn ->column-kd-tree ^org.apache.arrow.vector.VectorSchemaRoot [^BufferAllocator allocator ^Node kd-tree ^long k]
  (let [out-root (VectorSchemaRoot/create (->column-kd-tree-schema k) allocator)
        ^FixedSizeListVector point-vec (.getVector out-root point-vec-idx)
        ^BigIntVector coordinates-vec (.getDataVector point-vec)

        ^FixedSizeListVector in-point-vec (some-> kd-tree (.point-vec))
        ^BigIntVector in-coordinates-vec (some-> in-point-vec (.getDataVector))

        ^IKdTreePointAccess access (kd-tree-point-access kd-tree)
        stack (ArrayDeque.)]
    (when kd-tree
      (.push stack kd-tree))
    (loop [idx (int 0)]
      (if-let [^Node node (.poll stack)]
        (let [in-point-idx (.point-idx node)
              in-element-start-idx (.getElementStartIndex in-point-vec in-point-idx)]

          (.copyFromSafe point-vec in-point-idx idx in-point-vec)

          (when-let [right (.right node)]
            (.push stack right))
          (when-let [left (.left node)]
            (.push stack left))
          (recur (inc idx)))
        (doto out-root
          (.setRowCount idx)
          (build-breadth-first-tree-in-place true))))))

(defn ->arrow-buf-column-kd-tree ^org.apache.arrow.vector.VectorSchemaRoot [^ArrowBuf arrow-buf]
  (let [footer (util/read-arrow-footer arrow-buf)]
    (with-open [arrow-record-batch (util/->arrow-record-batch-view (first (.getRecordBatches footer)) arrow-buf)]
      (let [root (VectorSchemaRoot/create (.getSchema footer) (.getAllocator (.getReferenceManager arrow-buf)))]
        (.load (VectorLoader. root CommonsCompressionFactory/INSTANCE) arrow-record-batch)
        root))))

(def ^:private ^:const breadth-first-level-upper-limit 22)
(def ^:private ^:const breadth-first-scan-levels 2)

(deftype ColumnRangeSearchSpliterator [^IKdTreePointAccess access
                                       ^longs min-range
                                       ^longs max-range
                                       ^long k
                                       ^long n
                                       ^long height
                                       ^int axis-mask
                                       ^LongStack stack]
  Spliterator$OfLong
  (^void forEachRemaining [this ^LongConsumer c]
   (let [access access
         min-range min-range
         max-range max-range
         k k
         n n
         stack stack
         axis-mask axis-mask
         scan-level (max 0 (- height breadth-first-scan-levels))
         max-breadth-first-levels (min scan-level breadth-first-level-upper-limit)]
     (while (BitUtil/bitNot (.isEmpty stack))
       (let [idx (.poll stack)
             level (balanced-height idx)]
         (loop [level level
                visited-levels 0
                axis (BitUtil/rem level k)
                next-level-entries (long-array 1 idx)]
           (cond
             (>= level scan-level)
             (dotimes [x (alength next-level-entries)]
               (let [idx (aget next-level-entries x)]
                 (.forEachRemaining ^Spliterator$OfLong (->subtree-spliterator n idx)
                                    (reify LongConsumer
                                      (accept [_ idx]
                                        (when (and (.isInRange access idx min-range max-range axis-mask)
                                                   (BitUtil/bitNot (.isDeleted access idx)))
                                          (.accept c idx)))))))

             (< visited-levels max-breadth-first-levels)
             (let [new-next-level-entries (LongStream/builder)]
               (dotimes [x (alength next-level-entries)]
                 (let [idx (aget next-level-entries x)
                       partial-match-axis? (BitUtil/bitNot (BitUtil/isBitSet axis-mask axis))
                       axis-value (if partial-match-axis?
                                    0
                                    (.getCoordinate access idx axis))
                       min-match? (or partial-match-axis? (<= (aget min-range axis) axis-value))
                       max-match? (or partial-match-axis? (<= axis-value (aget max-range axis)))
                       left-idx (balanced-left-child idx)
                       right-idx (inc left-idx)
                       visit-left? (and min-match? (balanced-valid? n left-idx))
                       visit-right? (and max-match? (balanced-valid? n right-idx))]

                   (when (and min-match?
                              max-match?
                              (.isInRange access idx min-range max-range axis-mask)
                              (BitUtil/bitNot (.isDeleted access idx)))
                     (.accept c idx))

                   (when visit-left?
                     (.add new-next-level-entries left-idx))

                   (when visit-right?
                     (.add new-next-level-entries right-idx))))
               (let [new-next-level-entries (.toArray (.build new-next-level-entries))]
                 (when (pos? (alength new-next-level-entries))
                   (recur (inc level) (inc visited-levels) (next-axis axis k) new-next-level-entries))))

             :else
             (let [len (alength next-level-entries)]
               (dotimes [n len]
                 (.push stack (aget next-level-entries (dec (- len n))))))))))))

  (^boolean tryAdvance [this ^LongConsumer c]
   (loop []
     (if-not (.isEmpty stack)
       (let [idx (.poll stack)
             axis (BitUtil/rem (balanced-height idx) k)
             partial-match-axis? (BitUtil/bitNot (BitUtil/isBitSet axis-mask axis))
             axis-value (if partial-match-axis?
                          0
                          (.getCoordinate access idx axis))
             min-match? (or partial-match-axis? (<= (aget min-range axis) axis-value))
             max-match? (or partial-match-axis? (<= axis-value (aget max-range axis)))
             left-idx (balanced-left-child idx)
             right-idx (inc left-idx)
             visit-left? (and min-match? (balanced-valid? n left-idx))
             visit-right? (and max-match? (balanced-valid? n right-idx))]
         (cond
           (and visit-left? (BitUtil/bitNot visit-right?))
           (.push stack left-idx)

           (and visit-right? (BitUtil/bitNot visit-left?))
           (.push stack right-idx)

           :else
           (do (when visit-right?
                 (.push stack right-idx))
               (when visit-left?
                 (.push stack left-idx))))

         (if (and min-match?
                  max-match?
                  (.isInRange access idx min-range max-range axis-mask)
                  (BitUtil/bitNot (.isDeleted access idx)))
           (do (.accept c idx)
               true)
           (recur)))
       false)))

  (characteristics [_]
    (bit-or Spliterator/DISTINCT Spliterator/IMMUTABLE Spliterator/NONNULL Spliterator/ORDERED))

  (estimateSize [_]
    Long/MAX_VALUE)

  (trySplit [_]
    (when-let [split-stack (maybe-split-long-stack stack)]
      (ColumnRangeSearchSpliterator. access min-range max-range k n height axis-mask split-stack))))

(defn merge-kd-trees [^BufferAllocator allocator kd-tree-to kd-tree-from]
  (let [^long n (kd-tree-value-count kd-tree-from)
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

(defn- column-kd-tree-points [kd-tree]
  (let [^IKdTreePointAccess access (kd-tree-point-access kd-tree)]
    (.filter (LongStream/range 0 (long (kd-tree-value-count kd-tree)))
             (reify LongPredicate
               (test [_ x]
                 (BitUtil/bitNot (.isDeleted access x)))))))

(defn- column-kd-tree-range-search [kd-tree min-range max-range]
  (let [min-range (->longs min-range)
        max-range (->longs max-range)
        access (kd-tree-point-access kd-tree)
        ^long k (kd-tree-dimensions kd-tree)
        ^long n (kd-tree-value-count kd-tree)
        ^long height (kd-tree-height kd-tree)
        stack (LongStack.)]
    (when (pos? n)
      (.push stack 0))
    (StreamSupport/longStream
     (ColumnRangeSearchSpliterator. access min-range max-range k n height (range-bitmask min-range max-range) stack)
     false)))

(defn- column-kd-tree-height [kd-tree]
  (balanced-height (dec (long (kd-tree-value-count kd-tree)))))

(extend-protocol KdTree
  VectorSchemaRoot
  (kd-tree-insert [_ allocator point]
    (throw (UnsupportedOperationException.)))

  (kd-tree-delete [_ allocator point]
    (throw (UnsupportedOperationException.)))

  (kd-tree-range-search [kd-tree min-range max-range]
    (column-kd-tree-range-search kd-tree min-range max-range))

  (kd-tree-points [kd-tree]
    (column-kd-tree-points kd-tree))

  (kd-tree-height [kd-tree]
    (column-kd-tree-height kd-tree))

  (kd-tree-retain [this allocator]
    (util/slice-root this 0))

  (kd-tree-point-access [kd-tree]
    (KdTreeVectorPointAccess. (.getVector kd-tree point-vec-idx) (kd-tree-dimensions kd-tree)))

  (kd-tree-size [kd-tree]
    (.count ^LongStream (kd-tree-points kd-tree)))

  (kd-tree-value-count [kd-tree]
    (.getRowCount kd-tree))

  (kd-tree-dimensions [kd-tree]
    (.getListSize ^FixedSizeListVector (.getVector kd-tree point-vec-idx))))

(defn- write-points-in-place [^VectorSchemaRoot root ^ArrowFileWriter out points ^long batch-size]
  (let [^long k (kd-tree-dimensions root)
        ^IKdTreePointAccess out-access (kd-tree-point-access root)
        ^FixedSizeListVector point-vec (.getVector root point-vec-idx)]
    (if (satisfies? KdTree points)
      (let [^IKdTreePointAccess point-access (kd-tree-point-access points)]
        (.forEach ^LongStream (kd-tree-points points)
                  (reify LongConsumer
                    (accept [_ point-idx]
                      (let [idx (.getRowCount root)]
                        (.startNewValue point-vec idx)
                        (dotimes [n k]
                          (.setCoordinate out-access idx n (.getCoordinate point-access point-idx n)))
                        (.setRowCount root (inc idx))
                        (when (= (.getRowCount root) batch-size)
                          (.writeBatch out)
                          (.clear root)))))))
      (doseq [point points
              :let [idx (.getRowCount root)]]
        (.startNewValue point-vec idx)
        (write-coordinates out-access idx point)
        (.setRowCount root (inc idx))
        (when (= (.getRowCount root) batch-size)
          (.writeBatch out)
          (.clear root))))
    (when (pos? (.getRowCount root))
      (.writeBatch out)
      (.clear root))))

(deftype ArrowBufKdTreePointAccess [^IBlockCache block-cache ^int batch-shift ^int batch-mask ^int k ^boolean deletes?]
  IKdTreePointAccess
  (getPoint [_ idx]
    (let [block-idx (BitUtil/unsignedBitShiftRight idx batch-shift)
          idx (BitUtil/bitMask idx batch-mask)
          ^FixedSizeListVector point-vec (.getBlockVector block-cache block-idx)]
      (.getObject point-vec idx)))

  (getArrayPoint [_ idx]
    (let [block-idx (BitUtil/unsignedBitShiftRight idx batch-shift)
          idx (BitUtil/bitMask idx batch-mask)
          root (.getBlockVector block-cache block-idx)
          ^FixedSizeListVector point-vec (.getBlockVector block-cache block-idx)
          ^BigIntVector coordinates-vec (.getDataVector point-vec)
          point (long-array k)
          element-start-idx (unchecked-multiply-int idx k)]
      (dotimes [n k]
        (aset point n (.get coordinates-vec (unchecked-add-int element-start-idx n))))
      point))

  (getCoordinate [_ idx axis]
    (let [block-idx (BitUtil/unsignedBitShiftRight idx batch-shift)
          idx (BitUtil/bitMask idx batch-mask)
          ^FixedSizeListVector point-vec (.getBlockVector block-cache block-idx)
          element-start-idx (unchecked-multiply-int idx k)]
      (.get ^BigIntVector (.getDataVector point-vec) (unchecked-add-int element-start-idx axis))))

  (setCoordinate [_ idx axis value]
    (let [block-idx (BitUtil/unsignedBitShiftRight idx batch-shift)
          idx (BitUtil/bitMask idx batch-mask)
          ^FixedSizeListVector point-vec (.getBlockVector block-cache block-idx)
          element-start-idx (unchecked-multiply-int idx k)]
      (.set ^BigIntVector (.getDataVector point-vec) (unchecked-add-int element-start-idx axis) value)))

  (swapPoint [_ from-idx to-idx]
    (let [from-block-idx (BitUtil/unsignedBitShiftRight from-idx batch-shift)
          from-idx (BitUtil/bitMask from-idx batch-mask)
          to-block-idx (BitUtil/unsignedBitShiftRight to-idx batch-shift)
          to-idx (BitUtil/bitMask to-idx batch-mask)
          ^FixedSizeListVector from-point-vec (.getBlockVector block-cache from-block-idx)
          ^FixedSizeListVector to-point-vec (.getBlockVector block-cache to-block-idx)
          _ (when deletes?
              (let [tmp (.isNull to-point-vec to-idx)]
                (if (.isNull from-point-vec from-idx)
                  (.setNull to-point-vec to-idx)
                  (.setNotNull to-point-vec to-idx))
                (if tmp
                  (.setNull from-point-vec from-idx)
                  (.setNotNull from-point-vec from-idx))))
          ^BigIntVector from-coordinates-vec (.getDataVector from-point-vec)
          ^BigIntVector to-coordinates-vec (.getDataVector to-point-vec)
          from-idx (unchecked-multiply-int from-idx k)
          to-idx (unchecked-multiply-int to-idx k)]
      (dotimes [axis k]
        (let [from-idx (unchecked-add-int from-idx axis)
              to-idx (unchecked-add-int to-idx axis)
              tmp (.get from-coordinates-vec from-idx)]
          (.set from-coordinates-vec from-idx (.get to-coordinates-vec to-idx))
          (.set to-coordinates-vec to-idx tmp)))))

  (isDeleted [_ idx]
    (and deletes?
         (let [block-idx (BitUtil/unsignedBitShiftRight idx batch-shift)
               idx (BitUtil/bitMask idx batch-mask)
               ^FixedSizeListVector point-vec (.getBlockVector block-cache block-idx)]
           (.isNull point-vec idx))))

  (isInRange [_ idx min-range max-range mask]
    (let [block-idx (BitUtil/unsignedBitShiftRight idx batch-shift)
          idx (BitUtil/bitMask idx batch-mask)
          ^FixedSizeListVector point-vec (.getBlockVector block-cache block-idx)
          ^BigIntVector coordinates-vec (.getDataVector point-vec)
          element-start-idx (unchecked-multiply-int idx k)]
      (loop [n (int 0)]
        (if (= n k)
          true
          (if (BitUtil/isBitSet mask n)
            (let [x (.get coordinates-vec (unchecked-add-int element-start-idx n))]
              (if (and (<= (aget min-range n) x)
                       (<= x (aget max-range n)))
                (recur (inc n))
                false))
            (recur (inc n))))))))

(deftype ArrowBufKdTree [^ArrowBuf arrow-buf ^ArrowFooter footer ^int batch-shift ^long batch-mask ^long value-count ^int block-cache-size
                         ^IBlockCache block-cache
                         ^boolean deletes?
                         block-cache-fn]
  KdTree
  (kd-tree-insert [_ allocator point]
    (throw (UnsupportedOperationException.)))

  (kd-tree-delete [_ allocator point]
    (throw (UnsupportedOperationException.)))

  (kd-tree-range-search [kd-tree min-range max-range]
    (column-kd-tree-range-search kd-tree min-range max-range))

  (kd-tree-points [kd-tree]
    (column-kd-tree-points kd-tree))

  (kd-tree-height [kd-tree]
    (column-kd-tree-height kd-tree))

  (kd-tree-retain [this allocator]
    (ArrowBufKdTree. (doto arrow-buf
                       (.retain))
                     footer
                     batch-shift
                     batch-mask
                     value-count
                     block-cache-size
                     (block-cache-fn)
                     deletes?
                     block-cache-fn))

  (kd-tree-point-access [kd-tree]
    (ArrowBufKdTreePointAccess. block-cache batch-shift batch-mask (kd-tree-dimensions kd-tree) deletes?))

  (kd-tree-size [kd-tree]
    (if deletes?
      (.count ^LongStream (kd-tree-points kd-tree))
      value-count))

  (kd-tree-value-count [kd-tree]
    value-count)

  (kd-tree-dimensions [kd-tree]
    (.getListSize ^ArrowType$FixedSizeList (.getType (.findField (.getSchema footer) "point"))))

  Closeable
  (close [_]
    (util/try-close block-cache)
    (util/try-close arrow-buf)))

(def ^:const default-block-cache-size 16)

(defn ->arrow-buf-kd-tree
  (^core2.temporal.kd_tree.ArrowBufKdTree [^ArrowBuf arrow-buf]
   (->arrow-buf-kd-tree arrow-buf {}))
  (^core2.temporal.kd_tree.ArrowBufKdTree [^ArrowBuf arrow-buf {:keys [block-cache-size deletes?]
                                                                :or {block-cache-size default-block-cache-size
                                                                     deletes? true}}]
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
         batch-size (long (first batch-sizes))
         batch-size (if (= 1 (Long/bitCount batch-size))
                      batch-size
                      (inc Integer/MAX_VALUE))
         batch-mask (dec batch-size)
         batch-shift (Long/bitCount batch-mask)
         root-block-cache (reify IBlockCache
                            (getBlockVector [_ block-idx]
                              (let [allocator (.getAllocator (.getReferenceManager arrow-buf))]
                                (with-open [arrow-record-batch (util/->arrow-record-batch-view (.get (.getRecordBatches footer) block-idx) arrow-buf)
                                            root (VectorSchemaRoot/create (.getSchema footer) allocator)]
                                  (.load (VectorLoader. root CommonsCompressionFactory/INSTANCE) arrow-record-batch)
                                  (.getTo (doto (.getTransferPair (.getVector root point-vec-idx) allocator)
                                            (.transfer))))))

                            (close [_]))
         block-cache-fn (fn []
                          (IBlockCache$LatestBlockCache. (IBlockCache$ClockBlockCache. block-cache-size root-block-cache)))
         block-cache (block-cache-fn)]
     (ArrowBufKdTree. arrow-buf footer batch-shift batch-mask value-count block-cache-size block-cache deletes? block-cache-fn))))

(defn ->mmap-kd-tree ^core2.temporal.kd_tree.ArrowBufKdTree [^BufferAllocator allocator ^Path path]
  (let [nio-buffer (util/->mmap-path path)
        arrow-buf (util/->arrow-buf-view allocator nio-buffer)]
    (->arrow-buf-kd-tree arrow-buf)))

(def ^:private ^:const default-disk-kd-tree-batch-size (* 128 1024))

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
      (build-breadth-first-tree-in-place kd-tree true)
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
    (throw (UnsupportedOperationException.)))

  (isDeleted [_ idx]
    (if (< idx static-value-count)
      (.isDeleted static-access idx)
      (.isDeleted dynamic-access (- idx static-value-count)))))

(definterface IDynamicKdTreeAccess
  (^Object getDynamicKdTree []))

(deftype MergedKdTree [static-kd-tree ^:unsynchronized-mutable dynamic-kd-tree ^Roaring64Bitmap static-delete-bitmap ^long static-size ^long static-value-count]
  IDynamicKdTreeAccess
  (getDynamicKdTree [_]
    dynamic-kd-tree)

  KdTree
  (kd-tree-insert [this allocator point]
    (set! (.dynamic-kd-tree this) (kd-tree-insert dynamic-kd-tree allocator point))
    this)

  (kd-tree-delete [this allocator point]
    (let [static-delete? (boolean-array 1)]
      (.forEach ^LongStream (kd-tree-range-search static-kd-tree point point)
                (reify LongConsumer
                  (accept [_ x]
                    (aset static-delete? 0 true)
                    (.addLong static-delete-bitmap x))))
      (when (BitUtil/bitNot (aget static-delete? 0))
        (set! (.dynamic-kd-tree this) (kd-tree-delete dynamic-kd-tree allocator point))))
    this)

  (kd-tree-range-search [_ min-range max-range]
    (LongStream/concat (.filter ^LongStream (kd-tree-range-search static-kd-tree min-range max-range)
                                (reify LongPredicate
                                  (test [_ x]
                                    (BitUtil/bitNot (.contains static-delete-bitmap x)))))
                       (.map ^LongStream (kd-tree-range-search dynamic-kd-tree min-range max-range)
                             (reify LongUnaryOperator
                               (applyAsLong [_ x]
                                 (+ static-value-count x))))))

  (kd-tree-points [_]
    (LongStream/concat (.filter ^LongStream (kd-tree-points static-kd-tree)
                                (reify LongPredicate
                                  (test [_ x]
                                    (BitUtil/bitNot (.contains static-delete-bitmap x)))))
                       (.map ^LongStream (kd-tree-points dynamic-kd-tree)
                             (reify LongUnaryOperator
                               (applyAsLong [_ x]
                                 (+ static-value-count x))))))

  (kd-tree-height [_]
    (max (long (kd-tree-height static-kd-tree))
         (long (kd-tree-height dynamic-kd-tree))))

  (kd-tree-retain [kd-tree allocator]
    (MergedKdTree. (kd-tree-retain static-kd-tree allocator)
                   (kd-tree-retain dynamic-kd-tree allocator)
                   (.clone ^Roaring64Bitmap static-delete-bitmap)
                   static-size
                   static-value-count))

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
   (let [static-delete-bitmap (Roaring64Bitmap.)
         ^IKdTreePointAccess access (kd-tree-point-access dynamic-kd-tree)]
     (dotimes [n (long (kd-tree-value-count dynamic-kd-tree))]
       (when (.isDeleted access n)
         (let [point (.getArrayPoint access n)]
           (.forEach ^LongStream (kd-tree-range-search static-kd-tree point point)
                     (reify LongConsumer
                       (accept [_ x]
                         (.addLong static-delete-bitmap x)))))))
     (MergedKdTree. static-kd-tree dynamic-kd-tree static-delete-bitmap (kd-tree-size static-kd-tree) (kd-tree-value-count static-kd-tree)))))

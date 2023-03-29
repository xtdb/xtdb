(ns xtdb.temporal.kd-tree
  (:require [xtdb.types :as t]
            [xtdb.util :as util])
  (:import xtdb.BitUtil
           [java.io Closeable]
           java.lang.ref.Cleaner
           [java.util ArrayDeque List Queue]
           java.util.concurrent.LinkedBlockingQueue
           [java.util.function LongBinaryOperator LongConsumer LongFunction LongPredicate LongUnaryOperator]
           java.util.stream.LongStream
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.BigIntVector
           org.apache.arrow.vector.complex.FixedSizeListVector
           [org.apache.arrow.vector.types Types$MinorType]
           [org.apache.arrow.vector.types.pojo ArrowType$FixedSizeList Field]
           org.roaringbitmap.longlong.Roaring64Bitmap))

(set! *unchecked-math* :warn-on-boxed)

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IKdTreePointAccess
  (^java.util.List getPoint [^long idx])
  (^longs getArrayPoint [^long idx])
  (^long getCoordinate [^long idx ^int axis])
  (^void setCoordinate [^long idx ^int axis ^long value])
  (^void swapPoint [^long from-idx ^long to-idx])
  (^boolean isDeleted [^long idx]
   "entry is deleted from the kd-tree, not that the entity/row was deleted")
  (^boolean isInRange [^long idx ^longs min-range ^longs max-range ^int mask]))

(defprotocol KdTree
  (kd-tree-insert [_ allocator point])
  (kd-tree-delete [_ allocator point])
  (kd-tree-range-search [_ min-range max-range])
  (kd-tree-points [_ deletes?])
  (kd-tree-height [_])
  (kd-tree-retain [_ allocator])
  (kd-tree-point-access [_])
  (kd-tree-size [_])
  (kd-tree-value-count [_])
  (kd-tree-dimensions [_]))

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
    (-> (->node-kd-tree allocator (count point))
        (kd-tree-insert allocator point)))
  (kd-tree-delete [_ allocator point]
    (-> (kd-tree-insert nil allocator point)
        (kd-tree-delete allocator point)))
  (kd-tree-range-search [_ _ _]
    (LongStream/empty))
  (kd-tree-points [_ _deletes?]
    (LongStream/empty))
  (kd-tree-height [_] -1)
  (kd-tree-retain [_ _])
  (kd-tree-point-access [_]
    (NilPointAccess.))
  (kd-tree-size [_] 0)
  (kd-tree-value-count [_] 0)
  (kd-tree-dimensions [_] 0))

(deftype ListPointAccess [^List list]
  IKdTreePointAccess
  (getPoint [_ idx]
    (.get list idx))
  (getArrayPoint [_ idx]
    (->longs (.get list idx)))
  (getCoordinate [this idx axis]
    (aget ^longs (.getArrayPoint this idx) axis))
  (setCoordinate [_ _ _ _]
    (throw (UnsupportedOperationException.)))
  (swapPoint [_ _ _]
    (throw (UnsupportedOperationException.)))
  (isDeleted [_ _]
    false)
  (isInRange [this idx min-range max-range _axis]
    (let [point (.getArrayPoint this idx)
          len (alength point)]
      (loop [n (int 0)]
        (if (= n len)
          true
          (let [x (aget point n)]
            (if (and (<= (aget min-range n) x)
                     (<= x (aget max-range n)))
              (recur (inc n))
              false)))))))

(extend-protocol KdTree
  List
  (kd-tree-insert [_ _allocator _point]
    (throw (UnsupportedOperationException.)))
  (kd-tree-delete [_ _allocator _point]
    (throw (UnsupportedOperationException.)))
  (kd-tree-range-search [this min-range max-range]
    (let [min-range (->longs min-range)
          max-range (->longs max-range)
          ^IKdTreePointAccess access (kd-tree-point-access this)]
      (.filter (LongStream/range 0 (.size this))
               (reify LongPredicate
                 (test [_ x]
                   (.isInRange access x min-range max-range -1))))))
  (kd-tree-points [this _deletes?]
    (LongStream/range 0 (.size this)))
  (kd-tree-height [_] 0)
  (kd-tree-retain [this _]
    this)
  (kd-tree-point-access [this]
    (ListPointAccess. this))
  (kd-tree-size [this] (.size this))
  (kd-tree-value-count [this] (.size this))
  (kd-tree-dimensions [this] (count (first this))))

(defn- write-coordinates [^IKdTreePointAccess access ^long idx point]
  (if (instance? longs-class point)
    (dotimes [n (alength ^longs point)]
      (.setCoordinate access idx n (aget ^longs point n)))
    (dotimes [n (count point)]
      (.setCoordinate access idx n (long (nth point n))))))

(defn write-point ^long [^FixedSizeListVector point-vec ^IKdTreePointAccess access point]
  (let [idx (.getValueCount point-vec)]
    (.startNewValue point-vec idx)
    (write-coordinates access idx point)
    (.setValueCount point-vec (inc idx))
    idx))

(defn ->point-field ^org.apache.arrow.vector.types.pojo.Field [^long k]
  (t/->field "point" (ArrowType$FixedSizeList. k) false
             (t/->field "coordinates" (.getType Types$MinorType/BIGINT) false)))

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

(defn kd-tree->seq
  ([kd-tree]
   (kd-tree->seq kd-tree (kd-tree-points kd-tree false)))
  ([kd-tree ^LongStream stream]
   (let [^IKdTreePointAccess point-access (kd-tree-point-access kd-tree)]
     (-> stream
         (.mapToObj (reify LongFunction
                      (apply [_ x]
                        (.getPoint point-access x))))
         (.iterator)
         (iterator-seq)))))

(def ^:private ^:const leaf-size 64)

(declare ->leaf-node leaf-node-edit)

(defonce ^:private ^Cleaner leaf-cleaner (Cleaner/create))

(deftype LeafNode [^FixedSizeListVector point-vec ^Queue leaf-pool ^long superseded ^int idx ^int axis ^int size ^boolean root? pool-token]
  KdTree
  (kd-tree-insert [kd-tree allocator point]
    (leaf-node-edit kd-tree allocator point false))

  (kd-tree-delete [kd-tree allocator point]
    (leaf-node-edit kd-tree allocator point true))

  (kd-tree-range-search [_ min-range max-range]
    (let [^IKdTreePointAccess access (KdTreeVectorPointAccess. point-vec (.getListSize point-vec))
          min-range (->longs min-range)
          max-range (->longs max-range)
          axis-mask (range-bitmask min-range max-range)
          acc (LongStream/builder)]
      (dotimes [n size]
        (let [x (+ idx n)]
          (when (and (BitUtil/bitNot (BitUtil/isLongBitSet superseded n))
                     (BitUtil/bitNot (.isDeleted access x))
                     (.isInRange access x min-range max-range axis-mask))
            (.add acc x))))
      (.build acc)))

  (kd-tree-points [_ deletes?]
    (let [^IKdTreePointAccess access (KdTreeVectorPointAccess. point-vec (.getListSize point-vec))]
      (cond-> (LongStream/range 0 size)
        (pos? superseded) (.filter (reify LongPredicate
                                     (test [_ n]
                                       (BitUtil/bitNot (BitUtil/isLongBitSet superseded n)))))
        true (.map (reify LongUnaryOperator
                     (applyAsLong [_ n]
                       (+ idx n))))
        (BitUtil/bitNot deletes?) (.filter (reify LongPredicate
                                             (test [_ x]
                                               (BitUtil/bitNot (.isDeleted access x))))))))

  (kd-tree-height [_] 0)

  (kd-tree-retain [_ allocator]
    (LeafNode. (.getTo (doto (.getTransferPair point-vec allocator)
                         (.splitAndTransfer 0 (.getValueCount point-vec))))
               leaf-pool
               superseded
               idx
               axis
               size
               root?
               pool-token))

  (kd-tree-point-access [_]
    (KdTreeVectorPointAccess. point-vec (.getListSize point-vec)))

  (kd-tree-size [kd-tree]
    (.count ^LongStream (kd-tree-points kd-tree false)))

  (kd-tree-value-count [_]
    (.getValueCount point-vec))

  (kd-tree-dimensions [_]
    (.getListSize point-vec))

  Closeable
  (close [_]
    (when root?
      (util/try-close point-vec))))

(deftype InnerNode [^FixedSizeListVector point-vec ^Queue leaf-pool ^long axis-value ^int axis left right ^boolean root?]
  KdTree
  (kd-tree-insert [_ allocator point]
    (let [point (->longs point)]
      (if (< (aget point axis) axis-value)
        (InnerNode. point-vec leaf-pool axis-value axis (kd-tree-insert left allocator point) right root?)
        (InnerNode. point-vec leaf-pool axis-value axis left (kd-tree-insert right allocator point) root?))))

  (kd-tree-delete [_ allocator point]
    (let [point (->longs point)]
      (if (< (aget point axis) axis-value)
        (InnerNode. point-vec leaf-pool axis-value axis (kd-tree-delete left allocator point) right root?)
        (InnerNode. point-vec leaf-pool axis-value axis left (kd-tree-delete right allocator point) root?))))

  (kd-tree-range-search [kd-tree min-range max-range]
    (let [^IKdTreePointAccess access (KdTreeVectorPointAccess. point-vec (.getListSize point-vec))
          min-range (->longs min-range)
          max-range (->longs max-range)
          axis-mask (range-bitmask min-range max-range)
          acc (LongStream/builder)
          stack (ArrayDeque.)]
      (loop [node kd-tree]
        (cond
          (instance? InnerNode node)
          (let [^InnerNode node node
                axis (.axis node)
                axis-value (.axis-value node)
                visit-left? (< (aget min-range axis) axis-value)
                visit-right? (<= axis-value (aget max-range axis))]
            (if visit-left?
              (do (when visit-right?
                    (.push stack (.right node)))
                  (recur (.left node)))
              (if visit-right?
                (recur (.right node))
                (recur (.poll stack)))))

          (instance? LeafNode node)
          (let [^LeafNode node node
                size (.size node)
                idx (.idx node)
                superseded (.superseded node)]
            (if (zero? superseded)
              (dotimes [n size]
                (let [x (+ idx n)]
                  (when (and (BitUtil/bitNot (.isDeleted access x))
                             (.isInRange access x min-range max-range axis-mask))
                    (.add acc x))))
              (dotimes [n size]
                (let [x (+ idx n)]
                  (when (and (BitUtil/bitNot (BitUtil/isLongBitSet superseded n))
                             (BitUtil/bitNot (.isDeleted access x))
                             (.isInRange access x min-range max-range axis-mask))
                    (.add acc x)))))
            (recur (.poll stack)))

          :else
          (.build acc)))))

  (kd-tree-points [_ deletes?]
    (LongStream/concat (kd-tree-points left deletes?) (kd-tree-points right deletes?)))

  (kd-tree-height [_]
    (max (inc (long (kd-tree-height left)))
         (inc (long (kd-tree-height right)))))

  (kd-tree-retain [_ allocator]
    (InnerNode. (.getTo (doto (.getTransferPair point-vec allocator)
                          (.splitAndTransfer 0 (.getValueCount point-vec))))
                leaf-pool
                axis-value
                axis
                left
                right
                root?))

  (kd-tree-point-access [_]
    (KdTreeVectorPointAccess. point-vec (.getListSize point-vec)))

  (kd-tree-size [_]
    (+ (long (kd-tree-size left))
       (long (kd-tree-size right))))

  (kd-tree-value-count [_]
    (.getValueCount point-vec))

  (kd-tree-dimensions [_]
    (.getListSize point-vec))

  Closeable
  (close [_]
    (when root?
      (util/try-close point-vec))))

(defn- leaf-node-edit [^LeafNode kd-tree ^BufferAllocator allocator point deleted?]
  (let [point (->longs point)
        ^FixedSizeListVector point-vec (.point-vec kd-tree)
        leaf-pool (.leaf-pool kd-tree)
        k (.getListSize point-vec)
        superseded (.superseded kd-tree)
        idx (.idx kd-tree)
        axis (.axis kd-tree)
        size (.size kd-tree)
        root? (.root? kd-tree)
        pool-token (.pool-token kd-tree)
        ^IKdTreePointAccess access (KdTreeVectorPointAccess. point-vec k)]
    (if (< size leaf-size)
      (let [point-idx (+ idx size)]
        (write-coordinates access point-idx point)
        (if deleted?
          (.setNull point-vec point-idx)
          (.setNotNull point-vec point-idx))
        (let [new-superseded (-> (LongStream/range 0 size)
                                 (.filter (reify LongPredicate
                                            (test [_ n]
                                              (.isInRange access (+ idx n) point point -1))))
                                 (.reduce superseded (reify LongBinaryOperator
                                                       (applyAsLong [_ acc n]
                                                         (bit-or acc (bit-shift-left 1 n))))))]
          (LeafNode. point-vec leaf-pool new-superseded idx axis (inc size) root? pool-token)))
      (let [axis-values (-> (LongStream/range idx (+ idx size))
                            (.map (reify LongUnaryOperator
                                    (applyAsLong [_ x]
                                      (.getCoordinate access x axis))))
                            (.sorted)
                            (.toArray))
            axis-value (aget axis-values (BitUtil/unsignedBitShiftRight (alength axis-values) 1))
            next-axis (next-axis axis k)
            left (->leaf-node point-vec leaf-pool next-axis)
            right (->leaf-node point-vec leaf-pool next-axis)]
        (loop [n 0
               acc (InnerNode. point-vec leaf-pool axis-value axis left right root?)]
          (cond
            (= n leaf-size)
            (if deleted?
              (kd-tree-delete acc allocator point)
              (kd-tree-insert acc allocator point))

            (BitUtil/isLongBitSet superseded n)
            (recur (inc n) acc)

            :else
            (let [point-idx (+ idx n)
                  point (.getArrayPoint access point-idx)
                  acc (if (.isDeleted access point-idx)
                        (kd-tree-delete acc allocator point)
                        (kd-tree-insert acc allocator point))]
              (recur (inc n) acc))))))))

(defn- ->leaf-node
  ([^FixedSizeListVector point-vec ^Queue leaf-pool ^long axis]
   (->leaf-node point-vec leaf-pool axis false))
  ([^FixedSizeListVector point-vec ^Queue leaf-pool ^long axis root?]
   (let [idx (or (.poll leaf-pool)
                 (let [idx (.getValueCount point-vec)]
                   (.setValueCount point-vec (+ idx leaf-size))
                   idx))
         pool-token (Object.)]
     (.register leaf-cleaner pool-token #(.offer leaf-pool idx))
     (LeafNode. point-vec leaf-pool 0 idx axis 0 root? pool-token))))

(defn ->node-kd-tree ^java.io.Closeable [^BufferAllocator allocator ^long k]
  (let [^FixedSizeListVector point-vec (.createVector ^Field (->point-field k) allocator)
        leaf-pool (LinkedBlockingQueue.)]
    (->leaf-node point-vec leaf-pool 0 true)))

(defn merge-kd-trees ^java.io.Closeable [^BufferAllocator allocator kd-tree-to kd-tree-from]
  (let [^IKdTreePointAccess from-access (kd-tree-point-access kd-tree-from)
        it (.iterator ^LongStream (kd-tree-points kd-tree-from true))]
    (loop [acc kd-tree-to]
      (if (.hasNext it)
        (let [idx (.nextLong it)
              point (.getArrayPoint from-access idx)]
          (recur (if (.isDeleted from-access idx)
                   (kd-tree-delete acc allocator point)
                   (kd-tree-insert acc allocator point))))
        acc))))

(defn build-node-kd-tree ^java.io.Closeable [^BufferAllocator allocator kd-tree-from]
  (merge-kd-trees allocator nil kd-tree-from))

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

  (kd-tree-points [_ deletes?]
    (LongStream/concat (.filter ^LongStream (kd-tree-points static-kd-tree deletes?)
                                (reify LongPredicate
                                  (test [_ x]
                                    (or deletes? (BitUtil/bitNot (.contains static-delete-bitmap x))))))
                       (.map ^LongStream (kd-tree-points dynamic-kd-tree deletes?)
                             (reify LongUnaryOperator
                               (applyAsLong [_ x]
                                 (+ static-value-count x))))))

  (kd-tree-height [_]
    (max (long (kd-tree-height static-kd-tree))
         (long (kd-tree-height dynamic-kd-tree))))

  (kd-tree-retain [_ allocator]
    (MergedKdTree. (kd-tree-retain static-kd-tree allocator)
                   (kd-tree-retain dynamic-kd-tree allocator)
                   (.clone ^Roaring64Bitmap static-delete-bitmap)
                   static-size
                   static-value-count))

  (kd-tree-point-access [_]
    (MergedKdTreePointAccess. (kd-tree-point-access static-kd-tree) (kd-tree-point-access dynamic-kd-tree) static-value-count))

  (kd-tree-size [_]
    (+ (- static-size (.getLongCardinality static-delete-bitmap))
       (long (kd-tree-size dynamic-kd-tree))))

  (kd-tree-value-count [_]
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
  (^xtdb.temporal.kd_tree.MergedKdTree [static-kd-tree]
   (->merged-kd-tree static-kd-tree nil))
  (^xtdb.temporal.kd_tree.MergedKdTree [static-kd-tree dynamic-kd-tree]
   (let [static-delete-bitmap (Roaring64Bitmap.)
         ^IKdTreePointAccess access (kd-tree-point-access dynamic-kd-tree)]
     (.forEach ^LongStream (kd-tree-points dynamic-kd-tree true)
               (reify LongConsumer
                 (accept [_ n]
                   (when (.isDeleted access n)
                     (let [point (.getArrayPoint access n)]
                       (.forEach ^LongStream (kd-tree-range-search static-kd-tree point point)
                                 (reify LongConsumer
                                   (accept [_ x]
                                     (.addLong static-delete-bitmap x)))))))))
     (MergedKdTree. static-kd-tree dynamic-kd-tree static-delete-bitmap (kd-tree-size static-kd-tree) (kd-tree-value-count static-kd-tree)))))

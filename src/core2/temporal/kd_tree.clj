(ns core2.temporal.kd-tree
  (:require [core2.types :as t]
            [core2.util :as util]
            [clojure.tools.logging :as log])
  (:import [java.io Closeable]
           java.nio.file.Path
           java.nio.channels.FileChannel$MapMode
           [java.util ArrayDeque ArrayList Arrays Collection Comparator Date Deque HashMap
            IdentityHashMap List Map Spliterator Spliterator$OfInt Spliterators]
           [java.util.function Consumer Function IntBinaryOperator IntConsumer IntFunction IntPredicate IntUnaryOperator]
           [java.util.stream IntStream StreamSupport]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector BitVectorHelper BigIntVector BufferLayout IntVector TinyIntVector TypeLayout VectorSchemaRoot VectorUnloader]
           org.apache.arrow.vector.util.DataSizeRoundingUtil
           [org.apache.arrow.vector.complex FixedSizeListVector StructVector]
           [org.apache.arrow.vector.types.pojo ArrowType$FixedSizeList Field Schema]
           [org.apache.arrow.vector.types Types$MinorType]
           [org.apache.arrow.vector.ipc WriteChannel]
           [org.apache.arrow.vector.ipc.message ArrowBlock ArrowBuffer ArrowFieldNode ArrowFooter ArrowRecordBatch MessageSerializer]
           org.roaringbitmap.RoaringBitmap))

;; NOTE:

;; Current design is limited to Integer/MAX_SIZE entries.

;; As each insertion results in one insert, one delete of the
;; potentially previous version, and an insert to cap the times of
;; that previous version, we have 3 actual edits per insert. The
;; deletes are dropped during the flushing to disk, so we can say 2
;; edits. Corrections result in several more edits, depending on how
;; many existing rectangles they overlap.

;; This means this design support upto (/ Integer/MAX_SIZE 2) updates
;; (without corrections), which is the same number as a gigabyte, so
;; roughly a billion.

;; Each entry in the tree currently takes 61 bytes (1 byte
;; axis-delete-flag, 8 bytes split-value, 4 bytes skip-pointer, (* 6
;; 8) bytes points), but we can round this up to 64 for simplicity, as
;; each of the columns will also have a validity bit. This means that
;; the maximum tree would be roughly (ignoring overhead) 128G of raw
;; kd tree column data, and that each entity update takes 128 bytes.

;; One could engineer away the axis-delete-flag column (to support
;; deletes) and the split-value column (for performance, duplicates
;; axis value from the point), which makes each entry 52 bytes, so it
;; won't save that much.

;; The first temporal tree from TPC-H SF 0.1 is 5M and compresses to
;; about 1M with gzip, so say a factor of 5. But this doesn't help in
;; practice as the compressed tree cannot be queried.

;; The limitation of Integer/MAX_SIZE can be lifted as this is a per
;; vector count limit in Arrow Java (but not Arrow), and one can split
;; the tree into several record batches with some (solvable)
;; complications to the in-place sorting and point access. It would
;; remove the need to create an empty tree in place though, as one
;; could first write down blocks of the raw data split in batches, and
;; then sort that on disk.

;; Currently we rebuild the full tree per chunk in a stop-the-world
;; operation. This will keep slowing down ingest, as eventually this
;; tree will become quite large - upto 128G as mentioned above. There
;; are various ways of mitigating that, like introducing some form of
;; double buffering to the ingest, where there are 3 states -
;; live-live-chunk, live-background-indexing-chunk-minus-one,
;; done-chunks

;; Temporal Snapshots:

;; On registerNewChunk, Write a small temporal chunk of the current
;; live node-kd-tree (like previously, before the in-place disk tree
;; change).

;; "Reboot" the kd tree from object store by reading the latest
;; temporal snapshot (if one exists), and all later temporal chunks
;; added after this and add them to a tree of merged-kd-trees, with a
;; node-kd-tree for the dynamic live data. This is the same process
;; that happens at start of the node. Evict buffers used by the
;; previous tree from the buffer pool.

;; Kick off a background job to build a new temporal snapshot of the
;; latest temporal snapshot and all newer known temporal chunks as a
;; disk-kd-tree and store it in the object store. This snapshot will
;; be picked up and used at next "reboot" of the tree.

;; Pros:

;; This approach removes the merging of the large snapshot tree
;; from the index path, while also avoids any complications by
;; asynchronously merging trees, as while being built and stored in
;; the object store, the snapshots are only accessed during the
;; "reboot" which happens during the existing critical section in
;; finishChunk.

;; It also limits the change to be local to the temporal manager, and
;; doesn't have to introduce any double buffering or complicated logic
;; to the rest of the system.

;; Incremental step from what we have. The cons below should be
;; possible to solve later as separate pieces of work.

;; Cons:

;; Still has to merge the entire snapshot in one go, which should
;; preferably happen within the time to ingest a chunk for it not to
;; fall behind. If ingest slows down it will eventually catch up.

;; This change doesn't address the one billion edits limitation or
;; 128G size of the tree itself.

;; TODO:

;; - Add back per-chunk tree for simplicity, as the current dynamic
;; - tree might span several chunks and not be the same on all nodes.

;; - Static/Dynamic tree node.
;;   - Revisit index and point access.
;;   - Revisit deletion.
;;   - Revisit need for explicit axis.

;; Step 2:

;; Later, we can merge older chunks, either when registering new ones,
;; or in the background. Needs to take the fact that there are
;; multiple nodes into account. Different strategies, or a combination
;; can be envisioned:

;; 1. merging two neighbouring chunks into one larger chunk, combine.
;; 2. merging towards larger chunks, old to newest, reduce.

;; Merging chunks works like follows - assumes the intermediate chunk
;; fits into memory:

;; 1. find all deletions from newest chunk and add them to the deletion set.
;; 2. scan older chunk, skip deletions in the deletion set, and remove them from the deletion set.
;; 3. scan newer chunk, only keep deletions still in the deletion set.
;; 4. store merged tree.

;; Unless merging strictly in oldest-to-newest order, the combined
;; chunk will still have deletions, referring to even older chunks.

;; When building the current state on start-up, the combined chunks
;; supersede the chunks they were combined from in the object store.

;; Step 3:

;; Add an implementation of KdTree that can delegate to both a set of
;; existing Arrow chunks and the in-memory tree, avoiding having to
;; keep the entire tree in memory. Modifications goes to the dynamic
;; in-memory tree.

;; Merge the chunks in a streaming fashion on disk instead of reading
;; them into memory. Deletion set can still be stored in-memory.

(set! *unchecked-math* :warn-on-boxed)

(definterface IKdTreePointAccess
  (^java.util.List getPoint [^int idx])
  (^longs getArrayPoint [^int idx])
  (^long getCoordinate [^int idx ^int axis]))

(defprotocol KdTree
  (kd-tree-insert [_ allocator point])
  (kd-tree-delete [_ allocator point])
  (kd-tree-range-search [_ min-range max-range])
  (kd-tree-depth-first [_])
  (kd-tree-depth [_])
  (kd-tree-retain [_ allocator])
  (kd-tree-point-access [_])
  (kd-tree-size [_])
  (kd-tree-value-count [_]))

(deftype Node [^FixedSizeListVector point-vec ^int point-idx ^byte axis left right ^boolean deleted?]
  Closeable
  (close [_]
    (util/try-close point-vec)))

(defn next-axis ^long [^long axis ^long k]
  (let [next-axis (inc axis)]
    (if (= k next-axis)
      0
      next-axis)))

(defmacro in-range? [min-range point max-range]
  `(let [len# (alength ~point)]
     (loop [n# (int 0)]
       (if (= n# len#)
         true
         (let [x# (aget ~point n#)]
           (if (and (<= (aget ~min-range n#) x#)
                    (<= x# (aget ~max-range n#)))
             (recur (inc n#))
             false))))))

(defmacro ^:private in-range-column? [min-range point-vec coordinates-vec idx max-range]
  `(let [k# (alength ~min-range)
         element-start-idx# (.getElementStartIndex ~point-vec ~idx)]
     (loop [n# (int 0)]
       (if (= n# k#)
         true
         (let [x# (.get ~coordinates-vec (+ element-start-idx# n#))]
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
    (throw (IndexOutOfBoundsException.))))

(extend-protocol KdTree
  nil
  (kd-tree-insert [_ allocator point]
    (->node-kd-tree allocator [point]))
  (kd-tree-delete [_ allocator point]
    (-> (kd-tree-insert nil allocator point)
        (kd-tree-delete allocator point)))
  (kd-tree-range-search [_ _ _]
    (IntStream/empty))
  (kd-tree-depth-first [_]
    (IntStream/empty))
  (kd-tree-depth [_] 0)
  (kd-tree-retain [_ _])
  (kd-tree-point-access [_]
    (NilPointAccess.))
  (kd-tree-size [_] 0)
  (kd-tree-value-count [_] 0))

(defn- write-coordinates [^BigIntVector coordinates-vec ^long list-idx point]
  (if (instance? longs-class point)
    (dotimes [n (alength ^longs point)]
      (.setSafe coordinates-vec (+ list-idx n) (aget ^longs point n)))
    (dotimes [n (count point)]
      (.setSafe coordinates-vec (+ list-idx n) (long (nth point n))))))

(defn- write-point ^long [^FixedSizeListVector point-vec point]
  (let [^BigIntVector coordinates-vec (.getDataVector point-vec)
        idx (.getValueCount point-vec)
        list-idx (.startNewValue point-vec idx)]
    (write-coordinates coordinates-vec list-idx point)
    (.setValueCount point-vec (inc idx))
    idx))

(defn- ->point-field ^org.apache.arrow.vector.types.pojo.Field [^long k]
  (t/->field "point" (ArrowType$FixedSizeList. k) false
             (t/->field "coordinates" (.getType Types$MinorType/BIGINT) false)))

(defmacro ^:private swap-point [point-vec coordinates-vec n m]
  `(let [n-idx# (.getElementStartIndex ~point-vec ~n)
         m-idx# (.getElementStartIndex ~point-vec ~m)]
     (dotimes [idx# (.getListSize ~point-vec)]
       (let [n-idx# (+ n-idx# idx#)
             m-idx# (+ m-idx# idx#)
             tmp# (.get ~coordinates-vec n-idx#)]
         (.set ~coordinates-vec n-idx# (.get ~coordinates-vec m-idx#))
         (.set ~coordinates-vec m-idx# tmp#)))))

(defn- upper-int ^long [^long x]
  (unsigned-bit-shift-right x Integer/SIZE))

(defn- lower-int ^long [^long x]
  (bit-and x (Integer/toUnsignedLong -1)))

(defn- two-ints-as-long ^long [^long x ^long y]
  (bit-or (bit-shift-left x Integer/SIZE) y))

(defn- three-way-partition ^long [^FixedSizeListVector point-vec ^long low ^long hi ^long axis]
  (let [^BigIntVector coordinates-vec (.getDataVector point-vec)
        pivot-idx (+ (.getElementStartIndex point-vec (quot (+ low hi) 2)) axis)
        pivot (.get coordinates-vec pivot-idx)]
    (loop [i (int low)
           j (int low)
           k (inc (int hi))]
      (if (< j k)
        (let [diff (Long/compare (.get coordinates-vec (+ (.getElementStartIndex point-vec j) axis)) pivot)]
          (cond
            (neg? diff)
            (do (swap-point point-vec coordinates-vec i j)
                (recur (inc i) (inc j) k))

            (pos? diff)
            (let [k (dec k)]
              (swap-point point-vec coordinates-vec j k)
              (recur i j k))

            :else
            (recur i (inc j) k)))
        (two-ints-as-long i (dec k))))))

(defn- quick-select ^long [^FixedSizeListVector point-vec ^long low ^long hi ^long axis]
  (let [k (quot (+ low hi) 2)]
    (loop [low low
           hi (dec hi)]
      (if (< low hi)
        (let [left-right (three-way-partition point-vec low hi axis)
              left (upper-int left-right)
              right (lower-int left-right)]
          (cond
            (< k left)
            (recur low (dec left))

            (> k right)
            (recur (inc right) hi)

            :else
            left))
        low))))

(defn ->node-kd-tree ^core2.temporal.kd_tree.Node [^BufferAllocator allocator points]
  (when (not-empty points)
    (let [k (count (first points))
          ^FixedSizeListVector point-vec (.createVector ^Field (->point-field k) allocator)
          ^BigIntVector coordinates-vec (.getDataVector point-vec)]
      (doseq [point points]
        (write-point point-vec point))
      (try
        ((fn step [^long start ^long end ^long axis]
           (let [median (quick-select point-vec start end axis)
                 next-axis (next-axis axis k)]
             (when-not (= start median)
               (swap-point point-vec coordinates-vec start median))
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

(defmacro ^:private point-equals-list-element [point coordinates-vec element-start-idx k]
  `(loop [n# 0]
     (if (= n# ~k)
       true
       (if (= (.get ~coordinates-vec (+ ~element-start-idx n#)) (aget ~point n#))
         (recur (inc n#))
         false))))

(defn- maybe-split-stack ^java.util.Deque [^Deque stack]
  (let [split-size (quot (.size stack) 2)]
    (when (pos? split-size)
      (let [split-stack (ArrayDeque.)]
        (while (not= split-size (.size split-stack))
          (.add split-stack (.poll stack)))
        split-stack))))

(deftype NodeRangeSearchSpliterator [^FixedSizeListVector point-vec
                                     ^BigIntVector coordinates-vec
                                     ^longs min-range
                                     ^longs max-range
                                     ^Deque stack]
  Spliterator$OfInt
  (^void forEachRemaining [_ ^IntConsumer c]
    (loop []
      (when-let [^Node node (.poll stack)]
        (loop [node node]
          (let [left (.left node)
                right (.right node)
                axis (.axis node)
                point-idx (.point-idx node)
                point-axis (.get coordinates-vec (+ (.getElementStartIndex point-vec point-idx) axis))
                min-axis (aget min-range axis)
                max-axis (aget max-range axis)
                min-match? (< min-axis point-axis)
                max-match? (<= point-axis max-axis)
                visit-left? (and left min-match?)
                visit-right? (and right max-match?)]

            (when (and (or min-match? max-match?)
                       (not (.deleted? node))
                       (in-range-column? min-range point-vec coordinates-vec point-idx max-range))
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

  (^boolean tryAdvance [_ ^IntConsumer c]
    (loop []
      (if-let [^Node node (.poll stack)]
        (let [axis (.axis node)
              point-idx (.point-idx node)
              point-axis (.get coordinates-vec (+ (.getElementStartIndex point-vec point-idx) axis))
              left (.left node)
              right (.right node)
              min-axis (aget min-range axis)
              max-axis (aget max-range axis)
              min-match? (< min-axis point-axis)
              max-match? (<= point-axis max-axis)
              visit-left? (and left min-match?)
              visit-right? (and right max-match?)]

          (when visit-right?
            (.push stack right))

          (when visit-left?
            (.push stack left))

          (if (and (or min-match? max-match?)
                   (not (.deleted? node))
                   (in-range-column? min-range point-vec coordinates-vec point-idx max-range))
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
      (NodeRangeSearchSpliterator. point-vec coordinates-vec min-range max-range split-stack))))

(deftype NodeDepthFirstSpliterator [^Deque stack]
  Spliterator$OfInt
  (^void forEachRemaining [_ ^IntConsumer c]
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

  (^boolean tryAdvance [_ ^IntConsumer c]
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
        ^BigIntVector coordinates-vec (.getDataVector point-vec)
        k (.getListSize point-vec)]
    (loop [parent-axis (.axis kd-tree)
           node kd-tree
           build-path-fns ()]
      (if-not node
        (let [point-idx (write-point point-vec point)]
          (node-kd-tree-build-path build-path-fns (Node. point-vec point-idx (next-axis parent-axis k) nil nil deleted?)))
        (let [axis (.axis node)
              element-start-idx (.getElementStartIndex point-vec (.point-idx node))
              point-axis (.get coordinates-vec (+ element-start-idx axis))]
          (cond
            (point-equals-list-element point coordinates-vec element-start-idx k)
            (node-kd-tree-build-path build-path-fns (Node. point-vec (.point-idx node) (.axis node) (.left node) (.right node) deleted?))

            (< (aget point axis) point-axis)
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

(deftype KdTreeVectorPointAccess [^FixedSizeListVector point-vec]
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
      (.get coordinates-vec (+ element-start-idx axis)))))

(extend-protocol KdTree
  Node
  (kd-tree-insert [kd-tree allocator point]
    (node-kd-tree-edit kd-tree point false))

  (kd-tree-delete [kd-tree allocator point]
    (node-kd-tree-edit kd-tree point true))

  (kd-tree-range-search [kd-tree min-range max-range]
    (let [min-range (->longs min-range)
          max-range (->longs max-range)
          ^FixedSizeListVector point-vec (.point-vec kd-tree)
          ^BigIntVector coordinates-vec (.getDataVector point-vec)
          stack (doto (ArrayDeque.)
                  (.push kd-tree))]
      (StreamSupport/intStream (NodeRangeSearchSpliterator. point-vec coordinates-vec min-range max-range stack) false)))

  (kd-tree-depth-first [kd-tree]
    (let [stack (doto (ArrayDeque.)
                  (.push kd-tree))]
      (StreamSupport/intStream (NodeDepthFirstSpliterator. stack) false)))

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
    (KdTreeVectorPointAccess. (.point-vec kd-tree)))

  (kd-tree-size [kd-tree]
    (.count ^IntStream (kd-tree-depth-first kd-tree)))

  (kd-tree-value-count [kd-tree]
    (.getValueCount ^FixedSizeListVector (.point-vec kd-tree))))

(defn kd-tree->seq
  ([kd-tree]
   (kd-tree->seq kd-tree (kd-tree-depth-first kd-tree)))
  ([kd-tree ^IntStream stream]
   (let [^IKdTreePointAccess point-access (kd-tree-point-access kd-tree)]
     (-> stream
         (.mapToObj (reify IntFunction
                      (apply [_ x]
                        (.getPoint point-access x))))
         (.iterator)
         (iterator-seq)))))

(defn rebuild-node-kd-tree ^core2.temporal.kd_tree.Node [^BufferAllocator allocator kd-tree]
  (let [^IKdTreePointAccess point-access (kd-tree-point-access kd-tree)]
    (->node-kd-tree allocator (-> ^IntStream (kd-tree-depth-first kd-tree)
                                  (.mapToObj (reify IntFunction
                                               (apply [_ x]
                                                 (.getArrayPoint point-access x))))
                                  (.toArray)))))

(def ^:private ^:const point-vec-idx 3)

(defn- ->column-kd-tree-schema ^org.apache.arrow.vector.types.pojo.Schema [^long k]
  (Schema. [(t/->field "axis-delete-flag" (.getType Types$MinorType/TINYINT) false)
            (t/->field "split-value" (.getType Types$MinorType/BIGINT) false)
            (t/->field "skip-pointer" (.getType Types$MinorType/INT) false)
            (->point-field k)]))

(defn ->column-kd-tree ^org.apache.arrow.vector.VectorSchemaRoot [^BufferAllocator allocator ^Node kd-tree ^long k]
  (let [out-root (VectorSchemaRoot/create (->column-kd-tree-schema k) allocator)
        ^TinyIntVector axis-delete-flag-vec (.getVector out-root "axis-delete-flag")
        ^BigIntVector split-value-vec (.getVector out-root "split-value")
        ^IntVector skip-pointer-vec (.getVector out-root "skip-pointer")
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

(deftype ColumnStackEntry [^int start ^int end])

(deftype ColumnRangeSearchSpliterator [^TinyIntVector axis-delete-flag-vec
                                       ^BigIntVector split-value-vec
                                       ^IntVector skip-pointer-vec
                                       ^FixedSizeListVector point-vec
                                       ^BigIntVector coordinates-vec
                                       ^longs min-range
                                       ^longs max-range
                                       ^int k
                                       ^Deque stack]
  Spliterator$OfInt
  (^void forEachRemaining [this ^IntConsumer c]
    (loop []
      (when-let [^ColumnStackEntry stack-entry (.poll stack)]
        (loop [idx (.start stack-entry)
               end-idx (.end stack-entry)]
          (when (< idx end-idx)
            (let [axis-delete-flag (int (.get axis-delete-flag-vec idx))
                  deleted? (neg? axis-delete-flag)
                  axis (dec (Math/abs axis-delete-flag))
                  axis-value (.get split-value-vec idx)
                  min-match? (< (aget min-range axis) axis-value)
                  max-match? (<= axis-value (aget max-range axis))
                  left-idx (inc idx)
                  right-idx (.get skip-pointer-vec idx)
                  right-idx (if (neg? right-idx)
                              (int end-idx)
                              right-idx)
                  visit-left? (and (not= left-idx right-idx) min-match?)
                  visit-right? (and (not= right-idx end-idx) max-match?)]

              (when (and (or min-match? max-match?)
                         (not deleted?)
                         (in-range-column? min-range point-vec coordinates-vec idx max-range))
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

  (^boolean tryAdvance [this ^IntConsumer c]
    (loop []
      (if-let [^ColumnStackEntry stack-entry (.poll stack)]
        (let [idx (.start stack-entry)
              end-idx (.end stack-entry)]
          (if (< idx end-idx)
            (let [axis-delete-flag (int (.get axis-delete-flag-vec idx))
                  deleted? (neg? axis-delete-flag)
                  axis (dec (Math/abs axis-delete-flag))
                  axis-value (.get split-value-vec idx)
                  min-match? (< (aget min-range axis) axis-value)
                  max-match? (<= axis-value (aget max-range axis))
                  left-idx (inc idx)
                  right-idx (.get skip-pointer-vec idx)
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
                       (in-range-column? min-range point-vec coordinates-vec idx max-range))
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
      (ColumnRangeSearchSpliterator. axis-delete-flag-vec split-value-vec skip-pointer-vec point-vec coordinates-vec min-range max-range k split-stack))))

(defn merge-kd-trees [^BufferAllocator allocator kd-tree-to ^VectorSchemaRoot kd-tree-from]
  (let [^TinyIntVector axis-delete-flag-vec (.getVector kd-tree-from "axis-delete-flag")
        n (.getRowCount kd-tree-from)
        ^IKdTreePointAccess from-access (kd-tree-point-access kd-tree-from)]
    (loop [idx 0
           acc kd-tree-to]
      (if (= idx n)
        acc
        (let [axis-delete-flag (.get axis-delete-flag-vec idx)
              deleted? (neg? axis-delete-flag)
              point (.getArrayPoint from-access idx)]

          (recur (inc idx)
                 (if deleted?
                   (kd-tree-delete acc allocator point)
                   (kd-tree-insert acc allocator point))))))))

(extend-protocol KdTree
  VectorSchemaRoot
  (kd-tree-insert [_ allocator point]
    (throw (UnsupportedOperationException.)))

  (kd-tree-delete [_ allocator point]
    (throw (UnsupportedOperationException.)))

  (kd-tree-range-search [kd-tree min-range max-range]
    (let [min-range (->longs min-range)
          max-range (->longs max-range)
          ^TinyIntVector axis-delete-flag-vec (.getVector kd-tree "axis-delete-flag")
          ^BigIntVector split-value-vec (.getVector kd-tree "split-value")
          ^IntVector skip-pointer-vec (.getVector kd-tree "skip-pointer")
          ^FixedSizeListVector point-vec (.getVector kd-tree "point")
          ^BigIntVector coordinates-vec (.getDataVector point-vec)
          k (.getListSize point-vec)
          stack (doto (ArrayDeque.)
                  (.push (ColumnStackEntry. 0 (.getValueCount axis-delete-flag-vec))))]
      (StreamSupport/intStream
       (ColumnRangeSearchSpliterator. axis-delete-flag-vec split-value-vec skip-pointer-vec point-vec coordinates-vec min-range max-range k stack)
       false)))

  (kd-tree-depth-first [kd-tree]
    (let [^TinyIntVector axis-delete-flag-vec (.getVector kd-tree "axis-delete-flag")]
      (.filter (IntStream/range 0 (.getValueCount axis-delete-flag-vec))
               (reify IntPredicate
                 (test [_ x]
                   (pos? (.get axis-delete-flag-vec x)))))))

  (kd-tree-depth [kd-tree]
    (let [^IntVector skip-pointer-vec (.getVector kd-tree "skip-pointer")
          ^FixedSizeListVector point-vec (.getVector kd-tree "point")]
      ((fn step [^long idx ^long end-idx]
         (if (< idx end-idx)
           (let [left-idx (inc idx)
                 right-idx (.get skip-pointer-vec idx)
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
           0)) 0 (.getValueCount point-vec))))

  (kd-tree-retain [this allocator]
    (util/slice-root this 0))

  (kd-tree-point-access [kd-tree]
    (KdTreeVectorPointAccess. (.getVector kd-tree point-vec-idx)))

  (kd-tree-size [kd-tree]
    (.count ^IntStream (kd-tree-depth-first kd-tree)))

  (kd-tree-value-count [kd-tree]
    (.getValueCount (.getVector kd-tree point-vec-idx))))

(def ^:private ^java.lang.reflect.Field arrow-record-buffers-layout-field
  (doto (.getDeclaredField ArrowRecordBatch "buffersLayout")
    (.setAccessible true)))

(defn- ->empty-record-batch ^org.apache.arrow.vector.ipc.message.ArrowRecordBatch [^BufferAllocator allocator ^Schema schema ^long n]
  (let [step-fn (fn step [^long multiplier ^Field f]
                  (let [t (.getType f)]
                    (cons {:field-node (ArrowFieldNode. multiplier 0)
                           :sizes (vec (for [^BufferLayout bl (.getBufferLayouts (TypeLayout/getTypeLayout t))]
                                         (long (Math/ceil (/ (* multiplier (.getTypeBitWidth bl)) Byte/SIZE)))))}
                          (mapcat (partial step (if (instance? ArrowType$FixedSizeList t)
                                                  (* multiplier (.getListSize ^ArrowType$FixedSizeList t))
                                                  multiplier))
                                  (.getChildren f)))))
        field-nodes+sizes (mapcat (partial step-fn n) (.getFields schema))
        field-nodes (mapv :field-node field-nodes+sizes)
        sizes (mapcat :sizes field-nodes+sizes)
        offsets (reductions
                 (fn [^long offset ^long size]
                   (DataSizeRoundingUtil/roundUpTo8Multiple (+ offset size)))
                 0
                 sizes)
        buffers (vec (for [[offset size] (map vector offsets sizes)]
                       (ArrowBuffer. offset size)))
        ^ArrowBuffer last-buffer (last buffers)
        buffer-length (DataSizeRoundingUtil/roundUpTo8Multiple (+ (.getOffset last-buffer)
                                                                  (.getSize last-buffer)))
        record-batch (proxy [ArrowRecordBatch] [n field-nodes (repeat (count buffers) (.getEmpty allocator))]
                       (computeBodyLength []
                         buffer-length))]
    (.set arrow-record-buffers-layout-field record-batch buffers)
    record-batch))

(def ^:private ^java.lang.reflect.Field write-channel-current-position-field
  (doto (.getDeclaredField WriteChannel "currentPosition")
    (.setAccessible true)))

(defn- ->empty-disk-kd-tree ^java.nio.file.Path [^BufferAllocator allocator ^Path path n k]
  (util/mkdirs (.getParent path))
  (let [^Schema schema (->column-kd-tree-schema k)
        ^long n n
        ^long k k]
    (with-open [ch (util/->file-channel path util/write-new-file-opts)
                write-ch (WriteChannel. ch)]
      (doto write-ch
        (.write util/arrow-magic)
        (.align))

      (MessageSerializer/serialize write-ch schema)

      (let [^ArrowRecordBatch record-batch (->empty-record-batch allocator schema n)
            buffer-length (.computeBodyLength record-batch)
            start (.getCurrentPosition write-ch)
            metadata (MessageSerializer/serializeMetadata record-batch)
            metadata-length (.remaining metadata)
            prefix-size 8
            padding (long (mod (+ start metadata-length prefix-size) 8))
            metadata-length (long (if (zero? padding)
                                    metadata-length
                                    (+ metadata-length (- 8 padding))))]
        (doto write-ch
          (.writeIntLittleEndian MessageSerializer/IPC_CONTINUATION_TOKEN)
          (.writeIntLittleEndian metadata-length)
          (.write metadata)
          (.align))

        (.position ch (+ (.getCurrentPosition write-ch) buffer-length))
        (.set write-channel-current-position-field write-ch (.position ch))

        (doto write-ch
          (.writeIntLittleEndian MessageSerializer/IPC_CONTINUATION_TOKEN)
          (.writeIntLittleEndian 0))

        (let [block (ArrowBlock. start (+ metadata-length prefix-size) buffer-length)
              footer (ArrowFooter. schema [] [block])
              footer-size (.write write-ch footer false)]
          (.writeIntLittleEndian write-ch footer-size)))

      (.write write-ch util/arrow-magic))
    path))

(defn- write-points-in-place [^VectorSchemaRoot kd-tree points]
  (let [^FixedSizeListVector point-vec (.getVector kd-tree "point")
        ^BigIntVector coordinates-vec (.getDataVector point-vec)
        k (.getListSize point-vec)]
    (if (satisfies? KdTree points)
      (let [^IKdTreePointAccess point-access (kd-tree-point-access points)]
        (.reduce ^IntStream (kd-tree-depth-first points)
                 0
                 (reify IntBinaryOperator
                   (applyAsInt [_ idx point-idx]
                     (let [list-idx (.getElementStartIndex point-vec idx)]
                       (dotimes [n k]
                         (.set coordinates-vec (+ list-idx n) (.getCoordinate point-access point-idx n)))
                       (inc idx))))))
      (reduce
       (fn [^long idx point]
         (let [list-idx (.getElementStartIndex point-vec idx)]
           (write-coordinates coordinates-vec list-idx point)
           (inc idx)))
       0
       points))))

(defn- build-tree-in-place [^BufferAllocator allocator ^VectorSchemaRoot kd-tree points]
  (let [^TinyIntVector axis-delete-flag-vec (.getVector kd-tree "axis-delete-flag")
        ^BigIntVector split-value-vec (.getVector kd-tree "split-value")
        ^IntVector skip-pointer-vec (.getVector kd-tree "skip-pointer")
        ^FixedSizeListVector point-vec (.getVector kd-tree "point")
        ^BigIntVector coordinates-vec (.getDataVector point-vec)
        k (.getListSize point-vec)]

    (write-points-in-place kd-tree points)

    ((fn step [^long start ^long end ^long axis]
       (let [median (quick-select point-vec start end axis)
             next-axis (next-axis axis k)
             axis-delete-flag (inc axis)]
         (when-not (= start median)
           (swap-point point-vec coordinates-vec start median))
         (.set axis-delete-flag-vec start axis-delete-flag)
         (.copyFrom split-value-vec (+ (.getElementStartIndex point-vec start) axis) start coordinates-vec)
         (when (< start median)
           (step (inc start) (inc median) next-axis))
         (if (< (inc median) end)
           (do (.set skip-pointer-vec start (inc median))
               (step (inc median) end next-axis))
           (.set skip-pointer-vec start -1))
         false))
     0 (.getValueCount point-vec) 0)))

(defn ->disk-kd-tree
  (^java.nio.file.Path [^BufferAllocator allocator ^Path path points k]
   (->disk-kd-tree allocator path points (if (satisfies? KdTree points)
                                           (kd-tree-size points)
                                           (count points)) k))
  (^java.nio.file.Path [^BufferAllocator allocator ^Path path points n k]
   (let [^Path path (->empty-disk-kd-tree allocator path n k)
         nio-buffer (util/->mmap-path path FileChannel$MapMode/READ_WRITE)]
     (with-open [arrow-buf (util/->arrow-buf-view allocator nio-buffer)
                 chunks (util/->chunks arrow-buf)]
       (.tryAdvance chunks (reify Consumer
                             (accept [_ root]
                               (build-tree-in-place allocator root points))))
       (.force nio-buffer))
     path)))

(defn ->mmap-kd-tree ^org.apache.arrow.vector.VectorSchemaRoot [^BufferAllocator allocator ^Path path]
  (let [nio-buffer (util/->mmap-path path)
        res (promise)]
    (with-open [arrow-buf (util/->arrow-buf-view allocator nio-buffer)
                chunks (util/->chunks arrow-buf)]
      (.tryAdvance chunks (reify Consumer
                            (accept [_ root]
                              (deliver res (util/slice-root root 0))))))
    @res))

(deftype MergedKdTreePointAccess [^IKdTreePointAccess static-access ^IKdTreePointAccess dynamic-access ^int static-value-count]
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
      (.getCoordinate dynamic-access (- idx static-value-count) axis))))

(definterface IMergedKdTreeAccess
  (^Object getDynamicTree []))

(deftype MergedKdTree [static-kd-tree ^:unsynchronized-mutable dynamic-kd-tree ^RoaringBitmap static-delete-bitmap ^int static-size ^int static-value-count]
  KdTree
  (kd-tree-insert [this allocator point]
    (set! (.dynamic-kd-tree this) (kd-tree-insert dynamic-kd-tree allocator point))
    this)

  (kd-tree-delete [this allocator point]
    (set! (.dynamic-kd-tree this) (kd-tree-delete dynamic-kd-tree allocator point))
    (.forEach ^IntStream (kd-tree-range-search static-kd-tree point point)
              (reify IntConsumer
                (accept [_ x]
                  (.add static-delete-bitmap x))))
    this)

  (kd-tree-range-search [_ min-range max-range]
    (IntStream/concat (.filter ^IntStream (kd-tree-range-search static-kd-tree min-range max-range)
                               (reify IntPredicate
                                 (test [_ x]
                                   (not (.contains static-delete-bitmap x)))))
                      (.map ^IntStream (kd-tree-range-search dynamic-kd-tree min-range max-range)
                            (reify IntUnaryOperator
                              (applyAsInt [_ x]
                                (+ static-value-count x))))))

  (kd-tree-depth-first [_]
    (IntStream/concat (.filter ^IntStream (kd-tree-depth-first static-kd-tree)
                               (reify IntPredicate
                                 (test [_ x]
                                   (not (.contains static-delete-bitmap x)))))
                      (.map ^IntStream (kd-tree-depth-first dynamic-kd-tree)
                            (reify IntUnaryOperator
                              (applyAsInt [_ x]
                                (+ static-value-count x))))))

  (kd-tree-depth [_]
    (max (long (kd-tree-depth static-kd-tree))
         (long (kd-tree-depth dynamic-kd-tree))))

  (kd-tree-retain [kd-tree allocator]
    (MergedKdTree. (kd-tree-retain (.static-kd-tree kd-tree) allocator)
                   (kd-tree-retain (.dynamic-kd-tree kd-tree) allocator)
                   (.clone ^RoaringBitmap (.static-delete-bitmap kd-tree))
                   (.static-size kd-tree)
                   (.static-value-count kd-tree)))

  (kd-tree-point-access [kd-tree]
    (MergedKdTreePointAccess. (kd-tree-point-access static-kd-tree) (kd-tree-point-access dynamic-kd-tree) static-value-count))

  (kd-tree-size [kd-tree]
    (+ (- static-size (.getCardinality static-delete-bitmap))
       (long (kd-tree-size dynamic-kd-tree))))

  (kd-tree-value-count [kd-tree]
    (+ (long (kd-tree-value-count static-kd-tree))
       (long (kd-tree-value-count dynamic-kd-tree))))

  IMergedKdTreeAccess
  (getDynamicTree [_]
    dynamic-kd-tree)

  Closeable
  (close [_]
    (util/try-close static-kd-tree)
    (util/try-close dynamic-kd-tree)
    (.clear static-delete-bitmap)))

(defn ->merged-kd-tree
  (^core2.temporal.kd_tree.MergedKdTree [static-kd-tree]
   (->merged-kd-tree static-kd-tree nil))
  (^core2.temporal.kd_tree.MergedKdTree [static-kd-tree dynamic-kd-tree]
   (MergedKdTree. static-kd-tree dynamic-kd-tree (RoaringBitmap.) (kd-tree-size static-kd-tree) (kd-tree-value-count static-kd-tree))))

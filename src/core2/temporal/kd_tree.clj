(ns core2.temporal.kd-tree
  (:require [core2.types :as t])
  (:import [java.util ArrayDeque ArrayList Arrays Collection Comparator Date Deque HashMap
            IdentityHashMap List Map Spliterator Spliterators]
           [java.util.function Consumer Function Predicate ToLongFunction]
           [java.util.stream StreamSupport]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector BigIntVector IntVector TinyIntVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex StructVector]
           [org.apache.arrow.vector.types.pojo Field Schema]
           [org.apache.arrow.vector.types Types$MinorType]
           core2.temporal.TemporalCoordinates))

;; TODO:

;; Step 1:

;; Make Arrow trees use stack-less skip pointer traversal and balance
;; their median points like the node tree. This would require three
;; meta columns: the axis split value (long), the skip pointer index
;; (int) and a flag with axis + deletion bit (byte). Storing the axis
;; is necessary to resume traversal after following a skip
;; pointer. The points themselves can be stored as either a struct of
;; k columns, or as a fixed list vector with k elements per list.

;; For stack-less skip pointer traversal, see Figure 6 here:
;; http://www.cse.chalmers.se/edu/year/2009/course/TDA361/EfficiencyIssuesForRayTracing.pdf

;; Store the active tree as temporal-<chunk-idx>.arrow in the object
;; store when registering chunks.

;; We then read these chunks instead of scanning the columns to build
;; the new in-memory tree on start-up. Needs to take deletions into
;; account. Queries are always performed against this in-memory tree.

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

(defprotocol KdTree
  (kd-tree-insert [_ point])
  (kd-tree-delete [_ point])
  (kd-tree-range-search [_ min-range max-range])
  (kd-tree-depth-first [_]))

(defrecord Node [^longs point left right deleted?])

(defn- leaf? [^Node node]
  (and (nil? (.left node))
       (nil? (.right node))))

(defmacro ^:private next-axis [axis k]
  `(let [next-axis# (unchecked-inc-int ~axis)]
     (if (= ~k next-axis#)
       (int 0)
       next-axis#)))

(def ^:private ^Class longs-class (Class/forName "[J"))

(defn- ->longs ^longs [xs]
  (if (instance? longs-class xs)
    xs
    (long-array xs)))

(extend-protocol KdTree
  nil
  (kd-tree-insert [_ point]
    (Node. (->longs point) nil nil false))
  (kd-tree-delete [_ point]
    (Node. (->longs point) nil nil true))
  (kd-tree-range-search [_ _ _]
    (Spliterators/emptySpliterator))
  (kd-tree-depth-first [_]
    (Spliterators/emptySpliterator)))

(def ^:private ^Class objects-class
  (Class/forName "[Ljava.lang.Object;"))

(defn- find-median-index ^long [^objects points ^long start ^long end ^long axis]
  (let [median (quot (+ start end) 2)
        ^longs median-point (aget points median)
        median-value (aget median-point axis)]
    (loop [idx median]
      (if (= start idx)
        idx
        (let [prev-idx (dec idx)]
          (if (= median-value (aget ^longs (aget points prev-idx) axis))
            (recur prev-idx)
            idx))))))

(defn- ensure-points-array ^objects [points]
  (let [^objects points (if (instance? objects-class points)
                          points
                          (object-array points))]
    (dotimes [idx (alength points)]
      (let [point (aget points idx)]
        (when-not (instance? longs-class point)
          (aset points idx (->longs point)))))
    points))

(defn ->node-kd-tree ^core2.temporal.kd_tree.Node [points]
  (when (not-empty points)
    (let [^objects points (ensure-points-array points)
          n (alength points)
          k (count (aget points 0))]
      ((fn step [^long start ^long end ^long axis]
         (if (= (inc start) end)
           (Node. (aget points start) nil nil false)
           (do (Arrays/sort points start end (Comparator/comparingLong
                                              (reify ToLongFunction
                                                (applyAsLong [_ x]
                                                  (aget ^longs x axis)))))
               (let [median (find-median-index points start end axis)
                     axis (next-axis axis k)]
                 (Node. (aget points median)
                        (when (< start median)
                          (step start median axis))
                        (when (< (inc median) end)
                          (step (inc median) end axis))
                        false)))))
       0 (alength points) 0))))

(defn kd-tree->seq [kd-tree]
  (iterator-seq (Spliterators/iterator ^Spliterator (kd-tree-depth-first kd-tree))))

(defn rebuild-node-kd-tree [^Node kd-tree]
  (->node-kd-tree (.toArray (StreamSupport/stream ^Spliterator (kd-tree-depth-first kd-tree) false))))

(deftype NodeStackEntry [^Node node ^int axis])

(defmacro in-range? [mins xs maxs]
  `(let [mins# ~mins
         xs# ~xs
         maxs# ~maxs
         len# (alength xs#)]
     (loop [n# (int 0)]
       (if (= n# len#)
         true
         (let [x# (aget xs# n#)]
           (if (and (<= (aget mins# n#) x#)
                    (<= x# (aget maxs# n#)))
             (recur (inc n#))
             false))))))

(deftype NodeRangeSearchSpliterator [^longs min-range ^longs max-range ^int k ^Deque stack]
  Spliterator
  (forEachRemaining [_ c]
    (loop []
      (when-let [^NodeStackEntry entry (.poll stack)]
        (loop [^Node node (.node entry)
               axis (.axis entry)]
          (let [^longs point (.point node)
                left (.left node)
                right (.right node)
                point-axis (aget point axis)
                min-axis (aget min-range axis)
                max-axis (aget max-range axis)
                min-match? (< min-axis point-axis)
                max-match? (<= point-axis max-axis)
                visit-left? (and left min-match?)
                visit-right? (and right max-match?)
                axis (next-axis axis k)]

            (when (and (or min-match? max-match?)
                       (not (.deleted? node))
                       (in-range? min-range point max-range))
              (.accept c point))

            (cond
              (and visit-left? (not visit-right?))
              (recur left axis)

              (and visit-right? (not visit-left?))
              (recur right axis)

              :else
              (do (when visit-right?
                    (.push stack (NodeStackEntry. right axis)))

                  (when visit-left?
                    (recur left axis))))))
        (recur))))

  (tryAdvance [_ c]
    (loop []
      (if-let [^NodeStackEntry entry (.poll stack)]
        (let [^Node node (.node entry)
              axis (.axis entry)
              ^longs point (.point node)
              left (.left node)
              right (.right node)
              point-axis (aget point axis)
              min-axis (aget min-range axis)
              max-axis (aget max-range axis)
              min-match? (< min-axis point-axis)
              max-match? (<= point-axis max-axis)
              visit-left? (and left min-match?)
              visit-right? (and right max-match?)
              axis (next-axis axis k)]

          (when visit-right?
            (.push stack (NodeStackEntry. right axis)))

          (when visit-left?
            (.push stack (NodeStackEntry. left axis)))

          (if (and (or min-match? max-match?)
                   (not (.deleted? node))
                   (in-range? min-range point max-range))
            (do (.accept c point)
                true)
            (recur)))
        false)))

  (characteristics [_]
    (bit-or Spliterator/DISTINCT Spliterator/IMMUTABLE Spliterator/NONNULL Spliterator/ORDERED))

  (estimateSize [_]
    Long/MAX_VALUE)

  (trySplit [_]
    (let [split-size (quot (.size stack) 2)]
      (when (pos? split-size)
        (let [split-stack (ArrayDeque.)]
          (while (not= split-size (.size split-stack))
            (.push split-stack (.poll stack)))
          (NodeRangeSearchSpliterator. min-range max-range k split-stack))))))

(deftype NodeDepthFirstSpliterator [^int k ^Deque stack]
  Spliterator
  (forEachRemaining [_ c]
    (loop []
      (when-let [^NodeStackEntry entry (.poll stack)]
        (loop [^Node node (.node entry)
               axis (.axis entry)]
         (let [axis (next-axis axis k)
               left (.left node)
               right (.right node)]

           (when-not (.deleted? node)
             (.accept c (.point node)))

           (cond
             (and left (nil? right))
             (recur left axis)

             (and right (nil? left))
             (recur right axis)

             :else
             (do (when right
                   (.push stack (NodeStackEntry. right axis)))
                 (when left
                   (recur left axis))))))
        (recur))))

  (tryAdvance [_ c]
    (loop []
      (if-let [^NodeStackEntry entry (.poll stack)]
        (let [^Node node (.node entry)
              axis (.axis entry)
              axis (next-axis axis k)]
          (when-let [right (.right node)]
            (.push stack (NodeStackEntry. right axis)))
          (when-let [left (.left node)]
            (.push stack (NodeStackEntry. left axis)))

          (if-not (.deleted? node)
            (do (.accept c (.point node))
                true)
            (recur)))
        false)))

  (characteristics [_]
    (bit-or Spliterator/DISTINCT Spliterator/IMMUTABLE Spliterator/NONNULL Spliterator/ORDERED))

  (estimateSize [_]
    Long/MAX_VALUE)

  (trySplit [_]
    (let [split-size (quot (.size stack) 2)]
      (when (pos? split-size)
        (let [split-stack (ArrayDeque.)]
          (while (not= split-size (.size split-stack))
            (.push split-stack (.poll stack)))
          (NodeDepthFirstSpliterator. k split-stack))))))

(extend-protocol KdTree
  Node
  (kd-tree-insert [kd-tree point]
    (let [point (->longs point)
          k (alength point)]
      (loop [axis 0
             node kd-tree
             build-fn identity]
        (if-not node
          (build-fn (Node. point nil nil false))
          (let [^longs node-point (.point node)
                point-axis (aget node-point axis)]
            (cond
              (Arrays/equals point node-point)
              (build-fn (assoc node :deleted? false))

              (< (aget point axis) point-axis)
              (recur (next-axis axis k) (.left node) (comp build-fn (partial assoc node :left)))

              :else
              (recur (next-axis axis k) (.right node) (comp build-fn (partial assoc node :right)))))))))

  (kd-tree-delete [kd-tree point]
    (let [point (->longs point)
          k (alength point)]
      (loop [axis 0
             node kd-tree
             build-fn identity]
        (if-not node
          (build-fn (Node. point nil nil true))
          (let [^longs node-point (.point node)
                point-axis (aget node-point axis)]
            (cond
              (Arrays/equals point node-point)
              (build-fn (when-not (leaf? node)
                          (assoc node :deleted? true)))

              (< (aget point axis) point-axis)
              (recur (next-axis axis k) (.left node) (comp build-fn (partial assoc node :left)))

              :else
              (recur (next-axis axis k) (.right node) (comp build-fn (partial assoc node :right)))))))))

  (kd-tree-range-search [kd-tree min-range max-range]
    (let [min-range (->longs min-range)
          max-range (->longs max-range)
          k (count (some-> kd-tree (.point)))
          stack (doto (ArrayDeque.)
                  (.push (NodeStackEntry. kd-tree 0)))]
      (->NodeRangeSearchSpliterator min-range max-range k stack)))

  (kd-tree-depth-first [kd-tree]
    (let [k (count (some-> kd-tree (.point)))
          stack (doto (ArrayDeque.)
                  (.push (NodeStackEntry. kd-tree 0)))]
      (->NodeDepthFirstSpliterator k stack))))

(defn- ->column-kd-tree-schema ^org.apache.arrow.vector.types.pojo.Schema [^long k]
  (Schema. [(t/->field "axis_delete_flag" (.getType Types$MinorType/TINYINT) false)
            (t/->field "split_value" (.getType Types$MinorType/BIGINT) false)
            (t/->field "skip_pointer" (.getType Types$MinorType/INT) false)
            (apply t/->field "points" (.getType Types$MinorType/STRUCT) false
                    (for [idx (range k)]
                      (t/->field (str idx) (.getType Types$MinorType/BIGINT) false)))]))

(defn ->column-kd-tree ^org.apache.arrow.vector.VectorSchemaRoot [^BufferAllocator allocator ^Node kd-tree ^long k]
  (let [out-root (VectorSchemaRoot/create (->column-kd-tree-schema k) allocator)
        ^TinyIntVector axis-delete-flag-vec (.getVector out-root "axis_delete_flag")
        ^BigIntVector split-value-vec (.getVector out-root "split_value")
        ^IntVector skip-pointer-vec (.getVector out-root "skip_pointer")
        ^StructVector points-struct-vec (.getVector out-root "points")
        point-vecs (.getChildrenFromFields points-struct-vec)
        stack (ArrayDeque.)
        node->skip-idx-update (IdentityHashMap.)]
    (when kd-tree
      (.push stack (NodeStackEntry. kd-tree 0)))
    (loop [idx 0]
      (if-let [^NodeStackEntry stack-entry (.poll stack)]
        (let [^Node node (.node stack-entry)
              deleted? (.deleted? node)
              ^longs point (.point node)
              axis (.axis stack-entry)
              axis-delete-flag (if deleted?
                                 (- (inc axis))
                                 (inc axis))]
          (.setIndexDefined points-struct-vec idx)
          (.setSafe axis-delete-flag-vec idx axis-delete-flag)
          (.setSafe split-value-vec idx (aget point axis))
          (.setSafe skip-pointer-vec idx -1)
          (when-let [skip-idx (.remove node->skip-idx-update node)]
            (.setSafe skip-pointer-vec ^long skip-idx idx))
          (dotimes [n k]
            (let [^BigIntVector v (.get point-vecs n)]
              (.setSafe v idx (aget point n))))

          (let [axis (next-axis axis k)]
            (when-let [right (.right node)]
              (.put node->skip-idx-update right idx)
              (.push stack (NodeStackEntry. right axis)))
            (when-let [left (.left node)]
              (.push stack (NodeStackEntry. left axis)))
            (recur (inc idx))))
        (doto out-root
          (.setRowCount idx))))))

(defmacro ^:private in-range-column? [mins xs k idx maxs]
  (let [col-sym (with-meta (gensym "col") {:tag `BigIntVector})]
    `(let [point# (long-array ~k)]
       (loop [n# (int 0)]
         (if (= n# ~k)
           point#
           (let [~col-sym (.get ~xs n#)
                 x# (.get ~col-sym ~idx)]
             (when (and (<= (aget ~mins n#) x#)
                        (<= x# (aget ~maxs n#)))
               (aset point# n# x#)
               (recur (inc n#)))))))))

(deftype ColumnStackEntry [^int start ^int end])

(deftype ColumnRangeSearchSpliterator [^TinyIntVector axis-delete-flag-vec
                                       ^BigIntVector split-value-vec
                                       ^IntVector skip-pointer-vec
                                       ^List point-vecs
                                       ^longs min-range
                                       ^longs max-range
                                       ^int k
                                       ^Deque stack]
  Spliterator
  (forEachRemaining [this c]
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

              (when-let [point (and (or min-match? max-match?)
                                    (not deleted?)
                                    (in-range-column? min-range point-vecs k idx max-range))]
                (.accept c point))

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

  (tryAdvance [this c]
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

              (if-let [point (and (or min-match? max-match?)
                                  (not deleted?)
                                  (in-range-column? min-range point-vecs k idx max-range))]
                (do (.accept c point)
                    true)
                (recur)))
            (recur)))
        false)))

  (characteristics [_]
    (bit-or Spliterator/DISTINCT Spliterator/IMMUTABLE Spliterator/NONNULL Spliterator/ORDERED))

  (estimateSize [_]
    Long/MAX_VALUE)

  (trySplit [_]))


(deftype ColumnDepthFirstSpliterator [^TinyIntVector axis-delete-flag-vec
                                      ^List point-vecs
                                      ^int k
                                      ^:unsynchronized-mutable ^int idx]
  Spliterator
  (forEachRemaining [this c]
    (loop [idx idx]
      (if (= idx (.getValueCount axis-delete-flag-vec))
        (set! (.idx this) (.getValueCount axis-delete-flag-vec))
        (let [axis-delete-flag (.get axis-delete-flag-vec idx)
              deleted? (neg? axis-delete-flag)]

          (when-not deleted?
            (let [point (long-array k)]
              (dotimes [n k]
                (aset point n (.get ^BigIntVector (.get point-vecs n) idx)))
              (.accept c point)))

          (recur (inc idx))))))

  (tryAdvance [this c]
    (loop []
      (if (= idx (.getValueCount axis-delete-flag-vec))
        false
        (let [current-idx idx
              axis-delete-flag (.get axis-delete-flag-vec current-idx)
              deleted? (neg? axis-delete-flag)]

          (set! (.idx this) (inc current-idx))

          (if deleted?
            (recur)
            (let [point (long-array k)]
              (dotimes [n k]
                (aset point n (.get ^BigIntVector (.get point-vecs n) current-idx)))
              (.accept c point)
              true))))))

  (characteristics [_]
    (bit-or Spliterator/DISTINCT Spliterator/IMMUTABLE Spliterator/NONNULL Spliterator/ORDERED))

  (estimateSize [_]
    Long/MAX_VALUE)

  (trySplit [_]))

(defn merge-kd-trees ^core2.temporal.kd_tree.Node [^Node kd-tree-to ^VectorSchemaRoot kd-tree-from]
  (let [^TinyIntVector axis-delete-flag-vec (.getVector kd-tree-from "axis_delete_flag")
        point-vecs (.getChildrenFromFields ^StructVector (.getVector kd-tree-from "points"))
        k (.size point-vecs)
        n (.getRowCount kd-tree-from)]
    (loop [idx 0
           acc kd-tree-to]
      (if (= idx n)
        acc
        (let [axis-delete-flag (.get axis-delete-flag-vec idx)
              deleted? (neg? axis-delete-flag)
              point (long-array k)]

          (dotimes [n k]
            (aset point n (.get ^BigIntVector (.get point-vecs n) idx)))

          (recur (inc idx)
                 (if deleted?
                   (kd-tree-delete acc point)
                   (kd-tree-insert acc point))))))))

(extend-protocol KdTree
  VectorSchemaRoot
  (kd-tree-insert [_ point]
    (throw (UnsupportedOperationException.)))

  (kd-tree-delete [_ point]
    (throw (UnsupportedOperationException.)))

  (kd-tree-range-search [kd-tree min-range max-range]
    (let [min-range (->longs min-range)
          max-range (->longs max-range)
          ^TinyIntVector axis-delete-flag-vec (.getVector kd-tree "axis_delete_flag")
          ^BigIntVector split-value-vec (.getVector kd-tree "split_value")
          ^IntVector skip-pointer-vec (.getVector kd-tree "skip_pointer")
          point-vecs (.getChildrenFromFields ^StructVector (.getVector kd-tree "points"))
          k (.size point-vecs)
          stack (doto (ArrayDeque.)
                  (.push (ColumnStackEntry. 0 (.getValueCount axis-delete-flag-vec))))]
      (->ColumnRangeSearchSpliterator axis-delete-flag-vec split-value-vec skip-pointer-vec point-vecs min-range max-range k stack)))

  (kd-tree-depth-first [kd-tree]
    (let [^TinyIntVector axis-delete-flag-vec (.getVector kd-tree "axis_delete_flag")
          point-vecs (.getChildrenFromFields ^StructVector (.getVector kd-tree "points"))
          k (.size point-vecs)]
      (->ColumnDepthFirstSpliterator axis-delete-flag-vec point-vecs k 0))))

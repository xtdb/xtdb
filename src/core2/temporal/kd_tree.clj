(ns core2.temporal.kd-tree
  (:import [java.util ArrayDeque ArrayList Arrays Collection Comparator Date Deque HashMap
            List Map Spliterator Spliterator$OfInt Spliterators]
           [java.util.function Consumer IntConsumer Function Predicate ToLongFunction]
           [java.util.stream StreamSupport]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector BigIntVector VectorSchemaRoot]
           core2.temporal.TemporalCoordinates))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol KdTree
  (kd-tree-insert [_ location])
  (kd-tree-delete [_ location])
  (kd-tree-range-search [_ min-range max-range])
  (kd-tree-depth-first [_]))

(defrecord Node [^longs location left right deleted?])

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
  (kd-tree-insert [_ location]
    (Node. (->longs location) nil nil false))
  (kd-tree-delete [_ _])
  (kd-tree-range-search [_ _ _]
    (Spliterators/emptySpliterator))
  (kd-tree-depth-first [_]
    (Spliterators/emptySpliterator)))

(def ^:private ^Class objects-class
  (Class/forName "[Ljava.lang.Object;"))

(defn ->node-kd-tree ^core2.temporal.kd_tree.Node [points]
  (when (not-empty points)
    (let [^objects points (if (instance? objects-class points)
                            points
                            (object-array points))
          n (alength points)
          k (count (aget points 0))]
      (dotimes [idx n]
        (let [point (aget points idx)]
          (when-not (instance? longs-class point)
            (aset points idx (->longs point)))))
      ((fn step [^long start ^long end ^long axis]
         (if (= (inc start) end)
           (Node. (aget points start) nil nil false)
           (do (Arrays/sort points start end (Comparator/comparingLong
                                              (reify ToLongFunction
                                                (applyAsLong [_ x]
                                                  (aget ^longs x axis)))))
               (let [median (quot (+ start end) 2)
                     ^longs median-point (aget points median)
                     median-value (aget median-point axis)
                     ^long median (loop [idx median]
                                    (if (= start idx)
                                      idx
                                      (let [prev-idx (dec idx)]
                                        (if (= median-value (aget ^longs (aget points prev-idx) axis))
                                          (recur prev-idx)
                                          idx))))
                     axis (next-axis axis k)]
                 (Node. (aget points median)
                        (when (< start median)
                          (step start median axis))
                        (when (< (inc median) end)
                          (step (inc median) end axis))
                        false)))))
       0 (alength points) 0))))

(defn node-kd-tree->seq [^Node kd-tree]
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
          (let [^longs location (.location node)
                left (.left node)
                right (.right node)
                location-axis (aget location axis)
                min-axis (aget min-range axis)
                max-axis (aget max-range axis)
                min-match? (< min-axis location-axis)
                max-match? (<= location-axis max-axis)
                visit-left? (and left min-match?)
                visit-right? (and right max-match?)
                axis (next-axis axis k)]

            (when (and (or min-match? max-match?)
                       (not (.deleted? node))
                       (in-range? min-range location max-range))
              (.accept c location))

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
              ^longs location (.location node)
              left (.left node)
              right (.right node)
              location-axis (aget location axis)
              min-axis (aget min-range axis)
              max-axis (aget max-range axis)
              min-match? (< min-axis location-axis)
              max-match? (<= location-axis max-axis)
              visit-left? (and left min-match?)
              visit-right? (and right max-match?)
              axis (next-axis axis k)]

          (when visit-right?
            (.push stack (NodeStackEntry. right axis)))

          (when visit-left?
            (.push stack (NodeStackEntry. left axis)))

          (if (and (or min-match? max-match?)
                   (not (.deleted? node))
                   (in-range? min-range location max-range))
            (do (.accept c location)
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
             (.accept c (.location node)))

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
            (do (.accept c (.location node))
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
  (kd-tree-insert [kd-tree location]
    (let [location (->longs location)
          k (alength location)]
      (loop [axis 0
             node kd-tree
             build-fn identity]
        (if-not node
          (build-fn (Node. location nil nil false))
          (let [^longs location-node (.location node)
                location-axis (aget location-node axis)]
            (cond
              (Arrays/equals location location-node)
              (build-fn node)

              (< (aget location axis) location-axis)
              (recur (next-axis axis k) (.left node) (comp build-fn (partial assoc node :left)))

              :else
              (recur (next-axis axis k) (.right node) (comp build-fn (partial assoc node :right)))))))))

  (kd-tree-delete [kd-tree location]
    (let [location (->longs location)
          k (alength location)]
      (loop [axis 0
             node kd-tree
             build-fn identity]
        (if-not node
          (build-fn nil)
          (let [^longs location-node (.location node)
                location-axis (aget location-node axis)]
            (cond
              (Arrays/equals location location-node)
              (build-fn (when-not (leaf? node)
                          (assoc node :deleted? true)))

              (< (aget location axis) location-axis)
              (recur (next-axis axis k) (.left node) (comp build-fn (partial assoc node :left)))

              :else
              (recur (next-axis axis k) (.right node) (comp build-fn (partial assoc node :right)))))))))

  (kd-tree-range-search [kd-tree min-range max-range]
    (let [min-range (->longs min-range)
          max-range (->longs max-range)
          k (count (some-> kd-tree (.location)))
          stack (doto (ArrayDeque.)
                  (.push (NodeStackEntry. kd-tree 0)))]
      (->NodeRangeSearchSpliterator min-range max-range k stack)))

  (kd-tree-depth-first [kd-tree]
    (let [k (count (some-> kd-tree (.location)))
          stack (doto (ArrayDeque.)
                  (.push (NodeStackEntry. kd-tree 0)))]
      (->NodeDepthFirstSpliterator k stack))))

;; TODO: these don't try to balance their median points like the Node
;; tree. Are not used yet, will be revisited as and when we start
;; storing temporal Arrow chunks.

(deftype ColumnStackEntry [^int start ^int end ^int axis])

(defn ->column-kd-tree ^org.apache.arrow.vector.VectorSchemaRoot [^BufferAllocator allocator points]
  (let [points (object-array (mapv ->longs points))
        n (alength points)
        k (alength ^longs (aget points 0))
        columns (VectorSchemaRoot. ^List (repeatedly k #(doto (BigIntVector. "" allocator)
                                                          (.setInitialCapacity n)
                                                          (.allocateNew))))
        stack (doto (ArrayDeque.)
                (.push (ColumnStackEntry. 0 (alength points) 0)))]
    (loop []
      (when-let [^ColumnStackEntry entry (.poll stack)]
        (let [start (.start entry)
              end (.end entry)
              axis (.axis entry)]
          (Arrays/sort points start end (Comparator/comparingLong
                                         (reify ToLongFunction
                                           (applyAsLong [_ x]
                                             (aget ^longs x axis)))))
          (let [median (quot (+ start end) 2)
                axis (next-axis axis k)]
            (when (< (inc median) end)
              (.push stack (ColumnStackEntry. (inc median) end axis)))
            (when (< start median)
              (.push stack (ColumnStackEntry. start median axis)))))
        (recur)))
    (dotimes [n k]
      (let [^BigIntVector v (.getVector columns n)]
        (dotimes [m (alength points)]
          (.set v m (aget ^longs (aget points m) n)))))
    (doto columns
      (.setRowCount n))))

(defmacro ^:private in-range-column? [mins xs k idx maxs]
  (let [col-sym (with-meta (gensym "col") {:tag `BigIntVector})]
    `(let [idx# ~idx
           mins# ~mins
           xs# ~xs
           maxs# ~maxs
           len# ~k]
       (loop [n# (int 0)]
         (if (= n# len#)
           true
           (let [~col-sym (.getVector xs# n#)
                 x# (.get ~col-sym idx#)]
             (if (and (<= (aget mins# n#) x#)
                      (<= x# (aget maxs# n#)))
               (recur (inc n#))
               false)))))))

(deftype ColumnRangeSearchSpliterator [^VectorSchemaRoot kd-tree ^longs min-range ^longs max-range ^int k ^Deque stack]
  Spliterator$OfInt
  (^boolean tryAdvance [_ ^IntConsumer c]
    (loop []
      (if-let [^ColumnStackEntry entry (.poll stack)]
        (let [start (.start entry)
              end (.end entry)
              axis (.axis entry)
              median (quot (+ start end) 2)
              ^BigIntVector axis-column (.getVector kd-tree axis)
              axis-value (.get axis-column median)
              min-match? (<= (aget min-range axis) axis-value)
              max-match? (<= axis-value (aget max-range axis))
              axis (next-axis axis k)]

          (when (and max-match? (< (inc median) end))
            (.push stack (ColumnStackEntry. (inc median) end axis)))
          (when (and min-match? (< start median))
            (.push stack (ColumnStackEntry. start median axis)))

          (if (and min-match?
                   max-match?
                   (in-range-column? min-range kd-tree k median max-range))
            (do (.accept c median)
                true)
            (recur)))
        false)))

  (characteristics [_]
    (bit-or Spliterator/DISTINCT Spliterator/IMMUTABLE Spliterator/NONNULL Spliterator/ORDERED))

  (estimateSize [_]
    Long/MAX_VALUE)

  (trySplit [_]))

(extend-protocol KdTree
  VectorSchemaRoot
  (kd-tree-insert [_ location]
    (throw (UnsupportedOperationException.)))

  (kd-tree-delete [_ location]
    (throw (UnsupportedOperationException.)))

  (kd-tree-range-search [kd-tree min-range max-range]
    (let [min-range (->longs min-range)
          max-range (->longs max-range)
          k (alength min-range)
          stack (doto (ArrayDeque.)
                  (.push (ColumnStackEntry. 0 (.getRowCount kd-tree) 0)))]
      (->ColumnRangeSearchSpliterator kd-tree min-range max-range k stack)))

  (column-kd-tree-depth-first [kd-tree]
    (let [k (.size (.getFields (.getSchema kd-tree)))]
      (kd-tree-range-search kd-tree (repeat k Long/MIN_VALUE) (repeat k Long/MAX_VALUE)))))

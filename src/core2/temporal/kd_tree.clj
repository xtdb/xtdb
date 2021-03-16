(ns core2.temporal.kd-tree
  (:import [java.util ArrayDeque ArrayList Arrays Collection Comparator Date Deque HashMap
            List Map Spliterator Spliterator$OfInt Spliterators]
           [java.util.function Consumer IntConsumer Function Predicate ToLongFunction]
           [java.util.stream StreamSupport]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector BigIntVector VectorSchemaRoot]
           core2.temporal.TemporalCoordinates))

;; TODO:
;; participate in watermark via an opaque structure, persistent tree for now, mutable tree with time-stamps later.
;; update the uni-temporal manager to be backed by the kd-tree.
;; rebuild and balance the tree when finishing a chunk for now, store this tree as an Arrow chunk later.
;; add min/max temporal predicate support to scan, used independently of if temporal columns are scanned.
;; augment range query with active row-id min/max range from scan.
;; use opaque structure from watermark - the persistent tree itself - for range query.
;; ability to turn result coordinates of a range query into a root for scanned temporal columns.
;; take the row-ids from the root, and it in scan.
;; remove tombstones from temporal manager, the above will filter them out as they aren't in the tree.
;; take temporal roots row-id->repeat-count into account when aligning the other roots.

(set! *unchecked-math* :warn-on-boxed)

(defprotocol KdTree
  (kd-tree-insert [_ location])
  (kd-tree-delete [_ location])
  (kd-tree-range-search [_ min-range max-range])
  (kd-tree-depth-first [_]))

(defrecord Node [^longs location left right deleted?])

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
  (kd-tree-depth-first [_ _ _]
    (Spliterators/emptySpliterator)))

(defn ->node-kd-tree
  (^core2.temporal.kd_tree.Node [points]
   (->node-kd-tree (mapv ->longs points) 0))
  (^core2.temporal.kd_tree.Node [points ^long axis]
   (when-let [points (not-empty points)]
     (let [k (alength ^longs (first points))
           points (vec (sort-by #(aget ^longs % axis) points))
           median (quot (count points) 2)
           axis (next-axis axis k)]
       (->Node (nth points median)
               (->node-kd-tree (subvec points 0 median) axis)
               (->node-kd-tree (subvec points (inc median)) axis)
               false)))))

(defn node-kd-tree->seq [^Node kd-tree]
  (iterator-seq (Spliterators/iterator ^Spliterator (kd-tree-depth-first kd-tree))))

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
  (tryAdvance [_ c]
    (loop []
      (if-let [^NodeStackEntry entry (.poll stack)]
        (let [^Node node (.node entry)
              axis (.axis entry)
              ^longs location (.location node)
              location-axis (aget location axis)
              min-match? (<= (aget min-range axis) location-axis)
              max-match? (<= location-axis (aget max-range axis))
              axis (next-axis axis k)]
          (when-let [right (when max-match?
                             (.right node))]
            (.push stack (NodeStackEntry. right axis)))
          (when-let [left (when min-match?
                            (.left node))]
            (.push stack (NodeStackEntry. left axis)))

          (if (and min-match?
                   max-match?
                   (not (.deleted? node))
                   (in-range? min-range location max-range))
            (do (.accept c location)
                true)
            (recur)))
        false)))

  (characteristics [_]
    (bit-or Spliterator/DISTINCT Spliterator/IMMUTABLE Spliterator/NONNULL))

  (estimateSize [_]
    Long/MAX_VALUE)

  (trySplit [_]))

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

              (<= location-axis (aget location axis))
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
              (build-fn (assoc node :deleted? true))

              (< (aget location axis) location-axis)
              (recur (next-axis axis k) (.left node) (comp build-fn (partial assoc node :left)))

              (<= location-axis (aget location axis))
              (recur (next-axis axis k) (.right node) (comp build-fn (partial assoc node :right)))))))))

  (kd-tree-range-search [kd-tree min-range max-range]
    (let [min-range (->longs min-range)
          max-range (->longs max-range)
          k (count (some-> kd-tree (.location)))
          stack (doto (ArrayDeque.)
                  (.push (NodeStackEntry. kd-tree 0)))]
      (->NodeRangeSearchSpliterator min-range max-range k stack)))

  (kd-tree-depth-first [kd-tree]
    (let [k (count (some-> kd-tree (.location)))]
      (kd-tree-range-search kd-tree (repeat k Long/MIN_VALUE) (repeat k Long/MAX_VALUE)))))

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
          (Arrays/sort points start end (reify Comparator
                                          (compare [_ x y]
                                            (Long/compare (aget ^longs x axis)
                                                          (aget ^longs y axis)))))
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
    (bit-or Spliterator/DISTINCT Spliterator/IMMUTABLE Spliterator/NONNULL))

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


;; Bitemporal Spike, this will turn into the temporal manager.

(def ^java.util.Date end-of-time #inst "9999-12-31T23:59:59.999Z")

(defn ->coordinates ^core2.temporal.TemporalCoordinates [{:keys [id ^long row-id ^Date tt-start ^Date vt-start ^Date vt-end tombstone?]}]
  (let [coords (TemporalCoordinates. row-id)]
    (set! (.id coords) id)
    (set! (.validTime coords) (.getTime (or vt-start tt-start)))
    (set! (.validTimeEnd coords) (.getTime (or vt-end end-of-time)))
    (set! (.txTime coords) (.getTime tt-start))
    (set! (.txTimeEnd coords) (.getTime end-of-time))
    (set! (.tombstone coords) (boolean tombstone?))
    coords))

(defn insert-coordinates [kd-tree ^ToLongFunction id->long-id ^TemporalCoordinates coordinates]
  (let [k (int 6)
        id-idx (int 0)
        row-id-idx (int 1)
        vt-start-idx (int 2)
        vt-end-idx (int 3)
        tt-start-idx (int 4)
        tt-end-idx (int 5)
        id (.applyAsLong id->long-id (.id coordinates))
        row-id (.rowId coordinates)
        min-range (doto (long-array k)
                    (Arrays/fill Long/MIN_VALUE)
                    (aset id-idx id)
                    (aset vt-end-idx (.validTime coordinates))
                    (aset tt-end-idx (.txTimeEnd coordinates)))
        max-range (doto (long-array k)
                    (Arrays/fill Long/MAX_VALUE)
                    (aset id-idx id)
                    (aset vt-start-idx (dec (.validTimeEnd coordinates)))
                    (aset tt-end-idx (.txTimeEnd coordinates)))
        overlap (-> ^Spliterator (kd-tree-range-search
                                  kd-tree
                                  min-range
                                  max-range)
                    (Spliterators/iterator)
                    (iterator-seq))
        tt-start-ms (.txTime coordinates)
        vt-start-ms (.validTime coordinates)
        vt-end-ms (.validTimeEnd coordinates)
        end-of-time-ms (.getTime end-of-time)
        kd-tree (reduce kd-tree-delete kd-tree overlap)
        kd-tree (cond-> kd-tree
                  (not (.tombstone coordinates))
                  (kd-tree-insert (doto (long-array k)
                                    (aset id-idx id)
                                    (aset row-id-idx row-id)
                                    (aset vt-start-idx vt-start-ms)
                                    (aset vt-end-idx vt-end-ms)
                                    (aset tt-start-idx tt-start-ms)
                                    (aset tt-end-idx end-of-time-ms))))]
    (reduce
     (fn [kd-tree ^longs coord]
       (cond-> (kd-tree-insert kd-tree (doto (Arrays/copyOf coord k)
                                         (aset tt-end-idx tt-start-ms)))
         (< (aget coord vt-start-idx) vt-start-ms)
         (kd-tree-insert (doto (Arrays/copyOf coord k)
                           (aset tt-start-idx tt-start-ms)
                           (aset vt-end-idx vt-start-ms)))

         (> (aget coord vt-end-idx) vt-end-ms)
         (kd-tree-insert (doto (Arrays/copyOf coord k)
                           (aset tt-start-idx tt-start-ms)
                           (aset vt-start-idx vt-end-ms)))))
     kd-tree
     overlap)))

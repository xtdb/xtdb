(ns core2.temporal.kd-tree
  (:import [java.util ArrayDeque ArrayList Arrays Collection Comparator Deque List Random Spliterator Spliterator$OfInt Spliterators]
           [java.util.function Consumer IntConsumer Function Predicate]
           [java.util.stream Collectors StreamSupport]))

(set! *unchecked-math* :warn-on-boxed)

(defrecord Node [^longs location left right])

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

(defn ->kd-tree
  (^core2.temporal.kd_tree.Node [points]
   (->kd-tree (mapv ->longs points) 0))
  (^core2.temporal.kd_tree.Node [points ^long axis]
   (when-let [points (not-empty points)]
     (let [k (alength ^longs (first points))
           points (vec (sort-by #(aget ^longs % axis) points))
           median (quot (count points) 2)
           axis (next-axis axis k)]
       (->Node (nth points median)
               (->kd-tree (subvec points 0 median) axis)
               (->kd-tree (subvec points (inc median)) axis))))))

(deftype NodeStackEntry [^Node node ^int axis])

(defmacro ^:private in-range? [mins xs maxs]
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

(defn kd-tree-range-search ^java.util.Spliterator [^Node kd-tree min-range max-range]
  (let [min-range (->longs min-range)
        max-range (->longs max-range)
        k (count (some-> kd-tree (.location)))
        stack (doto (ArrayDeque.)
                (.push (NodeStackEntry. kd-tree 0)))]
    (->NodeRangeSearchSpliterator min-range max-range k stack)))

(deftype ColumnStackEntry [^int start ^int end ^int axis])

(defn ->column-kd-tree ^java.util.List [points]
  (let [points (object-array (mapv ->longs points))
        n (alength points)
        k (alength ^longs (aget points 0))
        columns (ArrayList. ^Collection (repeatedly k #(long-array n)))
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
                axis (next-axis axis k)
                ^longs location (aget points median)]
            (dotimes [n k]
              (aset ^longs (.get columns n) median (aget location n)))
            (when (< (inc median) end)
              (.push stack (ColumnStackEntry. (inc median) end axis)))
            (when (< start median)
              (.push stack (ColumnStackEntry. start median axis)))))
        (recur)))
    columns))

(defmacro ^:private in-range-column? [mins xs idx maxs]
  (let [col-sym (with-meta (gensym "col") {:tag 'longs})]
    `(let [idx# ~idx
           mins# ~mins
           xs# ~xs
           maxs# ~maxs
           len# (.size xs#)]
       (loop [n# (int 0)]
         (if (= n# len#)
           true
           (let [~col-sym (.get xs# n#)
                 x# (aget ~col-sym idx#)]
             (if (and (<= (aget mins# n#) x#)
                      (<= x# (aget maxs# n#)))
               (recur (inc n#))
               false)))))))

(deftype ColumnRangeSearchSpliterator [^List kd-tree ^longs min-range ^longs max-range ^int k ^Deque stack]
  Spliterator$OfInt
  (^boolean tryAdvance [_ ^IntConsumer c]
    (loop []
      (if-let [^ColumnStackEntry entry (.poll stack)]
        (let [start (.start entry)
              end (.end entry)
              axis (.axis entry)
              median (quot (+ start end) 2)
              ^longs axis-column (.get kd-tree axis)
              location-axis (aget axis-column median)
              min-match? (<= (aget min-range axis) location-axis)
              max-match? (<= location-axis (aget max-range axis))
              axis (next-axis axis k)]

          (when (and max-match? (< (inc median) end))
            (.push stack (ColumnStackEntry. (inc median) end axis)))
          (when (and min-match? (< start median))
            (.push stack (ColumnStackEntry. start median axis)))

          (if (and min-match?
                   max-match?
                   (in-range-column? min-range kd-tree median max-range))
            (do (.accept c median)
                true)
            (recur)))
        false)))

  (characteristics [_]
    (bit-or Spliterator/DISTINCT Spliterator/IMMUTABLE Spliterator/NONNULL))

  (estimateSize [_]
    Long/MAX_VALUE)

  (trySplit [_]))

(defn column-kd-tree-range-search ^java.util.Spliterator$OfInt [^List kd-tree min-range max-range]
  (let [min-range (->longs min-range)
        max-range (->longs max-range)
        k (alength min-range)
        stack (doto (ArrayDeque.)
                (.push (ColumnStackEntry. 0 (alength ^longs (first kd-tree)) 0)))]
    (->ColumnRangeSearchSpliterator kd-tree min-range max-range k stack)))

;; TODO:
;; Sanity check counts via stream count.
;; Try different implicit orders for implicit/column/ist.
(defn- run-test []
  (assert (= (-> (->kd-tree [[7 2] [5 4] [9 6] [4 7] [8 1] [2 3]])
                 (kd-tree-range-search [0 0] [8 4])
                 (StreamSupport/stream false)
                 (.toArray)
                 (->> (mapv vec)))

             (let [column-kd-tree (->column-kd-tree [[7 2] [5 4] [9 6] [4 7] [8 1] [2 3]])]
               (-> column-kd-tree
                   (column-kd-tree-range-search [0 0] [8 4])
                   (StreamSupport/intStream false)
                   (.toArray)
                   (->> (mapv #(mapv (fn [^longs col]
                                       (aget col %))
                                     column-kd-tree))))))
          "wikipedia-test")

  (let [rng (Random. 0)
        ns 100000
        qs 10000
        ts 5
        _ (prn :gen-points ns)
        points (time
                (vec (for [n (range ns)]
                       (long-array [(.nextLong rng)
                                    (.nextLong rng)]))))

        _ (prn :gen-queries qs)
        queries (time
                 (vec (for [n (range qs)
                            :let [mins [(.nextLong rng)
                                        (.nextLong rng)]
                                  maxs [(.nextLong rng)
                                        (.nextLong rng)]
                                  coords (map (comp sort vector) mins maxs)]]
                        [(long-array (map first coords))
                         (long-array (map second coords))])))]

    (prn :range-queries-scan qs)
    (time
     (doseq [[^longs min-range ^longs max-range] queries]
       (-> (.stream ^Collection points)
           (.filter (reify Predicate
                      (test [_ location]
                        (in-range? min-range ^longs location max-range))))
           (.count))))

    (prn :build-kd-tree ns)
    (let [kd-tree (time
                   (->kd-tree points))]

      (prn :range-queries-kd-tree qs)
      (dotimes [_ ts]
        (time
         (doseq [[min-range max-range] queries]
           (-> (kd-tree-range-search kd-tree min-range max-range)
               (StreamSupport/stream false)
               (.count))))))

    (prn :build-column-kd-tree ns)
    (let [kd-tree (time
                   (->column-kd-tree points))]
      (prn :range-queries-column-kd-tree qs)
      (dotimes [_ ts]
        (time
         (doseq [[min-range max-range] queries]
           (-> (column-kd-tree-range-search kd-tree min-range max-range)
               (StreamSupport/intStream false)
               (.count))))))))

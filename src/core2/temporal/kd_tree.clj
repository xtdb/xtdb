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

(deftype StackEntry [^Node node ^int axis])

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

(deftype RangeSearchSpliterator [^longs min-range ^longs max-range ^int k ^Deque stack]
  Spliterator
  (tryAdvance [_ c]
    (loop []
      (if-let [^StackEntry entry (.poll stack)]
        (let [^Node node (.node entry)
              axis (.axis entry)
              ^longs location (.location node)
              location-axis (aget location axis)
              min-match? (<= (aget min-range axis) location-axis)
              max-match? (<= location-axis (aget max-range axis))
              axis (next-axis axis k)]
          (when-let [right (when max-match?
                             (.right node))]
            (.push stack (StackEntry. right axis)))
          (when-let [left (when min-match?
                            (.left node))]
            (.push stack (StackEntry. left axis)))

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
                (.push (StackEntry. kd-tree 0)))]
    (->RangeSearchSpliterator min-range max-range k stack)))

(defn ->implicit-kd-tree ^objects [points]
  (let [points (object-array (mapv ->longs points))
        k (alength ^longs (aget points 0))
        stack (doto (ArrayDeque.)
                (.push (doto (int-array 3)
                        (aset 0 0)
                        (aset 1 (alength points))
                        (aset 2 0))))]
    (loop []
      (when-let [^ints entry (.poll stack)]
        (let [start (aget entry 0)
              end (aget entry 1)
              axis (aget entry 2)]
          (Arrays/sort points start end (reify Comparator
                                          (compare [_ x y]
                                            (Long/compare (aget ^longs x axis)
                                                          (aget ^longs y axis)))))
          (let [median (quot (+ start end) 2)
                axis (next-axis axis k)]
            (when (< start median)
              (.push stack (doto (int-array 3)
                             (aset 0 start)
                             (aset 1 median)
                             (aset 2 axis))))
            (when (< (inc median) end)
              (.push stack (doto (int-array 3)
                             (aset 0 (inc median))
                             (aset 1 end)
                             (aset 2 axis))))))
        (recur)))
    points))

(deftype ImplicitRangeSearchSpliterator [^objects kd-tree ^longs min-range ^longs max-range ^int k ^Deque stack]
  Spliterator
  (tryAdvance [_ c]
    (loop []
      (if-let [^ints entry (.poll stack)]
        (let [start (aget entry 0)
              end (aget entry 1)
              axis (aget entry 2)
              median (quot (+ start end) 2)
              ^longs location (aget kd-tree median)
              location-axis (aget location axis)
              min-match? (<= (aget min-range axis) location-axis)
              max-match? (<= location-axis (aget max-range axis))
              axis (next-axis axis k)]

          (when (and max-match? (< (inc median) end))
            (.push stack (doto (int-array 3)
                           (aset 0 (inc median))
                           (aset 1 end)
                           (aset 2 axis))))
          (when (and min-match? (< start median))
            (.push stack (doto (int-array 3)
                           (aset 0 start)
                           (aset 1 median)
                           (aset 2 axis))))


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

(defn implicit-kd-tree-range-search ^java.util.Spliterator [^objects kd-tree min-range max-range]
  (let [min-range (->longs min-range)
        max-range (->longs max-range)
        k (alength min-range)
        stack (doto (ArrayDeque.)
                (.push (doto (int-array 3)
                         (aset 0 0)
                         (aset 1 (alength kd-tree))
                         (aset 2 0))))]
    (->ImplicitRangeSearchSpliterator kd-tree min-range max-range k stack)))

(defn ->column-kd-tree ^java.util.List [points]
  (let [implicit-kd-tree (->implicit-kd-tree points)
        n (alength implicit-kd-tree)
        k (alength ^longs (aget implicit-kd-tree 0))]
    (vec (for [d (range k)]
           (long-array (map #(aget ^longs % d) implicit-kd-tree))))))

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
      (if-let [^ints entry (.poll stack)]
        (let [start (aget entry 0)
              end (aget entry 1)
              axis (aget entry 2)
              median (quot (+ start end) 2)
              ^longs axis-column (.get kd-tree axis)
              location-axis (aget axis-column median)
              min-match? (<= (aget min-range axis) location-axis)
              max-match? (<= location-axis (aget max-range axis))
              axis (next-axis axis k)]

          (when (and max-match? (< (inc median) end))
            (.push stack (doto (int-array 3)
                           (aset 0 (inc median))
                           (aset 1 end)
                           (aset 2 axis))))
          (when (and min-match? (< start median))
            (.push stack (doto (int-array 3)
                           (aset 0 start)
                           (aset 1 median)
                           (aset 2 axis))))

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
                (.push (doto (int-array 3)
                         (aset 0 0)
                         (aset 1 (alength ^longs (first kd-tree)))
                         (aset 2 0))))]
    (->ColumnRangeSearchSpliterator kd-tree min-range max-range k stack)))

(deftype IST [^longs ist ^objects points])

(defn ->ist-kd-tree ^core2.temporal.kd_tree.IST [points]
  (let [points (object-array (mapv ->longs points))
        ist (long-array (alength points))
        k (alength ^longs (aget points 0))
        stack (doto (ArrayDeque.)
                (.push (doto (int-array 3)
                        (aset 0 0)
                        (aset 1 (alength points))
                        (aset 2 0))))]
    (loop []
      (when-let [^ints entry (.poll stack)]
        (let [start (aget entry 0)
              end (aget entry 1)
              axis (aget entry 2)]
          (Arrays/sort points start end (reify Comparator
                                          (compare [_ x y]
                                            (Long/compare (aget ^longs x axis)
                                                          (aget ^longs y axis)))))
          (let [median (quot (+ start end) 2)
                ist-value (aget ^longs (aget points median) axis)
                axis (next-axis axis k)]
            (aset ist median ist-value)
            (when (< start median)
              (.push stack (doto (int-array 3)
                             (aset 0 start)
                             (aset 1 median)
                             (aset 2 axis))))
            (when (< (inc median) end)
              (.push stack (doto (int-array 3)
                             (aset 0 (inc median))
                             (aset 1 end)
                             (aset 2 axis))))))
        (recur)))
    (->IST ist points)))

(deftype ISTRangeSearchSpliterator [^longs ist ^objects kd-tree ^longs min-range ^longs max-range ^int k ^Deque stack]
  Spliterator$OfInt
  (^boolean tryAdvance [_ ^IntConsumer c]
   (loop []
     (if-let [^ints entry (.poll stack)]
       (let [start (aget entry 0)
             end (aget entry 1)
             axis (aget entry 2)
             median (quot (+ start end) 2)
             ist-value (aget ist median)
             min-match? (<= (aget min-range axis) ist-value)
             max-match? (<= ist-value (aget max-range axis))
             axis (next-axis axis k)]

         (when (and max-match? (< (inc median) end))
           (.push stack (doto (int-array 3)
                          (aset 0 (inc median))
                          (aset 1 end)
                          (aset 2 axis))))
         (when (and min-match? (< start median))
           (.push stack (doto (int-array 3)
                          (aset 0 start)
                          (aset 1 median)
                          (aset 2 axis))))


         (if (and min-match?
                  max-match?
                  (in-range? min-range ^longs (aget kd-tree median) max-range))
           (do (.accept c median)
               true)
           (recur)))
       false)))

  (characteristics [_]
    (bit-or Spliterator/DISTINCT Spliterator/IMMUTABLE Spliterator/NONNULL))

  (estimateSize [_]
    Long/MAX_VALUE)

  (trySplit [_]))

(defn ist-kd-tree-range-search ^java.util.Spliterator [^IST kd-tree min-range max-range]
  (let [min-range (->longs min-range)
        max-range (->longs max-range)
        k (alength min-range)
        stack (doto (ArrayDeque.)
                (.push (doto (int-array 3)
                         (aset 0 0)
                         (aset 1 (alength ^objects (.points kd-tree)))
                         (aset 2 0))))]
    (->ISTRangeSearchSpliterator (.ist kd-tree) (.points kd-tree) min-range max-range k stack)))

;; TODO:
;; Check counts via stream count.
;; Add internal search tree variant.
(defn- run-test []
  (assert (= (-> (->kd-tree [[7 2] [5 4] [9 6] [4 7] [8 1] [2 3]])
                 (kd-tree-range-search [0 0] [8 4])
                 (StreamSupport/stream false)
                 (.toArray)
                 (->> (mapv vec)))

             (-> (->implicit-kd-tree [[7 2] [5 4] [9 6] [4 7] [8 1] [2 3]])
                 (implicit-kd-tree-range-search [0 0] [8 4])
                 (StreamSupport/stream false)
                 (.toArray)
                 (->> (mapv vec)))

             (let [implicit-kd-tree (->implicit-kd-tree [[7 2] [5 4] [9 6] [4 7] [8 1] [2 3]])]
               (-> (->column-kd-tree [[7 2] [5 4] [9 6] [4 7] [8 1] [2 3]])
                   (column-kd-tree-range-search [0 0] [8 4])
                   (StreamSupport/intStream false)
                   (.toArray)
                   (->> (mapv #(vec (aget implicit-kd-tree %)))))))
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
                            :let [xs [(.nextLong rng)
                                      (.nextLong rng)]
                                  ys [(.nextLong rng)
                                      (.nextLong rng)]]]
                        [(long-array (sort xs))
                         (long-array (sort ys))])))]

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

    (prn :build-implicit-kd-tree ns)
    (let [kd-tree (time
                   (->implicit-kd-tree points))]
      (prn :range-queries-implicit-kd-tree qs)
      (dotimes [_ ts]
        (time
         (doseq [[min-range max-range] queries]
           (-> (implicit-kd-tree-range-search kd-tree min-range max-range)
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
               (.count))))))

    (prn :build-ist-kd-tree ns)
    (let [kd-tree (time
                   (->ist-kd-tree points))]
      (prn :range-queries-ist-kd-tree qs)
      (dotimes [_ ts]
        (time
         (doseq [[min-range max-range] queries]
           (-> (ist-kd-tree-range-search kd-tree min-range max-range)
               (StreamSupport/intStream false)
               (.count))))))))

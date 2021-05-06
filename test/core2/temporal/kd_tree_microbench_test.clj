(ns core2.temporal.kd-tree-microbench-test
  (:require [clojure.test :as t]
            [core2.util :as util]
            [core2.temporal.kd-tree :as kd])
  (:import [java.util Collection HashMap Random]
           [java.util.function Predicate]
           [java.util.stream LongStream]
           [org.apache.arrow.memory RootAllocator]
           [org.apache.arrow.vector VectorSchemaRoot]
           [core2.temporal.kd_tree ArrowBufKdTree Node]))

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

;; TODO: move to JMH.
(t/deftest ^:integration kd-tree-micro-bench
  (with-open [allocator (RootAllocator.)]
    (doseq [k (range 2 4)]
      (let [rng (Random. 0)
            _ (prn :k k)
            ns 100000
            qs 10000
            ts 3
            _ (prn :gen-points ns)
            points (time
                    (vec (for [n (range ns)]
                           (long-array (repeatedly k #(.nextLong rng))))))

            _ (prn :gen-queries qs)
            queries (time
                     (vec (for [n (range qs)
                                :let [min+max-pairs (repeatedly k #(sort [(.nextLong rng)
                                                                          (.nextLong rng)]))]]
                            [n
                             (long-array (map first min+max-pairs))
                             (long-array (map second min+max-pairs))])))
            query->count (HashMap.)
            test-dir (util/->path "target/kd-tree-micro-bench")]
        (util/delete-dir test-dir)

        (prn :range-queries-scan qs)
        (time
         (doseq [[query-id ^longs min-range ^longs max-range] queries]
           (.put query->count query-id (-> (.stream ^Collection points)
                                           (.filter (reify Predicate
                                                      (test [_ location]
                                                        (in-range? min-range ^longs location max-range))))
                                           (.count)))))

        (prn :build-node-kd-tree-insert ns)
        (with-open [^Node kd-tree (time
                                   (reduce
                                    (fn [acc point]
                                      (kd/kd-tree-insert acc allocator point))
                                    nil
                                    points))]

          (prn :range-queries-node-kd-tree-insert qs)
          (dotimes [_ ts]
            (time
             (doseq [[query-id min-range max-range] queries]
               (t/is (= (.get query->count query-id)
                        (.count ^LongStream (kd/kd-tree-range-search kd-tree min-range max-range))))))))


        (prn :build-node-kd-tree-bulk ns)
        (with-open [^Node kd-tree (time
                                   (kd/->node-kd-tree allocator points))]

          (prn :range-queries-node-kd-tree-bulk qs)
          (dotimes [_ ts]
            (time
             (doseq [[query-id min-range max-range] queries]
               (t/is (= (.get query->count query-id)
                        (.count ^LongStream (kd/kd-tree-range-search kd-tree min-range max-range)))))))

          (prn :build-column-kd-tree ns)
          (with-open [^VectorSchemaRoot column-kd-tree (time
                                                        (kd/->column-kd-tree allocator kd-tree k))]
            (prn :range-queries-column-kd-tree qs)
            (dotimes [_ ts]
              (time
               (doseq [[query-id min-range max-range] queries]
                 (t/is (= (.get query->count query-id)
                          (.count ^LongStream (kd/kd-tree-range-search column-kd-tree min-range max-range)))))))

            (prn :build-disk-kd-tree ns)
            (with-open [^ArrowBufKdTree disk-kd-tree (time
                                                      (->> (kd/->disk-kd-tree allocator (.resolve test-dir (format "kd_tree_%d.arrow" k)) points {:k k})
                                                           (kd/->mmap-kd-tree allocator)))]
              (prn :range-queries-disk-kd-tree qs)
              (dotimes [_ ts]
                (time
                 (doseq [[query-id min-range max-range] queries]
                   (t/is (= (.get query->count query-id)
                            (.count ^LongStream (kd/kd-tree-range-search disk-kd-tree min-range max-range)))))))

              (prn :build-compressed-disk-kd-tree ns)
              (with-open [^ArrowBufKdTree compressed-disk-kd-tree (time
                                                                   (->> (kd/->disk-kd-tree allocator (.resolve test-dir (format "kd_tree_%d.arrow" k))
                                                                                           points {:k k :compress-blocks? true})
                                                                        (kd/->mmap-kd-tree allocator)))]
                (prn :range-queries-compressed-disk-kd-tree qs)
                (dotimes [_ ts]
                  (time
                   (doseq [[query-id min-range max-range] queries]
                     (t/is (= (.get query->count query-id)
                              (.count ^LongStream (kd/kd-tree-range-search compressed-disk-kd-tree min-range max-range)))))))

                (let [_ (prn :node-kd-tree->seq)
                      kd-tree-seq (time (vec (kd/kd-tree->seq kd-tree)))
                      _ (prn :column-tree->seq)
                      col-tree-seq (time (vec (kd/kd-tree->seq column-kd-tree)))
                      _ (prn :disk-tree->seq)
                      disk-tree-seq (time (vec (kd/kd-tree->seq disk-kd-tree)))
                      _ (prn :compressed-disk-tree->seq)
                      compressed-disk-tree-seq (time (vec (kd/kd-tree->seq compressed-disk-kd-tree)))]

                  (t/is (= kd-tree-seq col-tree-seq disk-tree-seq compressed-disk-tree-seq)))))))))))

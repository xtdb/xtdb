(ns core2.temporal.kd-tree-microbench-test
  (:require [clojure.test :as t]
            [core2.temporal.kd-tree :as kd])
  (:import [java.util Collection HashMap Random]
           [java.util.function Predicate]
           [java.util.stream StreamSupport]
           [org.apache.arrow.memory RootAllocator]
           [org.apache.arrow.vector VectorSchemaRoot]))

;; TODO: move to JMH.
(t/deftest ^:integration kd-tree-micro-bench
  (with-open [allocator (RootAllocator.)]
    (doseq [k (range 2 4)]
      (let [rng (Random. 0)
            _ (prn :k k)
            ns 10000
            qs 1000
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
            query->count (HashMap.)]

        (prn :range-queries-scan qs)
        (time
         (doseq [[query-id ^longs min-range ^longs max-range] queries]
           (.put query->count query-id (-> (.stream ^Collection points)
                                           (.filter (reify Predicate
                                                      (test [_ location]
                                                        (kd/in-range? min-range ^longs location max-range))))
                                           (.count)))))

        (prn :build-kd-tree-insert ns)
        (let [kd-tree (time
                       (reduce
                        kd/kd-tree-insert
                        nil
                        points))]

          (prn :range-queries-kd-tree-insert qs)
          (dotimes [_ ts]
            (time
             (doseq [[query-id min-range max-range] queries]
               (t/is (= (.get query->count query-id)
                        (-> (kd/kd-tree-range-search kd-tree min-range max-range)
                            (StreamSupport/stream false)
                            (.count))))))))


        (prn :build-kd-tree-bulk ns)
        (let [kd-tree (time
                       (kd/->node-kd-tree points))]

          (prn :range-queries-kd-tree-bulk qs)
          (dotimes [_ ts]
            (time
             (doseq [[query-id min-range max-range] queries]
               (t/is (= (.get query->count query-id)
                        (-> (kd/kd-tree-range-search kd-tree min-range max-range)
                            (StreamSupport/stream false)
                            (.count)))))))

          (prn :build-column-kd-tree ns)
          (with-open [^VectorSchemaRoot column-kd-tree (time
                                                        (kd/->column-kd-tree allocator kd-tree k))]
            (prn :range-queries-column-kd-tree qs)
            (dotimes [_ ts]
              (time
               (doseq [[query-id min-range max-range] queries]
                 (t/is (= (.get query->count query-id)
                          (-> (kd/kd-tree-range-search column-kd-tree min-range max-range)
                              (StreamSupport/stream false)
                              (.count)))))))

            (t/is (= (mapv vec (kd/kd-tree->seq kd-tree))
                     (mapv vec (kd/kd-tree->seq column-kd-tree))))))))))

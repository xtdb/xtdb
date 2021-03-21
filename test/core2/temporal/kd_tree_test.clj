(ns core2.temporal.kd-tree-test
  (:require [clojure.test :as t]
            [core2.temporal :as temporal]
            [core2.temporal.kd-tree :as kd])
  (:import [java.util Collection Date HashMap List]
           [java.util.function Predicate ToLongFunction]
           [java.util.stream StreamSupport]
           [org.apache.arrow.memory RootAllocator]
           [org.apache.arrow.vector VectorSchemaRoot]
           [org.apache.arrow.vector.complex FixedSizeListVector]
           core2.temporal.kd_tree.Node))

;; NOTE: "Developing Time-Oriented Database Applications in SQL",
;; chapter 10 "Bitemporal Tables".

;; Uses transaction time splitting, so some rectangles differ, but
;; areas covered are the same. Could or maybe should coalesce.

(defn- ->row-map [^List point]
  (zipmap [:id :row-id :vt-start :vt-end :tt-start :tt-end]
          [(.get point temporal/id-idx)
           (.get point temporal/row-id-idx)
           (Date. ^long (.get point temporal/valid-time-idx))
           (Date. ^long (.get point temporal/valid-time-end-idx))
           (Date. ^long (.get point temporal/tx-time-idx))
           (Date. ^long (.get point temporal/tx-time-end-idx))]))

(defn- temporal-rows [kd-tree row-id->row]
  (vec (for [{:keys [row-id] :as row} (->> (map ->row-map (kd/kd-tree->seq kd-tree))
                                           (sort-by (juxt :tt-start :row-id)))]
         (merge row (get row-id->row row-id)))))

(t/deftest bitemporal-tx-time-split-test
  (let [kd-tree nil
        id->internal-id-map  (doto (HashMap.)
                               (.put 7797 7797))
        id->internal-id (reify ToLongFunction
                          (applyAsLong [_ x]
                            (.get id->internal-id-map x)))
        row-id->row (HashMap.)]
    ;; Current Insert
    ;; Eva Nielsen buys the flat at Skovvej 30 in Aalborg on January 10,
    ;; 1998.
    (with-open [allocator (RootAllocator.)
                ^Node kd-tree (temporal/insert-coordinates kd-tree
                                                           allocator
                                                           id->internal-id
                                                           (temporal/->coordinates {:id 7797
                                                                                    :row-id 1
                                                                                    :tt-start #inst "1998-01-10"}))]
      (.put row-id->row 1 {:customer-number 145})
      (t/is (= [{:id 7797,
                 :customer-number 145,
                 :row-id 1,
                 :vt-start #inst "1998-01-10T00:00:00.000-00:00",
                 :vt-end #inst "9999-12-31T23:59:59.999-00:00",
                 :tt-start #inst "1998-01-10T00:00:00.000-00:00",
                 :tt-end #inst "9999-12-31T23:59:59.999-00:00"}]
               (temporal-rows kd-tree row-id->row)))

      ;; Current Update
      ;; Peter Olsen buys the flat on January 15, 1998.
      (let [kd-tree (temporal/insert-coordinates kd-tree
                                                 allocator
                                                 id->internal-id
                                                 (temporal/->coordinates {:id 7797
                                                                          :row-id 2
                                                                          :tt-start #inst "1998-01-15"}))]
        (.put row-id->row 2 {:customer-number 827})
        (t/is (= [{:id 7797,
                   :row-id 1,
                   :customer-number 145,
                   :vt-start #inst "1998-01-10T00:00:00.000-00:00",
                   :vt-end #inst "9999-12-31T23:59:59.999-00:00",
                   :tt-start #inst "1998-01-10T00:00:00.000-00:00",
                   :tt-end #inst "1998-01-15T00:00:00.000-00:00"}
                  {:id 7797,
                   :customer-number 145,
                   :row-id 1,
                   :vt-start #inst "1998-01-10T00:00:00.000-00:00",
                   :vt-end #inst "1998-01-15T00:00:00.000-00:00",
                   :tt-start #inst "1998-01-15T00:00:00.000-00:00",
                   :tt-end #inst "9999-12-31T23:59:59.999-00:00"}
                  {:id 7797,
                   :row-id 2,
                   :customer-number 827,
                   :vt-start #inst "1998-01-15T00:00:00.000-00:00",
                   :vt-end #inst "9999-12-31T23:59:59.999-00:00",
                   :tt-start #inst "1998-01-15T00:00:00.000-00:00",
                   :tt-end #inst "9999-12-31T23:59:59.999-00:00"}]
                 (temporal-rows kd-tree row-id->row)))

        ;; Current Delete
        ;; Peter Olsen sells the flat on January 20, 1998.
        (let [kd-tree (temporal/insert-coordinates kd-tree
                                                   allocator
                                                   id->internal-id
                                                   (temporal/->coordinates {:id 7797
                                                                            :row-id 3
                                                                            :tt-start #inst "1998-01-20"
                                                                            :tombstone? true}))]
          (.put row-id->row 3 {:customer-number 827})
          (t/is (= [{:id 7797,
                     :customer-number 145,
                     :row-id 1,
                     :vt-start #inst "1998-01-10T00:00:00.000-00:00",
                     :vt-end #inst "9999-12-31T23:59:59.999-00:00",
                     :tt-start #inst "1998-01-10T00:00:00.000-00:00",
                     :tt-end #inst "1998-01-15T00:00:00.000-00:00"}
                    {:id 7797,
                     :customer-number 145,
                     :row-id 1,
                     :vt-start #inst "1998-01-10T00:00:00.000-00:00",
                     :vt-end #inst "1998-01-15T00:00:00.000-00:00",
                     :tt-start #inst "1998-01-15T00:00:00.000-00:00",
                     :tt-end #inst "9999-12-31T23:59:59.999-00:00"}
                    {:id 7797,
                     :customer-number 827,
                     :row-id 2,
                     :vt-start #inst "1998-01-15T00:00:00.000-00:00",
                     :vt-end #inst "9999-12-31T23:59:59.999-00:00",
                     :tt-start #inst "1998-01-15T00:00:00.000-00:00",
                     :tt-end #inst "1998-01-20T00:00:00.000-00:00"}
                    {:id 7797,
                     :customer-number 827,
                     :row-id 2,
                     :vt-start #inst "1998-01-15T00:00:00.000-00:00",
                     :vt-end #inst "1998-01-20T00:00:00.000-00:00",
                     :tt-start #inst "1998-01-20T00:00:00.000-00:00",
                     :tt-end #inst "9999-12-31T23:59:59.999-00:00"}]
                   (temporal-rows kd-tree row-id->row)))

          ;; Sequenced Insert
          ;; Eva actually purchased the flat on January 3, performed on January 23.
          (let [kd-tree (temporal/insert-coordinates kd-tree
                                                     allocator
                                                     id->internal-id
                                                     (temporal/->coordinates {:id 7797
                                                                              :row-id 4
                                                                              :tt-start #inst "1998-01-23"
                                                                              :vt-start #inst "1998-01-03"
                                                                              :vt-end #inst "1998-01-15"}))]
            (.put row-id->row 4 {:customer-number 145})
            (t/is (= [{:id 7797,
                       :customer-number 145,
                       :row-id 1,
                       :vt-start #inst "1998-01-10T00:00:00.000-00:00",
                       :vt-end #inst "9999-12-31T23:59:59.999-00:00",
                       :tt-start #inst "1998-01-10T00:00:00.000-00:00",
                       :tt-end #inst "1998-01-15T00:00:00.000-00:00"}
                      {:id 7797,
                       :customer-number 145,
                       :row-id 1,
                       :vt-start #inst "1998-01-10T00:00:00.000-00:00",
                       :vt-end #inst "1998-01-15T00:00:00.000-00:00",
                       :tt-start #inst "1998-01-15T00:00:00.000-00:00",
                       :tt-end #inst "1998-01-23T00:00:00.000-00:00"}
                      {:id 7797,
                       :customer-number 827,
                       :row-id 2,
                       :vt-start #inst "1998-01-15T00:00:00.000-00:00",
                       :vt-end #inst "9999-12-31T23:59:59.999-00:00",
                       :tt-start #inst "1998-01-15T00:00:00.000-00:00",
                       :tt-end #inst "1998-01-20T00:00:00.000-00:00"}
                      {:id 7797,
                       :row-id 2,
                       :customer-number 827,
                       :vt-start #inst "1998-01-15T00:00:00.000-00:00",
                       :vt-end #inst "1998-01-20T00:00:00.000-00:00",
                       :tt-start #inst "1998-01-20T00:00:00.000-00:00",
                       :tt-end #inst "9999-12-31T23:59:59.999-00:00"}
                      {:id 7797,
                       :customer-number 145,
                       :row-id 4,
                       :vt-start #inst "1998-01-03T00:00:00.000-00:00",
                       :vt-end #inst "1998-01-15T00:00:00.000-00:00",
                       :tt-start #inst "1998-01-23T00:00:00.000-00:00",
                       :tt-end #inst "9999-12-31T23:59:59.999-00:00"}]
                     (temporal-rows kd-tree row-id->row)))

            ;; NOTE: rows differs from book, but covered area is the same.
            ;; Sequenced Delete
            ;; A sequenced deletion performed on January 26: Eva actually purchased the flat on January 5.
            (let [kd-tree (temporal/insert-coordinates kd-tree
                                                       allocator
                                                       id->internal-id
                                                       (temporal/->coordinates {:id 7797
                                                                                :row-id 5
                                                                                :tt-start #inst "1998-01-26"
                                                                                :vt-start #inst "1998-01-02"
                                                                                :vt-end #inst "1998-01-05"
                                                                                :tombstone? true}))]
              (.put row-id->row 5 {:customer-number 145})
              (t/is (= [{:id 7797,
                         :customer-number 145,
                         :row-id 1,
                         :vt-start #inst "1998-01-10T00:00:00.000-00:00",
                         :vt-end #inst "9999-12-31T23:59:59.999-00:00",
                         :tt-start #inst "1998-01-10T00:00:00.000-00:00",
                         :tt-end #inst "1998-01-15T00:00:00.000-00:00"}
                        {:id 7797,
                         :customer-number 145,
                         :row-id 1,
                         :vt-start #inst "1998-01-10T00:00:00.000-00:00",
                         :vt-end #inst "1998-01-15T00:00:00.000-00:00",
                         :tt-start #inst "1998-01-15T00:00:00.000-00:00",
                         :tt-end #inst "1998-01-23T00:00:00.000-00:00"}
                        {:id 7797,
                         :customer-number 827,
                         :row-id 2,
                         :vt-start #inst "1998-01-15T00:00:00.000-00:00",
                         :vt-end #inst "9999-12-31T23:59:59.999-00:00",
                         :tt-start #inst "1998-01-15T00:00:00.000-00:00",
                         :tt-end #inst "1998-01-20T00:00:00.000-00:00"}
                        {:id 7797,
                         :customer-number 827,
                         :row-id 2,
                         :vt-start #inst "1998-01-15T00:00:00.000-00:00",
                         :vt-end #inst "1998-01-20T00:00:00.000-00:00",
                         :tt-start #inst "1998-01-20T00:00:00.000-00:00",
                         :tt-end #inst "9999-12-31T23:59:59.999-00:00"}
                        {:id 7797,
                         :customer-number 145,
                         :row-id 4,
                         :vt-start #inst "1998-01-03T00:00:00.000-00:00",
                         :vt-end #inst "1998-01-15T00:00:00.000-00:00",
                         :tt-start #inst "1998-01-23T00:00:00.000-00:00",
                         :tt-end #inst "1998-01-26T00:00:00.000-00:00"}
                        {:id 7797,
                         :customer-number 145,
                         :row-id 4,
                         :vt-start #inst "1998-01-05T00:00:00.000-00:00",
                         :vt-end #inst "1998-01-15T00:00:00.000-00:00",
                         :tt-start #inst "1998-01-26T00:00:00.000-00:00",
                         :tt-end #inst "9999-12-31T23:59:59.999-00:00"}]
                       (temporal-rows kd-tree row-id->row)))

              ;; NOTE: rows differs from book, but covered area is the same.
              ;; Sequenced Update
              ;; A sequenced update performed on January 28: Peter actually purchased the flat on January 12.
              (let [kd-tree (temporal/insert-coordinates kd-tree
                                                         allocator
                                                         id->internal-id
                                                         (temporal/->coordinates {:id 7797
                                                                                  :row-id 6
                                                                                  :tt-start #inst "1998-01-28"
                                                                                  :vt-start #inst "1998-01-12"
                                                                                  :vt-end #inst "1998-01-15"}))]
                (.put row-id->row 6 {:customer-number 827})
                (t/is (= [{:id 7797,
                           :customer-number 145,
                           :row-id 1,
                           :vt-start #inst "1998-01-10T00:00:00.000-00:00",
                           :vt-end #inst "9999-12-31T23:59:59.999-00:00",
                           :tt-start #inst "1998-01-10T00:00:00.000-00:00",
                           :tt-end #inst "1998-01-15T00:00:00.000-00:00"}
                          {:id 7797,
                           :customer-number 145,
                           :row-id 1,
                           :vt-start #inst "1998-01-10T00:00:00.000-00:00",
                           :vt-end #inst "1998-01-15T00:00:00.000-00:00",
                           :tt-start #inst "1998-01-15T00:00:00.000-00:00",
                           :tt-end #inst "1998-01-23T00:00:00.000-00:00"}
                          {:id 7797,
                           :customer-number 827,
                           :row-id 2,
                           :vt-start #inst "1998-01-15T00:00:00.000-00:00",
                           :vt-end #inst "9999-12-31T23:59:59.999-00:00",
                           :tt-start #inst "1998-01-15T00:00:00.000-00:00",
                           :tt-end #inst "1998-01-20T00:00:00.000-00:00"}
                          {:id 7797,
                           :customer-number 827,
                           :row-id 2,
                           :vt-start #inst "1998-01-15T00:00:00.000-00:00",
                           :vt-end #inst "1998-01-20T00:00:00.000-00:00",
                           :tt-start #inst "1998-01-20T00:00:00.000-00:00",
                           :tt-end #inst "9999-12-31T23:59:59.999-00:00"}
                          {:id 7797,
                           :customer-number 145,
                           :row-id 4,
                           :vt-start #inst "1998-01-03T00:00:00.000-00:00",
                           :vt-end #inst "1998-01-15T00:00:00.000-00:00",
                           :tt-start #inst "1998-01-23T00:00:00.000-00:00",
                           :tt-end #inst "1998-01-26T00:00:00.000-00:00"}
                          {:id 7797,
                           :customer-number 145,
                           :row-id 4,
                           :vt-start #inst "1998-01-05T00:00:00.000-00:00",
                           :vt-end #inst "1998-01-15T00:00:00.000-00:00",
                           :tt-start #inst "1998-01-26T00:00:00.000-00:00",
                           :tt-end #inst "1998-01-28T00:00:00.000-00:00"}
                          {:id 7797,
                           :customer-number 145,
                           :row-id 4,
                           :vt-start #inst "1998-01-05T00:00:00.000-00:00",
                           :vt-end #inst "1998-01-12T00:00:00.000-00:00",
                           :tt-start #inst "1998-01-28T00:00:00.000-00:00",
                           :tt-end #inst "9999-12-31T23:59:59.999-00:00"}
                          {:id 7797,
                           :customer-number 827,
                           :row-id 6,
                           :vt-start #inst "1998-01-12T00:00:00.000-00:00",
                           :vt-end #inst "1998-01-15T00:00:00.000-00:00",
                           :tt-start #inst "1998-01-28T00:00:00.000-00:00",
                           :tt-end #inst "9999-12-31T23:59:59.999-00:00"}]
                         (temporal-rows kd-tree row-id->row))

                      (t/testing "rebuilding tree results in tree with same points"
                        (let [points (mapv vec (kd/kd-tree->seq kd-tree))]
                          (with-open [new-tree (kd/->node-kd-tree allocator (shuffle points))
                                      ^Node rebuilt-tree (reduce (fn [acc point]
                                                                   (kd/kd-tree-insert acc allocator point)) nil (shuffle points))]
                            (t/is (= (sort points)
                                     (sort (mapv vec (kd/kd-tree->seq new-tree)))))
                            (t/is (= (sort points)
                                     (sort (mapv vec (kd/kd-tree->seq rebuilt-tree)))))))))))))))))

(t/deftest kd-tree-sanity-check
  (let [points [[7 2] [5 4] [9 6] [4 7] [8 1] [2 3]]]
    (with-open [allocator (RootAllocator.)
                kd-tree (kd/->node-kd-tree allocator points)
                ^Node insert-kd-tree (reduce
                                      (fn [acc point]
                                        (kd/kd-tree-insert acc allocator point))
                                      nil
                                      points)
                ^VectorSchemaRoot column-kd-tree (kd/->column-kd-tree allocator kd-tree 2)]
      (t/is (= [[7 2] [5 4] [2 3] [8 1]]

               (-> kd-tree
                   (kd/kd-tree-range-search [0 0] [8 4])
                   (StreamSupport/intStream false)
                   (.toArray)
                   (->> (mapv (partial kd/kd-tree-point kd-tree))))

               (-> insert-kd-tree
                   (kd/kd-tree-range-search [0 0] [8 4])
                   (StreamSupport/intStream false)
                   (.toArray)
                   (->> (mapv (partial kd/kd-tree-point insert-kd-tree))))

               (-> column-kd-tree
                   (kd/kd-tree-range-search [0 0] [8 4])
                   (StreamSupport/intStream false)
                   (.toArray)
                   (->> (mapv (partial kd/kd-tree-point column-kd-tree)))))
            "wikipedia-test")

      (t/testing "seq"
        (t/is (= (kd/kd-tree->seq kd-tree)
                 (kd/kd-tree->seq column-kd-tree))))

      (t/testing "empty tree"
        (with-open [^Node kd-tree (kd/->node-kd-tree allocator [[1 2]])]
          (t/is (= [[1 2]] (kd/kd-tree->seq kd-tree))))

        (t/is (nil? (kd/->node-kd-tree allocator [])))

        (with-open [^Node kd-tree (kd/kd-tree-insert nil allocator [1 2])]
          (t/is (= [[1 2]] (kd/kd-tree->seq kd-tree))))

        (with-open [^Node kd-tree (kd/kd-tree-delete nil allocator [1 2])]
          (t/is (empty? (kd/kd-tree->seq kd-tree)))))

      (t/testing "merge"
        (with-open [new-tree-with-tombstone (kd/->node-kd-tree allocator [[4 7] [8 1] [2 3]])]
          (let [node-to-delete [2 1]
                ^Node new-tree-with-tombstone (kd/kd-tree-delete new-tree-with-tombstone allocator node-to-delete)]
            (with-open [old-tree-with-node-to-be-deleted (kd/->node-kd-tree allocator [[7 2] [5 4] [9 6] node-to-delete])
                        ^VectorSchemaRoot column-kd-tree (kd/->column-kd-tree allocator
                                                                              new-tree-with-tombstone
                                                                              2)
                        merged-tree (kd/merge-kd-trees allocator old-tree-with-node-to-be-deleted column-kd-tree)
                        rebuilt-tree (kd/rebuild-node-kd-tree allocator merged-tree)]
              (t/is (= (kd/kd-tree->seq kd-tree)
                       (kd/kd-tree->seq merged-tree)
                       (kd/kd-tree->seq rebuilt-tree))))))))))

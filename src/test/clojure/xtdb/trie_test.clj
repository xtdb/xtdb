(ns xtdb.trie-test
  (:require [clojure.test :as t :refer [deftest]]
            [clojure.walk :as walk]
            [xtdb.test-util :as tu]
            [xtdb.trie :as trie])
  (:import (clojure.lang MapEntry)
           (org.apache.arrow.memory RootAllocator)
           (xtdb.trie ArrowHashTrie ArrowHashTrie$Leaf)))

(deftest test-merge-plan-with-nil-nodes-2700
  (with-open [al (RootAllocator.)
              t1-root (tu/open-arrow-hash-trie-root al [[nil 0 nil 1] 2 nil 3])
              log-root (tu/open-arrow-hash-trie-root al 0)
              log2-root (tu/open-arrow-hash-trie-root al [nil nil 0 1])]

    (t/is (= {:path [],
              :node [:branch
                     [{:path [0],
                       :node [:branch
                              [{:path [0 0], :node [:leaf [nil nil {:page-idx 0} nil]]}
                               {:path [0 1], :node [:leaf [nil {:page-idx 0} {:page-idx 0} nil]]}
                               {:path [0 2], :node [:leaf [nil nil {:page-idx 0} nil]]}
                               {:path [0 3], :node [:leaf [nil {:page-idx 1} {:page-idx 0} nil]]}]]}
                      {:path [1], :node [:leaf [nil {:page-idx 2} {:page-idx 0} nil]]}
                      {:path [2], :node [:leaf [nil nil {:page-idx 0} {:page-idx 0}]]}
                      {:path [3], :node [:leaf [nil {:page-idx 3} {:page-idx 0} {:page-idx 1}]]}]]}
             (trie/postwalk-merge-tries [nil (ArrowHashTrie/from t1-root) (ArrowHashTrie/from log-root) (ArrowHashTrie/from log2-root)]
                                        (fn [path [mn-tag mn-arg :as merge-node]]
                                          {:path (vec path)
                                           :node (case mn-tag
                                                   :branch merge-node
                                                   :leaf [:leaf (mapv (fn [^ArrowHashTrie$Leaf leaf]
                                                                        (when leaf
                                                                          {:page-idx (.getPageIndex leaf)}))
                                                                      mn-arg)])}))))

    #_ ; TODO -> scan-test
    (t/is (= {:path [],
              :node [:branch
                     [{:path [0],
                       :node [:branch
                              [nil {:path [0 1],
                                    :node [:leaf [nil {:page-idx 0} {:page-idx 0} nil]]} nil nil]]} nil nil nil]]}
             (->> (trie/->merge-plan [nil {:trie (ArrowHashTrie/from t1-root)} {:trie (ArrowHashTrie/from log-root)} {:trie (ArrowHashTrie/from log2-root)}]
                                     (let [iid-ptr (ArrowBufPointer. iid-arrow-buf 0 (.capacity iid-bb))]
                                       #(zero? (HashTrie/compareToPath iid-ptr %)))
                                     page-idx-preds
                                     (repeat 4 iid-bloom-bitmap))
                  (walk/postwalk (fn [x]
                                   (if (map-entry? x)
                                     (cond (= :path (key x)) (MapEntry/create :path (into [] (val x)))
                                           (= :trie-leaf-file (key x)) {}
                                           :else x)
                                     x)))))
          "testing iid fast path case")))

(t/deftest test-selects-current-tries
  (letfn [(f [table-tries]
            (->> (trie/current-table-tries (for [[level rf rt] table-tries]
                                             {:level level, :row-from rf, :row-to rt}))
                 (mapv (juxt :level :row-from :row-to))))]
    (t/is (= [] (f [])))

    (t/is (= [[0 0 1] [0 1 2] [0 2 3]]
             (f [[0 0 1] [0 1 2] [0 2 3]])))

    (t/is (= [[1 0 2] [0 2 3]]
             (f [[1 0 2] [0 0 1] [0 1 2] [0 2 3]])))

    (t/is (= [[2 0 4] [1 4 6] [0 6 7] [0 7 8]]
             (f [[2 0 4]
                 [1 0 2] [1 2 4] [1 4 6]
                 [0 0 1] [0 1 2] [0 2 3] [0 3 4] [0 4 5] [0 5 6] [0 6 7] [0 7 8]])))))

(ns xtdb.trie-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.test-util :as tu]
            [xtdb.trie :as trie]
            [xtdb.util :as util])
  (:import (org.apache.arrow.memory RootAllocator)
           org.apache.arrow.vector.VectorSchemaRoot
           (xtdb.trie ArrowHashTrie ArrowHashTrie$Leaf)))

(deftest test-merge-plan-with-nil-nodes-2700
  (letfn [(->arrow-hash-trie [^VectorSchemaRoot meta-root]
            (ArrowHashTrie. (.getVector meta-root "nodes")))]

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
               (trie/postwalk-merge-plan [nil {:trie (->arrow-hash-trie t1-root)} {:trie (->arrow-hash-trie log-root)} {:trie (->arrow-hash-trie log2-root)}]
                                         (fn [path [mn-tag & mn-args :as merge-node]]
                                           {:path (vec path)
                                            :node (case mn-tag
                                                    :branch merge-node
                                                    :leaf (let [[_segments nodes] mn-args]
                                                            [:leaf (mapv (fn [^ArrowHashTrie$Leaf leaf]
                                                                           (when leaf
                                                                             {:page-idx (.getDataPageIndex leaf)}))
                                                                         nodes)]))})))))))

(t/deftest test-selects-current-tries
  (letfn [(f [trie-keys]
            (->> (trie/current-trie-files (for [[level rf nr] trie-keys]
                                            (trie/->table-meta-file-path (util/->path "tables/xt_docs") (trie/->log-trie-key level rf nr))))
                 (mapv (comp (juxt :level :row-from :next-row) trie/parse-trie-file-path))))]
    (t/is (= [] (f [])))

    (t/is (= [[0 0 1] [0 1 2] [0 2 3]]
             (f [[0 0 1] [0 1 2] [0 2 3]])))

    (t/is (= [[1 0 2] [0 2 3]]
             (f [[1 0 2] [0 0 1] [0 1 2] [0 2 3]])))

    (t/is (= [[2 0 4] [1 4 6] [0 6 7] [0 7 8]]
             (f [[2 0 4]
                 [1 0 2] [1 2 4] [1 4 6]
                 [0 0 1] [0 1 2] [0 2 3] [0 3 4] [0 4 5] [0 5 6] [0 6 7] [0 7 8]])))))

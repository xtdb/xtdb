(ns xtdb.trie-test
  (:require [clojure.test :as t :refer [deftest]]
            [clojure.walk :as walk]
            [xtdb.test-util :as tu]
            [xtdb.trie :as trie])
  (:import (clojure.lang MapEntry)
           (xtdb.trie ArrowHashTrie)
           (org.apache.arrow.memory RootAllocator)))

(deftest test-merge-plan-with-nil-nodes-2700
  (with-open [al (RootAllocator.)
              t1-root (tu/open-arrow-hash-trie-root al [[nil 1 nil 2] 1 nil 3])
              log-root (tu/open-arrow-hash-trie-root al 1)
              log2-root (tu/open-arrow-hash-trie-root al [nil nil 1 2])]
    (t/is (= {:path [],
              :node
              [:branch
               [{:path [0],
                 :node
                 [:branch
                  [{:path [0 0],
                    :node [:leaf [{:ordinal 2, :trie-leaf {:page-idx 1}}]]}
                   {:path [0 1],
                    :node
                    [:leaf
                     [{:ordinal 1, :trie-leaf {:page-idx 1}}
                      {:ordinal 2, :trie-leaf {:page-idx 1}}]]}
                   {:path [0 2],
                    :node [:leaf [{:ordinal 2, :trie-leaf {:page-idx 1}}]]}
                   {:path [0 3],
                    :node
                    [:leaf
                     [{:ordinal 1, :trie-leaf {:page-idx 2}}
                      {:ordinal 2, :trie-leaf {:page-idx 1}}]]}]]}
                {:path [1],
                 :node
                 [:leaf
                  [{:ordinal 1, :trie-leaf {:page-idx 1}}
                   {:ordinal 2, :trie-leaf {:page-idx 1}}]]}
                {:path [2],
                 :node
                 [:leaf
                  [{:ordinal 2, :trie-leaf {:page-idx 1}}
                   {:ordinal 3, :trie-leaf {:page-idx 1}}]]}
                {:path [3],
                 :node
                 [:leaf
                  [{:ordinal 1, :trie-leaf {:page-idx 3}}
                   {:ordinal 2, :trie-leaf {:page-idx 1}}
                   {:ordinal 3, :trie-leaf {:page-idx 2}}]]}]]}
             (->> (trie/->merge-plan [nil (ArrowHashTrie/from t1-root) (ArrowHashTrie/from log-root) (ArrowHashTrie/from log2-root)] nil)
                  (walk/postwalk (fn [x]
                                   (if (and (map-entry? x) (= :path (key x)))
                                     (MapEntry/create :path (into [] (val x)))
                                     x))))))))

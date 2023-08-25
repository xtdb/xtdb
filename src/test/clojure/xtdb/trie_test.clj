(ns xtdb.trie-test
  (:require [clojure.test :as t :refer [deftest]]
            [clojure.walk :as walk]
            [xtdb.test-util :as tu]
            [xtdb.trie :as trie])
  (:import (clojure.lang MapEntry)
           (org.roaringbitmap RoaringBitmap)
           (xtdb.trie ArrowHashTrie)
           (org.apache.arrow.memory RootAllocator)))

(deftest test-merge-plan-with-nil-nodes-2700
  (with-open [al (RootAllocator.)
              t1-root (tu/open-arrow-hash-trie-root al [[nil 0 nil 1] 2 nil 3])
              log-root (tu/open-arrow-hash-trie-root al 0)
              log2-root (tu/open-arrow-hash-trie-root al [nil nil 0 1])]
    (let [trie-page-idxs [(RoaringBitmap.)
                          (RoaringBitmap/bitmapOf (int-array '(0 1 2 3)))
                          (RoaringBitmap/bitmapOf (int-array '(0)))
                          (RoaringBitmap/bitmapOf (int-array '(0 1)))]]
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
               (->> (trie/->merge-plan [nil (ArrowHashTrie/from t1-root) (ArrowHashTrie/from log-root) (ArrowHashTrie/from log2-root)]
                                       trie-page-idxs)
                    (walk/postwalk (fn [x]
                                     (if (and (map-entry? x) (= :path (key x)))
                                       (MapEntry/create :path (into [] (val x)))
                                       x)))))))))

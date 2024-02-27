(ns xtdb.trie-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.test-util :as tu]
            [xtdb.trie :as trie]
            [xtdb.util :as util]
            [xtdb.time :as time])
  (:import (org.apache.arrow.memory RootAllocator)
           org.apache.arrow.vector.VectorSchemaRoot
           (xtdb.trie ArrowHashTrie ArrowHashTrie$Leaf)))

(deftest parses-trie-paths
  (letfn [(parse [trie-key]
            (-> (trie/parse-trie-file-path (util/->path (str trie-key ".arrow")))
                (update :part #(some-> % vec))
                (mapv [:level :part :next-row :rows])))]
    (t/is (= [0 nil 46 32] (parse (trie/->log-l0-l1-trie-key 0 46 32))))
    (t/is (= [2 [0 0 1 3] 120 nil] (parse (trie/->log-l2+-trie-key 2 (byte-array [0 0 1 3]) 120))))))

(deftest test-merge-plan-with-nil-nodes-2700
  (letfn [(->arrow-hash-trie [^VectorSchemaRoot meta-root]
            (ArrowHashTrie. (.getVector meta-root "nodes")))]

    (with-open [al (RootAllocator.)
                t1-root (tu/open-arrow-hash-trie-root al [{Long/MAX_VALUE [nil 0 nil 1]} 2 nil
                                                          {Long/MAX_VALUE 3, (time/instant->micros (time/->instant #inst "2023-01-01")) 4}])
                log-root (tu/open-arrow-hash-trie-root al 0)
                log2-root (tu/open-arrow-hash-trie-root al [nil nil 0 1])]

      (t/is (= [{:path [1], :pages [{:seg :t1, :page-idx 2} {:seg :log, :page-idx 0}]}
                {:path [2], :pages [{:seg :log, :page-idx 0} {:seg :log2, :page-idx 0}]}
                {:path [3], :pages [{:seg :t1, :page-idx 4} {:seg :t1, :page-idx 3} {:seg :log, :page-idx 0} {:seg :log2, :page-idx 1}]}
                {:path [0 0], :pages [{:seg :log, :page-idx 0}]}
                {:path [0 1], :pages [{:seg :t1, :page-idx 0} {:seg :log, :page-idx 0}]}
                {:path [0 2], :pages [{:seg :log, :page-idx 0}]}
                {:path [0 3], :pages [{:seg :t1, :page-idx 1} {:seg :log, :page-idx 0}]}]
               (->> (trie/->merge-plan [nil
                                        {:seg :t1, :trie (->arrow-hash-trie t1-root)}
                                        {:seg :log, :trie (->arrow-hash-trie log-root)}
                                        {:seg :log2, :trie (->arrow-hash-trie log2-root)}]
                                       {})
                    (map (fn [{:keys [path mp-nodes]}]
                           {:path (vec path)
                            :pages (mapv (fn [{:keys [segment ^ArrowHashTrie$Leaf node]}]
                                           {:seg (:seg segment), :page-idx (.getDataPageIndex node)})
                                         mp-nodes)}))
                    (sort-by :path)))))))

(defn ->trie-file-name
  " L0/L1 keys are submitted as [level next-row rows]; L2+ as [level part-vec next-row]"
  [[level & args]]

  (case (long level)
    (0 1) (let [[next-row rows] args]
            (util/->path (str (trie/->log-l0-l1-trie-key level next-row (or rows 0)) ".arrow")))

    (let [[part next-row] args]
      (util/->path (str (trie/->log-l2+-trie-key level (byte-array part) next-row) ".arrow")))))

(t/deftest test-selects-current-tries
  (letfn [(f [trie-keys]
            (->> (trie/current-trie-files (map ->trie-file-name trie-keys))
                 (mapv (comp (juxt :level (comp #(some-> % vec) :part) :next-row)
                             trie/parse-trie-file-path))))]

    (t/is (= [] (f [])))

    (t/testing "L0/L1 only"
      (t/is (= [[0 nil 1] [0 nil 2] [0 nil 3]]
               (f [[0 1] [0 2] [0 3]])))

      (t/is (= [[1 nil 2] [0 nil 3]]
               (f [[1 2] [0 1] [0 2] [0 3]]))
            "L1 file supersedes two L0 files"))

    (t/testing "L2"
      (t/is (= [[1 nil 2]]
               (f [[2 [0] 2] [2 [3] 2] [1 2]]))
            "L2 file doesn't supersede because not all parts complete")

      (t/is (= [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]]
               (f [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]
                   [1 2]]))
            "now the L2 file is complete")

      (t/is (= [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2] [0 nil 3]]
               (f [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]
                   [1 2]
                   [0 1] [0 2] [0 3]]))
            "L2 file supersedes L1, L1 supersedes L0, left with a single L0 file"))

    (t/testing "L3+"
      (t/is (= [[3 [0 0] 2] [3 [0 1] 2] [3 [0 2] 2] [3 [0 3] 2]
                [2 [1] 2] [2 [2] 2] [2 [3] 2] ; L2 path 0 covered
                [0 nil 3]]
               (f [[3 [0 0] 2] [3 [0 1] 2] [3 [0 2] 2] [3 [0 3] 2]
                   [3 [1 0] 2] [3 [1 2] 2] [3 [1 3] 2] ; L2 path 1 not covered yet, missing [1 1]
                   [2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]
                   [1 2]
                   [0 1] [0 2] [0 3]]))
            "L3 covered idx 0 but not 1")

      (t/is (= [[4 [0 1 0] 2] [4 [0 1 1] 2] [4 [0 1 2] 2] [4 [0 1 3] 2]
                [3 [0 0] 2] [3 [0 2] 2] [3 [0 3] 2] ; L3 path [0 1] covered
                [2 [1] 2] [2 [2] 2] [2 [3] 2] ; L2 path 0 covered
                [0 nil 3]]

               (f [[4 [0 1 0] 2] [4 [0 1 1] 2] [4 [0 1 2] 2] [4 [0 1 3] 2]
                   [3 [0 0] 2] [3 [0 1] 2] [3 [0 2] 2] [3 [0 3] 2]
                   [3 [1 0] 2] [3 [1 2] 2] [3 [1 3] 2] ; L2 path 1 not covered yet, missing [1 1]
                   [2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]
                   [1 2]
                   [0 1] [0 2] [0 3]]))
            "L4 covers L3 path [0 1]")

      (t/is (= [[4 [0 1 0] 2] [4 [0 1 1] 2] [4 [0 1 2] 2] [4 [0 1 3] 2]
                [3 [0 0] 2] [3 [0 2] 2] [3 [0 3] 2] ; L3 path [0 1] covered
                [2 [1] 2] [2 [2] 2] [2 [3] 2] ; L2 path 0 covered even though [0 1] GC'd
                [0 nil 3]]

               (f [[4 [0 1 0] 2] [4 [0 1 1] 2] [4 [0 1 2] 2] [4 [0 1 3] 2]
                   [3 [0 0] 2] [3 [0 2] 2] [3 [0 3] 2]
                   [3 [1 0] 2] [3 [1 2] 2] [3 [1 3] 2] ; L2 path 1 not covered yet, missing [1 1]
                   [2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]
                   [1 2]
                   [0 1] [0 2] [0 3]]))
            "L4 covers L3 path [0 1], L3 [0 1] GC'd, still correctly covered"))

    (t/testing "empty levels"
      (t/is (= [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2] [0 nil 3]]
               (f [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]
                   [0 1] [0 2] [0 3]]))
            "L1 empty")

      (t/is (= [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]]
               (f [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]]))
            "L1 and L0 empty")

      (t/is (= [[3 [0 0] 2] [3 [0 1] 2] [3 [0 2] 2] [3 [0 3] 2]
                [3 [1 0] 2] [3 [1 1] 2] [3 [1 2] 2] [3 [1 3] 2]
                [3 [2 0] 2] [3 [2 1] 2] [3 [2 2] 2] [3 [2 3] 2]
                [3 [3 0] 2] [3 [3 1] 2] [3 [3 2] 2] [3 [3 3] 2]
                [0 nil 3]]
               (f [[3 [0 0] 2] [3 [0 1] 2] [3 [0 2] 2] [3 [0 3] 2]
                   [3 [1 0] 2] [3 [1 1] 2] [3 [1 2] 2] [3 [1 3] 2]
                   [3 [2 0] 2] [3 [2 1] 2] [3 [2 2] 2] [3 [2 3] 2]
                   [3 [3 0] 2] [3 [3 1] 2] [3 [3 2] 2] [3 [3 3] 2]
                   [1 2]
                   [0 1] [0 2] [0 3]]))
            "L2 missing, still covers L1"))))

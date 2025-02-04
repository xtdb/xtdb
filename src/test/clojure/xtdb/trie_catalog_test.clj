(ns xtdb.trie-catalog-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]
            [xtdb.trie :as trie]
            [xtdb.trie-catalog :as cat]
            [xtdb.util :as util]))

(defn ->added-trie
  "L0/L1 keys are submitted as [level block-idx rows]; L2+ as [level part-vec block-idx]"
  [[level & args]]
  (case (long level)
    (0 1) (let [[block-idx size] args]
            (cat/->added-trie "foo"
                              (trie/->l0-l1-trie-key level block-idx)

                              size))
    (let [[part block-idx] args]
      (cat/->added-trie "foo"
                        (trie/->l2+-trie-key level (byte-array part) block-idx)
                        -1))))

(t/deftest test-selects-current-tries
  (letfn [(f [trie-keys]
            (-> trie-keys
                (->> (transduce (map ->added-trie)
                                (partial cat/apply-trie-notification {:l1-size-limit 20})))
                (cat/current-tries)
                (->> (sort-by (juxt (comp - :level) :part :block-idx))
                     (mapv (juxt :level (comp #(some-> % vec) :part) :block-idx)))))]

    (t/is (= [] (f [])))

    (t/testing "L0/L1 only"
      (t/is (= [[0 [] 0] [0 [] 1] [0 [] 2]]
               (f [[0 0 10] [0 1 10] [0 2 10]])))

      (t/is (= [[1 [] 1] [0 [] 2]]
               (f [[1 1 20] [0 0 10] [0 1 10] [0 2 10]]))
            "L1 file supersedes two L0 files")

      (t/is (= [[1 [] 1] [1 [] 3] [0 [] 4]]
               (f [[0 0 10] [0 1 10] [0 2 10] [0 3 10] [0 4 10]
                   [1 0 10] [1 1 20] [1 2 10] [1 3 20]]))
            "Superseded L1 files should not get returned"))

    (t/testing "L2"
      (t/is (= [[1 [] 0]]
               (f [[0 0 2] [1 0 2]
                   [2 [0] 0] [2 [3] 0]]))
            "L2 file doesn't supersede because not all parts complete")

      (t/is (= [[2 [0] 0] [2 [1] 0] [2 [2] 0] [2 [3] 0]]
               (f [[0 0 2] [1 0 2]
                   [2 [0] 0] [2 [1] 0] [2 [2] 0] [2 [3] 0]]))
            "now the L2 file is complete")

      (t/is (= [[2 [0] 1] [2 [1] 1] [2 [2] 1] [2 [3] 1] [0 [] 2]]
               (f [[0 0 1] [0 1 1] [0 2 1]
                   [1 0 1] [1 1 2]
                   [2 [0] 1] [2 [1] 1] [2 [2] 1] [2 [3] 1]]))
            "L2 file supersedes L1, L1 supersedes L0, left with a single L0 file"))

    (t/testing "L3+"
      (t/is (= [[3 [0 0] 1] [3 [0 1] 1] [3 [0 2] 1] [3 [0 3] 1]
                ;; L2 path 0 covered
                [2 [1] 1] [2 [2] 1] [2 [3] 1]
                [0 [] 2]]

               (f [[0 0 1] [0 1 1] [0 2 1]
                   [1 1 2]
                   [2 [0] 1] [2 [1] 1] [2 [2] 1] [2 [3] 1]
                   [3 [0 0] 1] [3 [0 1] 1] [3 [0 2] 1] [3 [0 3] 1]

                   ;; L2 path 1 not covered yet, missing [1 1]
                   [3 [1 0] 1] [3 [1 2] 1] [3 [1 3] 1]]))

            "L3 covered idx 0 but not 1")

      (t/is (= [[4 [0 1 0] 1] [4 [0 1 1] 1] [4 [0 1 2] 1] [4 [0 1 3] 1]
                [3 [0 0] 1] [3 [0 2] 1] [3 [0 3] 1] ; L3 path [0 1] covered
                [2 [1] 1] [2 [2] 1] [2 [3] 1] ; L2 path 0 covered
                [0 [] 2]]

               (f [[0 0 1] [0 1 1] [0 2 1]
                   [1 1 2]
                   [2 [0] 1] [2 [1] 1] [2 [2] 1] [2 [3] 1]
                   [3 [0 0] 1] [3 [0 1] 1] [3 [0 2] 1] [3 [0 3] 1]
                   [3 [1 0] 1] [3 [1 2] 1] [3 [1 3] 1] ; L2 path 1 not covered yet, missing [1 1]
                   [4 [0 1 0] 1] [4 [0 1 1] 1] [4 [0 1 2] 1] [4 [0 1 3] 1]]))
            "L4 covers L3 path [0 1]"))))

(t/deftest reconstructs-state-on-startup
  (let [node-dir (util/->path "target/trie-catalog-test/reconstructs-state")]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir, :compactor-threads 0})]
      (let [cat (cat/trie-catalog node)]
        (xt/execute-tx node [[:put-docs :foo {:xt/id 1}]])
        (tu/finish-block! node)

        (xt/execute-tx node [[:put-docs :foo {:xt/id 2}]])
        (tu/finish-block! node)

        (t/is (= #{"public/foo" "xt/txs"} (cat/table-names cat)))
        (t/is (= [{:trie-key "l00-b01", :state :live}
                  {:trie-key "l00-b00", :state :live}]
                 (->> (cat/current-tries (cat/trie-state cat "public/foo"))
                      (mapv #(select-keys % [:trie-key :state])))))))

    (with-open [node (tu/->local-node {:node-dir node-dir, :compactor-threads 0})]
      (let [cat (cat/trie-catalog node)]
        (t/is (= #{"public/foo" "xt/txs"} (cat/table-names cat)))
        (t/is (= [{:trie-key "l00-b01", :state :live}
                  {:trie-key "l00-b00", :state :live}]
                 (->> (cat/current-tries (cat/trie-state cat "public/foo"))
                      (mapv #(select-keys % [:trie-key :state])))))))))

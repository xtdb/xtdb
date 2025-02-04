(ns xtdb.trie-catalog-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]
            [xtdb.trie :as trie]
            [xtdb.trie-catalog :as cat]
            [xtdb.util :as util]))

(defn ->added-trie
  " L0/L1 keys are submitted as [level first-row next-row rows]; L2+ as [level part-vec next-row]"
  [[level & args]]
  (cat/->added-trie "foo"
                    (case (long level)
                      (0 1) (let [[first-row next-row rows] args]
                              (trie/->log-l0-l1-trie-key level first-row next-row (or rows 0)))
                      (let [[part next-row] args]
                        (trie/->log-l2+-trie-key level (byte-array part) next-row)))
                    -1))

(t/deftest test-selects-current-tries
  (letfn [(f [trie-keys]
            (-> (transduce (map ->added-trie) cat/apply-trie-notification trie-keys)
                (cat/current-tries)
                (->> (sort-by (juxt (comp - :level) :part :next-row))
                     (mapv (juxt :level (comp #(some-> % vec) :part) :next-row)))))]

    (t/is (= [] (f [])))

    (t/testing "L0/L1 only"
      (t/is (= [[0 [] 1] [0 [] 2] [0 [] 3]]
               (f [[0 0 1 1] [0 1 2 1] [0 2 3 1]])))

      (t/is (= [[1 [] 2] [0 [] 3]]
               (f [[1 0 2 2] [0 0 1 1] [0 1 2 1] [0 2 3 1]]))
            "L1 file supersedes two L0 files")

      (t/is (= [[1 [] 3] [1 [] 4] [0 [] 5]]
               (f [[0 0 1 1] [0 1 2 1] [0 2 3 1] [0 3 4 1] [0 4 5 1]
                   [1 0 1 1] [1 0 2 2] [1 0 3 3] [1 3 4 1]]))
            "Superseded L1 files should not get returned"))

    (t/testing "L2"
      (t/is (= [[1 [] 2]]
               (f [[0 0 2 2] [1 0 2 2]
                   [2 [0] 2] [2 [3] 2]]))
            "L2 file doesn't supersede because not all parts complete")

      (t/is (= [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]]
               (f [[0 0 2 2] [1 0 2 2]
                   [2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]]))
            "now the L2 file is complete")

      (t/is (= [[2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2] [0 [] 3]]
               (f [[0 0 1 1] [0 1 2 1] [0 2 3 1]
                   [1 0 2 2]
                   [2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]]))
            "L2 file supersedes L1, L1 supersedes L0, left with a single L0 file"))

    (t/testing "L3+"
      (t/is (= [[3 [0 0] 2] [3 [0 1] 2] [3 [0 2] 2] [3 [0 3] 2]
                ;; L2 path 0 covered
                [2 [1] 2] [2 [2] 2] [2 [3] 2]
                [0 [] 3]]

               (f [[0 0 1 1] [0 1 2 1] [0 2 3 1]
                   [1 0 2 2]
                   [2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]
                   [3 [0 0] 2] [3 [0 1] 2] [3 [0 2] 2] [3 [0 3] 2]

                   ;; L2 path 1 not covered yet, missing [1 1]
                   [3 [1 0] 2] [3 [1 2] 2] [3 [1 3] 2]]))

            "L3 covered idx 0 but not 1")

      (t/is (= [[4 [0 1 0] 2] [4 [0 1 1] 2] [4 [0 1 2] 2] [4 [0 1 3] 2]
                [3 [0 0] 2] [3 [0 2] 2] [3 [0 3] 2] ; L3 path [0 1] covered
                [2 [1] 2] [2 [2] 2] [2 [3] 2] ; L2 path 0 covered
                [0 [] 3]]

               (f [[0 0 1 1] [0 1 2 1] [0 2 3 1]
                   [1 0 2 2]
                   [2 [0] 2] [2 [1] 2] [2 [2] 2] [2 [3] 2]
                   [3 [0 0] 2] [3 [0 1] 2] [3 [0 2] 2] [3 [0 3] 2]
                   [3 [1 0] 2] [3 [1 2] 2] [3 [1 3] 2] ; L2 path 1 not covered yet, missing [1 1]
                   [4 [0 1 0] 2] [4 [0 1 1] 2] [4 [0 1 2] 2] [4 [0 1 3] 2]]))
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
        (t/is (= [{:trie-key "log-l00-fr02-nr04-rs1",
                   :state :live}
                  {:trie-key "log-l00-fr00-nr02-rs1",
                   :state :live}]
                 (->> (cat/current-tries (cat/trie-state cat "public/foo"))
                      (mapv #(select-keys % [:trie-key :state])))))))

    (with-open [node (tu/->local-node {:node-dir node-dir, :compactor-threads 0})]
      (let [cat (cat/trie-catalog node)]
        (t/is (= #{"public/foo" "xt/txs"} (cat/table-names cat)))
        (t/is (= [{:trie-key "log-l00-fr02-nr04-rs1",
                   :state :live}
                  {:trie-key "log-l00-fr00-nr02-rs1",
                   :state :live}]
                 (->> (cat/current-tries (cat/trie-state cat "public/foo"))
                      (mapv #(select-keys % [:trie-key :state])))))))))

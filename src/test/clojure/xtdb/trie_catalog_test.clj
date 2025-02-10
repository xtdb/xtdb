(ns xtdb.trie-catalog-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]
            [xtdb.trie-catalog :as cat]
            [xtdb.util :as util]
            [xtdb.trie :as trie]))

(t/deftest test-selects-current-tries
  (letfn [(f [& trie-keys]
            (-> trie-keys
                (->> (transduce (map (fn [[trie-key size]]
                                       (-> (trie/parse-trie-key trie-key)
                                           (assoc :data-file-size (or size -1)))))
                                (completing (partial cat/apply-trie-notification {:l1-size-limit 20}))
                                {}))
                (cat/current-tries)
                (->> (into #{} (map :trie-key)))))]

    (t/is (= #{} (f)))

    (t/testing "L0/L1 only"
      (t/is (= #{"l00-rc-b00" "l00-rc-b01" "l00-rc-b02"}
               (f ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10])))

      (t/is (= #{"l01-rc-b01" "l00-rc-b02"}
               (f ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10] ["l01-rc-b01" 20]))
            "L1 file supersedes two L0 files")

      (t/is (= #{"l01-rc-b01" "l01-rc-b03" "l00-rc-b04"}
               (f ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10] ["l00-rc-b03" 10] ["l00-rc-b04" 10]
                  ["l01-rc-b00" 10] ["l01-rc-b01" 20] ["l01-rc-b02" 10] ["l01-rc-b03" 20]))
            "Superseded L1 files should not get returned"))

    (t/testing "L2"
      (t/is (= #{"l01-rc-b00"}
               (f ["l00-rc-b00" 2] ["l01-rc-b00" 2]
                  ["l02-rc-p0-b00"] ["l02-rc-p3-b00"]))
            "L2 file doesn't supersede because not all parts complete")

      (t/is (= #{"l02-rc-p0-b00" "l02-rc-p1-b00" "l02-rc-p2-b00" "l02-rc-p3-b00"}
               (f ["l00-rc-b00" 2] ["l01-rc-b00" 2]
                  ["l02-rc-p0-b00"] ["l02-rc-p1-b00"] ["l02-rc-p2-b00"] ["l02-rc-p3-b00"]))
            "now the L2 file is complete")

      (t/is (= #{"l02-rc-p0-b01" "l02-rc-p1-b01" "l02-rc-p2-b01" "l02-rc-p3-b01" "l00-rc-b02"}
               (f ["l00-rc-b00" 1] ["l00-rc-b01" 1] ["l00-rc-b02" 1]
                  ["l01-rc-b00" 1] ["l01-rc-b01" 2]
                  ["l02-rc-p0-b01"] ["l02-rc-p1-b01"] ["l02-rc-p2-b01"] ["l02-rc-p3-b01"]))
            "L2 file supersedes L1, L1 supersedes L0, left with a single L0 file"))

    (t/testing "L3+"
      (t/is (= #{"l03-rc-p00-b01" "l03-rc-p01-b01" "l03-rc-p02-b01" "l03-rc-p03-b01"
                 ;; L2 path 0 covered
                 "l02-rc-p1-b01" "l02-rc-p2-b01" "l02-rc-p3-b01"
                 "l00-rc-b02"}

               (f ["l00-rc-b00" 1] ["l00-rc-b01" 1] ["l00-rc-b02" 1]
                  ["l01-rc-b01" 2]
                  ["l02-rc-p0-b01"] ["l02-rc-p1-b01"] ["l02-rc-p2-b01"] ["l02-rc-p3-b01"]
                  ["l03-rc-p00-b01"] ["l03-rc-p01-b01"] ["l03-rc-p02-b01"] ["l03-rc-p03-b01"]

                  ;; L2 path 1 not covered yet, missing [1 1]
                  ["l03-rc-p10-b01"] ["l03-rc-p12-b01"] ["l03-rc-p13-b01"]))

            "L3 covered idx 0 but not 1")

      (t/is (= #{"l04-rc-p010-b01" "l04-rc-p011-b01" "l04-rc-p012-b01" "l04-rc-p013-b01"
                 "l03-rc-p00-b01" "l03-rc-p02-b01" "l03-rc-p03-b01" ; L3 path [0 1] covered
                 "l02-rc-p1-b01" "l02-rc-p2-b01" "l02-rc-p3-b01" ; L2 path 0 covered
                 "l00-rc-b02"}

               (f ["l00-rc-b00" 1] ["l00-rc-b01" 1] ["l00-rc-b02" 1]
                  ["l01-rc-b01" 2]
                  ["l02-rc-p0-b01"] ["l02-rc-p1-b01"] ["l02-rc-p2-b01"] ["l02-rc-p3-b01"]
                  ["l03-rc-p00-b01"] ["l03-rc-p01-b01"] ["l03-rc-p02-b01"] ["l03-rc-p03-b01"]
                  ["l03-rc-p10-b01"] ["l03-rc-p12-b01"] ["l03-rc-p13-b01"] ; L2 path 1 not covered yet, missing [1 1]
                  ["l04-rc-p010-b01"] ["l04-rc-p011-b01"] ["l04-rc-p012-b01"] ["l04-rc-p013-b01"]))
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

        (t/is (= #{"public/foo" "xt/txs"} (.getTableNames cat)))
        (t/is (= #{"l00-rc-b00" "l00-rc-b01"}
                 (->> (cat/current-tries (cat/trie-state cat "public/foo"))
                      (into #{} (map :trie-key)))))))

    (with-open [node (tu/->local-node {:node-dir node-dir, :compactor-threads 0})]
      (let [cat (cat/trie-catalog node)]
        (t/is (= #{"public/foo" "xt/txs"} (.getTableNames cat)))
        (t/is (= #{"l00-rc-b01" "l00-rc-b00"}
                 (->> (cat/current-tries (cat/trie-state cat "public/foo"))
                      (into #{} (map :trie-key)))))))))

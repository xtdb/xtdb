(ns xtdb.trie-catalog-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]
            [xtdb.trie-catalog :as cat]
            [xtdb.util :as util]
            [xtdb.trie :as trie]))

(defn- apply-msgs [& trie-keys]
  (-> trie-keys
      (->> (transduce (map (fn [[trie-key size]]
                             (-> (trie/parse-trie-key trie-key)
                                 (assoc :data-file-size (or size -1)))))
                      (completing (partial cat/apply-trie-notification {:file-size-target 20}))
                      {}))))

(defn- curr-tries [& trie-keys]
  (-> (apply apply-msgs trie-keys)
      (cat/current-tries)
      (->> (into #{} (map :trie-key)))))

(t/deftest test-stale-msg
  (letfn [(stale? [tries trie-key]
            (boolean (cat/stale-msg? tries (-> (trie/parse-trie-key trie-key)
                                               (update :part vec)))))]

    (t/is (false? (stale? nil "l00-rc-b00")))

    (let [l0s (apply-msgs ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10])]
      (t/is (true? (stale? l0s "l00-rc-b00")))
      (t/is (true? (stale? l0s "l00-rc-b02")))
      (t/is (false? (stale? l0s "l00-rc-b03"))))

    (let [l1 (apply-msgs ["l01-r20200101-b01" 10])]
      (t/is (false? (stale? l1 "l01-r20200102-b00")))
      (t/is (true? (stale? l1 "l01-r20200101-b01")))
      (t/is (false? (stale? l1 "l01-r20200102-b01")))
      (t/is (false? (stale? l1 "l01-r20190101-b02")))
      (t/is (false? (stale? l1 "l01-rc-b01")))
      (t/is (false? (stale? l1 "l01-rc-b02"))))))

(t/deftest test-l0-l1-tries
  (t/is (= #{} (curr-tries)))

  (t/is (= #{"l00-rc-b00" "l00-rc-b01" "l00-rc-b02"}
           (curr-tries ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10])))

  (t/is (= #{"l00-rc-b00" "l00-rc-b01" "l00-rc-b02"}
           (curr-tries ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10]
                       ["l01-r20200101-b01" 5] ["l01-r20200102-b01" 5]))
        "historical tries nascent until we see the current")

  (t/is (= #{"l01-r20200101-b01" "l01-r20200102-b01" "l01-rc-b01" "l00-rc-b02"}
           (curr-tries ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10]
                       ["l01-r20200101-b01" 5] ["l01-r20200102-b01" 5] ["l01-rc-b01" 15]))
        "seen current, now historical tries are live too")

  (t/is (= #{"l01-r20200101-b01" "l01-rc-b02" "l01-rc-b03"}
           (curr-tries ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10] ["l00-rc-b02" 5]
                       ["l01-rc-b00" 10] ["l01-r20200101-b01" 15] ["l01-rc-b01" 5] ["l01-rc-b02" 20] ["l01-rc-b03" 5]))
        "L1C files can oscillate in size until they're full")

  (t/is (= #{"l01-rc-b01" "l00-rc-b02"}
           (curr-tries ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10] ["l01-rc-b01" 20]))
        "L1 file supersedes two L0 files")

  (t/is (= #{"l01-rc-b01" "l01-rc-b03" "l00-rc-b04"}
           (curr-tries ["l00-rc-b00" 10] ["l00-rc-b01" 10] ["l00-rc-b02" 10] ["l00-rc-b03" 10] ["l00-rc-b04" 10]
                       ["l01-rc-b00" 10] ["l01-rc-b01" 20] ["l01-rc-b02" 10] ["l01-rc-b03" 20]))
        "Superseded L1 files should not get returned"))

(t/deftest test-selects-l2-tries
  (t/is (= #{"l01-rc-b00"}
           (curr-tries ["l01-rc-b00" 2]
                       ["l02-rc-p0-b00"] ["l02-rc-p3-b00"]))
        "L2 file doesn't supersede because not all parts complete")

  (t/is (= #{"l02-rc-p0-b00" "l02-rc-p1-b00" "l02-rc-p2-b00" "l02-rc-p3-b00"}
           (curr-tries ["l01-rc-b00" 2]
                       ["l02-rc-p0-b00"] ["l02-rc-p1-b00"] ["l02-rc-p2-b00"] ["l02-rc-p3-b00"]))
        "now the L2 file is complete")

  (t/is (= #{"l02-rc-p0-b01" "l02-rc-p1-b01" "l02-rc-p2-b01" "l02-rc-p3-b01" "l00-rc-b02"}
           (curr-tries ["l00-rc-b00" 1] ["l00-rc-b01" 1] ["l00-rc-b02" 1]
                       ["l01-rc-b00" 1] ["l01-rc-b01" 2]
                       ["l02-rc-p0-b01"] ["l02-rc-p1-b01"] ["l02-rc-p2-b01"] ["l02-rc-p3-b01"]))
        "L2 file supersedes L1, L1 supersedes L0, left with a single L0 file"))

(t/deftest test-selects-l2h-tries
  (t/is (= #{"l02-r20200102-b00" "l02-r20200101-b01" "l01-r20200102-b01" "l01-rc-b01"}
           (curr-tries ["l01-r20200101-b01" 5] ["l01-r20200102-b01" 5] ["l01-rc-b01" 15] ["l01-r20200102-b00" 5]
                       ["l02-r20200101-b01" 5] ["l02-r20200102-b00" 5]))
        "L2H supersedes L1H with the same recency")

  (t/is (= #{"l02-r20200101-b02" "l02-r20200101-b03"
             "l02-r20200102-b01"
             "l02-r20200103-b00" "l02-r20200103-b01"}
           (curr-tries ["l02-r20200101-b00" 5] ["l02-r20200101-b01" 10] ["l02-r20200101-b02" 20] ["l02-r20200101-b03" 5]
                       ["l02-r20200102-b00" 10] ["l02-r20200102-b01" 15]
                       ["l02-r20200103-b00" 20] ["l02-r20200103-b01" 15]))
        "L2H is levelled within the recency partition"))

(t/deftest test-l3+
  (t/is (= #{"l03-rc-p00-b01" "l03-rc-p01-b01" "l03-rc-p02-b01" "l03-rc-p03-b01"
             ;; L2 path 0 covered
             "l02-rc-p1-b01" "l02-rc-p2-b01" "l02-rc-p3-b01"}

           (curr-tries ["l02-rc-p0-b01"] ["l02-rc-p1-b01"] ["l02-rc-p2-b01"] ["l02-rc-p3-b01"]
                       ["l03-rc-p00-b01"] ["l03-rc-p01-b01"] ["l03-rc-p02-b01"] ["l03-rc-p03-b01"]

                       ;; L2 path 1 not covered yet, missing [1 1]
                       ["l03-rc-p10-b01"] ["l03-rc-p12-b01"] ["l03-rc-p13-b01"]))

        "L3 covered idx 0 but not 1")

  (t/is (= #{"l03-r20200101-p0-b01" "l03-r20200101-p1-b01" "l03-r20200101-p2-b01" "l03-r20200101-p3-b01"

             ;; L2 20200101 covered
             "l02-r20200102-b00"}

           (curr-tries ["l02-r20200101-b01"] ["l02-r20200102-b00"]
                       ["l03-r20200101-p0-b01"] ["l03-r20200101-p1-b01"] ["l03-r20200101-p2-b01"] ["l03-r20200101-p3-b01"]

                       ;; L2 20200102 not covered yet, missing [1]
                       ["l03-r20200102-p0-b01"] ["l03-r20200102-p2-b01"] ["l03-r20200102-p3-b01"]))

        "L3 covered idx 0 but not 1")

  (t/is (= #{"l04-rc-p010-b01" "l04-rc-p011-b01" "l04-rc-p012-b01" "l04-rc-p013-b01"
             "l03-rc-p00-b01" "l03-rc-p02-b01" "l03-rc-p03-b01"} ; L3 path [0 1] covered

           (curr-tries ["l03-rc-p00-b01"] ["l03-rc-p01-b01"] ["l03-rc-p02-b01"] ["l03-rc-p03-b01"]
                       ["l03-rc-p10-b01"] ["l03-rc-p12-b01"] ["l03-rc-p13-b01"] ; L2 path 1 not covered yet, missing [1 1]
                       ["l04-rc-p010-b01"] ["l04-rc-p011-b01"] ["l04-rc-p012-b01"] ["l04-rc-p013-b01"]))
        "L4 covers L3 path [0 1]")

  (t/is (= #{"l04-r20200101-p00-b01" "l04-r20200101-p01-b01" "l04-r20200101-p02-b01" "l04-r20200101-p03-b01"
             ;; L3 path 0 covered
             "l03-r20200101-p1-b01" "l03-r20200101-p2-b01" "l03-r20200101-p3-b01"}

           (curr-tries ["l03-r20200101-p0-b01"] ["l03-r20200101-p1-b01"] ["l03-r20200101-p2-b01"] ["l03-r20200101-p3-b01"]
                       ["l04-r20200101-p00-b01"] ["l04-r20200101-p01-b01"] ["l04-r20200101-p02-b01"] ["l04-r20200101-p03-b01"]

                       ;; L2 path 1 not covered yet, missing [1 1]
                       ["l04-r20200101-p10-b01"] ["l04-r20200101-p12-b01"] ["l04-r20200101-p13-b01"]))

        "L4H covered idx 0 but not 1"))

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
                      (into #{} (map :trie-key)))))))

    (t/testing "artifically adding tries"

      (with-open [node (tu/->local-node {:node-dir node-dir, :compactor-threads 0})]
        (let [cat (cat/trie-catalog node)]
          (.addTries cat
                     (->> [["l00-rc-b00" 1] ["l00-rc-b01" 1] ["l00-rc-b02" 1] ["l00-rc-b03" 1]
                           ["l01-rc-b00" 2] ["l01-rc-b01" 2] ["l01-rc-b02" 2]
                           ["l02-rc-p0-b01" 4] ["l02-rc-p1-b01" 4] ["l02-rc-p2-b01" 4] ["l02-rc-p3-b01"4]]
                          (map #(apply trie/->trie-details "public/foo" %))))
          (tu/finish-block! node))))

    (with-open [node (tu/->local-node {:node-dir node-dir, :compactor-threads 0})]
      (let [cat (cat/trie-catalog node)]
        (t/is (= #{"l00-rc-b03"
                   "l01-rc-b02"
                   "l02-rc-p0-b01"
                   "l02-rc-p1-b01"
                   "l02-rc-p2-b01"
                   "l02-rc-p3-b01"}
                 (->> (cat/current-tries (cat/trie-state cat "public/foo"))
                      (into (sorted-set) (map :trie-key)))))))))

(t/deftest handles-l1h-l1c-ordering-4301
  ;; L1H and L1C are in different partitions, so (strictly speaking) we should handle these out of order
  ;; (in practice, we always submit L1C after L1H - but this keeps the invariant definition simpler to understand)
  (t/is (= #{"l01-r20200101-b01" "l01-r20200102-b01" "l01-rc-b01"}
           (curr-tries ["l01-r20200101-b01" 5] ["l01-rc-b01" 15] ["l01-r20200102-b01" 5]))))

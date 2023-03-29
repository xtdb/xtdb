(ns xtdb.operator-test
  (:require [clojure.test :as t]
            [xtdb.datalog :as xt]
            [xtdb.expression.metadata :as expr.meta]
            [xtdb.node :as node]
            [xtdb.metadata :as meta]
            [xtdb.test-util :as tu])
  (:import (xtdb.metadata IMetadataManager)
           (java.time LocalTime)
           (org.roaringbitmap RoaringBitmap)))

(t/use-fixtures :once tu/with-allocator)

(t/deftest test-find-gt-ivan
  (with-open [node (node/start-node {:xtdb/live-chunk {:rows-per-block 2, :rows-per-chunk 10}})]
    (-> (xt/submit-tx node [[:put :xt_docs {:name "Håkan", :id :hak}]])
        (tu/then-await-tx* node))

    (tu/finish-chunk! node)

    (xt/submit-tx node [[:put :xt_docs {:name "Dan", :id :dan}]
                        [:put :xt_docs {:name "Ivan", :id :iva}]])

    (let [tx1 (-> (xt/submit-tx node [[:put :xt_docs {:name "James", :id :jms}]
                                      [:put :xt_docs {:name "Jon", :id :jon}]])
                  (tu/then-await-tx* node))]

      (tu/finish-chunk! node)

      (let [^IMetadataManager metadata-mgr (tu/component node ::meta/metadata-manager)]
        (letfn [(test-query-ivan [expected tx]
                  (t/is (= expected
                           (set (tu/query-ra '[:scan {:table xt_docs} [id {name (> name "Ivan")}]]
                                             {:node node, :basis {:tx tx}}))))

                  (t/is (= expected
                           (set (tu/query-ra '[:scan {:table xt_docs} [id {name (> name ?name)}]]
                                             {:node node, :basis {:tx tx}, :params {'?name "Ivan"}})))))]

          (t/is (= #{0 1} (set (keys (.chunksMetadata metadata-mgr)))))

          (let [expected-match [(meta/map->ChunkMatch
                                 {:chunk-idx 1, :block-idxs (doto (RoaringBitmap.) (.add 1)), :col-names #{"_row-id" "id" "name"}})]]
            (t/is (= expected-match
                     (meta/matching-chunks metadata-mgr "xt_docs"
                                           (expr.meta/->metadata-selector '(> name "Ivan") '#{name} {})))
                  "only needs to scan chunk 1, block 1")
            (t/is (= expected-match
                     (with-open [params (tu/open-params {'?name "Ivan"})]
                       (meta/matching-chunks metadata-mgr "xt_docs"
                                             (expr.meta/->metadata-selector '(> name ?name) '#{name} params))))
                  "only needs to scan chunk 1, block 1"))

          (let [tx2 (xt/submit-tx node [[:put :xt_docs {:name "Jeremy", :id :jdt}]])]

            (test-query-ivan #{{:id :jms, :name "James"}
                               {:id :jon, :name "Jon"}}
                             tx1)

            (test-query-ivan #{{:id :jms, :name "James"}
                               {:id :jon, :name "Jon"}
                               {:id :jdt, :name "Jeremy"}}
                             tx2)))))))

(t/deftest test-find-eq-ivan
  (with-open [node (node/start-node {:xtdb/live-chunk {:rows-per-block 3, :rows-per-chunk 10}})]
    (-> (xt/submit-tx node [[:put :xt_docs {:name "Håkan", :id :hak}]
                            [:put :xt_docs {:name "James", :id :jms}]
                            [:put :xt_docs {:name "Ivan", :id :iva}]])
        (tu/then-await-tx* node))

    (tu/finish-chunk! node)
    (-> (xt/submit-tx node [[:put :xt_docs {:name "Håkan", :id :hak}]

                            [:put :xt_docs {:name "James", :id :jms}]])
        (tu/then-await-tx* node))

    (tu/finish-chunk! node)
    (let [^IMetadataManager metadata-mgr (tu/component node ::meta/metadata-manager)]
      (t/is (= #{0 3} (set (keys (.chunksMetadata metadata-mgr)))))
      (let [expected-match [(meta/map->ChunkMatch
                             {:chunk-idx 0, :block-idxs (doto (RoaringBitmap.) (.add 0)), :col-names #{"_row-id" "id" "name"}})]]
        (t/is (= expected-match
                 (meta/matching-chunks metadata-mgr "xt_docs"
                                       (expr.meta/->metadata-selector '(= name "Ivan") '#{name} {})))
              "only needs to scan chunk 0, block 0")

        (t/is (= expected-match
                 (with-open [params (tu/open-params {'?name "Ivan"})]
                   (meta/matching-chunks metadata-mgr "xt_docs"
                                         (expr.meta/->metadata-selector '(= name ?name) '#{name} params))))
              "only needs to scan chunk 0, block 0"))

      (t/is (= #{{:name "Ivan"}}
               (set (tu/query-ra '[:scan {:table xt_docs} [{name (= name "Ivan")}]]
                                 {:node node}))))

      (t/is (= #{{:name "Ivan"}}
               (set (tu/query-ra '[:scan {:table xt_docs} [{name (= name ?name)}]]
                                 {:node node, :params {'?name "Ivan"}})))))))

(t/deftest test-temporal-bounds
  (with-open [node (node/start-node {})]
    (let [{tt1 :sys-time} (xt/submit-tx node [[:put :xt_docs {:id :my-doc, :last-updated "tx1"}]])
          {tt2 :sys-time} (xt/submit-tx node [[:put :xt_docs {:id :my-doc, :last-updated "tx2"}]])]
      (letfn [(q [& temporal-constraints]
                (->> (tu/query-ra [:scan '{:table xt_docs, :for-sys-time :all-time}
                                   (into '[last-updated] temporal-constraints)]
                                  {:node node, :params {'?sys-time1 tt1, '?sys-time2 tt2}})
                     (into #{} (map :last-updated))))]
        (t/is (= #{"tx1" "tx2"}
                 (q)))

        (t/is (= #{"tx1"}
                 (q '{system_time_start (<= system_time_start ?sys-time1)})))

        (t/is (= #{}
                 (q '{system_time_start (< system_time_start ?sys-time1)})))

        (t/is (= #{"tx1" "tx2"}
                 (q '{system_time_start (<= system_time_start ?sys-time2)})))

        (t/is (= #{"tx1" "tx2"}
                 (q '{system_time_start (> system_time_start ?sys-time1)})))

        (t/is (= #{}
                 (q '{system_time_end (< system_time_end ?sys-time2)})))

        (t/is (= #{"tx1"}
                 (q '{system_time_end (<= system_time_end ?sys-time2)})))

        (t/is (= #{"tx1" "tx2"}
                 (q '{system_time_end (> system_time_end ?sys-time2)})))

        (t/is (= #{"tx1" "tx2"}
                 (q '{system_time_end (>= system_time_end ?sys-time2)})))

        (t/testing "multiple constraints"
          (t/is (= #{"tx1"}
                   (q '{system_time_start (and (<= system_time_start ?sys-time1)
                                               (<= system_time_start ?sys-time2))})))

          (t/is (= #{"tx1"}
                   (q '{system_time_start (and (<= system_time_start ?sys-time2)
                                               (<= system_time_start ?sys-time1))})))

          (t/is (= #{"tx1" "tx2"}
                   (q '{system_time_end (and (> system_time_end ?sys-time2)
                                             (> system_time_end ?sys-time1))})))

          (t/is (= #{"tx1" "tx2"}
                   (q '{system_time_end (and (> system_time_end ?sys-time1)
                                             (> system_time_end ?sys-time2))}))))

        (t/is (= #{}
                 (q '{system_time_start (<= system_time_start ?sys-time1)}
                    '{system_time_end (< system_time_end ?sys-time2)})))

        (t/is (= #{"tx1"}
                 (q '{system_time_start (<= system_time_start ?sys-time1)}
                    '{system_time_end (<= system_time_end ?sys-time2)})))

        (t/is (= #{"tx1"}
                 (q '{system_time_start (<= system_time_start ?sys-time1)}
                    '{system_time_end (> system_time_end ?sys-time1)}))
              "as of tt1")

        (t/is (= #{"tx1" "tx2"}
                 (q '{system_time_start (<= system_time_start ?sys-time2)}
                    '{system_time_end (> system_time_end ?sys-time2)}))
              "as of tt2")))))

(t/deftest test-fixpoint-operator
  (t/testing "factorial"
    (t/is (= [{:a 0, :b 1}
              {:a 1, :b 1}
              {:a 2, :b 2}
              {:a 3, :b 6}
              {:a 4, :b 24}
              {:a 5, :b 120}
              {:a 6, :b 720}
              {:a 7, :b 5040}
              {:a 8, :b 40320}]
             (tu/query-ra '[:fixpoint Fact
                            [:table ?table]
                            [:select
                             (<= a 8)
                             [:project
                              [{a (+ a 1)}
                               {b (* (+ a 1) b)}]
                              Fact]]]
                          {:table-args {'?table [{:a 0 :b 1}]}}))))

  (t/testing "transitive closure"
    (t/is (= [{:x "a", :y "b"}
              {:x "b", :y "c"}
              {:x "c", :y "d"}
              {:x "d", :y "a"}
              {:x "d", :y "b"}
              {:x "a", :y "c"}
              {:x "b", :y "d"}
              {:x "c", :y "a"}
              {:x "c", :y "b"}
              {:x "d", :y "c"}
              {:x "a", :y "d"}
              {:x "b", :y "a"}
              {:x "b", :y "b"}
              {:x "c", :y "c"}
              {:x "d", :y "d"}
              {:x "a", :y "a"}]
             (tu/query-ra '[:fixpoint Path
                            [:table ?table]
                            [:project [x y]
                             [:join [{z z}]
                              [:rename {y z} Path]
                              [:rename {x z} Path]]]]
                          {:table-args {'?table [{:x "a" :y "b"}
                                                 {:x "b" :y "c"}
                                                 {:x "c" :y "d"}
                                                 {:x "d" :y "a"}]}})))))

(t/deftest test-assignment-operator
  (t/is (= [{:a 1 :b 1}]
           (tu/query-ra '[:assign [X [:table ?x]
                                   Y [:table ?y]]
                          [:join [{a b}] X Y]]
                        {:table-args '{?x [{:a 1}]
                                       ?y [{:b 1}]}})))

  (t/testing "can see earlier assignments"
    (t/is (= [{:a 1 :b 1}]
             (tu/query-ra '[:assign [X [:table ?x]
                                     Y [:join [{a b}] X [:table ?y]]
                                     X Y]
                            X]
                          {:table-args '{?x [{:a 1}]
                                         ?y [{:b 1}]}})))))

(t/deftest test-unwind-operator
  (t/is (= [{:a 1, :b [1 2], :b* 1}
            {:a 1, :b [1 2], :b* 2}
            {:a 2, :b [3 4 5], :b* 3}
            {:a 2, :b [3 4 5], :b* 4}
            {:a 2, :b [3 4 5], :b* 5}]
           (tu/query-ra '[:unwind {b* b}
                          [:table ?x]]
                        {:table-args '{?x [{:a 1, :b [1 2]} {:a 2, :b [3 4 5]}]}})))

  (t/is (= [{:a 1, :b* 1} {:a 1, :b* 2}]
           (tu/query-ra '[:project [a b*]
                          [:unwind {b* b}
                           [:table ?x]]]
                        {:table-args '{?x [{:a 1, :b [1 2]} {:a 2, :b []}]}}))
        "skips rows with empty lists")

  (t/is (= [{:a 1, :b* 1} {:a 1, :b* 2}]
           (tu/query-ra '[:project [a b*]
                          [:unwind {b* b}
                           [:table ?x]]]
                        {:table-args '{?x [{:a 2, :b 1} {:a 1, :b [1 2]}]}}))
        "skips rows with non-list unwind column")

  (t/is (= [{:a 1, :b* 1} {:a 1, :b* "foo"}]
           (tu/query-ra '[:project [a b*]
                          [:unwind {b* b}
                           [:table ?x]]]
                        {:table-args '{?x [{:a 1, :b [1 "foo"]}]}}))
        "handles multiple types")

  (t/is (= [{:a 1, :b* 1, :$ordinal 1}
            {:a 1, :b* 2, :$ordinal 2}
            {:a 2, :b* 3, :$ordinal 1}
            {:a 2, :b* 4, :$ordinal 2}
            {:a 2, :b* 5, :$ordinal 3}]
           (tu/query-ra '[:project [a b* $ordinal]
                          [:unwind {b* b} {:ordinality-column $ordinal}
                           [:table ?x]]]
                        {:table-args '{?x [{:a 1 :b [1 2]} {:a 2 :b [3 4 5]}]}}))
        "with ordinality"))

(t/deftest test-project-row-number
  (t/is (= [{:a 12, :$row-num 1}, {:a 0, :$row-num 2}, {:a 100, :$row-num 3}]
           (tu/query-ra '[:project [a {$row-num (row-number)}]
                          [:table ?a]]

                        {:table-args {'?a [{:a 12} {:a 0} {:a 100}]}}))))

(t/deftest test-project-append-columns
  (t/is (= [{:a 12, :$row-num 1}, {:a 0, :$row-num 2}, {:a 100, :$row-num 3}]
           (tu/query-ra '[:project {:append-columns? true} [{$row-num (row-number)}]
                          [:table ?a]]

                        {:table-args {'?a [{:a 12} {:a 0} {:a 100}]}}))))

(t/deftest test-array-agg
  (t/is (= [{:a 1, :bs [1 3 6]}
            {:a 2, :bs [2 4]}
            {:a 3, :bs [5]}]
           (tu/query-ra '[:group-by [a {bs (array-agg b)}]
                          [:table ?ab]]
                        {:table-args {'?ab [{:a 1, :b 1}
                                            {:a 2, :b 2}
                                            {:a 1, :b 3}
                                            {:a 2, :b 4}
                                            {:a 3, :b 5}
                                            {:a 1, :b 6}]}}))))

(t/deftest test-between
  (t/is (= [[true true] [false true]
            [true true] [false true]
            [false false] [false false]
            [false false] [false false]
            [true true] [false true]]
           (map (juxt :b :bs)
                (tu/query-ra '[:project [{b (between x l r)}
                                         {bs (between-symmetric x l r)}]
                               [:table ?xlr]]
                             {:table-args {'?xlr (map #(zipmap [:x :l :r] %)
                                                      [[5 0 10] [5 10 0]
                                                       [0 0 10] [0 10 0]
                                                       [-1 0 10] [-1 10 0]
                                                       [11 0 10] [11 10 0]
                                                       [10 0 10] [10 10 0]])}})))))

(t/deftest test-join-theta
  (t/is (= [{:x3 "31" :x4 "13"} {:x3 "31" :x4 "31"}]
           (tu/query-ra '[:join [(= x3 "31")]
                          [:table [{x3 "13"} {x3 "31"}]]
                          [:table [{x4 "13"} {x4 "31"}]]]
                        {})))

  (t/is (= [{:x3 "31"}]
           (tu/query-ra '[:join [{x3 x3} (= x3 "31")]
                          [:table [{x3 "13"} {x3 "31"}]]
                          [:table [{x3 "13"} {x3 "31"}]]]
                        {})))

  (t/is (= []
           (tu/query-ra '[:join [false]
                          [:table [{x3 "13"} {x3 "31"}]]
                          [:table [{x4 "13"} {x4 "31"}]]]
                        {})))

  (t/is (= []
           (tu/query-ra '[:join
                          [(= x1 x3)]
                          [:join [false]
                           [:table [{x1 1}]]
                           [:table [{x2 2}]]]
                          [:table [{x3 1}]]]
                        {}))))

(t/deftest test-current-times-111
  (t/is (= 1
           (->> (tu/query-ra '[:project [{ts (current-timestamp)}]
                               [:table [{} {} {}]]]
                             {})
                (into #{} (map :ts))
                count)))

  (let [times (->> (tu/query-ra '[:project [{ts (local-time 1)}]
                                  [:table [{}]]]
                                {})
                   (into #{} (map :ts)))]
    (t/is (= 1 (count times)))

    (let [nanos (.toNanoOfDay ^LocalTime (first times))
          modulus (long 1e8)]
      (t/is (= nanos (* modulus (quot nanos modulus)))
            "check rounding"))))

(t/deftest test-empty-rel-still-throws-149
  (t/is (thrown-with-msg? IllegalArgumentException
                          #"Unknown symbol: '\?x13'"
                          (tu/query-ra '[:select (= ?x13 x4)
                                         [:table []]]
                                       {}))))

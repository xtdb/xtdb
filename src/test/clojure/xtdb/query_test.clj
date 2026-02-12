(ns xtdb.query-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.db-catalog :as db]
            [xtdb.expression.metadata :as expr.meta]
            [xtdb.metadata :as meta]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.trie :as trie]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (java.time LocalTime)
           (xtdb.metadata PageMetadata)
           (xtdb.trie Trie)))

(t/use-fixtures :once tu/with-allocator)
(t/use-fixtures :each tu/with-node)

(defn with-page-metadata [node meta-file-path f]
  (let [metadata-mgr (.getMetadataManager (db/primary-db node))]
    (util/with-open [page-metadata (.openPageMetadataSync metadata-mgr meta-file-path)]
      (f page-metadata))))

(t/deftest test-find-gt-ivan
  (with-open [node (xtn/start-node (assoc tu/*node-opts* :indexer {:rows-per-block 10}))]
    (xt/execute-tx node [[:put-docs :xt_docs {:name "Håkan", :xt/id :hak}]])

    (tu/flush-block! node)
    (c/compact-all! node #xt/duration "PT1S")

    (xt/submit-tx node [[:put-docs :xt_docs {:name "Dan", :xt/id :dan, :ordinal 0}]
                        [:put-docs :xt_docs {:name "Ivan", :xt/id :iva, :ordinal 1}]])

    (xt/execute-tx node [[:put-docs :xt_docs {:name "James", :xt/id :jms, :ordinal 2}]
                         [:put-docs :xt_docs {:name "Jon", :xt/id :jon, :ordinal 3}]])

    (tu/flush-block! node)
    (c/compact-all! node #xt/duration "PT1S")

    (let [block-cat (.getBlockCatalog (db/primary-db node))]
      (letfn [(test-query-ivan [expected]
                (t/is (= expected
                         (set (tu/query-ra '[:scan {:table #xt/table xt_docs, :columns [_id name {ordinal (> ordinal 1)}]}]
                                           {:node node}))))

                (t/is (= expected
                         (set (tu/query-ra '[:scan {:table #xt/table xt_docs, :columns [_id name {ordinal (> ordinal ?ordinal)}]}]
                                           {:node node, :args {:ordinal 1}})))))]

        (t/is (= 1 (.getCurrentBlockIndex block-cat)))

        (util/with-open [args (tu/open-args {:ordinal 1})]
          (t/testing "only needs to scan block 1, page 1"
            (let [lit-sel (expr.meta/->metadata-selector tu/*allocator* '(> ordinal 1) '{ordinal #xt/field {"ordinal" :i64}} vw/empty-args)
                  param-sel (expr.meta/->metadata-selector tu/*allocator* '(> ordinal ?ordinal) '{ordinal #xt/field {"ordinal" :i64}} args)]
              (t/testing "L0 files have min-max metadata, so we have to match them"
                (with-page-metadata node (Trie/metaFilePath #xt/table xt_docs ^String (trie/->l0-trie-key 0))
                  (fn [^PageMetadata page-metadata]
                    (t/is (false? (.test (.build lit-sel page-metadata) 0)))
                    (t/is (false? (.test (.build param-sel page-metadata) 0)))))

                (with-page-metadata node (Trie/metaFilePath #xt/table xt_docs ^String (trie/->l0-trie-key 1))
                  (fn [^PageMetadata page-metadata]
                    (t/is (true? (.test (.build lit-sel page-metadata) 0)))
                    (t/is (true? (.test (.build param-sel page-metadata) 0))))))

              (t/testing "first L1 file has content metadata, doesn't match"
                (with-page-metadata node (Trie/metaFilePath #xt/table xt_docs ^String (trie/->l1-trie-key nil 0))
                  (fn [^PageMetadata page-metadata]
                    (t/is (false? (.test (.build lit-sel page-metadata) 0)))
                    (t/is (false? (.test (.build param-sel page-metadata) 0))))))

              (t/testing "combined L1 file matches"
                (with-page-metadata node (Trie/metaFilePath #xt/table xt_docs ^String (trie/->l1-trie-key nil 1))
                  (fn [^PageMetadata page-metadata]
                    (t/is (true? (.test (.build lit-sel page-metadata) 0)))
                    (t/is (true? (.test (.build param-sel page-metadata) 0)))))))))

        (test-query-ivan #{{:xt/id :jms, :name "James", :ordinal 2}
                           {:xt/id :jon, :name "Jon", :ordinal 3}})

        (xt/submit-tx node [[:put-docs :xt_docs {:name "Jeremy", :xt/id :jdt, :ordinal 4}]])

        (test-query-ivan #{{:xt/id :jms, :name "James", :ordinal 2}
                           {:xt/id :jon, :name "Jon", :ordinal 3}
                           {:xt/id :jdt, :name "Jeremy", :ordinal 4}})))))

(t/deftest test-find-eq-ivan
  (with-open [node (xtn/start-node (assoc tu/*node-opts* :indexer {:rows-per-block 10}))]
    (xt/execute-tx node [[:put-docs :xt_docs {:name "Håkan", :xt/id :hak}]
                         [:put-docs :xt_docs {:name "James", :xt/id :jms}]
                         [:put-docs :xt_docs {:name "Ivan", :xt/id :iva}]])

    (tu/flush-block! node)
    (c/compact-all! node #xt/duration "PT1S")
    (xt/execute-tx node [[:put-docs :xt_docs {:name "Håkan", :xt/id :hak}]

                         [:put-docs :xt_docs {:name "James", :xt/id :jms}]])

    (tu/flush-block! node)
    (c/compact-all! node #xt/duration "PT1S")

    (let [block-cat (.getBlockCatalog (db/primary-db node))]
      (t/is (= 1 (.getCurrentBlockIndex block-cat)))

      (t/testing "only needs to scan block 1, page 1"
        (util/with-open [args (tu/open-args {:name "Ivan"})]
          (let [lit-sel (expr.meta/->metadata-selector tu/*allocator* '(== name "Ivan") '{name #xt/field {"name" :utf8}} vw/empty-args)
                param-sel (expr.meta/->metadata-selector tu/*allocator* '(== name ?name) '{name #xt/field {"name" :utf8}} args)]
            (t/testing "L0 has no bloom filter metadata -> always match"
              (with-page-metadata node (Trie/metaFilePath #xt/table xt_docs ^String (trie/->l0-trie-key 0))
                (fn [^PageMetadata page-metadata]
                  (t/is (true? (.test (.build lit-sel page-metadata) 0)))
                  (t/is (true? (.test (.build param-sel page-metadata) 0)))))

              (with-page-metadata node (Trie/metaFilePath #xt/table xt_docs ^String (trie/->l0-trie-key 1))
                (fn [^PageMetadata page-metadata]
                  (t/is (true? (.test (.build lit-sel page-metadata) 0)))
                  (t/is (true? (.test (.build param-sel page-metadata) 0))))))

            (t/testing "first L1 file matches"
              (with-page-metadata node (Trie/metaFilePath #xt/table xt_docs ^String (trie/->l1-trie-key nil 0))
                (fn [^PageMetadata page-metadata]
                  (t/is (true? (.test (.build lit-sel page-metadata) 0)))
                  (t/is (true? (.test (.build param-sel page-metadata) 0))))))

            (t/testing "combined L1 file also matches"
              (with-page-metadata node (Trie/metaFilePath #xt/table xt_docs ^String (trie/->l1-trie-key nil 1))
                (fn [^PageMetadata page-metadata]
                  (t/is (true? (.test (.build lit-sel page-metadata) 0)))
                  (t/is (true? (.test (.build param-sel page-metadata) 0)))))))))

      (t/is (= #{{:name "Ivan"}}
               (set (tu/query-ra '[:scan {:table #xt/table xt_docs, :columns [{name (== name "Ivan")}]}]
                                 {:node node}))))

      (t/is (= #{{:name "Ivan"}}
               (set (tu/query-ra '[:scan {:table #xt/table xt_docs, :columns [{name (== name ?name)}]}]
                                 {:node node, :args {:name "Ivan"}})))))))

(t/deftest test-temporal-bounds
  (let [tx1 (xt/execute-tx tu/*node* [[:put-docs :xt_docs {:xt/id :my-doc, :last-updated "tx1"}]])
        tt1 (.getSystemTime tx1)
        tx2 (xt/execute-tx tu/*node* [[:put-docs :xt_docs {:xt/id :my-doc, :last-updated "tx2"}]])
        tt2 (.getSystemTime tx2)]
    (letfn [(q [& temporal-constraints]
              (->> (tu/query-ra [:scan {:table #xt/table xt_docs, :for-system-time :all-time, :for-valid-time :all-time,
                                        :columns (into '[last_updated] temporal-constraints)}]
                                {:node tu/*node*, :args {:system-time1 tt1, :system-time2 tt2}})
                   (into #{} (map :last-updated))))]
      (t/is (= #{"tx1" "tx2"}
               (q)))

      (t/is (= #{"tx1"}
               (q '{_system_from (<= _system_from ?system_time1)})))

      (t/is (= #{}
               (q '{_system_from (< _system_from ?system_time1)})))

      (t/is (= #{"tx1" "tx2"}
               (q '{_system_from (<= _system_from ?system_time2)})))

      ;; this test depends on how one cuts rectangles
      (t/is (= #{"tx2"} #_#{"tx1" "tx2"}
               (q '{_system_from (> _system_from ?system_time1)})))

      (t/is (= #{}
               (q '{_system_to (< _system_to ?system_time2)})))

      (t/is (= #{"tx1"}
               (q '{_system_to (<= _system_to ?system_time2)})))

      (t/is (= #{"tx1" "tx2"}
               (q '{_system_to (> (coalesce _system_to xtdb/end-of-time) ?system_time2)})))

      (t/is (= #{"tx1" "tx2"}
               (q '{_system_to (>= (coalesce _system_to xtdb/end-of-time) ?system_time2)})))

      (t/testing "multiple constraints"
        (t/is (= #{"tx1"}
                 (q '{_system_from (and (<= _system_from ?system_time1)
                                        (<= _system_from ?system_time2))})))

        (t/is (= #{"tx1"}
                 (q '{_system_from (and (<= _system_from ?system_time2)
                                        (<= _system_from ?system_time1))})))

        (t/is (= #{"tx1" "tx2"}
                 (q '{_system_to (and (> (coalesce _system_to xtdb/end-of-time) ?system_time2)
                                      (> (coalesce _system_to xtdb/end-of-time) ?system_time1))})))

        (t/is (= #{"tx1" "tx2"}
                 (q '{_system_to (and (> (coalesce _system_to xtdb/end-of-time) ?system_time1)
                                      (> (coalesce _system_to xtdb/end-of-time) ?system_time2))}))))

      (t/is (= #{}
               (q '{_system_from (<= _system_from ?system_time1)}
                  '{_system_to (< _system_to ?system_time2)})))

      (t/is (= #{"tx1"}
               (q '{_system_from (<= _system_from ?system_time1)}
                  '{_system_to (<= _system_to ?system_time2)})))

      (t/is (= #{"tx1"}
               (q '{_system_from (<= _system_from ?system_time1)}
                  '{_system_to (> _system_to ?system_time1)}))
            "as of tt1")

      (t/is (= #{"tx1" "tx2"}
               (q '{_system_from (<= _system_from ?system_time2)}
                  '{_system_to (> (coalesce _system_to xtdb/end-of-time) ?system_time2)}))
            "as of tt2"))))

(t/deftest test-project-row-number
  (t/is (= [{:a 12, :row-num 1}, {:a 0, :row-num 2}, {:a 100, :row-num 3}]
           (tu/query-ra '[:project {:projections [a {row-num (row-number)}]}
                          [:table {:param ?a}]]

                        {:args {:a [{:a 12} {:a 0} {:a 100}]}}))))

(t/deftest test-project-append-columns
  (t/is (= [{:a 12, :row-num 1}, {:a 0, :row-num 2}, {:a 100, :row-num 3}]
           (tu/query-ra '[:project {:append-columns? true, :projections [{row-num (row-number)}]}
                          [:table {:param ?a}]]

                        {:args {:a [{:a 12} {:a 0} {:a 100}]}}))))

(t/deftest test-array-agg
  (t/is (= [{:a 1, :bs [1 3 6]}
            {:a 2, :bs [2 4]}
            {:a 3, :bs [5]}]
           (tu/query-ra '[:group-by {:columns [a {bs (array-agg b)}]}
                          [:table {:param ?ab}]]
                        {:args {:ab [{:a 1, :b 1}
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
                (tu/query-ra '[:project {:projections [{b (between x l r)}
                                                       {bs (between-symmetric x l r)}]}
                               [:table {:param ?xlr}]]
                             {:args {:xlr (map #(zipmap [:x :l :r] %)
                                               [[5 0 10] [5 10 0]
                                                [0 0 10] [0 10 0]
                                                [-1 0 10] [-1 10 0]
                                                [11 0 10] [11 10 0]
                                                [10 0 10] [10 10 0]])}})))))

(t/deftest test-join-theta
  (t/is (= [{:x3 "31" :x4 "13"} {:x3 "31" :x4 "31"}]
           (tu/query-ra '[:join {:conditions [(== x3 "31")]}
                          [:table {:rows [{x3 "13"} {x3 "31"}]}]
                          [:table {:rows [{x4 "13"} {x4 "31"}]}]]
                        {})))

  (t/is (= [{:x3 "31"}]
           (tu/query-ra '[:join {:conditions [{x3 x3} (== x3 "31")]}
                          [:table {:rows [{x3 "13"} {x3 "31"}]}]
                          [:table {:rows [{x3 "13"} {x3 "31"}]}]]
                        {})))

  (t/is (= []
           (tu/query-ra '[:join {:conditions [false]}
                          [:table {:rows [{x3 "13"} {x3 "31"}]}]
                          [:table {:rows [{x4 "13"} {x4 "31"}]}]]
                        {})))

  (t/is (= []
           (tu/query-ra '[:join
                          {:conditions [(== x1 x3)]}
                          [:join {:conditions [false]}
                           [:table {:rows [{x1 1}]}]
                           [:table {:rows [{x2 2}]}]]
                          [:table {:rows [{x3 1}]}]]
                        {}))))

(t/deftest test-current-times-111
  (t/is (= 1
           (->> (tu/query-ra '[:project {:projections [{ts (current-timestamp)}]}
                               [:table {:rows [{} {} {}]}]]
                             {})
                (into #{} (map :ts))
                count)))

  (let [times (->> (tu/query-ra '[:project {:projections [{ts (local-time 1)}]}
                                  [:table {:rows [{}]}]]
                                {})
                   (into #{} (map :ts)))]
    (t/is (= 1 (count times)))

    (let [nanos (.toNanoOfDay ^LocalTime (first times))
          modulus (long 1e8)]
      (t/is (= nanos (* modulus (quot nanos modulus)))
            "check rounding"))))

(t/deftest test-empty-rel-still-throws-149
  (t/is (anomalous? [:incorrect nil #"Unknown symbol: '\?x13'"]
                    (tu/query-ra '[:select {:predicate (== ?x13 x4)}
                                   [:table {:rows []}]]
                                 {}))))

(t/deftest test-left-outer-join-with-composite-types-2393
  (t/is (= {:res [{{:a 12, :b 12, :c {:foo 1}} 1, {:a 12, :b 12, :c {:foo 2}} 1, {:a 0} 1}
                  {{:a 12, :b 12, :c {:foo 1}} 1, {:a 12, :b 12, :c {:foo 2}} 1, {:a 100, :b 100, :c {:foo 2}} 1}]
            :types '{a #xt/type :i64, c #xt/type [:? :struct {"foo" :i64}], b #xt/type [:? :i64]}}
           (-> (tu/query-ra [:left-outer-join '{:conditions [{a b}]}
                             [::tu/pages
                              [[{:a 12}, {:a 0}]
                               [{:a 12}, {:a 100}]]]
                             [::tu/pages
                              [[{:b 12, :c {:foo 1}}, {:b 2, :c {:foo 1}}]
                               [{:b 12, :c {:foo 2}}, {:b 100, :c {:foo 2}}]]]]
                            {:preserve-pages? true, :with-types? true})
               (update :res (partial mapv frequencies))))
        "testing left-outer-join with structs")

  (t/is (= {:res [{{:a 12, :c [1], :b 12} 1, {:a 12, :c [2], :b 12} 1, {:a 0} 1}
                  {{:a 12, :c [1], :b 12} 1, {:a 12, :c [2], :b 12} 1, {:a 100, :c [4], :b 100} 1}]
            :types '{a #xt/type :i64, b #xt/type [:? :i64], c #xt/type [:? :list :i64]}}
           (-> (tu/query-ra [:left-outer-join '{:conditions [{a b}]}
                             [::tu/pages
                              [[{:a 12}, {:a 0}]
                               [{:a 12}, {:a 100}]]]
                             [::tu/pages
                              [[{:b 12, :c (list 1)}, {:b 2, :c (list 3)}]
                               [{:b 12, :c (list 2)}, {:b 100, :c (list 4)}]]]]
                            {:preserve-pages? true, :with-types? true})
               (update :res (partial mapv frequencies))))
        "testing left-outer-join with lists"))

(t/deftest test-string-inequality-metadata-4516
  (xt/execute-tx tu/*node* ["INSERT INTO test RECORDS {_id: '1'}, {_id: '2'}, {_id: '3'}, {_id: '4'}"])

  (t/is (= #{{:xt/id "1"} {:xt/id "2"}}
           (set (xt/q tu/*node* "SELECT * FROM test WHERE _id < '3'"))))

  (tu/flush-block! tu/*node*)

  (t/is (= #{{:xt/id "1"} {:xt/id "2"}}
           (set (xt/q tu/*node* "SELECT * FROM test WHERE _id < '3'")))))

(t/deftest test-multi-db-scans
  (with-open [node (xtn/start-node)
              xtdb-conn (.build (.createConnectionBuilder node))]

    (jdbc/execute! xtdb-conn ["ATTACH DATABASE new_db"])

    (with-open [new-db-conn (.build (-> (.createConnectionBuilder node) (.database "new_db")))]

      (xt/execute-tx xtdb-conn [[:put-docs :foo {:xt/id "xtdb-db"}]])
      (xt/execute-tx new-db-conn [[:put-docs :foo {:xt/id :new-db}]])

      (t/is (= {:res [{:xt/id "xtdb-db"}],
                :types '{_id #xt/type :utf8}}
               (tu/query-ra '[:scan {:table #xt/table foo, :columns [_id]}]
                            {:node node, :with-types? true})))

      (t/is (= {:res [{:xt/id :new-db}],
                :types '{_id #xt/type :keyword}}
               (tu/query-ra '[:scan {:table #xt/table [new_db foo], :columns [_id]}]
                            {:node node, :default-db "new_db", :with-types? true})))

      (let [{:keys [res types]} (tu/query-ra '[:union-all {}
                                               [:scan {:table #xt/table [xtdb foo], :columns [_id]}]
                                               [:scan {:table #xt/table [new_db foo], :columns [_id]}]]
                                             {:node node, :with-types? true})]
        (t/is (= #{{:xt/id "xtdb-db"} {:xt/id :new-db}} (set res)))

        ;; these two are equivalent - for some reason CI gives different ordering
        (t/is (or (= '{_id #xt/type #{:utf8 :keyword}}
                     types)
                  (= '{_id #xt/type #{:keyword :utf8}}
                     types)))))))

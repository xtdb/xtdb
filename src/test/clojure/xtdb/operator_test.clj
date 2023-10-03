(ns xtdb.operator-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.buffer-pool :as bp]
            [xtdb.expression.metadata :as expr.meta]
            [xtdb.metadata :as meta]
            [xtdb.node :as node]
            [xtdb.test-util :as tu]
            [xtdb.trie :as trie]
            [xtdb.util :as util])
  (:import (java.time LocalTime)
           (xtdb.buffer_pool IBufferPool)
           (xtdb.metadata IMetadataManager ITableMetadata)))

(t/use-fixtures :once tu/with-allocator)

(defn with-table-metadata [node meta-file-name f]
  (let [^IBufferPool buffer-pool (tu/component node ::bp/buffer-pool)
        ^IMetadataManager metadata-mgr (tu/component node ::meta/metadata-manager)]
    (util/with-open [{meta-rdr :rdr} (trie/open-meta-file buffer-pool meta-file-name)]
      (f (.tableMetadata metadata-mgr meta-rdr meta-file-name)))))

(t/deftest test-find-gt-ivan
  (with-open [node (node/start-node {:xtdb/indexer {:rows-per-chunk 10}})]
    (-> (xt/submit-tx node [[:put :xt_docs {:name "Håkan", :xt/id :hak}]])
        (tu/then-await-tx node))

    (tu/finish-chunk! node)

    (xt/submit-tx node [[:put :xt_docs {:name "Dan", :xt/id :dan}]
                        [:put :xt_docs {:name "Ivan", :xt/id :iva}]])

    (let [tx1 (-> (xt/submit-tx node [[:put :xt_docs {:name "James", :xt/id :jms}]
                                      [:put :xt_docs {:name "Jon", :xt/id :jon}]])
                  (tu/then-await-tx node))]

      (tu/finish-chunk! node)

      (let [^IMetadataManager metadata-mgr (tu/component node ::meta/metadata-manager)]
        (letfn [(test-query-ivan [expected tx]
                  (t/is (= expected
                           (set (tu/query-ra '[:scan {:table xt_docs} [xt/id {name (> name "Ivan")}]]
                                             {:node node, :basis {:tx tx}}))))

                  (t/is (= expected
                           (set (tu/query-ra '[:scan {:table xt_docs} [xt/id {name (> name ?name)}]]
                                             {:node node, :basis {:tx tx}, :params {'?name "Ivan"}})))))]

          (t/is (= #{0 2} (set (keys (.chunksMetadata metadata-mgr)))))

          (util/with-open [params (tu/open-params {'?name "Ivan"})]
            (t/testing "only needs to scan chunk 1, page 1"
              (let [lit-sel (expr.meta/->metadata-selector '(> name "Ivan") '{name :utf8} {})
                    param-sel (expr.meta/->metadata-selector '(> name ?name) '{name :utf8} params)]
                (with-table-metadata node (trie/->table-meta-file-name "xt_docs" (trie/->log-trie-key 0 0 2))
                  (fn [^ITableMetadata table-metadata]
                    (t/is (false? (.test (.build lit-sel table-metadata) 0)))
                    (t/is (false? (.test (.build param-sel table-metadata) 0)))))

                (with-table-metadata node (trie/->table-meta-file-name "xt_docs" (trie/->log-trie-key 0 2 8))
                  (fn [^ITableMetadata table-metadata]
                    (t/is (true? (.test (.build lit-sel table-metadata) 0)))
                    (t/is (true? (.test (.build param-sel table-metadata) 0))))))))

          (let [tx2 (xt/submit-tx node [[:put :xt_docs {:name "Jeremy", :xt/id :jdt}]])]

            (test-query-ivan #{{:xt/id :jms, :name "James"}
                               {:xt/id :jon, :name "Jon"}}
                             tx1)

            (test-query-ivan #{{:xt/id :jms, :name "James"}
                               {:xt/id :jon, :name "Jon"}
                               {:xt/id :jdt, :name "Jeremy"}}
                             tx2)))))))

(t/deftest test-find-eq-ivan
  (with-open [node (node/start-node {:xtdb/indexer {:rows-per-chunk 10}})]
    (-> (xt/submit-tx node [[:put :xt_docs {:name "Håkan", :xt/id :hak}]
                            [:put :xt_docs {:name "James", :xt/id :jms}]
                            [:put :xt_docs {:name "Ivan", :xt/id :iva}]])
        (tu/then-await-tx node))

    (tu/finish-chunk! node)
    (-> (xt/submit-tx node [[:put :xt_docs {:name "Håkan", :xt/id :hak}]

                            [:put :xt_docs {:name "James", :xt/id :jms}]])
        (tu/then-await-tx node))

    (tu/finish-chunk! node)
    (let [^IMetadataManager metadata-mgr (tu/component node ::meta/metadata-manager)]
      (t/is (= #{0 4} (set (keys (.chunksMetadata metadata-mgr)))))

      (t/testing "only needs to scan chunk 1, page 1"
        (util/with-open [params (tu/open-params {'?name "Ivan"})]
          (let [lit-sel (expr.meta/->metadata-selector '(= name "Ivan") '{name :utf8} {})
                param-sel (expr.meta/->metadata-selector '(= name ?name) '{name :utf8} params)]
            (with-table-metadata node (trie/->table-meta-file-name "xt_docs" (trie/->log-trie-key 0 0 4))
              (fn [^ITableMetadata table-metadata]
                (t/is (true? (.test (.build lit-sel table-metadata) 0)))
                (t/is (true? (.test (.build param-sel table-metadata) 0)))))

            (with-table-metadata node (trie/->table-meta-file-name "xt_docs" (trie/->log-trie-key 0 4 7))
              (fn [^ITableMetadata table-metadata]
                (t/is (false? (.test (.build lit-sel table-metadata) 0)))
                (t/is (false? (.test (.build param-sel table-metadata) 0))))))))

      (t/is (= #{{:name "Ivan"}}
               (set (tu/query-ra '[:scan {:table xt_docs} [{name (= name "Ivan")}]]
                                 {:node node}))))

      (t/is (= #{{:name "Ivan"}}
               (set (tu/query-ra '[:scan {:table xt_docs} [{name (= name ?name)}]]
                                 {:node node, :params {'?name "Ivan"}})))))))

(t/deftest test-temporal-bounds
  (with-open [node (node/start-node {})]
    (let [{tt1 :system-time} (xt/submit-tx node [[:put :xt_docs {:xt/id :my-doc, :last-updated "tx1"}]])
          {tt2 :system-time} (xt/submit-tx node [[:put :xt_docs {:xt/id :my-doc, :last-updated "tx2"}]])]
      (letfn [(q [& temporal-constraints]
                (->> (tu/query-ra [:scan '{:table xt_docs, :for-system-time :all-time}
                                   (into '[last-updated] temporal-constraints)]
                                  {:node node, :params {'?system-time1 tt1, '?system-time2 tt2}
                                   :default-all-valid-time? true})
                     (into #{} (map :last-updated))))]
        (t/is (= #{"tx1" "tx2"}
                 (q)))

        (t/is (= #{"tx1"}
                 (q '{xt$system_from (<= xt$system_from ?system-time1)})))

        (t/is (= #{}
                 (q '{xt$system_from (< xt$system_from ?system-time1)})))

        (t/is (= #{"tx1" "tx2"}
                 (q '{xt$system_from (<= xt$system_from ?system-time2)})))

        ;; this test depends on how one cuts rectangles
        (t/is (= #{"tx2"} #_#{"tx1" "tx2"}
                 (q '{xt$system_from (> xt$system_from ?system-time1)})))

        (t/is (= #{}
                 (q '{xt$system_to (< xt$system_to ?system-time2)})))

        (t/is (= #{"tx1"}
                 (q '{xt$system_to (<= xt$system_to ?system-time2)})))

        (t/is (= #{"tx1" "tx2"}
                 (q '{xt$system_to (> xt$system_to ?system-time2)})))

        (t/is (= #{"tx1" "tx2"}
                 (q '{xt$system_to (>= xt$system_to ?system-time2)})))

        (t/testing "multiple constraints"
          (t/is (= #{"tx1"}
                   (q '{xt$system_from (and (<= xt$system_from ?system-time1)
                                            (<= xt$system_from ?system-time2))})))

          (t/is (= #{"tx1"}
                   (q '{xt$system_from (and (<= xt$system_from ?system-time2)
                                            (<= xt$system_from ?system-time1))})))

          (t/is (= #{"tx1" "tx2"}
                   (q '{xt$system_to (and (> xt$system_to ?system-time2)
                                          (> xt$system_to ?system-time1))})))

          (t/is (= #{"tx1" "tx2"}
                   (q '{xt$system_to (and (> xt$system_to ?system-time1)
                                          (> xt$system_to ?system-time2))}))))

        (t/is (= #{}
                 (q '{xt$system_from (<= xt$system_from ?system-time1)}
                    '{xt$system_to (< xt$system_to ?system-time2)})))

        (t/is (= #{"tx1"}
                 (q '{xt$system_from (<= xt$system_from ?system-time1)}
                    '{xt$system_to (<= xt$system_to ?system-time2)})))

        (t/is (= #{"tx1"}
                 (q '{xt$system_from (<= xt$system_from ?system-time1)}
                    '{xt$system_to (> xt$system_to ?system-time1)}))
              "as of tt1")

        (t/is (= #{"tx1" "tx2"}
                 (q '{xt$system_from (<= xt$system_from ?system-time2)}
                    '{xt$system_to (> xt$system_to ?system-time2)}))
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

(t/deftest test-left-outer-join-with-composite-types-2393
  (t/is (= {:res [{{:a 12, :b 12, :c {:foo 1}} 1, {:a 12, :b 12, :c {:foo 2}} 1, {:a 0, :b nil, :c nil} 1}
                  {{:a 12, :b 12, :c {:foo 1}} 1, {:a 12, :b 12, :c {:foo 2}} 1, {:a 100, :b 100, :c {:foo 2}} 1}]
            :col-types '{a :i64, c [:union #{[:struct {foo :i64}] :null}], b [:union #{:null :i64}]}}
           (-> (tu/query-ra [:left-outer-join '[{a b}]
                             [::tu/blocks
                              [[{:a 12}, {:a 0}]
                               [{:a 12}, {:a 100}]]]
                             [::tu/blocks
                              [[{:b 12, :c {:foo 1}}, {:b 2, :c {:foo 1}}]
                               [{:b 12, :c {:foo 2}}, {:b 100, :c {:foo 2}}]]]]
                            {:preserve-blocks? true, :with-col-types? true})
               (update :res (partial mapv frequencies))))
        "testing left-outer-join with structs")
  (t/is (= {:res [{{:a 12, :c [1], :b 12} 1, {:a 12, :c [2], :b 12} 1, {:a 0, :c nil, :b nil} 1}
                  {{:a 12, :c [1], :b 12} 1, {:a 12, :c [2], :b 12} 1, {:a 100, :c [4], :b 100} 1}]
            :col-types '{a :i64, c [:union #{[:list :i64] :null}], b [:union #{:null :i64}]}}
           (-> (tu/query-ra [:left-outer-join '[{a b}]
                             [::tu/blocks
                              [[{:a 12}, {:a 0}]
                               [{:a 12}, {:a 100}]]]
                             [::tu/blocks
                              [[{:b 12, :c (list 1)}, {:b 2, :c (list 3)}]
                               [{:b 12, :c (list 2)}, {:b 100, :c (list 4)}]]]]
                            {:preserve-blocks? true, :with-col-types? true})
               (update :res (partial mapv frequencies))))
        "testing left-outer-join with lists"))

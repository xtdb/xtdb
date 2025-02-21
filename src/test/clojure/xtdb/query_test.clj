(ns xtdb.query-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.expression.metadata :as expr.meta]
            [xtdb.metadata :as meta]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.trie :as trie]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw]
            [xtdb.compactor :as c])
  (:import (java.time LocalTime)
           (xtdb.metadata IMetadataManager ITableMetadata)))

(t/use-fixtures :once tu/with-allocator)
(t/use-fixtures :each tu/with-node)

(defn with-table-metadata [node meta-file-path f]
  (let [^IMetadataManager metadata-mgr (tu/component node ::meta/metadata-manager)]
    (util/with-open [table-metadata (.openTableMetadata metadata-mgr meta-file-path)]
      (f table-metadata))))

(t/deftest test-find-gt-ivan
  (with-open [node (xtn/start-node (merge tu/*node-opts* {:indexer {:rows-per-block 10}}))]
    (-> (xt/submit-tx node [[:put-docs :xt_docs {:name "Håkan", :xt/id :hak}]])
        (tu/then-await-tx node))

    (tu/finish-block! node)
    (c/compact-all! node)

    (xt/submit-tx node [[:put-docs :xt_docs {:name "Dan", :xt/id :dan}]
                        [:put-docs :xt_docs {:name "Ivan", :xt/id :iva}]])

    (-> (xt/submit-tx node [[:put-docs :xt_docs {:name "James", :xt/id :jms}]
                            [:put-docs :xt_docs {:name "Jon", :xt/id :jon}]])
        (tu/then-await-tx node))

    (tu/finish-block! node)
    (c/compact-all! node)

    (let [^IMetadataManager metadata-mgr (tu/component node ::meta/metadata-manager)]
      (letfn [(test-query-ivan [expected]
                (t/is (= expected
                         (set (tu/query-ra '[:scan {:table public/xt_docs} [_id {name (> name "Ivan")}]]
                                           {:node node}))))

                (t/is (= expected
                         (set (tu/query-ra '[:scan {:table public/xt_docs} [_id {name (> name ?name)}]]
                                           {:node node, :args {:name "Ivan"}})))))]

        (t/is (= 1 (:block-idx (.latestBlockMetadata metadata-mgr))))

        (util/with-open [args (tu/open-args {:name "Ivan"})]
          (t/testing "only needs to scan block 1, page 1"
            (let [lit-sel (expr.meta/->metadata-selector '(> name "Ivan") '{name :utf8} vw/empty-args)
                  param-sel (expr.meta/->metadata-selector '(> name ?name) '{name :utf8} args)]
              (t/testing "L0 files don't have content metadata, so we have to match them"
                (with-table-metadata node (trie/->table-meta-file-path "public$xt_docs" (trie/->l0-l1-trie-key 0 0))
                  (fn [^ITableMetadata table-metadata]
                    (t/is (true? (.test (.build lit-sel table-metadata) 0)))
                    (t/is (true? (.test (.build param-sel table-metadata) 0)))))

                (with-table-metadata node (trie/->table-meta-file-path "public$xt_docs" (trie/->l0-l1-trie-key 0 1))
                  (fn [^ITableMetadata table-metadata]
                    (t/is (true? (.test (.build lit-sel table-metadata) 0)))
                    (t/is (true? (.test (.build param-sel table-metadata) 0))))))

              (t/testing "first L1 file has content metadata, doesn't match"
                (with-table-metadata node (trie/->table-meta-file-path "public$xt_docs" (trie/->l0-l1-trie-key 1 0))
                  (fn [^ITableMetadata table-metadata]
                    (t/is (false? (.test (.build lit-sel table-metadata) 0)))
                    (t/is (false? (.test (.build param-sel table-metadata) 0))))))

              (t/testing "combined L1 file matches"
                (with-table-metadata node (trie/->table-meta-file-path "public$xt_docs" (trie/->l0-l1-trie-key 1 1))
                  (fn [^ITableMetadata table-metadata]
                    (t/is (true? (.test (.build lit-sel table-metadata) 0)))
                    (t/is (true? (.test (.build param-sel table-metadata) 0)))))))))

        (test-query-ivan #{{:xt/id :jms, :name "James"}
                           {:xt/id :jon, :name "Jon"}})

        (xt/submit-tx node [[:put-docs :xt_docs {:name "Jeremy", :xt/id :jdt}]])

        (test-query-ivan #{{:xt/id :jms, :name "James"}
                           {:xt/id :jon, :name "Jon"}
                           {:xt/id :jdt, :name "Jeremy"}})))))

(t/deftest test-find-eq-ivan
  (with-open [node (xtn/start-node (merge tu/*node-opts* {:indexer {:rows-per-block 10}}))]
    (-> (xt/submit-tx node [[:put-docs :xt_docs {:name "Håkan", :xt/id :hak}]
                            [:put-docs :xt_docs {:name "James", :xt/id :jms}]
                            [:put-docs :xt_docs {:name "Ivan", :xt/id :iva}]])
        (tu/then-await-tx node))

    (tu/finish-block! node)
    (c/compact-all! node)
    (-> (xt/submit-tx node [[:put-docs :xt_docs {:name "Håkan", :xt/id :hak}]

                            [:put-docs :xt_docs {:name "James", :xt/id :jms}]])
        (tu/then-await-tx node))

    (tu/finish-block! node)
    (c/compact-all! node)
    (let [^IMetadataManager metadata-mgr (tu/component node ::meta/metadata-manager)]
      (t/is (= 1 (:block-idx (.latestBlockMetadata metadata-mgr))))

      (t/testing "only needs to scan block 1, page 1"
        (util/with-open [args (tu/open-args {:name "Ivan"})]
          (let [lit-sel (expr.meta/->metadata-selector '(= name "Ivan") '{name :utf8} vw/empty-args)
                param-sel (expr.meta/->metadata-selector '(= name ?name) '{name :utf8} args)]
            (t/testing "L0 has no content metadata -> always match"
              (with-table-metadata node (trie/->table-meta-file-path "public$xt_docs" (trie/->l0-l1-trie-key 0 0))
                (fn [^ITableMetadata table-metadata]
                  (t/is (true? (.test (.build lit-sel table-metadata) 0)))
                  (t/is (true? (.test (.build param-sel table-metadata) 0)))))

              (with-table-metadata node (trie/->table-meta-file-path "public$xt_docs" (trie/->l0-l1-trie-key 0 1))
                (fn [^ITableMetadata table-metadata]
                  (t/is (true? (.test (.build lit-sel table-metadata) 0)))
                  (t/is (true? (.test (.build param-sel table-metadata) 0))))))

            (t/testing "first L1 file matches"
              (with-table-metadata node (trie/->table-meta-file-path "public$xt_docs" (trie/->l0-l1-trie-key 1 0))
                (fn [^ITableMetadata table-metadata]
                  (t/is (true? (.test (.build lit-sel table-metadata) 0)))
                  (t/is (true? (.test (.build param-sel table-metadata) 0))))))

            (t/testing "combined L1 file also matches"
              (with-table-metadata node (trie/->table-meta-file-path "public$xt_docs" (trie/->l0-l1-trie-key 1 1))
                (fn [^ITableMetadata table-metadata]
                  (t/is (true? (.test (.build lit-sel table-metadata) 0)))
                  (t/is (true? (.test (.build param-sel table-metadata) 0)))))))))

      (t/is (= #{{:name "Ivan"}}
               (set (tu/query-ra '[:scan {:table public/xt_docs} [{name (= name "Ivan")}]]
                                 {:node node}))))

      (t/is (= #{{:name "Ivan"}}
               (set (tu/query-ra '[:scan {:table public/xt_docs} [{name (= name ?name)}]]
                                 {:node node, :args {:name "Ivan"}})))))))

(t/deftest test-temporal-bounds
  (let [tx1 (xt/execute-tx tu/*node* [[:put-docs :xt_docs {:xt/id :my-doc, :last-updated "tx1"}]])
        tt1 (.getSystemTime tx1)
        tx2 (xt/execute-tx tu/*node* [[:put-docs :xt_docs {:xt/id :my-doc, :last-updated "tx2"}]])
        tt2 (.getSystemTime tx2)]
    (letfn [(q [& temporal-constraints]
              (->> (tu/query-ra [:scan '{:table public/xt_docs, :for-system-time :all-time, :for-valid-time :all-time}
                                 (into '[last_updated] temporal-constraints)]
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
                          {:args {:table [{:a 0 :b 1}]}}))))

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
                          {:args {:table [{:x "a" :y "b"}
                                          {:x "b" :y "c"}
                                          {:x "c" :y "d"}
                                          {:x "d" :y "a"}]}})))))

(t/deftest test-assignment-operator
  (t/is (= [{:a 1 :b 1}]
           (tu/query-ra '[:assign [X [:table ?x]
                                   Y [:table ?y]]
                          [:join [{a b}] X Y]]
                        {:args {:x [{:a 1}]
                                :y [{:b 1}]}})))

  (t/testing "can see earlier assignments"
    (t/is (= [{:a 1 :b 1}]
             (tu/query-ra '[:assign [X [:table ?x]
                                     Y [:join [{a b}] X [:table ?y]]
                                     X Y]
                            X]
                          {:args {:x [{:a 1}]
                                  :y [{:b 1}]}})))))

(t/deftest test-project-row-number
  (t/is (= [{:a 12, :row-num 1}, {:a 0, :row-num 2}, {:a 100, :row-num 3}]
           (tu/query-ra '[:project [a {row-num (row-number)}]
                          [:table ?a]]

                        {:args {:a [{:a 12} {:a 0} {:a 100}]}}))))

(t/deftest test-project-append-columns
  (t/is (= [{:a 12, :row-num 1}, {:a 0, :row-num 2}, {:a 100, :row-num 3}]
           (tu/query-ra '[:project {:append-columns? true} [{row-num (row-number)}]
                          [:table ?a]]

                        {:args {:a [{:a 12} {:a 0} {:a 100}]}}))))

(t/deftest test-array-agg
  (t/is (= [{:a 1, :bs [1 3 6]}
            {:a 2, :bs [2 4]}
            {:a 3, :bs [5]}]
           (tu/query-ra '[:group-by [a {bs (array-agg b)}]
                          [:table ?ab]]
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
                (tu/query-ra '[:project [{b (between x l r)}
                                         {bs (between-symmetric x l r)}]
                               [:table ?xlr]]
                             {:args {:xlr (map #(zipmap [:x :l :r] %)
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
  (t/is (= {:res [{{:a 12, :b 12, :c {:foo 1}} 1, {:a 12, :b 12, :c {:foo 2}} 1, {:a 0} 1}
                  {{:a 12, :b 12, :c {:foo 1}} 1, {:a 12, :b 12, :c {:foo 2}} 1, {:a 100, :b 100, :c {:foo 2}} 1}]
            :col-types '{a :i64, c [:union #{[:struct {foo :i64}] :null}], b [:union #{:null :i64}]}}
           (-> (tu/query-ra [:left-outer-join '[{a b}]
                             [::tu/pages
                              [[{:a 12}, {:a 0}]
                               [{:a 12}, {:a 100}]]]
                             [::tu/pages
                              [[{:b 12, :c {:foo 1}}, {:b 2, :c {:foo 1}}]
                               [{:b 12, :c {:foo 2}}, {:b 100, :c {:foo 2}}]]]]
                            {:preserve-pages? true, :with-col-types? true})
               (update :res (partial mapv frequencies))))
        "testing left-outer-join with structs")

  (t/is (= {:res [{{:a 12, :c [1], :b 12} 1, {:a 12, :c [2], :b 12} 1, {:a 0} 1}
                  {{:a 12, :c [1], :b 12} 1, {:a 12, :c [2], :b 12} 1, {:a 100, :c [4], :b 100} 1}]
            :col-types '{a :i64, c [:union #{[:list :i64] :null}], b [:union #{:null :i64}]}}
           (-> (tu/query-ra [:left-outer-join '[{a b}]
                             [::tu/pages
                              [[{:a 12}, {:a 0}]
                               [{:a 12}, {:a 100}]]]
                             [::tu/pages
                              [[{:b 12, :c (list 1)}, {:b 2, :c (list 3)}]
                               [{:b 12, :c (list 2)}, {:b 100, :c (list 4)}]]]]
                            {:preserve-pages? true, :with-col-types? true})
               (update :res (partial mapv frequencies))))
        "testing left-outer-join with lists"))

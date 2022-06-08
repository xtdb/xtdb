(ns core2.operator-test
  (:require [clojure.test :as t]
            [core2.api :as c2]
            [core2.expression.metadata :as expr.meta]
            [core2.indexer :as idx]
            [core2.local-node :as node]
            [core2.metadata :as meta]
            [core2.operator :as op]
            [core2.snapshot :as snap]
            [core2.test-util :as tu]
            [core2.types :as types])
  (:import (core2.indexer IChunkManager)
           (core2.metadata IMetadataManager)
           (core2.snapshot ISnapshotFactory)
           (java.time LocalTime)
           (org.roaringbitmap RoaringBitmap)
           org.apache.arrow.vector.types.pojo.Schema))

(t/deftest test-find-gt-ivan
  (with-open [node (node/start-node {::idx/indexer {:max-rows-per-chunk 10, :max-rows-per-block 2}})]
    (-> (c2/submit-tx node [[:put {:name "Håkan", :_id :hak}]])
        (tu/then-await-tx node))

    (tu/finish-chunk node)

    (c2/submit-tx node [[:put {:name "Dan", :_id :dan}]
                        [:put {:name "Ivan", :_id :iva}]])

    (-> (c2/submit-tx node [[:put {:name "James", :_id :jms}]
                            [:put {:name "Jon", :_id :jon}]])
        (tu/then-await-tx node))

    (tu/finish-chunk node)

    (let [^IMetadataManager metadata-mgr (tu/component node ::meta/metadata-manager)
          ^IChunkManager indexer (tu/component node ::idx/indexer)
          ^ISnapshotFactory snapshot-factory (tu/component node ::snap/snapshot-factory)]
      (letfn [(test-query-ivan [expected db]
                (t/is (= expected
                         (set (op/query-ra '[:scan [_id {name (> name "Ivan")}]] db))))

                (t/is (= expected
                         (set (op/query-ra '[:scan [_id {name (> name ?name)}]]
                                           {'$ db, '?name "Ivan"})))))]

        (let [db (snap/snapshot snapshot-factory)]
          (t/is (= #{0 1} (.knownChunks metadata-mgr)))
          (with-open [watermark (.getWatermark indexer)]
            (let [expected-match [(meta/map->ChunkMatch
                                   {:chunk-idx 1, :block-idxs (doto (RoaringBitmap.) (.add 1))})]]
              (t/is (= expected-match
                       (meta/matching-chunks metadata-mgr watermark
                                             (expr.meta/->metadata-selector '(> name "Ivan") '#{name} {})))
                    "only needs to scan chunk 1, block 1")
              (t/is (= expected-match
                       (meta/matching-chunks metadata-mgr watermark
                                             (expr.meta/->metadata-selector '(> name ?name) '#{name} {'?name "Ivan"})))
                    "only needs to scan chunk 1, block 1")))

          (-> (c2/submit-tx node [[:put {:name "Jeremy", :_id :jdt}]])
              (tu/then-await-tx node))

          (test-query-ivan #{{:_id :jms, :name "James"}
                             {:_id :jon, :name "Jon"}}
                           db))

        (let [db (snap/snapshot snapshot-factory)]
          (test-query-ivan #{{:_id :jms, :name "James"}
                             {:_id :jon, :name "Jon"}
                             {:_id :jdt, :name "Jeremy"}}
                           db))))))

(t/deftest test-find-eq-ivan
  (with-open [node (node/start-node {::idx/indexer {:max-rows-per-chunk 10, :max-rows-per-block 3}})]
    (-> (c2/submit-tx node [[:put {:name "Håkan", :_id :hak}]
                            [:put {:name "James", :_id :jms}]
                            [:put {:name "Ivan", :_id :iva}]])
        (tu/then-await-tx node))

    (tu/finish-chunk node)

    (-> (c2/submit-tx node [[:put {:name "Håkan", :_id :hak}]
                            [:put {:name "James", :_id :jms}]])
        (tu/then-await-tx node))

    (tu/finish-chunk node)
    (let [^IMetadataManager metadata-mgr (tu/component node ::meta/metadata-manager)
          ^IChunkManager indexer (tu/component node ::idx/indexer)
          ^ISnapshotFactory snapshot-factory (tu/component node ::snap/snapshot-factory)
          db (snap/snapshot snapshot-factory)]
      (with-open [watermark (.getWatermark indexer)]
        (t/is (= #{0 3} (.knownChunks metadata-mgr)))
        (let [expected-match [(meta/map->ChunkMatch
                               {:chunk-idx 0, :block-idxs (doto (RoaringBitmap.) (.add 0))})]]
          (t/is (= expected-match
                   (meta/matching-chunks metadata-mgr watermark
                                         (expr.meta/->metadata-selector '(= name "Ivan") '#{name} {})))
                "only needs to scan chunk 0, block 0")

          (t/is (= expected-match
                   (meta/matching-chunks metadata-mgr watermark
                                         (expr.meta/->metadata-selector '(= name ?name) '#{name} {'?name "Ivan"})))
                "only needs to scan chunk 0, block 0"))

        (t/is (= #{{:name "Ivan"}}
                 (set (op/query-ra '[:scan [{name (= name "Ivan")}]] db))))

        (t/is (= #{{:name "Ivan"}}
                 (set (op/query-ra '[:scan [{name (= name ?name)}]]
                                   {'$ db, '?name "Ivan"}))))))))

(t/deftest test-temporal-bounds
  (with-open [node (node/start-node {})]
    (let [{tt1 :tx-time} @(c2/submit-tx node [[:put {:_id :my-doc, :last-updated "tx1"}]])
          _ (Thread/sleep 10) ; to prevent same-ms transactions
          {tt2 :tx-time, :as tx2} @(c2/submit-tx node [[:put {:_id :my-doc, :last-updated "tx2"}]])
          db (snap/snapshot (tu/component node ::snap/snapshot-factory) tx2)]
      (letfn [(q [& temporal-constraints]
                (->> (op/query-ra [:scan (into '[last-updated]
                                               temporal-constraints)]
                                  {'$ db, '?tt1 tt1, '?tt2 tt2})
                     (into #{} (map :last-updated))))]
        (t/is (= #{"tx2"}
                 (q)))

        (t/is (= #{"tx1"}
                 (q '{_tx-time-start (<= _tx-time-start ?tt1)})))

        (t/is (= #{}
                 (q '{_tx-time-start (< _tx-time-start ?tt1)})))

        (t/is (= #{"tx1" "tx2"}
                 (q '{_tx-time-start (<= _tx-time-start ?tt2)})))

        (t/is (= #{"tx2"}
                 (q '{_tx-time-start (> _tx-time-start ?tt1)})))

        (t/is (= #{}
                 (q '{_tx-time-end (< _tx-time-end ?tt2)})))

        (t/is (= #{"tx1"}
                 (q '{_tx-time-end (<= _tx-time-end ?tt2)})))

        (t/is (= #{"tx2"}
                 (q '{_tx-time-end (> _tx-time-end ?tt2)})))

        (t/is (= #{"tx1" "tx2"}
                 (q '{_tx-time-end (>= _tx-time-end ?tt2)})))

        (t/testing "multiple constraints"
          (t/is (= #{"tx1"}
                   (q '{_tx-time-start (and (<= _tx-time-start ?tt1)
                                            (<= _tx-time-start ?tt2))})))

          (t/is (= #{"tx1"}
                   (q '{_tx-time-start (and (<= _tx-time-start ?tt2)
                                            (<= _tx-time-start ?tt1))})))

          (t/is (= #{"tx2"}
                   (q '{_tx-time-end (and (> _tx-time-end ?tt2)
                                          (> _tx-time-end ?tt1))})))

          (t/is (= #{"tx2"}
                   (q '{_tx-time-end (and (> _tx-time-end ?tt1)
                                          (> _tx-time-end ?tt2))}))))

        (t/is (= #{}
                 (q '{_tx-time-start (<= _tx-time-start ?tt1)}
                    '{_tx-time-end (< _tx-time-end ?tt2)})))

        (t/is (= #{"tx1"}
                 (q '{_tx-time-start (<= _tx-time-start ?tt1)}
                    '{_tx-time-end (<= _tx-time-end ?tt2)})))

        (t/is (= #{"tx1"}
                 (q '{_tx-time-start (<= _tx-time-start ?tt1)}
                    '{_tx-time-end (> _tx-time-end ?tt1)}))
              "as of tt1")

        (t/is (= #{"tx2"}
                 (q '{_tx-time-start (<= _tx-time-start ?tt2)}
                    '{_tx-time-end (> _tx-time-end ?tt2)}))
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
             (op/query-ra '[:fixpoint Fact
                            [:table $table]
                            [:select
                             (<= a 8)
                             [:project
                              [{a (+ a 1)}
                               {b (* (+ a 1) b)}]
                              Fact]]]
                          {'$table [{:a 0 :b 1}]}
                          {}))))

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
             (op/query-ra '[:fixpoint Path
                            [:table $table]
                            [:project [x y]
                             [:join [{z z}]
                              [:rename {y z} Path]
                              [:rename {x z} Path]]]]
                          {'$table [{:x "a" :y "b"}
                                    {:x "b" :y "c"}
                                    {:x "c" :y "d"}
                                    {:x "d" :y "a"}]}
                          {})))))

(t/deftest test-assignment-operator
  (t/is (= [{:a 1 :b 1}]
           (op/query-ra '[:assign [X [:table $x]
                                   Y [:table $y]]
                          [:join [{a b}] X Y]]
                        '{$x [{:a 1}]
                          $y [{:b 1}]})))

  (t/testing "can see earlier assignments"
    (t/is (= [{:a 1 :b 1}]
             (op/query-ra '[:assign [X [:table $x]
                                     Y [:join [{a b}] X [:table $y]]
                                     X Y]
                            X]
                          '{$x [{:a 1}]
                            $y [{:b 1}]})))))

(t/deftest test-unwind-operator
  (t/is (= [{:a 1, :b [1 2], :b* 1}
            {:a 1, :b [1 2], :b* 2}
            {:a 2, :b [3 4 5], :b* 3}
            {:a 2, :b [3 4 5], :b* 4}
            {:a 2, :b [3 4 5], :b* 5}]
           (op/query-ra '[:unwind {b* b}
                          [:table $x]]
                        '{$x [{:a 1, :b [1 2]} {:a 2, :b [3 4 5]}]})))

  (t/is (= [{:a 1, :b* 1} {:a 1, :b* 2}]
           (op/query-ra '[:project [a b*]
                          [:unwind {b* b}
                           [:table $x]]]
                        '{$x [{:a 1, :b [1 2]} {:a 2, :b []}]}))
        "skips rows with empty lists")

  (t/is (= [{:a 1, :b* 1} {:a 1, :b* 2}]
           (op/query-ra '[:project [a b*]
                          [:unwind {b* b}
                           [:table $x]]]
                        '{$x [{:a 2, :b 1} {:a 1, :b [1 2]}]}))
        "skips rows with non-list unwind column")

  (t/is (= [{:a 1, :b* 1} {:a 1, :b* "foo"}]
           (op/query-ra '[:project [a b*]
                          [:unwind {b* b}
                           [:table $x]]]
                        '{$x [{:a 1, :b [1 "foo"]}]}))
        "handles multiple types")

  (t/is (= [{:a 1, :b* 1, :$ordinal 1}
            {:a 1, :b* 2, :$ordinal 2}
            {:a 2, :b* 3, :$ordinal 1}
            {:a 2, :b* 4, :$ordinal 2}
            {:a 2, :b* 5, :$ordinal 3}]
           (op/query-ra '[:project [a b* $ordinal]
                          [:unwind {b* b} {:ordinality-column $ordinal}
                           [:table $x]]]
                        '{$x [{:a 1 :b [1 2]} {:a 2 :b [3 4 5]}]}))
        "with ordinality"))

(t/deftest test-max-1-row-operator
  (t/is (= [{:a 1, :b 2}]
           (op/query-ra '[:max-1-row [:table $x]]
                        '{$x [{:a 1, :b 2}]})))

  (t/is (thrown-with-msg? RuntimeException
                          #"cardinality violation"
                          (op/query-ra '[:max-1-row [:table $x]]
                                       '{$x [{:a 1, :b 2} {:a 3, :b 4}]}))
        "throws on cardinality > 1")

  (t/testing "returns null on empty"
    (t/is (= [{}]
             (op/query-ra '[:max-1-row [:table $x]]
                          '{$x []})))

    (t/is (= [{:a nil, :b nil}]
             (op/query-ra '[:max-1-row [:table #{a b} $x]]
                          '{$x []})))))

(t/deftest test-apply-operator
  (letfn [(q [mode]
            (op/query-ra [:apply mode '{c-id ?c-id}
                          [::tu/blocks (Schema. [(types/->field "c-id" types/varchar-type false)
                                                 (types/->field "c-name" types/varchar-type false)])
                           [[{:c-id "c1", :c-name "Alan"}
                             {:c-id "c2", :c-name "Bob"}
                             {:c-id "c3", :c-name "Charlie"}]]]
                          [:select '(= o-customer-id ?c-id)
                           [::tu/blocks (Schema. [(types/->field "o-customer-id" types/varchar-type false)
                                                  (types/->field "o-value" types/float8-type false)])
                            [[{:o-customer-id "c1", :o-value 12.34}
                              {:o-customer-id "c1", :o-value 14.80}
                              {:o-customer-id "c2", :o-value 91.46}
                              {:o-customer-id "c4", :o-value 55.32}]]]]]
                         {}))]

    (t/is (= [{:c-id "c1", :c-name "Alan", :o-customer-id "c1", :o-value 12.34}
              {:c-id "c1", :c-name "Alan", :o-customer-id "c1", :o-value 14.80}
              {:c-id "c2", :c-name "Bob", :o-customer-id "c2", :o-value 91.46}]

             (q :cross-join)))

    (t/is (= [{:c-id "c1", :c-name "Alan", :o-customer-id "c1", :o-value 12.34}
              {:c-id "c1", :c-name "Alan", :o-customer-id "c1", :o-value 14.80}
              {:c-id "c2", :c-name "Bob", :o-customer-id "c2", :o-value 91.46}
              {:c-id "c3", :c-name "Charlie", :o-customer-id nil, :o-value nil}]

             (q :left-outer-join)))

    (t/is (= [{:c-id "c1", :c-name "Alan"}, {:c-id "c2", :c-name "Bob"}]
             (q :semi-join)))

    (t/is (= [{:c-id "c3", :c-name "Charlie"}]
             (q :anti-join)))))

(t/deftest test-project-row-number
  (t/is (= [{:a 12, :$row-num 1}, {:a 0, :$row-num 2}, {:a 100, :$row-num 3}]
           (op/query-ra '[:project [a {$row-num (row-number)}]
                          [:table $a]]

                        {'$a [{:a 12} {:a 0} {:a 100}]}))))

(t/deftest test-project-append-columns
  (t/is (= [{:a 12, :$row-num 1}, {:a 0, :$row-num 2}, {:a 100, :$row-num 3}]
           (op/query-ra '[:project {:append-columns? true} [{$row-num (row-number)}]
                          [:table $a]]

                        {'$a [{:a 12} {:a 0} {:a 100}]}))))

(t/deftest test-array-agg
  (t/is (= [{:a 1, :bs [1 3 6]}
            {:a 2, :bs [2 4]}
            {:a 3, :bs [5]}]
           (op/query-ra '[:group-by [a {bs (array-agg b)}]
                          [:table $ab]]
                        {'$ab [{:a 1, :b 1}
                               {:a 2, :b 2}
                               {:a 1, :b 3}
                               {:a 2, :b 4}
                               {:a 3, :b 5}
                               {:a 1, :b 6}]}))))

(t/deftest test-between
  (t/is (= [[true true] [false true]
            [true true] [false true]
            [false false] [false false]
            [false false] [false false]
            [true true] [false true]]
           (map (juxt :b :bs)
                (op/query-ra '[:project [{b (between x l r)}
                                         {bs (between-symmetric x l r)}]
                               [:table $xlr]]
                             {'$xlr (map #(zipmap [:x :l :r] %)
                                         [[5 0 10] [5 10 0]
                                          [0 0 10] [0 10 0]
                                          [-1 0 10] [-1 10 0]
                                          [11 0 10] [11 10 0]
                                          [10 0 10] [10 10 0]])})))))

(t/deftest test-join-theta
  (t/is (= [{:x3 "31" :x4 "13"} {:x3 "31" :x4 "31"}]
           (op/query-ra '[:join [(= x3 "31")]
                          [:table [{x3 "13"} {x3 "31"}]]
                          [:table [{x4 "13"} {x4 "31"}]]]
                        {})))

  (t/is (= [{:x3 "31"}]
           (op/query-ra '[:join [{x3 x3} (= x3 "31")]
                          [:table [{x3 "13"} {x3 "31"}]]
                          [:table [{x3 "13"} {x3 "31"}]]]
                        {})))

  (t/is (= []
           (op/query-ra '[:join [false]
                          [:table [{x3 "13"} {x3 "31"}]]
                          [:table [{x4 "13"} {x4 "31"}]]]
                        {})))

  (t/is (= []
           (op/query-ra '[:join
                          [(= x1 x3)]
                          [:join [false]
                           [:table [{x1 1}]]
                           [:table [{x2 2}]]]
                          [:table [{x3 1}]]]
                        {}))))

(t/deftest test-current-times-111
  (t/is (= 1
           (->> (op/query-ra '[:project [{ts (current-timestamp)}]
                               [:table [{} {} {}]]]
                             {})
                (into #{} (map :ts))
                count)))

  (let [times (->> (op/query-ra '[:project [{ts (local-time 1)}]
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
                          (op/query-ra '[:select (= ?x13 x4)
                                         [:table []]]
                                       {}))))

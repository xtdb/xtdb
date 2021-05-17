(ns core2.operator-test
  (:require [clojure.test :as t]
            [core2.core :as c2]
            [core2.expression :as expr]
            [core2.expression.metadata :as expr.meta]
            [core2.metadata :as meta]
            [core2.test-util :as tu])
  (:import core2.metadata.IMetadataManager
           org.roaringbitmap.RoaringBitmap))

(t/deftest test-find-gt-ivan
  (with-open [node (c2/start-node {:core2/indexer {:max-rows-per-chunk 10, :max-rows-per-block 2}})]
    @(-> (c2/submit-tx node [{:op :put, :doc {:name "Håkan", :_id 0}}])
         (tu/then-await-tx node))

    (tu/finish-chunk node)

    @(c2/submit-tx node [{:op :put, :doc {:name "Dan", :_id 1}}
                         {:op :put, :doc {:name "Ivan", :_id 2}}])

    @(-> (c2/submit-tx node [{:op :put, :doc {:name "James", :_id 3}}
                             {:op :put, :doc {:name "Jon", :_id 4}}])
         (tu/then-await-tx node))

    (tu/finish-chunk node)

    (let [^IMetadataManager metadata-mgr (:core2/metadata-manager @(:!system node))]
      (letfn [(test-query-ivan [expected db]
                (with-open [res (c2/open-q db '[:scan [{name (> name "Ivan")}]])]
                  (t/is (= expected
                           (into #{} (mapcat seq) (tu/<-cursor res)))))

                (with-open [res (c2/open-q {'$ db, '?name "Ivan"}
                                           '[:scan [{name (> name ?name)}]])]
                  (t/is (= expected
                           (into #{} (mapcat seq) (tu/<-cursor res))))))]

        (with-open [db (c2/open-db node)]
          (t/is (= #{0 1} (.knownChunks metadata-mgr)))
          (let [expected-match [(meta/map->ChunkMatch
                                  {:chunk-idx 1, :block-idxs (doto (RoaringBitmap.) (.add 1))})]]
            (t/is (= expected-match
                     (meta/matching-chunks metadata-mgr (.watermark db)
                                           (expr.meta/->metadata-selector (expr/form->expr '(> name "Ivan")) {})))
                  "only needs to scan chunk 1, block 1")
            (t/is (= expected-match
                     (meta/matching-chunks metadata-mgr (.watermark db)
                                           (expr.meta/->metadata-selector (expr/form->expr '(> name ?name)) {'?name "Ivan"})))
                  "only needs to scan chunk 1, block 1"))

          @(-> (c2/submit-tx node [{:op :put, :doc {:name "Jeremy", :_id 5}}])
               (tu/then-await-tx node))

          (test-query-ivan #{{:name "James"}
                             {:name "Jon"}}
                           db))

        (with-open [db (c2/open-db node)]
          (test-query-ivan #{{:name "James"}
                             {:name "Jon"}
                             {:name "Jeremy"}}
                           db))))))

(t/deftest test-find-eq-ivan
  (with-open [node (c2/start-node {:core2/indexer {:max-rows-per-chunk 10, :max-rows-per-block 3}})]
    @(-> (c2/submit-tx node [{:op :put, :doc {:name "Håkan", :_id 1}}
                             {:op :put, :doc {:name "James", :_id 2}}
                             {:op :put, :doc {:name "Ivan", :_id 3}}])
         (tu/then-await-tx node))

    (tu/finish-chunk node)

    @(-> (c2/submit-tx node [{:op :put, :doc {:name "Håkan", :_id 1}}
                             {:op :put, :doc {:name "James", :_id 2}}])
         (tu/then-await-tx node))

    (tu/finish-chunk node)
    (let [^IMetadataManager metadata-mgr (:core2/metadata-manager @(:!system node))]
      (with-open [db (c2/open-db node)]
        (t/is (= #{0 3} (.knownChunks metadata-mgr)))
        (let [expected-match [(meta/map->ChunkMatch
                               {:chunk-idx 0, :block-idxs (doto (RoaringBitmap.) (.add 0))})]]
          (t/is (= expected-match
                   (meta/matching-chunks metadata-mgr (.watermark db)
                                         (expr.meta/->metadata-selector (expr/form->expr '(= name "Ivan")) {})))
                "only needs to scan chunk 0, block 0")

          (t/is (= expected-match
                   (meta/matching-chunks metadata-mgr (.watermark db)
                                         (expr.meta/->metadata-selector (expr/form->expr '(= name ?name)) {'?name "Ivan"})))
                "only needs to scan chunk 0, block 0"))

        (with-open [res (c2/open-q db '[:scan [{name (= name "Ivan")}]])]
          (t/is (= #{{:name "Ivan"}}
                   (into #{} (mapcat seq) (tu/<-cursor res)))))

        (with-open [res (c2/open-q {'$ db, '?name "Ivan"}
                                   '[:scan [{name (= name ?name)}]])]
          (t/is (= #{{:name "Ivan"}}
                   (into #{} (mapcat seq) (tu/<-cursor res)))))))))

(t/deftest test-fixpoint-operator
  (t/testing "factorial"
    (with-open [fixpoint-cursor (c2/open-q {'table [{:a 0 :b 1}]}
                                           '[:fixpoint Fact
                                             [:table table]
                                             [:select
                                              (<= a 8)
                                              [:project
                                               [{a (+ a 1)}
                                                {b (* (+ a 1) b)}]
                                               Fact]]])]
      (t/is (= [[{:a 0, :b 1}]
                [{:a 1, :b 1}]
                [{:a 2, :b 2}]
                [{:a 3, :b 6}]
                [{:a 4, :b 24}]
                [{:a 5, :b 120}]
                [{:a 6, :b 720}]
                [{:a 7, :b 5040}]
                [{:a 8, :b 40320}]]
               (tu/<-cursor fixpoint-cursor)))))

  (t/testing "transitive closure"
    (with-open [fixpoint-cursor (c2/open-q {'table [{:x "a" :y "b"}
                                                    {:x "b" :y "c"}
                                                    {:x "c" :y "d"}
                                                    {:x "d" :y "a"}]}
                                           '[:fixpoint Path
                                             [:table table]
                                             [:project [x y]
                                              [:join {z z}
                                               [:rename {y z} Path]
                                               [:rename {x z} Path]]]])]

      (t/is (= [[{:x "a", :y "b"}
                 {:x "b", :y "c"}
                 {:x "c", :y "d"}
                 {:x "d", :y "a"}]
                [{:x "d", :y "b"}
                 {:x "a", :y "c"}
                 {:x "b", :y "d"}
                 {:x "c", :y "a"}]
                [{:x "c", :y "b"}
                 {:x "d", :y "c"}
                 {:x "a", :y "d"}
                 {:x "b", :y "a"}]
                [{:x "b", :y "b"}
                 {:x "c", :y "c"}
                 {:x "d", :y "d"}
                 {:x "a", :y "a"}]]
               (tu/<-cursor fixpoint-cursor))))))

(t/deftest test-assignment-operator
  (with-open [assignment-cursor (c2/open-q '{x [{:a 1}]
                                             y [{:b 1}]}
                                           '[:assign [X [:table x]
                                                      Y [:table y]]
                                             [:join {a b} X Y]])]
    (t/is (= [[{:a 1 :b 1}]] (tu/<-cursor assignment-cursor))))

  (t/testing "can see earlier assignments"
    (with-open [assignment-cursor (c2/open-q '{x [{:a 1}]
                                               y [{:b 1}]}
                                             '[:assign [X [:table x]
                                                        Y [:join {a b} X [:table y]]
                                                        X Y]
                                               X])]
      (t/is (= [[{:a 1 :b 1}]] (tu/<-cursor assignment-cursor))))))

(ns xtdb.operator.distinct-test
  (:require [clojure.test :as t]
            [xtdb.operator.distinct :as distinct]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-distinct
  (t/is (= [#{{:a 12, :b 10} {:a 0, :b 15}}
            #{{:a 100, :b 15}}
            #{{:a 10, :b 15}}]
           (->> (tu/query-ra [:distinct {}
                              [::tu/pages
                               [[{:a 12 :b 10}, {:a 0 :b 15}]
                                [{:a 100 :b 15} {:a 0 :b 15}]
                                [{:a 100 :b 15}]
                                [{:a 10 :b 15} {:a 10 :b 15}]]]]
                             {:preserve-pages? true})
                (mapv set))))

  (t/testing "already distinct"
    (t/is (= [#{{:a 12, :b 10} {:a 0, :b 15}}
              #{{:a 100, :b 15}}]
             (->> (tu/query-ra [:distinct {}
                                [::tu/pages
                                 [[{:a 12 :b 10}, {:a 0 :b 15}]
                                  [{:a 100 :b 15}]]]]
                               {:preserve-pages? true})
                  (mapv set)))))

  (t/testing "empty input and output"
    (t/is (empty? (tu/query-ra [:distinct {}
                                [::tu/pages '{a #xt/type :i64} []]]))))

  (t/testing "distinct null"
    (t/is (= [{}]
             (tu/query-ra [:distinct {}
                           [::tu/pages '{a #xt/type :null}
                            [[{:a nil}, {:a nil}]
                             [{:a nil}]]]])))))

(t/deftest test-relation-map-over-pages-of-different-types
  (util/with-open [rel-map (distinct/->relation-map tu/*allocator*
                                                    {:build-vec-types {'a (types/merge-types (types/value->vec-type 1)
                                                                                             (types/value->vec-type "1"))}
                                                     :build-key-col-names ['a]})
                   ints (tu/open-vec "a" [1 2])
                   strs (tu/open-vec "a" ["1" "2"])]
    (letfn [(new? [page idx]
              (neg? (.addIfNotPresent (.buildFromRelation rel-map (vr/rel-reader [page])) idx)))]
      (t/is (new? ints 0))
      (t/is (new? strs 0) "\"1\" is distinct from 1")
      (t/is (not (new? ints 0)))
      (t/is (not (new? strs 0)))
      (t/is (new? strs 1)))))

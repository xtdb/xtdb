(ns crux.project-test
  (:require [clojure.test :as t]
            [crux.api :as crux]
            [crux.fixtures :as fix :refer [*api*]]
            [crux.query :as q]
            [clojure.java.io :as io]))

(t/use-fixtures :each fix/with-node)

(t/deftest test-project
  (fix/submit+await-tx (for [doc (read-string (slurp (io/resource "data/james-bond.edn")))]
                         [:crux.tx/put doc]))

  (let [->lookup-docs (let [f @#'q/lookup-docs]
                        (fn [!lookup-counts]
                          (fn [v db]
                            (swap! !lookup-counts conj (count (::q/hashes (meta v))))
                            (f v db))))
        db (crux/db *api*)]

    (t/testing "simple props"
      (let [expected #{[{:vehicle/brand "Aston Martin", :vehicle/model "DB5"}]
                       [{:vehicle/brand "Aston Martin", :vehicle/model "DB10"}]
                       [{:vehicle/brand "Aston Martin", :vehicle/model "DBS"}]
                       [{:vehicle/brand "Aston Martin", :vehicle/model "DBS V12"}]
                       [{:vehicle/brand "Aston Martin", :vehicle/model "V8 Vantage Volante"}]
                       [{:vehicle/brand "Aston Martin", :vehicle/model "V12 Vanquish"}]}]
        (let [!lookup-counts (atom [])]
          (with-redefs [q/lookup-docs (->lookup-docs !lookup-counts)]
            (t/is (= expected
                     (crux/q db '{:find [(eql/project ?v [:vehicle/brand :vehicle/model])]
                                  :where [[?v :vehicle/brand "Aston Martin"]]})))
            (t/is (= [6] @!lookup-counts) "batching lookups")))

        (let [!lookup-counts (atom [])]
          (with-redefs [q/lookup-docs (->lookup-docs !lookup-counts)]
            (t/is (= expected
                     (crux/q db '{:find [(eql/project ?v [:vehicle/brand :vehicle/model])]
                                  :where [[?v :vehicle/brand "Aston Martin"]]
                                  :batch-size 3})))
            (t/is (= [3 3] @!lookup-counts) "batching lookups")))))

    (t/testing "forward joins"
      (let [!lookup-counts (atom [])]
        (with-redefs [q/lookup-docs (->lookup-docs !lookup-counts)]
          (t/is (= #{[{:film/year "2002",
                       :film/name "Die Another Day"
                       :film/bond {:person/name "Pierce Brosnan"},
                       :film/director {:person/name "Lee Tamahori"},
                       :film/vehicles [{:vehicle/brand "Jaguar", :vehicle/model "XKR"}
                                       {:vehicle/brand "Aston Martin", :vehicle/model "V12 Vanquish"}
                                       {:vehicle/brand "Ford", :vehicle/model "Thunderbird"}
                                       {:vehicle/brand "Ford", :vehicle/model "Fairlane"}]}]}
                   (crux/q db '{:find [(eql/project ?f [{:film/bond [:person/name]}
                                                        {:film/director [:person/name]}
                                                        {:film/vehicles [:vehicle/brand :vehicle/model]}
                                                        :film/name :film/year])]
                                :where [[?f :film/name "Die Another Day"]]})))
          (t/is (= [1 6] @!lookup-counts) "batching lookups"))))

    (t/testing "reverse joins"
      (let [!lookup-counts (atom [])]
        (with-redefs [q/lookup-docs (->lookup-docs !lookup-counts)]
          (t/is (= #{[{:person/name "Daniel Craig",
                       :film/_bond [#:film{:name "Skyfall", :year "2012"}
                                    #:film{:name "Spectre", :year "2015"}
                                    #:film{:name "Casino Royale", :year "2006"}
                                    #:film{:name "Quantum of Solace", :year "2008"}]}]}
                   (crux/q db '{:find [(eql/project ?dc [:person/name
                                                         {:film/_bond [:film/name :film/year]}])]
                                :where [[?dc :person/name "Daniel Craig"]]})))
          (t/is (= [5] @!lookup-counts) "batching lookups"))))

    (t/testing "project *"
      (t/is (= #{[{:crux.db/id :daniel-craig
                   :person/name "Daniel Craig",
                   :type :person}]}
               (crux/q db '{:find [(eql/project ?dc [*])]
                            :where [[?dc :person/name "Daniel Craig"]]}))))))

(t/deftest test-union
  (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :foo
                                       :type :a
                                       :x 2
                                       :y "this"
                                       :z :not-this}]
                        [:crux.tx/put {:crux.db/id :bar
                                       :type :b
                                       :y "not this"
                                       :z 5}]])

  (t/is (= #{[{:crux.db/id :foo, :x 2, :y "this"}]
             [{:crux.db/id :bar, :z 5}]}
           (crux/q (crux/db *api*)
                   '{:find [(eql/project ?it [{:type {:a [:x :y], :b [:z]}}
                                              :crux.db/id])]
                     :where [[?it :crux.db/id]]}))))

(ns xtdb.query-test
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.test :as t]
            [clojure.walk :as w]
            [xtdb.api :as xt]
            [xtdb.fixtures :as fix :refer [*api*]]
            [xtdb.fixtures.kv :as fkv]
            [xtdb.fixtures.tpch :as tpch]
            [xtdb.index :as idx]
            [xtdb.query :as q]
            [xtdb.io :as xio])
  (:import (java.util Arrays Date UUID)
           (java.util.concurrent TimeoutException)))

(t/use-fixtures :each fkv/with-each-kv-store* fkv/with-kv-store-opts* fix/with-node)

(t/deftest test-sanity-check
  (fix/transact! *api* (fix/people [{:name "Ivan"}]))
  (t/is (first (xt/q (xt/db *api*) '{:find [e]
                                     :where [[e :name "Ivan"]]}))))

(t/deftest test-basic-query
  (fix/transact! *api* (fix/people [{:xt/id :ivan :name "Ivan" :last-name "Ivanov"}
                                    {:xt/id :petr :name "Petr" :last-name "Petrov"}]))

  (t/testing "Can query value by single field"
    (t/is (= #{["Ivan"]} (xt/q (xt/db *api*) '{:find [name]
                                               :where [[e :name "Ivan"]
                                                       [e :name name]]})))
    (t/is (= #{["Petr"]} (xt/q (xt/db *api*) '{:find [name]
                                               :where [[e :name "Petr"]
                                                       [e :name name]]}))))

  (t/testing "Can query entity by single field"
    (t/is (= #{[:ivan]} (xt/q (xt/db *api*) '{:find [e]
                                              :where [[e :name "Ivan"]]})))
    (t/is (= #{[:petr]} (xt/q (xt/db *api*) '{:find [e]
                                              :where [[e :name "Petr"]]}))))

  (t/testing "Can query using multiple terms"
    (t/is (= #{["Ivan" "Ivanov"]} (xt/q (xt/db *api*) '{:find [name last-name]
                                                        :where [[e :name name]
                                                                [e :last-name last-name]
                                                                [e :name "Ivan"]
                                                                [e :last-name "Ivanov"]]}))))

  (t/testing "Negate query based on subsequent non-matching clause"
    (t/is (= #{} (xt/q (xt/db *api*) '{:find [e]
                                       :where [[e :name "Ivan"]
                                               [e :last-name "Ivanov-does-not-match"]]}))))

  (t/testing "Can query for multiple results"
    (t/is (= #{["Ivan"] ["Petr"]}
             (xt/q (xt/db *api*) '{:find [name] :where [[e :name name]]}))))


  (fix/transact! *api* (fix/people [{:xt/id :smith :name "Smith" :last-name "Smith"}]))
  (t/testing "Can query across fields for same value"
    (t/is (= #{[:smith]}
             (xt/q (xt/db *api*) '{:find [p1] :where [[p1 :name name]
                                                      [p1 :last-name name]]}))))

  (t/testing "Can query across fields for same value when value is passed in"
    (t/is (= #{[:smith]}
             (xt/q (xt/db *api*) '{:find [p1] :where [[p1 :name name]
                                                      [p1 :last-name name]
                                                      [p1 :name "Smith"]]})))))

(t/deftest test-returning-maps
  (fix/transact! *api* (fix/people [{:xt/id :ivan :name "Ivan" :last-name "Ivanov"}
                                    {:xt/id :petr :name "Petr" :last-name "Petrov"}]))

  (let [db (xt/db *api*)]
    (t/is (= #{{:user/name "Ivan", :user/last-name "Ivanov"}
               {:user/name "Petr", :user/last-name "Petrov"}}
             (xt/q (xt/db *api*)
                   '{:find [?name ?last-name]
                     :keys [user/name user/last-name]
                     :where [[e :name ?name]
                             [e :last-name ?last-name]]})))

    (t/is (= #{{'user/name "Ivan", 'user/last-name "Ivanov"}
               {'user/name "Petr", 'user/last-name "Petrov"}}
             (xt/q (xt/db *api*)
                   '{:find [?name ?last-name]
                     :syms [user/name user/last-name]
                     :where [[e :name ?name]
                             [e :last-name ?last-name]]})))

    (t/is (= #{{"name" "Ivan", "last-name" "Ivanov"}
               {"name" "Petr", "last-name" "Petrov"}}
             (xt/q (xt/db *api*)
                   '{:find [?name ?last-name]
                     :strs [name last-name]
                     :where [[e :name ?name]
                             [e :last-name ?last-name]]})))

    (t/is (thrown? IllegalArgumentException
                   (xt/q db
                         '{:find [name last-name]
                           :keys [name]
                           :where [[e :name name]
                                   [e :last-name last-name]
                                   [e :name "Ivan"]
                                   [e :last-name "Ivanov"]]}))
          "throws on arity mismatch")))

(t/deftest test-query-with-arguments
  (let [[ivan petr] (fix/transact! *api* (fix/people [{:name "Ivan" :last-name "Ivanov"}
                                                      {:name "Petr" :last-name "Petrov"}]))]

    (t/testing "Can query entity by single field"
      (t/is (= #{[(:xt/id ivan)]} (xt/q (xt/db *api*) '{:find [e]
                                                        :where [[e :name name]]
                                                        :args [{:name "Ivan"}]})))
      (t/is (= #{[(:xt/id petr)]} (xt/q (xt/db *api*) '{:find [e]
                                                        :where [[e :name name]]
                                                        :args [{:name "Petr"}]}))))

    (t/testing "Can query entity by entity position"
      (t/is (= #{["Ivan"]
                 ["Petr"]} (xt/q (xt/db *api*) {:find '[name]
                                                :where '[[e :name name]]
                                                :args [{:e (:xt/id ivan)}
                                                       {:e (:xt/id petr)}]})))

      (t/is (= #{["Ivan" "Ivanov"]
                 ["Petr" "Petrov"]} (xt/q (xt/db *api*) {:find '[name last-name]
                                                         :where '[[e :name name]
                                                                  [e :last-name last-name]]
                                                         :args [{:e (:xt/id ivan)}
                                                                {:e (:xt/id petr)}]}))))

    (t/testing "Can match on both entity and value position"
      (t/is (= #{["Ivan"]} (xt/q (xt/db *api*) {:find '[name]
                                                :where '[[e :name name]]
                                                :args [{:e (:xt/id ivan)
                                                        :name "Ivan"}]})))

      (t/is (= #{} (xt/q (xt/db *api*) {:find '[name]
                                        :where '[[e :name name]]
                                        :args [{:e (:xt/id ivan)
                                                :name "Petr"}]}))))

    (t/testing "Can query entity by single field with several arguments"
      (t/is (= #{[(:xt/id ivan)]
                 [(:xt/id petr)]} (xt/q (xt/db *api*) '{:find [e]
                                                        :where [[e :name name]]
                                                        :args [{:name "Ivan"}
                                                               {:name "Petr"}]}))))

    (t/testing "Can query entity by single field with literals"
      (t/is (= #{[(:xt/id ivan)]} (xt/q (xt/db *api*) '{:find [e]
                                                        :where [[e :name name]
                                                                [e :last-name "Ivanov"]]
                                                        :args [{:name "Ivan"}
                                                               {:name "Petr"}]})))

      (t/is (= #{["Ivan"]} (xt/q (xt/db *api*) {:find '[name]
                                                :where '[[e :name name]
                                                         [e :last-name "Ivanov"]]
                                                :args [{:e (:xt/id ivan)}
                                                       {:e (:xt/id petr)}]}))))

    (t/testing "Can query entity by non existent argument"
      (t/is (= #{} (xt/q (xt/db *api*) '{:find [e]
                                         :where [[e :name name]]
                                         :args [{:name "Bob"}]}))))

    (t/testing "Can query entity with empty arguments"
      (t/is (= #{[(:xt/id ivan)]
                 [(:xt/id petr)]} (xt/q (xt/db *api*) '{:find [e]
                                                        :where [[e :name name]]
                                                        :args []}))))

    (t/testing "Can query entity with tuple arguments"
      (t/is (= #{[(:xt/id ivan)]
                 [(:xt/id petr)]} (xt/q (xt/db *api*) '{:find [e]
                                                        :where [[e :name name]
                                                                [e :last-name last-name]]
                                                        :args [{:name "Ivan" :last-name "Ivanov"}
                                                               {:name "Petr" :last-name "Petrov"}]}))))

    (t/testing "Can query predicates based on arguments alone"
      (t/is (= #{["Ivan"]} (xt/q (xt/db *api*) '{:find [name]
                                                 :where [[(re-find #"I" name)]]
                                                 :args [{:name "Ivan"}
                                                        {:name "Petr"}]})))

      (t/is (= #{["Ivan"]} (xt/q (xt/db *api*) '{:find [name]
                                                 :where [[(re-find #"I" name)]
                                                         [(= last-name "Ivanov")]]
                                                 :args [{:name "Ivan" :last-name "Ivanov"}
                                                        {:name "Petr" :last-name "Petrov"}]})))

      (t/is (= #{["Ivan"]
                 ["Petr"]} (xt/q (xt/db *api*) '{:find [name]
                                                 :where [[(string? name)]]
                                                 :args [{:name "Ivan"}
                                                        {:name "Petr"}]})))

      (t/is (= #{["Ivan" "Ivanov"]
                 ["Petr" "Petrov"]} (xt/q (xt/db *api*) '{:find [name
                                                                 last-name]
                                                          :where [[(not= last-name name)]]
                                                          :args [{:name "Ivan" :last-name "Ivanov"}
                                                                 {:name "Petr" :last-name "Petrov"}]})))

      (t/is (= #{["Ivan"]} (xt/q (xt/db *api*) '{:find [name]
                                                 :where [[(string? name)]
                                                         [(re-find #"I" name)]]
                                                 :args [{:name "Ivan"}
                                                        {:name "Petr"}]})))

      (t/is (= #{} (xt/q (xt/db *api*) '{:find [name]
                                         :where [[(number? name)]]
                                         :args [{:name "Ivan"}
                                                {:name "Petr"}]})))

      (t/is (= #{} (xt/q (xt/db *api*) '{:find [name]
                                         :where [(not [(string? name)])]
                                         :args [{:name "Ivan"}
                                                {:name "Petr"}]})))

      (t/testing "Can use range constraints on arguments"
        (t/is (= #{} (xt/q (xt/db *api*) '{:find [age]
                                           :where [[(>= age 21)]]
                                           :args [{:age 20}]})))

        (t/is (= #{[22]} (xt/q (xt/db *api*) '{:find [age]
                                               :where [[(>= age 21)]]
                                               :args [{:age 22}]})))))))

(t/deftest test-query-with-in-bindings
  (let [[ivan petr] (fix/transact! *api* (fix/people [{:name "Ivan" :last-name "Ivanov"}
                                                      {:name "Petr" :last-name "Petrov"}]))]

    (t/is (= #{[(:xt/id ivan)]} (xt/q (xt/db *api*)
                                      '{:find [e]
                                        :in [$ name]
                                        :where [[e :name name]]}
                                      "Ivan")))

    (t/testing "the db var is optional"
      (t/is (= #{[(:xt/id ivan)]} (xt/q (xt/db *api*)
                                        '{:find [e]
                                          :in [name]
                                          :where [[e :name name]]}
                                        "Ivan"))))

    (t/is (= #{[(:xt/id ivan)]} (xt/q (xt/db *api*)
                                      '{:find [e]
                                        :in [$ name last-name]
                                        :where [[e :name name]
                                                [e :last-name last-name]]}
                                      "Ivan" "Ivanov")))

    (t/is (= #{[(:xt/id ivan)]} (xt/q (xt/db *api*)
                                      '{:find [e]
                                        :in [$ [name]]
                                        :where [[e :name name]]}
                                      ["Ivan"])))

    (t/is (= #{[(:xt/id ivan)] [(:xt/id petr)]}
             (xt/q (xt/db *api*)
                   '{:find [e]
                     :in [$ [[name]]]
                     :where [[e :name name]]}
                   [["Ivan"] ["Petr"]])))

    (t/is (= #{[(:xt/id ivan)] [(:xt/id petr)]}
             (xt/q (xt/db *api*)
                   '{:find [e]
                     :in [$ [name ...]]
                     :where [[e :name name]]}
                   ["Ivan" "Petr"])))

    (t/testing "can access the db"
      (t/is (= #{["class xtdb.query.QueryDatasource"]} (xt/q (xt/db *api*) '{:find [ts]
                                                                             :in [$]
                                                                             :where [[(str t) ts]
                                                                                     [(type $) t]]}))))
    (t/testing "where clause is optional"
      (t/is (= #{[1]} (xt/q (xt/db *api*)
                            '{:find [x]
                              :in [$ x]}
                            1))))

    (t/testing "can use both args and in"
      (t/is (= #{[2]} (xt/q (xt/db *api*)
                            '{:find [x]
                              :in [$ [x ...]]
                              :args [{:x 1}
                                     {:x 2}]}
                            [2 3]))))

    (t/testing "var bindings need to be distinct"
      (t/is (thrown-with-msg?
             IllegalArgumentException
             #"In binding variables not distinct"
             (xt/q (xt/db *api*) '{:find [x]
                                   :in [$ [x x]]} [1 1]))))))

(t/deftest test-multiple-results
  (fix/transact! *api* (fix/people [{:name "Ivan" :last-name "1"}
                                    {:name "Ivan" :last-name "2"}]))
  (t/is (= 2
           (count (xt/q (xt/db *api*) '{:find [e] :where [[e :name "Ivan"]]})))))

(t/deftest test-query-using-keywords
  (fix/transact! *api* (fix/people [{:name "Ivan" :sex :male}
                                    {:name "Petr" :sex :male}
                                    {:name "Doris" :sex :female}
                                    {:name "Jane" :sex :female}]))

  (t/testing "Can query by single field"
    (t/is (= #{["Ivan"] ["Petr"]} (xt/q (xt/db *api*) '{:find [name]
                                                        :where [[e :name name]
                                                                [e :sex :male]]})))
    (t/is (= #{["Doris"] ["Jane"]} (xt/q (xt/db *api*) '{:find [name]
                                                         :where [[e :name name]
                                                                 [e :sex :female]]})))))

(t/deftest test-basic-query-at-t
  (let [[malcolm] (fix/transact! *api* (fix/people [{:xt/id :malcolm :name "Malcolm" :last-name "Sparks"}])
                                 #inst "1986-10-22")]
    (fix/transact! *api* (fix/people [{:xt/id :malcolm :name "Malcolma" :last-name "Sparks"}]) #inst "1986-10-24")
    (let [q '{:find [e]
              :where [[e :name "Malcolma"]
                      [e :last-name "Sparks"]]}]
      (t/is (= #{} (xt/q (xt/db *api* #inst "1986-10-23")
                         q)))
      (t/is (= #{[(:xt/id malcolm)]} (xt/q (xt/db *api*) q))))))

(t/deftest test-query-across-entities-using-join
  ;; Five people, two of which share the same name:
  (fix/transact! *api* (fix/people [{:name "Ivan"} {:name "Petr"} {:name "Sergei"} {:name "Denis"} {:name "Denis"}]))

  (t/testing "Five people, without a join"
    (t/is (= 5 (count (xt/q (xt/db *api*) '{:find [p1]
                                            :where [[p1 :name name]
                                                    [p1 :age age]
                                                    [p1 :salary salary]]})))))

  (t/testing "Five people, a cartesian product - joining without unification"
    (t/is (= 25 (count (xt/q (xt/db *api*) '{:find [p1 p2]
                                             :where [[p1 :name]
                                                     [p2 :name]]})))))

  (t/testing "A single first result, joined to all possible subsequent results in next term"
    (t/is (= 5 (count (xt/q (xt/db *api*) '{:find [p1 p2]
                                            :where [[p1 :name "Ivan"]
                                                    [p2 :name]]})))))

  (t/testing "A single first result, with no subsequent results in next term"
    (t/is (= 0 (count (xt/q (xt/db *api*) '{:find [p1]
                                            :where [[p1 :name "Ivan"]
                                                    [p2 :name "does-not-match"]]})))))

  (t/testing "Every person joins once, plus 2 more matches"
    (t/is (= 7 (count (xt/q (xt/db *api*) '{:find [p1 p2]
                                            :where [[p1 :name name]
                                                    [p2 :name name]]}))))))

(t/deftest test-join-over-two-attributes
  (fix/transact! *api* (fix/people [{:xt/id :ivan :name "Ivan" :last-name "Ivanov"}
                                    {:xt/id :petr :name "Petr" :follows #{"Ivanov"}}]))

  (t/is (= #{[:petr]} (xt/q (xt/db *api*) '{:find [e2]
                                            :where [[e :last-name last-name]
                                                    [e2 :follows last-name]
                                                    [e :name "Ivan"]]}))))

(t/deftest test-blanks
  (fix/transact! *api* (fix/people [{:name "Ivan"} {:name "Petr"} {:name "Sergei"}]))

  (t/is (= #{["Ivan"] ["Petr"] ["Sergei"]}
           (xt/q (xt/db *api*) '{:find [name]
                                 :where [[_ :name name]]}))))

(t/deftest test-exceptions
  (t/testing "Unbound query variable"
    (t/is (thrown-with-msg?
           IllegalArgumentException
           #"Find refers to unknown variables: #\{bah\}"
           (xt/q (xt/db *api*) '{:find [bah]
                                 :where [[e :name]]})))

    ;; #1729
    (t/is (thrown-with-msg?
           IllegalArgumentException
           #"Find refers to unknown variables: #\{bah\}"
           (xt/q (xt/db *api*) '{:find [(sum bah)]
                                 :where [[e :name]]})))

    (t/is (thrown-with-msg?
           IllegalArgumentException
           #"Clause refers to unknown variable: bah"
           (xt/q (xt/db *api*) '{:find [x]
                                 :where [[x :foo]
                                         [(+ 1 bah)]]})))

    (t/is (thrown-with-msg?
           IllegalArgumentException
           #"Clause refers to unknown variable: x"
           (xt/q (xt/db *api*) '{:find [e]
                                 :where [[e :name v]
                                         [(> 2 x)]]})))))

(t/deftest test-circular-dependencies
  (t/is (empty?
         (xt/q (xt/db *api*)
               '{:find [bah]
                 :in [$ bah]
                 :where [[(+ 1 bah) bah]]}
               1)))

  (t/is (= #{[1]}
           (xt/q (xt/db *api*)
                 '{:find [bah]
                   :in [$ bah]
                   :where [[(identity bah) bah]]}
                 1)))

  (t/is (= #{[1]}
           (xt/q (xt/db *api*)
                 '{:find [bar]
                   :in [$ [[bar foo]]]
                   :where [[(identity foo) bar]
                           [(identity bar) foo]]}
                 [[1 1]
                  [1 2]])))

  (t/is (= #{[0 1]
             [1 2]}
           (xt/q (xt/db *api*)
                 '{:find [bar foo]
                   :in [$ [[bar foo]]]
                   :where [[(+ 1 bar) foo]
                           [(- foo 1) bar]]}
                 [[0 1]
                  [1 2]
                  [1 3]])))

  (t/is (= #{[1 0]
             [2 1]}
           (xt/q (xt/db *api*)
                 '{:find [bar foo]
                   :in [$ [[bar foo]]]
                   :where [[(+ 1 foo) bar]
                           [(- bar 1) foo]]}
                 [[1 0]
                  [2 1]
                  [3 1]]))))

(t/deftest test-not-query
  (t/is (= '[[:triple {:e e :a :name :v name}]
             [:triple {:e e :a :name :v "Ivan"}]
             [:not [[:triple {:e e :a :last-name :v "Ivannotov"}]]]]

           (s/conform :xtdb.query/where '[[e :name name]
                                          [e :name "Ivan"]
                                          (not [e :last-name "Ivannotov"])])))

  (fix/transact! *api* (fix/people [{:xt/id :ivan-ivanov-1 :name "Ivan" :last-name "Ivanov"}
                                    {:xt/id :ivan-ivanov-2 :name "Ivan" :last-name "Ivanov"}
                                    {:xt/id :ivan-ivanovtov-1 :name "Ivan" :last-name "Ivannotov"}]))

  (t/testing "literal v"
    (t/is (= 1 (count (xt/q (xt/db *api*) '{:find [e]
                                            :where [[e :name name]
                                                    [e :name "Ivan"]
                                                    (not [e :last-name "Ivanov"])]}))))

    (t/is (= 1 (count (xt/q (xt/db *api*) '{:find [e]
                                            :where [[e :name name]
                                                    (not [e :last-name "Ivanov"])]}))))

    (t/is (= 1 (count (xt/q (xt/db *api*) '{:find [e]
                                            :where [[e :name "Ivan"]
                                                    (not [e :last-name "Ivanov"])]}))))

    (t/is (= 2 (count (xt/q (xt/db *api*) '{:find [e]
                                            :where [[e :name name]
                                                    [e :name "Ivan"]
                                                    (not [e :last-name "Ivannotov"])]}))))

    (t/testing "multiple clauses in not"
      (t/is (= 2 (count (xt/q (xt/db *api*) '{:find [e]
                                              :where [[e :name name]
                                                      [e :name "Ivan"]
                                                      (not [e :last-name "Ivannotov"]
                                                           [e :name "Ivan"])]}))))

      (t/is (= 2 (count (xt/q (xt/db *api*) '{:find [e]
                                              :where [[e :name name]
                                                      [e :name "Ivan"]
                                                      (not [e :last-name "Ivannotov"]
                                                           [(string? name)])]}))))

      (t/is (= 3 (count (xt/q (xt/db *api*) '{:find [e]
                                              :where [[e :name name]
                                                      [e :name "Ivan"]
                                                      (not [e :last-name "Ivannotov"]
                                                           [(number? name)])]}))))

      (t/is (= 3 (count (xt/q (xt/db *api*) '{:find [e]
                                              :where [[e :name name]
                                                      [e :name "Ivan"]
                                                      (not [e :last-name "Ivannotov"]
                                                           [e :name "Bob"])]}))))))

  (t/testing "variable v"
    (t/is (= 0 (count (xt/q (xt/db *api*) '{:find [e]
                                            :where [[e :name name]
                                                    [e :name "Ivan"]
                                                    (not [e :name name])]}))))

    (t/is (= 0 (count (xt/q (xt/db *api*) '{:find [e]
                                            :where [[e :name name]
                                                    (not [e :name name])]}))))

    (t/is (= 2 (count (xt/q (xt/db *api*) '{:find [e]
                                            :where [[e :name name]
                                                    [:ivan-ivanovtov-1 :last-name i-name]
                                                    (not [e :last-name i-name])]})))))

  (t/testing "literal entities"
    (t/is (= 0 (count (xt/q (xt/db *api*) '{:find [e]
                                            :where [[e :name name]
                                                    (not [:ivan-ivanov-1 :name name])]}))))

    (t/is (= 1 (count (xt/q (xt/db *api*) '{:find [e]
                                            :where [[e :last-name last-name]
                                                    (not [:ivan-ivanov-1 :last-name last-name])]}))))))

(t/deftest test-or-query
  (fix/transact! *api* (fix/people [{:name "Ivan" :last-name "Ivanov"}
                                    {:name "Ivan" :last-name "Ivanov"}
                                    {:name "Ivan" :last-name "Ivannotov"}
                                    {:name "Bob" :last-name "Controlguy"}]))

  ;; Here for dev reasons, delete when appropiate
  (t/is (= '[[:triple {:e e :a :name :v name}]
             [:triple {:e e :a :name :v "Ivan"}]
             [:or {:branches [[[:triple {:e e :a :last-name :v "Ivanov"}]]]}]]
           (s/conform :xtdb.query/where '[[e :name name]
                                          [e :name "Ivan"]
                                          (or [e :last-name "Ivanov"])])))

  (t/testing "Or works as expected"
    (t/is (= 3 (count (xt/q (xt/db *api*) '{:find [e]
                                            :where [[e :name name]
                                                    [e :name "Ivan"]
                                                    (or [e :last-name "Ivanov"]
                                                        [e :last-name "Ivannotov"])]}))))

    (t/is (= 4 (count (xt/q (xt/db *api*) '{:find [e]
                                            :where [(or [e :last-name "Ivanov"]
                                                        [e :last-name "Ivannotov"]
                                                        [e :last-name "Controlguy"])]}))))

    (t/is (= 0 (count (xt/q (xt/db *api*) '{:find [e]
                                            :where [(or [e :last-name "Controlguy"])
                                                    (or [e :last-name "Ivanov"]
                                                        [e :last-name "Ivannotov"])]}))))

    (t/is (= 0 (count (xt/q (xt/db *api*) '{:find [e]
                                            :where [(or [e :last-name "Ivanov"])
                                                    (or [e :last-name "Ivannotov"])]}))))

    (t/is (= 0 (count (xt/q (xt/db *api*) '{:find [e]
                                            :where [[e :last-name "Controlguy"]
                                                    (or [e :last-name "Ivanov"]
                                                        [e :last-name "Ivannotov"])]}))))

    (t/is (= 3 (count (xt/q (xt/db *api*) '{:find [e]
                                            :where [[e :name name]
                                                    (or [e :last-name "Ivanov"]
                                                        [e :name "Bob"])]})))))

  (t/testing "Or edge case - can take a single clause"
    ;; Unsure of the utility
    (t/is (= 2 (count (xt/q (xt/db *api*) '{:find [e]
                                            :where [[e :name name]
                                                    [e :name "Ivan"]
                                                    (or [e :last-name "Ivanov"])]})))))

  (t/is (= #{["Ivan" "Ivanov"]
             ["Ivan" :optional]} (xt/q (xt/db *api*) '{:find [name l]
                                                       :where [[e :name name]
                                                               [e :name "Ivan"]
                                                               (or (and [e :last-name "Ivanov"]
                                                                        [e :last-name l])
                                                                   (and [(identity e)]
                                                                        [(identity :optional) l]))]}))))

(t/deftest test-or-query-can-use-and
  (let [[ivan] (fix/transact! *api* (fix/people [{:name "Ivan" :sex :male}
                                                 {:name "Bob" :sex :male}
                                                 {:name "Ivana" :sex :female}]))]

    (t/is (= #{["Ivan"]
               ["Ivana"]}
             (xt/q (xt/db *api*) '{:find [name]
                                   :where [[e :name name]
                                           (or [e :sex :female]
                                               (and [e :sex :male]
                                                    [e :name "Ivan"]))]})))

    (t/is (= #{[(:xt/id ivan)]}
             (xt/q (xt/db *api*) '{:find [e]
                                   :where [(or [e :name "Ivan"])]})))

    (t/is (= #{}
             (xt/q (xt/db *api*) '{:find [name]
                                   :where [[e :name name]
                                           (or (and [e :sex :female]
                                                    [e :name "Ivan"]))]})))))

(t/deftest test-ors-must-use-same-vars
  (fix/submit+await-tx [[::xt/put {:xt/id 1, :type :a}]
                        [::xt/put {:xt/id 2, :type :b}]
                        [::xt/put {:xt/id 3, :type :b, :extra :val}]
                        [::xt/put {:xt/id 4, :type :c}]])

  (t/is (thrown-with-msg?
         IllegalArgumentException
         #"`or` branches must have the same free variables"
         (xt/q (xt/db *api*) '{:find [e]
                               :where [[e :name name]
                                       (or [e1 :last-name "Ivanov"]
                                           [e2 :last-name "Ivanov"])]})))

  (t/is (thrown-with-msg?
         IllegalArgumentException
         #"`or` branches must have the same free variables"
         (xt/q (xt/db *api*) '{:find [x]
                               :where [(or-join [x]
                                                [e1 :last-name "Ivanov"])]})))

  (t/is (= #{[1] [3]}
           (xt/q (xt/db *api*)
                 '{:find [?e]
                   :where [[?e :type ?type]
                           (or-join [?e ?type]
                                    [(= ?type :a)]
                                    [(= ?e 3)])]}))
        "don't need to mention all bound or vars though")

  (t/is (= #{[1] [3]}
           (xt/q (xt/db *api*)
                 '{:find [?e]
                   :where [[?e :type ?type]
                           (or-join [?e ?type]
                                    (and [(= ?type :a)]
                                         [(any? ?e)])
                                    (and [(= ?e 3)]
                                         [(any? ?type)]))]}))
        "we used to have to use a lot of `any?` - check for backwards compatibility"))

(t/deftest test-ors-can-introduce-new-bindings
  (let [[_petr ivan _ivanova] (fix/transact! *api* (fix/people [{:name "Petr" :last-name "Smith" :sex :male}
                                                                {:name "Ivan" :last-name "Ivanov" :sex :male}
                                                                {:name "Ivanova" :last-name "Ivanov" :sex :female}]))]

    (t/testing "?p2 introduced only inside of an Or"
      (t/is (= #{[(:xt/id ivan)]} (xt/q (xt/db *api*) '{:find [?p2]
                                                        :where [(or (and [?p2 :name "Petr"]
                                                                         [?p2 :sex :female])
                                                                    (and [?p2 :last-name "Ivanov"]
                                                                         [?p2 :sex :male]))]}))))))

(t/deftest test-not-join
  (fix/transact! *api* (fix/people [{:name "Ivan" :last-name "Ivanov"}
                                    {:name "Malcolm" :last-name "Ofsparks"}
                                    {:name "Dominic" :last-name "Monroe"}]))

  (t/testing "Rudimentary not-join"
    (t/is (= #{["Ivan"] ["Malcolm"]}
             (xt/q (xt/db *api*) '{:find [name]
                                   :where [[e :name name]
                                           (not-join [e]
                                                     [e :last-name "Monroe"])]})))

    (t/is (= #{["Ivan"] ["Malcolm"]}
             (xt/q (xt/db *api*) '{:find [name]
                                   :where [[e :name name]
                                           (not-join [e]
                                                     [e :last-name last-name]
                                                     [(= last-name "Monroe")])]})))

    (t/is (= #{["Dominic"]}
             (xt/q (xt/db *api*) '{:find [name]
                                   :where [[e :name name]
                                           (not-join [e]
                                                     [e :last-name last-name]
                                                     [(not= last-name "Monroe")])]})))))

(t/deftest test-mixing-expressions
  (fix/transact! *api* (fix/people [{:name "Ivan" :last-name "Ivanov"}
                                    {:name "Derek" :last-name "Ivanov"}
                                    {:name "Bob" :last-name "Ivannotov"}
                                    {:name "Fred" :last-name "Ivannotov"}]))

  (t/testing "Or can use not expression"
    (t/is (= #{["Ivan"] ["Derek"] ["Fred"]}
             (xt/q (xt/db *api*) '{:find [name]
                                   :where [[e :name name]
                                           (or [e :last-name "Ivanov"]
                                               (not [e :name "Bob"]))]}))))

  (t/testing "Not can use Or expression"
    (t/is (= #{["Fred"]} (xt/q (xt/db *api*) '{:find [name]
                                               :where [[e :name name]
                                                       (not (or [e :last-name "Ivanov"]
                                                                [e :name "Bob"]))]})))))

(t/deftest test-predicate-expression
  (fix/transact! *api* (fix/people [{:xt/id :ivan :name "Ivan" :last-name "Ivanov" :age 30}
                                    {:xt/id :bob :name "Bob" :last-name "Ivanov" :age 40}
                                    {:xt/id :dominic :name "Dominic" :last-name "Monroe" :age 50}]))

  (t/testing "range expressions"
    (t/is (= #{["Ivan"] ["Bob"]}
             (xt/q (xt/db *api*) '{:find [name]
                                   :where [[e :name name]
                                           [e :age age]
                                           [(< age 50)]]})))

    (t/is (= #{["Dominic"]}
             (xt/q (xt/db *api*) '{:find [name]
                                   :where [[e :name name]
                                           [e :age age]
                                           [(>= age 50)]]})))

    (t/testing "fallback to built in predicate for vars"
      (t/is (= #{["Ivan" 30 "Ivan" 30]
                 ["Ivan" 30 "Bob" 40]
                 ["Ivan" 30 "Dominic" 50]
                 ["Bob" 40 "Bob" 40]
                 ["Bob" 40 "Dominic" 50]
                 ["Dominic" 50 "Dominic" 50]}
               (xt/q (xt/db *api*) '{:find [name age1 name2 age2]
                                     :where [[e :name name]
                                             [e :age age1]
                                             [e2 :name name2]
                                             [e2 :age age2]
                                             [(<= age1 age2)]]})))

      (t/is (= #{["Ivan" "Dominic"]
                 ["Ivan" "Bob"]
                 ["Dominic" "Bob"]}
               (xt/q (xt/db *api*) '{:find [name1 name2]
                                     :where [[e :name name1]
                                             [e2 :name name2]
                                             [(> name1 name2)]]})))))

  (t/testing "clojure.core predicate"
    (t/is (= #{["Bob"] ["Dominic"]}
             (xt/q (xt/db *api*) '{:find [name]
                                   :where [[e :name name]
                                           [(re-find #"o" name)]]})))

    (t/testing "No results"
      (t/is (empty? (xt/q (xt/db *api*) '{:find [name]
                                          :where [[e :name name]
                                                  [(re-find #"X" name)]]}))))

    (t/testing "Not predicate"
      (t/is (= #{["Ivan"]}
               (xt/q (xt/db *api*) '{:find [name]
                                     :where [[e :name name]
                                             (not [(re-find #"o" name)])]}))))

    (t/testing "Entity variable"
      (t/is (= #{["Ivan"]}
               (xt/q (xt/db *api*) '{:find [name]
                                     :where [[e :name name]
                                             [(= :ivan e)]]})))

      (t/testing "Filtered by value"
        (t/is (= #{[:bob] [:ivan]}
                 (xt/q (xt/db *api*) '{:find [e]
                                       :where [[e :last-name last-name]
                                               [(= "Ivanov" last-name)]]})))

        (t/is (= #{[:ivan]}
                 (xt/q (xt/db *api*) '{:find [e]
                                       :where [[e :last-name last-name]
                                               [e :age age]
                                               [(= "Ivanov" last-name)]
                                               [(= 30 age)]]})))))

    (t/testing "Several variables"
      (t/is (= #{["Bob"]}
               (xt/q (xt/db *api*) '{:find [name]
                                     :where [[e :name name]
                                             [e :age age]
                                             [(= 40 age)]
                                             [(re-find #"o" name)]
                                             [(not= age name)]]})))

      (t/is (= #{[:bob "Ivanov"]}
               (xt/q (xt/db *api*) '{:find [e last-name]
                                     :where [[e :last-name last-name]
                                             [e :age age]
                                             [(re-find #"ov$" last-name)]
                                             (not [(= age 30)])]})))

      (t/testing "No results"
        (t/is (= #{}
                 (xt/q (xt/db *api*) '{:find [name]
                                       :where [[e :name name]
                                               [e :age age]
                                               [(re-find #"o" name)]
                                               [(= age name)]]})))))

    (t/testing "Bind result to var"
      (t/is (= #{["Dominic" 25] ["Ivan" 15] ["Bob" 20]}
               (xt/q (xt/db *api*) '{:find [name half-age]
                                     :where [[e :name name]
                                             [e :age age]
                                             [(quot age 2) half-age]]})))

      (t/testing "Order of joins is rearranged to ensure arguments are bound"
        (t/is (= #{["Dominic" 25] ["Ivan" 15] ["Bob" 20]}
                 (xt/q (xt/db *api*) '{:find [name half-age]
                                       :where [[e :name name]
                                               [e :age real-age]
                                               [(quot real-age 2) half-age]]}))))

      (t/testing "Binding more than once intersects result"
        (t/is (= #{["Ivan" 15]}
                 (xt/q (xt/db *api*) '{:find [name half-age]
                                       :where [[e :name name]
                                               [e :age real-age]
                                               [(quot real-age 2) half-age]
                                               [(- real-age 15) half-age]]}))))

      (t/testing "Binding can use range predicates"
        (t/is (= #{["Dominic" 25]}
                 (xt/q (xt/db *api*) '{:find [name half-age]
                                       :where [[e :name name]
                                               [e :age real-age]
                                               [(quot real-age 2) half-age]
                                               [(> half-age 20)]]})))))))

(t/deftest test-attributes-with-multiple-values
  (fix/transact! *api* (fix/people [{:xt/id :ivan :name "Ivan" :last-name "Ivanov" :age 30 :friends #{:bob :dominic}}
                                    {:xt/id :bob :name "Bob" :last-name "Ivanov" :age 40 :friends #{:ivan :dominic}}
                                    {:xt/id :dominic :name "Dominic" :last-name "Monroe" :age 50 :friends #{:bob}}]))

  (t/testing "can find multiple values"
    (t/is (= #{[:bob] [:dominic]}
             (xt/q (xt/db *api*) '{:find [f]
                                   :where [[i :name "Ivan"]
                                           [i :friends f]]}))))

  (t/testing "can find based on single value"
    (t/is (= #{[:ivan]}
             (xt/q (xt/db *api*) '{:find [i]
                                   :where [[i :name "Ivan"]
                                           [i :friends :bob]]}))))

  (t/testing "join intersects values"
    (t/is (= #{[:bob]}
             (xt/q (xt/db *api*) '{:find [f]
                                   :where [[i :name "Ivan"]
                                           [i :friends f]
                                           [d :name "Dominic"]
                                           [d :friends f]]}))))

  (t/testing "clojure.core predicate filters values"
    (t/is (= #{[:bob]}
             (xt/q (xt/db *api*) '{:find [f]
                                   :where [[i :name "Ivan"]
                                           [i :friends f]
                                           [(= f :bob)]]})))

    (t/is (= #{[:dominic]}
             (xt/q (xt/db *api*) '{:find [f]
                                   :where [[i :name "Ivan"]
                                           [i :friends f]
                                           [(not= f :bob)]]}))))

  (t/testing "unification filters values"
    (t/is (= #{[:bob]}
             (xt/q (xt/db *api*) '{:find [f]
                                   :where [[i :name "Ivan"]
                                           [i :friends f]
                                           [(== f :bob)]]})))

    (t/is (= #{[:bob] [:dominic]}
             (xt/q (xt/db *api*) '{:find [f]
                                   :where [[i :name "Ivan"]
                                           [i :friends f]
                                           [(== f #{:bob :dominic})]]})))

    (t/is (= #{[:dominic]}
             (xt/q (xt/db *api*) '{:find [f]
                                   :where [[i :name "Ivan"]
                                           [i :friends f]
                                           [(!= f :bob)]]}))))

  (t/testing "not filters values"
    (t/is (= #{[:ivan :dominic]}
             (xt/q (xt/db *api*) '{:find [i f]
                                   :where [[i :name "Ivan"]
                                           [i :friends f]
                                           (not [(= f :bob)])]})))))

(t/deftest test-can-use-idents-as-entities
  (fix/transact! *api* (fix/people [{:xt/id :ivan :name "Ivan" :last-name "Ivanov"}
                                    {:xt/id :petr :name "Petr" :last-name "Petrov" :mentor :ivan}]))

  (t/testing "Can query by single field"
    (t/is (= #{[:petr]} (xt/q (xt/db *api*) '{:find [p]
                                              :where [[i :name "Ivan"]
                                                      [p :mentor i]]})))

    (t/testing "Other direction"
      (t/is (= #{[:petr]} (xt/q (xt/db *api*) '{:find [p]
                                                :where [[p :mentor i]
                                                        [i :name "Ivan"]]})))))

  (t/testing "Can query by known entity"
    (t/is (= #{["Ivan"]} (xt/q (xt/db *api*) '{:find [n]
                                               :where [[:ivan :name n]]})))

    (t/is (= #{["Ivan"]} (xt/q (xt/db *api*) '{:find [n]
                                               :where [[:petr :mentor i]
                                                       [i :name n]]})))

    (t/is (= #{["Ivan"]} (xt/q (xt/db *api*) '{:find [n]
                                               :where [[p :name "Petr"]
                                                       [p :mentor i]
                                                       [i :name n]]})))

    (t/is (= #{["Ivan"]} (xt/q (xt/db *api*) '{:find [n]
                                               :where [[p :mentor i]
                                                       [i :name n]]})))

    (t/is (= #{[:ivan]} (xt/q (xt/db *api*) '{:find [i]
                                              :where [[p :name "Petr"]
                                                      [p :mentor i]]})))

    (t/testing "Other direction"
      (t/is (= #{["Ivan"]} (xt/q (xt/db *api*) '{:find [n]
                                                 :where [[i :name n]
                                                         [:petr :mentor i]]}))))
    (t/testing "No matches"
      (t/is (= #{} (xt/q (xt/db *api*) '{:find [n]
                                         :where [[:ivan :mentor x]
                                                 [x :name n]]})))

      (t/testing "Other direction"
        (t/is (= #{} (xt/q (xt/db *api*) '{:find [n]
                                           :where [[x :name n]
                                                   [:ivan :mentor x]]})))))

    (t/testing "Literal entity and literal value"
      (t/is (= #{[true]} (xt/q (xt/db *api*) '{:find [found?]
                                               :where [[:ivan :name "Ivan"]
                                                       [(identity true) found?]]})))

      (t/is (= #{} (xt/q (xt/db *api*) '{:find [found?]
                                         :where [[:ivan :name "Bob"]
                                                 [(identity true) found?]]}))))))

(t/deftest test-join-and-seek-bugs
  (fix/transact! *api* (fix/people [{:xt/id :ivan :name "Ivan" :last-name "Ivanov"}
                                    {:xt/id :petr :name "Petr" :last-name "Petrov" :mentor :ivan}]))

  (t/testing "index seek bugs"
    (t/is (= #{} (xt/q (xt/db *api*) '{:find [i]
                                       :where [[p :name "Petrov"]
                                               [p :mentor i]]})))


    (t/is (= #{} (xt/q (xt/db *api*) '{:find [p]
                                       :where [[p :name "Pet"]]})))

    (t/is (= #{} (xt/q (xt/db *api*) '{:find [p]
                                       :where [[p :name "I"]]})))

    (t/is (= #{} (xt/q (xt/db *api*) '{:find [p]
                                       :where [[p :name "Petrov"]]})))

    (t/is (= #{} (xt/q (xt/db *api*) '{:find [i]
                                       :where [[p :name "Pet"]
                                               [p :mentor i]]})))

    (t/is (= #{} (xt/q (xt/db *api*) '{:find [i]
                                       :where [[p :name "Petrov"]
                                               [p :mentor i]]}))))

  (t/testing "join bugs"
    (t/is (= #{} (xt/q (xt/db *api*) '{:find [p]
                                       :where [[p :name "Ivan"]
                                               [p :mentor i]]})))

    (t/is (= #{} (xt/q (xt/db *api*) '{:find [i]
                                       :where [[p :name "Ivan"]
                                               [p :mentor i]]})))))

(t/deftest test-queries-with-variables-only
  (fix/transact! *api* (fix/people [{:xt/id :ivan :name "Ivan" :mentor :petr}
                                    {:xt/id :petr :name "Petr" :mentor :oleg}
                                    {:xt/id :oleg :name "Oleg" :mentor :ivan}]))

  (t/is (= #{[:oleg "Oleg" :petr "Petr"]
             [:ivan "Ivan" :oleg "Oleg"]
             [:petr "Petr" :ivan "Ivan"]} (xt/q (xt/db *api*) '{:find [e1 n1 e2 n2]
                                                                :where [[e1 :name n1]
                                                                        [e2 :mentor e1]
                                                                        [e2 :name n2]]}))))

(t/deftest test-index-unification
  (fix/transact! *api* (fix/people [{:xt/id :ivan :name "Ivan" :last-name "Ivanov"}
                                    {:xt/id :petr :name "Petr" :last-name "Petrov" :mentor :ivan}]))

  (t/is (= #{[:petr :petr]} (xt/q (xt/db *api*) '{:find [p1 p2]
                                                  :where [[p1 :name "Petr"]
                                                          [p2 :mentor i]
                                                          [(== p1 p2)]]})))

  (t/is (= #{} (xt/q (xt/db *api*) '{:find [p1 p2]
                                     :where [[p1 :name "Petr"]
                                             [p2 :mentor i]
                                             [(== p1 i)]]})))

  (t/is (= #{} (xt/q (xt/db *api*) '{:find [p1 p2]
                                     :where [[p1 :name "Petr"]
                                             [p2 :mentor i]
                                             [(== p1 i)]]})))

  (t/is (= #{[:petr :petr]} (xt/q (xt/db *api*) '{:find [p1 p2]
                                                  :where [[p1 :name "Petr"]
                                                          [p2 :mentor i]
                                                          [(!= p1 i)]]})))

  (t/is (= #{} (xt/q (xt/db *api*) '{:find [p1 p2]
                                     :where [[p1 :name "Petr"]
                                             [p2 :mentor i]
                                             [(!= p1 p2)]]})))


  (t/is (= #{} (xt/q (xt/db *api*) '{:find [p]
                                     :where [[p :name "Petr"]
                                             [p :mentor i]
                                             [(== p i)]]})))

  (t/testing "unify with literal"
    (t/is (= #{[:petr]} (xt/q (xt/db *api*) '{:find [p]
                                              :where [[p :name n]
                                                      [(== n "Petr")]]})))

    (t/is (= #{[:ivan]} (xt/q (xt/db *api*) '{:find [p]
                                              :where [[p :name n]
                                                      [(!= n "Petr")]]}))))

  (t/testing "unify with entity"
    (t/is (= #{["Petr"]} (xt/q (xt/db *api*) '{:find [n]
                                               :where [[p :name n]
                                                       [(== p :petr)]]})))

    (t/is (= #{["Ivan"]} (xt/q (xt/db *api*) '{:find [n]
                                               :where [[i :name n]
                                                       [(!= i :petr)]]}))))

  (t/testing "multiple literals in set"
    (t/is (= #{[:petr] [:ivan]} (xt/q (xt/db *api*) '{:find [p]
                                                      :where [[p :name n]
                                                              [(== n #{"Petr" "Ivan"})]]})))

    (t/is (= #{[:ivan]} (xt/q (xt/db *api*) '{:find [p]
                                              :where [[p :name n]
                                                      [(!= n #{"Petr"})]]})))

    (t/is (= #{} (xt/q (xt/db *api*) '{:find [p]
                                       :where [[p :name n]
                                               [(== n #{})]]})))

    (t/is (= #{[:petr] [:ivan]} (xt/q (xt/db *api*) '{:find [p]
                                                      :where [[p :name n]
                                                              [(!= n #{})]]})))))

(t/deftest test-get-attr
  (fix/transact! *api* (fix/people [{:xt/id :ivan :name "Ivan" :age 21 :friends #{:petr :oleg}}]))
  (t/testing "existing attribute"
    (t/is (= #{[:ivan [21]]}
             (xt/q (xt/db *api*) '{:find [e age]
                                   :where [[e :name "Ivan"]
                                           [(get-attr e :age) age]]})))

    (t/is (= #{[:ivan 21]}
             (xt/q (xt/db *api*) '{:find [e age]
                                   :where [[e :name "Ivan"]
                                           [(get-attr e :age) [age ...]]]})))

    (t/is (empty? (xt/q (xt/db *api*) '{:find [e age]
                                        :where [[e :name "Oleg"]
                                                [(get-attr e :age) [age ...]]]})))

    (t/is (= #{}
             (xt/q (xt/db *api*) '{:find [e age]
                                   :where [[e :name "Ivan"]
                                           [(get-attr e :age) [age ...]]
                                           [(> age 30)]]}))))

  (t/testing "many valued attribute"
    (t/is (= #{[:ivan :petr]
               [:ivan :oleg]}
             (xt/q (xt/db *api*) '{:find [e friend]
                                   :where [[e :name "Ivan"]
                                           [(get-attr e :friends) [friend ...]]]}))))

  (t/testing "unknown attribute"
    (t/is (empty? (xt/q (xt/db *api*) '{:find [e email]
                                        :where [[e :name "Ivan"]
                                                [(get-attr e :email) [email ...]]]})))

    (t/testing "unknown attribute as scalar"
      (t/is (= #{[:ivan nil]} (xt/q (xt/db *api*) '{:find [e email]
                                                    :where [[e :name "Ivan"]
                                                            [(get-attr e :email) email]]})))))

  (t/testing "optional found attribute"
    (t/is (= #{[:ivan 21]}
             (xt/q (xt/db *api*) '{:find [e age]
                                   :where [[e :name "Ivan"]
                                           [(get-attr e :age 0) [age ...]]]}))))

  (t/testing "use as predicate"
    (t/is (= #{[:ivan]}
             (xt/q (xt/db *api*) '{:find [e]
                                   :where [[e :name "Ivan"]
                                           [(get-attr e :name)]]})))

    (t/is (empty?
           (xt/q (xt/db *api*) '{:find [e]
                                 :where [[e :name "Ivan"]
                                         [(get-attr e :email)]]}))))

  (t/testing "optional not found attribute"
    (t/is (= #{[:ivan ["N/A"]]}
             (xt/q (xt/db *api*) '{:find [e email]
                                   :where [[e :name "Ivan"]
                                           [(get-attr e :email "N/A") email]]})))

    (t/is (= #{[:ivan "N/A"]}
             (xt/q (xt/db *api*) '{:find [e email]
                                   :where [[e :name "Ivan"]
                                           [(get-attr e :email "N/A") [email ...]]]})))

    (t/is (= #{[:ivan nil]}
             (xt/q (xt/db *api*) '{:find [e email]
                                   :where [[e :name "Ivan"]
                                           [(get-attr e :email nil) [email ...]]]})))))

(t/deftest test-get-valid-time
  (let [now #inst "2000"
        later1 #inst "2050"
        later2 #inst "2100"
        later3 #inst "2150"
        tx0 (fix/submit+await-tx [[::xt/put {:xt/id :foo, :v 0} now]
                                  [::xt/put {:xt/id :bar, :v 0} now later2]])]

    (t/testing "tx0"
      (t/testing "existing start date at the edge"
        (t/is (= #{[:foo now nil]}
                 (xt/q (xt/db *api* now)
                       '{:find [?e ?start-time ?end-time]
                         :where [[?e :xt/id :foo]
                                 [(get-start-valid-time ?e) ?start-time]
                                 [(get-end-valid-time ?e) ?end-time]]}))))

      (t/testing "existing end date at the edge"
        (t/is (= #{[:bar later2]}
                 (xt/q (xt/db *api* now)
                       '{:find [?e ?end-time]
                         :where [[?e :xt/id :bar]
                                 [(get-end-valid-time ?e) ?end-time]]}))))

      (t/testing "existing end date"
        (t/is (= #{[:bar now later2]}
                 (xt/q (xt/db *api* #inst "2025") ;; in-between
                       '{:find [?e ?start-time ?end-time]
                         :where [[?e :xt/id :bar]
                                 [(get-start-valid-time ?e) ?start-time]
                                 [(get-end-valid-time ?e) ?end-time]]}))))

      (t/testing "existing and non-exisiting end dates"
        (t/is (= #{[:foo now nil] [:bar now later2]}
                 (xt/q (xt/db *api* now)
                       '{:find [?e ?start-time ?end-time]
                         :where [[?e :xt/id]
                                 [(get-start-valid-time ?e) ?start-time]
                                 [(get-end-valid-time ?e) ?end-time]]}))))

      (t/testing "non-existing entity"
        (t/is (= #{}
                 (xt/q (xt/db *api* now)
                       '{:find [?e ?start-time ?end-time]
                         :where [[?e :xt/id :non-existing]
                                 [(get-start-valid-time ?e) ?start-time]
                                 [(get-end-valid-time ?e) ?end-time]]}))))

      (t/testing "testing default value"
        (t/is (= #{[:foo :the/default]}
                 (xt/q (xt/db *api* now)
                       '{:find [?e ?end-time]
                         :where [[?e :xt/id :foo]
                                 [(get-end-valid-time ?e :the/default) ?end-time]]})))))

    (fix/submit+await-tx [[::xt/put {:xt/id :foo, :v 1} later2 later3]
                          [::xt/put {:xt/id :bar, :v 1} later1 later2]])
    (xt/sync *api*)

    (t/testing "entity change"
      (t/is (= #{[:bar now later1]}
               (xt/q (xt/db *api* now)
                     '{:find [?e ?start-time ?end-time]
                       :where [[?e :xt/id :bar]
                               [(get-start-valid-time ?e) ?start-time]
                               [(get-end-valid-time ?e) ?end-time]]})))

      (t/is (= #{[:bar later1 later2]}
               (xt/q (xt/db *api* later1)
                     '{:find [?e ?start-time ?end-time]
                       :where [[?e :xt/id :bar]
                               [(get-start-valid-time ?e) ?start-time]
                               [(get-end-valid-time ?e) ?end-time]]})))

      (t/is (= #{[:bar later1 later2]}
               (xt/q (xt/db *api* #inst "2075")
                     '{:find [?e ?start-time ?end-time]
                       :where [[?e :xt/id :bar]
                               [(get-start-valid-time ?e) ?start-time]
                               [(get-end-valid-time ?e) ?end-time]]}))))

    (t/testing "back in tx-time"
      (t/is (= #{[:foo 0 now nil]}
               (xt/q (xt/db *api* {::xt/tx tx0})
                     '{:find [?e ?v ?start-time ?end-time]
                       :where [[?e :xt/id :foo]
                               [?e :v ?v]
                               [(get-start-valid-time ?e) ?start-time]
                               [(get-end-valid-time ?e) ?end-time]]})))

      (t/is (= #{[:foo 0 now nil]}
               (xt/q (xt/db *api* {::xt/valid-time #inst "2175", ::xt/tx tx0})
                     '{:find [?e ?v ?start-time ?end-time]
                       :where [[?e :xt/id :foo]
                               [?e :v ?v]
                               [(get-start-valid-time ?e) ?start-time]
                               [(get-end-valid-time ?e) ?end-time]]})))

      (let [tx0 (fix/submit+await-tx [[::xt/put {:xt/id :baz, :v 0} later1 later2]])
            tx1 (fix/submit+await-tx [[::xt/put {:xt/id :baz, :v 1} now later3]])]
        (t/is (= #{[:baz 0 later1 later2]}
                 (xt/q (xt/db *api* {::xt/valid-time #inst "2075", ::xt/tx tx0})
                       '{:find [?e ?v ?start-time ?end-time]
                         :where [[?e :xt/id :baz]
                                 [?e :v ?v]
                                 [(get-start-valid-time ?e) ?start-time]
                                 [(get-end-valid-time ?e) ?end-time]]})))

        (t/is (= #{[:baz 1 now later3]}
                 (xt/q (xt/db *api* {::xt/valid-time #inst "2025", ::xt/tx tx1})
                       '{:find [?e ?v ?start-time ?end-time]
                         :where [[?e :xt/id :baz]
                                 [?e :v ?v]
                                 [(get-start-valid-time ?e) ?start-time]
                                 [(get-end-valid-time ?e) ?end-time]]})))))

    (fix/submit+await-tx [[::xt/put {:xt/id :toto, :v 0} now later1]])

    (t/testing "unification"
      (t/is (= #{[:bar :toto] [:toto :bar]}
               (xt/q (xt/db *api* now)
                     '{:find [?e1 ?e2]
                       :where [[?e1 :xt/id]
                               [?e2 :xt/id]
                               [(!= ?e1 ?e2)]
                               [(get-end-valid-time ?e1) ?end-time]
                               [(get-end-valid-time ?e2) ?end-time]]}))))))

(t/deftest test-byte-array-values
  (fix/transact! *api* (fix/people [{:xt/id :ivan :name "Ivan" :photo (byte-array [0 1 2])}
                                    {:xt/id :petr :name "Petr" :photo (byte-array [3 4 5])}
                                    {:xt/id :oleg :name "Oleg" :photo (byte-array [0 1 2])}]))

  (t/is (Arrays/equals (byte-array [0 1 2])
                       ^bytes (ffirst
                               (xt/q (xt/db *api*) '{:find [photo]
                                                     :where [[:ivan :photo photo]]}))))

  (t/testing "joins"
    (t/is (= #{[:ivan]
               [:oleg]}
             (xt/q (xt/db *api*)
                   '{:find [e]
                     :in [$ photo]
                     :where [[e :photo photo]]}
                   (byte-array [0 1 2]))))

    (t/is (= #{[:oleg]}
             (xt/q (xt/db *api*)
                   '{:find [e]
                     :where [[:ivan :photo photo]
                             [e :name "Oleg"]
                             [e :photo photo]]})))))

(t/deftest test-multiple-values-literals
  (fix/transact! *api* (fix/people [{:xt/id :ivan :name "Ivan" :age 21 :friends #{:petr :oleg}}
                                    {:xt/id :petr :name "Petr" :age 30 :friends #{:ivan}}]))
  (t/testing "set"
    (t/is (empty? (xt/q (xt/db *api*) '{:find [e]
                                        :where [[e :name #{}]]})))

    (t/is (empty? (xt/q (xt/db *api*) '{:find [e]
                                        :where [[e :name #{"Oleg"}]]})))

    (t/is (= #{[:ivan]}
             (xt/q (xt/db *api*) '{:find [e]
                                   :where [[e :name #{"Ivan" "Oleg"}]]})))

    (t/is (= #{[:ivan] [:petr]}
             (xt/q (xt/db *api*) '{:find [e]
                                   :where [[e :name #{"Ivan" "Petr"}]]})))

    (t/is (= #{[:ivan]}
             (xt/q (xt/db *api*) '{:find [e]
                                   :where [[e :friends #{:petr :oleg}]]})))

    (t/is (= #{[:ivan] [:petr]}
             (xt/q (xt/db *api*) '{:find [e]
                                   :where [[e :friends #{:petr :ivan}]]}))))

  (t/testing "entity position"
    (t/is (empty? (xt/q (xt/db *api*) '{:find [name]
                                        :where [[#{} :name name]]})))

    (t/is (empty? (xt/q (xt/db *api*) '{:find [name]
                                        :where [[#{:oleg} :name name]]})))

    (t/is (= #{["Ivan"]}
             (xt/q (xt/db *api*) '{:find [name]
                                   :where [[#{:ivan :oleg} :name name]]})))

    (t/is (= #{["Ivan"] ["Petr"]}
             (xt/q (xt/db *api*) '{:find [name]
                                   :where [[#{:ivan :petr} :name name]]}))))

  (t/testing "vectors are not supported"
    (t/is (thrown-with-msg? IllegalArgumentException
                            #"Query didn't match expected structure"
                            (xt/q (xt/db *api*) '{:find [e]
                                                  :where [[e :name [:ivan]]]})))))

(t/deftest test-collection-returns
  (t/testing "vectors"
    (t/is (= #{[1]
               [2]}
             (xt/q (xt/db *api*)
                   '{:find [x]
                     :where [[(vector 1 2) [x ...]]]})))

    (t/is (= #{}
             (xt/q (xt/db *api*)
                   '{:find [x]
                     :where [[(vector) [x ...]]]}))))

  (t/testing "sets"
    (t/is (= #{[1]
               [2]}
             (xt/q (xt/db *api*)
                   '{:find [x]
                     :where [[(sorted-set 1 2) [x ...]]]})))



    (t/is (= #{}
             (xt/q (xt/db *api*)
                   '{:find [x]
                     :where [[(sorted-set) [x ...]]]}))))

  (t/testing "maps"
    (t/is (= #{[[1 2]]}
             (xt/q (xt/db *api*)
                   '{:find [x]
                     :where [[(hash-map 1 2) [x ...]]]})))

    (t/is (= #{}
             (xt/q (xt/db *api*)
                   '{:find [x]
                     :where [[(hash-map) [x ...]]]})))))

(t/deftest test-tuple-returns
  (t/is (= #{[1 2]}
           (xt/q (xt/db *api*) '{:find [x y]
                                 :where [[(identity [1 2]) [x y]]]})))

  (t/is (= #{[2]}
           (xt/q (xt/db *api*) '{:find [x]
                                 :where [[(identity [1 2]) [_ x]]]})))

  (t/is (= #{[1]}
           (xt/q (xt/db *api*) '{:find [x]
                                 :where [[(identity [1 2]) [x]]]})))

  (t/testing "can bind excess vars to nil"
    (t/is (= #{[nil nil]} (xt/q (xt/db *api*) '{:find [x y]
                                                :where [[(identity []) [x y]]]})))))

(t/deftest test-relation-returns
  (t/is (= #{[1 2]
             [3 4]}
           (xt/q (xt/db *api*) '{:find [x y]
                                 :where [[(identity #{[1 2] [3 4]}) [[x y]]]]})))

  (t/is (empty? (xt/q (xt/db *api*) '{:find [x y]
                                      :where [[(identity #{}) [[x y]]]]})))

  (t/testing "distinct tuples"
    (t/is (= #{[1 2]}
             (xt/q (xt/db *api*) '{:find [x y]
                                   :where [[(identity [[1 2] [1 2]]) [[x y]]]]}))))

  (t/testing "tuple var bindings need to be distinct"
    (t/is (thrown-with-msg?
           IllegalArgumentException
           #"Return variables not distinct"
           (xt/q (xt/db *api*) '{:find [x]
                                 :where [[(identity #{[1 2] [3 4]}) [[x x]]]]}))))

  (t/testing "can bind sub tuple"
    (t/is (= #{[1]
               [3]}
             (xt/q (xt/db *api*) '{:find [x]
                                   :where [[(identity #{[1 2] [3 4]}) [[x]]]]})))

    (t/is (= #{[2]
               [4]}
             (xt/q (xt/db *api*) '{:find [x]
                                   :where [[(identity #{[1 2] [3 4]}) [[_ x]]]]})))

    (t/is (= #{[4]}
             (xt/q (xt/db *api*) '{:find [x]
                                   :where [[(identity #{[1 2] [3 4]}) [[_ x]]]
                                           [(identity #{[4 2]}) [[x _]]]]})))

    (t/testing "can bind excess vars to nil"
      (t/is (= #{[nil]}
               (xt/q (xt/db *api*) '{:find [x]
                                     :where [[(identity #{[1] [3]}) [[_ x]]]]})))))

  (t/testing "can bind full tuple using collection binding"
    (t/is (= #{[[1 2]]
               [[3 4]]}
             (xt/q (xt/db *api*) '{:find [x]
                                   :where [[(identity #{[1 2] [3 4]}) [x ...]]]})))))

(t/deftest test-sub-queries
  (t/is (= #{[4]}
           (xt/q (xt/db *api*) '{:find [x]
                                 :where [[(q {:find [y]
                                              :where [[(identity 2) x]
                                                      [(+ x 2) y]]})
                                          [[x]]]]})))

  (t/testing "can bind resulting relation as scalar"
    (t/is (= #{[[[4]]]}
             (xt/q (xt/db *api*) '{:find [x]
                                   :where [[(q {:find [y]
                                                :where [[(identity 2) x]
                                                        [(+ x 2) y]]})
                                            x]]})))

    (t/testing "can bind empty resulting relation as scalar"
      (t/is (= #{[nil]}
               (xt/q (xt/db *api*) '{:find [x]
                                     :where [[(q {:find [x]
                                                  :where [[(vector) [x ...]]]})
                                              x]]})))))

  (t/testing "can bind resulting relation as tuple"
    (t/is (= #{[[4]]}
             (xt/q (xt/db *api*) '{:find [x]
                                   :where [[(q {:find [y]
                                                :where [[(identity 2) x]
                                                        [(+ x 2) y]]})
                                            [x]]]})))

    (t/testing "can bind empty resulting relation as tuple"
      (t/is (= #{[nil]}
               (xt/q (xt/db *api*) '{:find [x]
                                     :where [[(q {:find [x]
                                                  :where [[(vector) [x ...]]]})
                                              [x]]]})))))

  (t/is (empty?
         (xt/q (xt/db *api*) '{:find [x]
                               :where [[(q {:find [y]
                                            :where [[(identity 2) x]
                                                    [(+ x 2) y]
                                                    [(odd? y)]]})
                                        [[x]]]]})))

  (t/testing "can provide arguments"
    (t/is (= #{[1 2 3]}
             (xt/q (xt/db *api*) '{:find [x y z]
                                   :where [[(q {:find [x y z]
                                                :in [$ x]
                                                :where [[(identity 2) y]
                                                        [(+ x y) z]]} 1)
                                            [[x y z]]]]})))

    (t/is (= #{[1 3 4]}
             (xt/q (xt/db *api*) '{:find [x y z]
                                   :where [[(identity 1) x]
                                           [(q {:find [z]
                                                :in [$ x]
                                                :where [[(+ x 2) z]]} x)
                                            [[y]]]
                                           [(+ x y) z]]})))

    (t/is (= #{[1]}
             (xt/q (xt/db *api*) '{:find [x]
                                   :where [[(q {:find [y]
                                                :in [$ x]
                                                :where [[(identity x) y]]} 1)
                                            [[x]]]]})))

    (t/is (= #{[1 3 4]}
             (xt/q (xt/db *api*) '{:find [x y z]
                                   :where [[(identity 1) x]
                                           [(q {:find [z]
                                                :in [$ x]
                                                :where [[(+ x 2) z]]} x)
                                            [[y]]]
                                           [(+ x y) z]]})))

    (t/testing "can handle quoted sub query"
      (t/is (= #{[2]}
               (xt/q (xt/db *api*) '{:find [x]
                                     :where [[(q '{:find [y]
                                                   :where [[(identity 2) y]]})
                                              [[x]]]]}))))

    (t/testing "can handle vector sub queries"
      (t/is (= #{[2]}
               (xt/q (xt/db *api*) '{:find [x]
                                     :where [[(q [:find y
                                                  :where [(identity 2) y]])
                                              [[x]]]]}))))

    (t/testing "can handle string sub queries"
      (t/is (= #{[2]}
               (xt/q (xt/db *api*) '{:find [x]
                                     :where [[(q "[:find y
                                                     :where [(identity 2) y]]")
                                              [[x]]]]})))))

  (t/testing "can inherit rules from parent query"
    (t/is (empty?
           (xt/q (xt/db *api*) '{:find [x]
                                 :where [[(q {:find [y]
                                              :where [[(identity 2) x]
                                                      [(+ x 2) y]
                                                      (is-odd? y)]})
                                          [[x]]]]
                                 :rules [[(is-odd? x)
                                          [(odd? x)]]]}))))

  (t/testing "can use as predicate"
    (t/is (= #{[2]}
             (xt/q (xt/db *api*) '{:find [x]
                                   :where [[(identity 2) x]
                                           [(q {:find [x]
                                                :in [$ x]
                                                :where [[(even? x)]]}
                                               x)]]})))

    (t/is (empty? (xt/q (xt/db *api*) '{:find [x]
                                        :where [[(identity 2) x]
                                                [(q {:find [y]
                                                     :in [$ y]
                                                     :where [[(odd? y)]]}
                                                    x)]]})))

    (t/is (= #{[2]}
             (xt/q (xt/db *api*) '{:find [x]
                                   :where [[(identity 2) x]
                                           (not [(q {:find [y]
                                                     :in [$ y]
                                                     :where [[(odd? y)]]}
                                                    x)])]})))))

(t/deftest test-simple-numeric-range-search
  (t/is (= '[[:triple {:e i, :a :age, :v age}]
             [:range [[:sym-val {:op <, :sym age, :val 20}]]]]
           (s/conform :xtdb.query/where '[[i :age age]
                                          [(< age 20)]])))

  (fix/transact! *api* (fix/people [{:xt/id :ivan :name "Ivan" :last-name "Ivanov" :age 21}
                                    {:xt/id :petr :name "Petr" :last-name "Petrov" :age 18}]))

  (t/testing "Min search case"
    (t/is (= #{[:ivan]} (xt/q (xt/db *api*) '{:find [i]
                                              :where [[i :age age]
                                                      [(> age 20)]]})))
    (t/is (= #{} (xt/q (xt/db *api*) '{:find [i]
                                       :where [[i :age age]
                                               [(> age 21)]]})))

    (t/is (= #{[:ivan]} (xt/q (xt/db *api*) '{:find [i]
                                              :where [[i :age age]
                                                      [(>= age 21)]]}))))

  (t/testing "Max search case"
    (t/is (= #{[:petr]} (xt/q (xt/db *api*) '{:find [i]
                                              :where [[i :age age]
                                                      [(< age 20)]]})))
    (t/is (= #{} (xt/q (xt/db *api*) '{:find [i]
                                       :where [[i :age age]
                                               [(< age 18)]]})))
    (t/is (= #{[:petr]} (xt/q (xt/db *api*) '{:find [i]
                                              :where [[i :age age]
                                                      [(<= age 18)]]})))
    (t/is (= #{[18]} (xt/q (xt/db *api*) '{:find [age]
                                           :where [[:petr :age age]
                                                   [(<= age 18)]]}))))

  (t/testing "Reverse symbol and value"
    (t/is (= #{[:ivan]} (xt/q (xt/db *api*) '{:find [i]
                                              :where [[i :age age]
                                                      [(<= 20 age)]]})))
    (t/is (= #{[:petr]} (xt/q (xt/db *api*) '{:find [i]
                                              :where [[i :age age]
                                                      [(>= 20 age)]]})))

    (t/testing "Range inversion edge cases, #612"
      (t/is (= #{[:ivan]} (xt/q (xt/db *api*) '{:find [i]
                                                :where [[i :age age]
                                                        [(<= 21 age)]]})))
      (t/is (= #{} (xt/q (xt/db *api*) '{:find [i]
                                         :where [[i :age age]
                                                 [(> 18 age)]]}))))))

(t/deftest test-mutiple-values
  (fix/transact! *api* (fix/people [{:xt/id :ivan :name "Ivan"}
                                    {:xt/id :oleg :name "Oleg"}
                                    {:xt/id :petr :name "Petr" :follows #{:ivan :oleg}}]))

  (t/testing "One way"
    (t/is (= #{[:ivan] [:oleg]} (xt/q (xt/db *api*) '{:find [x]
                                                      :where [[i :name "Petr"]
                                                              [i :follows x]]}))))

  (t/testing "The other way"
    (t/is (= #{[:petr]} (xt/q (xt/db *api*) '{:find [i]
                                              :where [[x :name "Ivan"]
                                                      [i :follows x]]})))))

(t/deftest test-sanitise-join
  (fix/transact! *api* (fix/people [{:xt/id :ivan :name "Ivan" :last-name "Ivanov"}]))
  (t/testing "Can query by single field"
    (t/is (= #{[:ivan]} (xt/q (xt/db *api*) '{:find [e2]
                                              :where [[e :last-name "Ivanov"]
                                                      [e :last-name name1]
                                                      [e2 :last-name name1]]})))))

(t/deftest test-basic-rules
  (t/is (= '[[:triple {:e i, :a :age, :v age}]
             [:rule {:name over-twenty-one?, :args [age]}]]
           (s/conform :xtdb.query/where '[[i :age age]
                                          (over-twenty-one? age)])))

  (t/is (= [{:head '{:name over-twenty-one?, :args [:just-args [age]]},
             :body '[[:range [[:sym-val {:op >=, :sym age, :val 21}]]]]}
            '{:head {:name over-twenty-one?, :args [:just-args [age]]},
              :body [[:not [[:range [[:sym-val {:op <, :sym age, :val 21}]]]]]]}]
           (s/conform :xtdb.query/rules '[[(over-twenty-one? age)
                                           [(>= age 21)]]
                                          [(over-twenty-one? age)
                                           (not [(< age 21)])]])))

  (fix/transact! *api* (fix/people [{:xt/id :ivan :name "Ivan" :last-name "Ivanov" :age 21}
                                    {:xt/id :petr :name "Petr" :last-name "Petrov" :age 18}]))

  (t/testing "without rule"
    (t/is (= #{[:ivan]} (xt/q (xt/db *api*) '{:find [i]
                                              :where [[i :age age]
                                                      [(>= age 21)]]}))))

  (t/testing "rule using same variable name as body"
    (t/is (= #{[:ivan]} (xt/q (xt/db *api*) '{:find [i]
                                              :where [[i :age age]
                                                      (over-twenty-one? age)]
                                              :rules [[(over-twenty-one? age)
                                                       [(>= age 21)]]]}))))

  (t/testing "rules directly on arguments"
    (t/is (= #{[21]} (xt/q (xt/db *api*) '{:find [age]
                                           :where [(over-twenty-one? age)]
                                           :args [{:age 21}]
                                           :rules [[(over-twenty-one? age)
                                                    [(>= age 21)]]]})))

    (t/is (= #{} (xt/q (xt/db *api*) '{:find [age]
                                       :where [(over-twenty-one? age)]
                                       :args [{:age 20}]
                                       :rules [[(over-twenty-one? age)
                                                [(>= age 21)]]]}))))

  (t/testing "rule using required bound args"
    (t/is (= #{[:ivan]} (xt/q (xt/db *api*) '{:find [i]
                                              :where [[i :age age]
                                                      (over-twenty-one? age)]
                                              :rules [[(over-twenty-one? [age])
                                                       [(>= age 21)]]]}))))

  (t/testing "rule using different variable name from body"
    (t/is (= #{[:ivan]} (xt/q (xt/db *api*) '{:find [i]
                                              :where [[i :age age]
                                                      (over-twenty-one? age)]
                                              :rules [[(over-twenty-one? x)
                                                       [(>= x 21)]]]}))))

  (t/testing "nested rules"
    (t/is (= #{[:ivan]} (xt/q (xt/db *api*) '{:find [i]
                                              :where [[i :age age]
                                                      (over-twenty-one? age)]
                                              :rules [[(over-twenty-one? x)
                                                       (over-twenty-one-internal? x)]
                                                      [(over-twenty-one-internal? y)
                                                       [(>= y 21)]]]}))))

  (t/testing "rule using multiple arguments"
    (t/is (= #{[:ivan]} (xt/q (xt/db *api*) '{:find [i]
                                              :where [[i :age age]
                                                      (over-age? age 21)]
                                              :rules [[(over-age? [age] required-age)
                                                       [(>= age required-age)]]]}))))

  (t/testing "rule using multiple branches"
    (t/is (= #{[:ivan]} (xt/q (xt/db *api*) '{:find [i]
                                              :where [(is-ivan-or-bob? i)]
                                              :rules [[(is-ivan-or-bob? i)
                                                       [i :name "Ivan"]
                                                       [i :last-name "Ivanov"]]
                                                      [(is-ivan-or-bob? i)
                                                       [i :name "Bob"]]]})))

    (t/is (= #{["Petr"]} (xt/q (xt/db *api*) '{:find [name]
                                               :where [[i :name name]
                                                       (not (is-ivan-or-bob? i))]
                                               :rules [[(is-ivan-or-bob? i)
                                                        [i :name "Ivan"]]
                                                       [(is-ivan-or-bob? i)
                                                        [i :name "Bob"]]]})))

    (t/is (= #{[:ivan]
               [:petr]} (xt/q (xt/db *api*) '{:find [i]
                                              :where [(is-ivan-or-petr? i)]
                                              :rules [[(is-ivan-or-petr? i)
                                                       [i :name "Ivan"]]
                                                      [(is-ivan-or-petr? i)
                                                       [i :name "Petr"]]]}))))

  (t/is (thrown-with-msg?
         IllegalArgumentException
         #"Unknown rule:"
         (xt/q (xt/db *api*) '{:find [i]
                               :where [[i :age age]
                                       (over-twenty-one? age)]})))

  (t/is (thrown-with-msg?
         IllegalArgumentException
         #"Rule invocation has wrong arity, expected: 1"
         (xt/q (xt/db *api*) '{:find [i]
                               :where [[i :age age]
                                       (over-twenty-one? i age)]
                               :rules [[(over-twenty-one? x)
                                        [(>= x 21)]]]})))

  (t/is (thrown-with-msg?
         IllegalArgumentException
         #"Rule definitions require same arity:"
         (xt/q (xt/db *api*) '{:find [i]
                               :where [[i :age age]
                                       (is-ivan-or-petr? i name)]
                               :rules [[(is-ivan-or-petr? i name)
                                        [i :name "Ivan"]]
                                       [(is-ivan-or-petr? i)
                                        [i :name "Petr"]]]}))))

;; https://github.com/xtdb/xtdb/issues/70

(t/deftest test-lookup-by-value-bug-70
  (fix/transact! *api* (fix/people (cons {:xt/id :ivan :name "Ivan" :last-name "Ivanov" :age 30}
                                         (repeat 1000 {:age 20}))))

  (let [n 10
        acceptable-limit-slowdown 0.1
        factors (->> #(let [direct-hit-ns-start (System/nanoTime)]
                        (t/is (= #{[:ivan]} (xt/q (xt/db *api*) '{:find [i]
                                                                  :where [[i :age 30]]})))
                        (let [direct-hit-ns (- (System/nanoTime) direct-hit-ns-start)
                              limited-hit-ns-start (System/nanoTime)]
                          (t/is (= 1 (count (xt/q (xt/db *api*) '{:find [i]
                                                                  :where [[i :age 20]]
                                                                  :limit 1}))))
                          (let [limited-hit-ns (- (System/nanoTime) limited-hit-ns-start)]
                            (double (/ (min direct-hit-ns limited-hit-ns)
                                       (max direct-hit-ns limited-hit-ns))))))
                     (repeatedly n))]
    (t/is (>= (/ (reduce + factors) n) acceptable-limit-slowdown))))

;; https://github.com/xtdb/xtdb/issues/348

(t/deftest test-range-join-order-bug-348
  (fix/transact! *api* (fix/people
                        (for [n (range 100)]
                          {:xt/id (keyword (str "ivan-" n))
                           :name "Ivan"
                           :name1 "Ivan"
                           :number-1 n})))

  (fix/transact! *api* (fix/people
                        (for [n (range 10000)]
                          {:xt/id (keyword (str "oleg-" n))
                           :name "Oleg"
                           :name1 "Oleg"
                           :number-2 n})))

  (let [n 10
        acceptable-limit-slowdown 0.1
        factors (->> #(let [small-set-ns-start (System/nanoTime)]
                        (t/is (= #{[:ivan-50]}
                                 (xt/q (xt/db *api*) '{:find [e]
                                                       :where [[e :number-1 a]
                                                               [e :name n]
                                                               [(<= a 50)]
                                                               [(>= a 50)]]})))
                        (let [small-set-ns (- (System/nanoTime) small-set-ns-start)
                              large-set-ns-start (System/nanoTime)]
                          (t/is (= #{[:oleg-5000]}
                                   (xt/q (xt/db *api*) '{:find [e]
                                                         :where [[e :number-2 a]
                                                                 [e :name n]
                                                                 [(<= a 5000)]
                                                                 [(>= a 5000)]]})))
                          (let [large-set-ns (- (System/nanoTime) large-set-ns-start)]
                            (double (/ (min small-set-ns large-set-ns)
                                       (max small-set-ns large-set-ns))))))
                     (repeatedly n))]
    (t/is (>= (/ (reduce + factors) n) acceptable-limit-slowdown))))

;; TODO: validate this test.
(t/deftest test-range-args-performance-906
  (fix/transact! *api* (fix/people
                        (for [n (range 10000)]
                          {:xt/id (keyword (str "oleg-" n))
                           :name "Oleg"
                           :number n})))

  (let [acceptable-slowdown-factor 2
        literal-ns-start (System/nanoTime)]
    (t/is (= #{[:oleg-9999]}
             (xt/q (xt/db *api*) '{:find [e]
                                   :where [[e :number a]
                                           [e :name n]
                                           [(>= a 9999)]]})))
    (let [literal-ns (- (System/nanoTime) literal-ns-start)
          args-ns-start (System/nanoTime)]
      (t/is (= #{[:oleg-9999]}
               (xt/q (xt/db *api*) '{:find [e]
                                     :where [[e :number a]
                                             [e :name n]
                                             [(>= a b)]]
                                     :args [{:b 9999}]})))
      (let [args-ns (- (System/nanoTime) args-ns-start)]
        (t/is (< (double (/ args-ns literal-ns)) acceptable-slowdown-factor)
              (pr-str args-ns " " literal-ns))))))

(t/deftest test-or-range-vars-bug-949
  (fix/transact! *api* (fix/people [{:xt/id :ivan :name "Ivan" :age 30}]))
  (t/is (= #{[:ivan "Ivan"]}
           (xt/q (xt/db *api*)
                 '{:find [e name]
                   :where [[e :name name]
                           [(get-attr e :age) age]
                           (or [(= x y)])
                           [(str age) x]
                           [(str age) y]]}))))

;; https://github.com/xtdb/xtdb/issues/71

(t/deftest test-query-limits-bug-71
  (dotimes [i 10]
    (fix/transact! *api* (fix/people [{:name "Ivan" :last-name "Ivanov"}
                                      {:name "Petr" :last-name "Petrov"}
                                      {:name "Petr" :last-name "Ivanov"}]))

    (t/is (= 2 (count (xt/q (xt/db *api*) '{:find [l]
                                            :where [[_ :last-name l]]
                                            :limit 2}))))))

;; https://github.com/xtdb/xtdb/issues/93

(t/deftest test-self-join-bug-93
  (fix/transact! *api* (fix/people [{:xt/id :ivan :name "Ivan" :friend :ivan :boss :petr}
                                    {:xt/id :petr :name "Petr"}]))

  (t/is (= #{[:petr]} (xt/q (xt/db *api*)
                            '{:find [b]
                              :where [[e :friend e]
                                      [e :boss b]]}))))

(t/deftest test-or-bug-146
  (fix/transact! *api* (fix/people [{:xt/id :ivan :name "Ivan" :extra "Petr" :age 20}
                                    {:xt/id :oleg :name "Oleg" :extra #inst "1980" :age 30}
                                    {:xt/id :petr :name "Petr" :age 40}]))

  ;; This wasn't the bug, but a useful test, lead to fixes in SPARQL
  ;; translator, and an example on how to use this.
  (t/testing "Or with non existing attribute in one leg and different types "
    (t/is (= #{["Ivan" "Petr" 20 :ivan]
               ["Oleg" #inst "1980" 30 :oleg]
               ["Petr" :none 40 :petr]}
             (xt/q (xt/db *api*) '{:find [n x a e]
                                   :where [[e :name n]
                                           [e :age a]
                                           [e :xt/id e]
                                           (or-join [e x]
                                                    [e :extra x]
                                                    (and [(identity :none) x]
                                                         (not [e :extra])))]})))))

(t/deftest test-arguments-bug-247
  (t/is (= #{} (xt/q (xt/db *api*)
                     '{:find [?x]
                       :where [[?x :name]]
                       :args [{:?x "Clojure"}]}))))

(t/deftest test-npe-arguments-bug-314
  (t/is (= #{} (xt/q (xt/db *api*)
                     '{:find [e]
                       :where
                       [[e :xt/id _]]
                       :args [{}]}))))

;; NOTE: Range constraints only apply when one of the arguments to the
;; constraint is a literal value. So when using :args to supply it, it
;; will always be a predicate which hence cannot jump directly to the
;; expected place in the index.
(t/deftest test-optimise-range-constraints-bug-505
  (let [tx (vec (for [i (range 5000)]
                  [::xt/put
                   {:xt/id i
                    :offer i}]))
        submitted-tx (doto (xt/submit-tx *api* tx)
                       (->> (xt/await-tx *api*)))])

  (let [db (xt/db *api*)
        ;; Much higher than needed, but catches the bug without flakiness.
        range-slowdown-factor 100
        entity-time (let [start-time (System/nanoTime)]
                      (t/testing "entity id lookup"
                        (t/is (= 250 (ffirst (xt/q db '{:find [e]
                                                        :where [[250 :xt/id e]]})))))
                      (- (System/nanoTime) start-time))
        base-query '{:find [e]
                     :where [[e :offer i]
                             [(test-pred i test-val)]]
                     :limit 1}
        inputs '[[[[2]] = 2]
                 [[[0]] < 10]
                 [[[0]] < 9223372036854775807]
                 [[] < -100]
                 [[[50]] >= 50]
                 [[[0]] <= 5]
                 [[[0]] > -100]
                 [[[0]] >= -100]]]
    (doseq [[expected p v :as input] inputs]
      (dotimes [_ 5]
        (let [q (w/postwalk
                 #(case %
                    test-pred p
                    test-val v
                    %)
                 base-query)
              start-time (System/nanoTime)
              actual (xt/q db q)
              query-time (- (System/nanoTime) start-time)]
          (t/is (= expected actual) (str input))
          (t/is (< query-time (* entity-time range-slowdown-factor)) (str input)))))))

;; NOTE: Micro-benchmark that shows relative bounds, acceptable
;; slowdown factors can be tweaked to force it to fail.
(t/deftest test-non-entity-id-lookup-issue-287
  (let [ivan {:xt/id :ivan :name "Ivan"}
        number-of-docs 500
        id-slowdown-factor 2
        entity-slowdown-factor 10
        tx (xt/submit-tx *api* (vec (for [n (range number-of-docs)]
                                      [::xt/put (assoc ivan :xt/id (keyword (str "ivan-" n)) :id n)])))
        _ (xt/await-tx *api* tx)
        db (xt/db *api*)
        entity-time (let [start-time (System/nanoTime)]
                      (t/testing "entity id lookup"
                        (t/is (= :ivan-2 (:xt/id (xt/entity db :ivan-2)))))
                      (- (System/nanoTime) start-time))
        id-time (let [start-time (System/nanoTime)]
                  (t/testing "query based on primary key"
                    (t/is (= #{[:ivan-1]} (xt/q db
                                                '{:find [e]
                                                  :where [[e :xt/id :ivan-1]]}))))
                  (- (System/nanoTime) start-time))
        secondary-time (let [start-time (System/nanoTime)]
                         (t/testing "query based on secondary attribute"
                           (t/is (= #{[:ivan-3]} (xt/q db
                                                       '{:find [e]
                                                         :where [[e :id 3]]}))))
                         (- (System/nanoTime) start-time))]
    (t/is (<= secondary-time (* entity-slowdown-factor entity-time)))
    (t/is (<= secondary-time (* id-slowdown-factor id-time)))))

(t/deftest test-query-and-match
  (t/testing "can create new user"
    (let [{::xt/keys [tx-time tx-id] :as submitted-tx}
          (xt/submit-tx *api* [[::xt/match :ivan nil]
                               [::xt/put {:xt/id :ivan, :name "Ivan 1st"}]])]
      (xt/await-tx *api* submitted-tx)
      (t/is (true? (xt/tx-committed? *api* submitted-tx)))

      (t/is (= #{["Ivan 1st"]} (xt/q (xt/db *api* tx-time tx-time)
                                     '{:find [n]
                                       :where [[:ivan :name n]]})))

      (t/is (= tx-id (::xt/tx-id (xt/entity-tx (xt/db *api* tx-time tx-time) :ivan))))

      (t/is (= {:xt/id :ivan
                :name "Ivan 1st"} (xt/entity (xt/db *api* tx-time tx-time) :ivan)))))

  (t/testing "cannot create existing user"
    (let [{::xt/keys [tx-time tx-id] :as submitted-tx}
          (xt/submit-tx *api* [[::xt/match :ivan nil]
                               [::xt/put {:xt/id :ivan, :name "Ivan 2nd"}]])]

      (xt/await-tx *api* submitted-tx)
      (t/is (false? (xt/tx-committed? *api* submitted-tx)))

      (t/is (= #{["Ivan 1st"]} (xt/q (xt/db *api* tx-time tx-time)
                                     '{:find [n]
                                       :where [[:ivan :name n]]})))

      (t/is (not= tx-id (:tx-id (xt/entity-tx (xt/db *api* tx-time tx-time) :ivan))))))

  (t/testing "can update existing user"
    (let [{::xt/keys [tx-time] :as submitted-update-tx}
          (xt/submit-tx *api* [[::xt/match :ivan {:xt/id :ivan, :name "Ivan 1st"}]
                               [::xt/put {:xt/id :ivan, :name "Ivan 2nd"}]])]
      (xt/await-tx *api* submitted-update-tx)
      (t/is (true? (xt/tx-committed? *api* submitted-update-tx)))
      (t/is (= #{["Ivan 2nd"]} (xt/q (xt/db *api* tx-time tx-time)
                                     '{:find [n]
                                       :where [[:ivan :name n]]})))

      (t/testing "match sees interim state through the transaction"
        (let [{::xt/keys [tx-time] :as submitted-tx}
              (xt/submit-tx *api* [[::xt/match :ivan {:xt/id :ivan, :name "Ivan 2nd"}]
                                   [::xt/put {:xt/id :ivan, :name "Ivan 3rd"}]
                                   [::xt/match :ivan {:xt/id :ivan, :name "Ivan 3rd"}]
                                   [::xt/put {:xt/id :ivan :name "Ivan 4th"}]])]
          (xt/await-tx *api* submitted-tx)

          (t/is (xt/tx-committed? *api* submitted-tx))

          (t/is (= #{["Ivan 4th"]}
                   (xt/q (xt/db *api* tx-time tx-time)
                         '{:find [n]
                           :where [[:ivan :name n]]})))))

      (t/testing "normal put works after match"
        (let [{::xt/keys [tx-time] :as submitted-tx}
              (xt/submit-tx *api* [[::xt/put
                                    {:xt/id :ivan
                                     :name "Ivan 5th"}]])]

          (xt/await-tx *api* submitted-tx)
          (t/is (true? (xt/tx-committed? *api* submitted-tx)))
          (t/is (= #{["Ivan 5th"]} (xt/q (xt/db *api* tx-time
                                                tx-time) '{:find [n]
                                                           :where [[:ivan :name n]]})))

          (t/testing "earlier submitted txs can still be checked"
            (t/is (true? (xt/tx-committed? *api* submitted-update-tx)))))))))

;; https://www.comp.nus.edu.sg/~ooibc/stbtree95.pdf
;; This test is based on section 7. Support for complex queries in
;; bitemporal databases

;; p1 NY [0,3] [4,now]
;; p1 LA [4,now] [4,now]
;; p2 SFO [0,now] [0,5]
;; p2 SFO [0,5] [5,now]
;; p3 LA [0,now] [0,4]
;; p3 LA [0,4] [4,7]
;; p3 LA [0,7] [7,now]
;; p3 SFO [8,0] [8,now]
;; p4 NY [2,now] [2,3]
;; p4 NY [2,3] [3,now]
;; p4 LA [8,now] [8,now]
;; p5 LA [1O,now] [1O,now]
;; p6 NY [12,now] [12,now]
;; p7 NY [11,now] [11,now]

;; Find all persons who are known to be present in the United States
;; on day 2 (valid time), as of day 3 (transaction time)
;; t2 p2 SFO, t5 p3 LA, t9 p4 NY, t10 p4 NY (?)

(t/deftest test-bitemp-query-from-indexing-temporal-data-using-existing-b+-trees-paper
  ;; Day 0, represented as #inst "2018-12-31"
  (xt/submit-tx *api* [[::xt/put
                        {:xt/id :p2
                         :entry-pt :SFO
                         :arrival-time #inst "2018-12-31"
                         :departure-time :na}
                        #inst "2018-12-31"]
                       [::xt/put
                        {:xt/id :p3
                         :entry-pt :LA
                         :arrival-time #inst "2018-12-31"
                         :departure-time :na}
                        #inst "2018-12-31"]])
  ;; Day 1, nothing happens.
  (xt/submit-tx *api* [])
  ;; Day 2
  (xt/submit-tx *api* [[::xt/put
                        {:xt/id :p4
                         :entry-pt :NY
                         :arrival-time #inst "2019-01-02"
                         :departure-time :na}
                        #inst "2019-01-02"]])
  ;; Day 3
  (let [third-day-submitted-tx (xt/submit-tx *api* [[::xt/put
                                                     {:xt/id :p4
                                                      :entry-pt :NY
                                                      :arrival-time #inst "2019-01-02"
                                                      :departure-time #inst "2019-01-03"}
                                                     #inst "2019-01-03"]])]
    ;; this introduces enough delay s.t. tx3 and tx4 don't happen in the same millisecond
    ;; (see test further down)
    ;; because we only ask for the DB at the tx-time, currently, not the tx-id
    ;; see #421
    (Thread/sleep 10)

    ;; Day 4, correction, adding missing trip on new arrival.
    (xt/submit-tx *api* [[::xt/put
                          {:xt/id :p1
                           :entry-pt :NY
                           :arrival-time #inst "2018-12-31"
                           :departure-time :na}
                          #inst "2018-12-31"]
                         [::xt/put
                          {:xt/id :p1
                           :entry-pt :NY
                           :arrival-time #inst "2018-12-31"
                           :departure-time #inst "2019-01-03"}
                          #inst "2019-01-03"]
                         [::xt/put
                          {:xt/id :p1
                           :entry-pt :LA
                           :arrival-time #inst "2019-01-04"
                           :departure-time :na}
                          #inst "2019-01-04"]
                         [::xt/put
                          {:xt/id :p3
                           :entry-pt :LA
                           :arrival-time #inst "2018-12-31"
                           :departure-time #inst "2019-01-04"}
                          #inst "2019-01-04"]])
    ;; Day 5
    (xt/submit-tx *api* [[::xt/put
                          {:xt/id :p2
                           :entry-pt :SFO
                           :arrival-time #inst "2018-12-31"
                           :departure-time #inst "2018-12-31"}
                          #inst "2019-01-05"]])
    ;; Day 6, nothing happens.
    (xt/submit-tx *api* [])
    ;; Day 7-12, correction of deletion/departure on day 4. Shows
    ;; how valid time cannot be the same as arrival time.
    (xt/submit-tx *api* [[::xt/put
                          {:xt/id :p3
                           :entry-pt :LA
                           :arrival-time #inst "2018-12-31"
                           :departure-time :na}
                          #inst "2019-01-04"]
                         [::xt/put
                          {:xt/id :p3
                           :entry-pt :LA
                           :arrival-time #inst "2018-12-31"
                           :departure-time #inst "2019-01-07"}
                          #inst "2019-01-07"]])
    (xt/submit-tx *api* [[::xt/put
                          {:xt/id :p3
                           :entry-pt :SFO
                           :arrival-time #inst "2019-01-08"
                           :departure-time :na}
                          #inst "2019-01-08"]
                         [::xt/put
                          {:xt/id :p4
                           :entry-pt :LA
                           :arrival-time #inst "2019-01-08"
                           :departure-time :na}
                          #inst "2019-01-08"]])
    (xt/submit-tx *api* [[::xt/put
                          {:xt/id :p3
                           :entry-pt :SFO
                           :arrival-time #inst "2019-01-08"
                           :departure-time #inst "2019-01-08"}
                          #inst "2019-01-09"]])
    (xt/submit-tx *api* [[::xt/put
                          {:xt/id :p5
                           :entry-pt :LA
                           :arrival-time #inst "2019-01-10"
                           :departure-time :na}
                          #inst "2019-01-10"]])
    (xt/submit-tx *api* [[::xt/put
                          {:xt/id :p7
                           :entry-pt :NY
                           :arrival-time #inst "2019-01-11"
                           :departure-time :na}
                          #inst "2019-01-11"]])

    (doto (xt/submit-tx *api* [[::xt/put
                                {:xt/id :p6
                                 :entry-pt :NY
                                 :arrival-time #inst "2019-01-12"
                                 :departure-time :na}
                                #inst "2019-01-12"]])
      (->> (xt/await-tx *api*)))

    (t/is (= #{[:p2 :SFO #inst "2018-12-31" :na]
               [:p3 :LA #inst "2018-12-31" :na]
               [:p4 :NY #inst "2019-01-02" :na]}
             (xt/q (xt/db *api* #inst "2019-01-02" (::xt/tx-time third-day-submitted-tx))
                   '{:find [p entry-pt arrival-time departure-time]
                     :where [[p :entry-pt entry-pt]
                             [p :arrival-time arrival-time]
                             [p :departure-time departure-time]]})))))

;; Tests borrowed from Datascript:
;; https://github.com/tonsky/datascript/tree/master/test/datascript/test

(defn- populate-datascript-test-db []
  (fix/transact! *api* [{:xt/id 1 :name "Ivan" :age 10}
                        {:xt/id 2 :name "Ivan" :age 20}
                        {:xt/id 3 :name "Oleg" :age 10}
                        {:xt/id 4 :name "Oleg" :age 20}
                        {:xt/id 5 :name "Ivan" :age 10}
                        {:xt/id 6 :name "Ivan" :age 20}]))

(t/deftest datascript-test-not
  (populate-datascript-test-db)
  (let [db (xt/db *api*)]
    (t/are [q res] (= (xt/q db {:find '[?e] :where (quote q)})
                      (into #{} (map vector) res))
      [[?e :name]
       (not [?e :name "Ivan"])]
      #{3 4}

      [[?e :name]
       (not
        [?e :name "Ivan"]
        [?e :age  10])]
      #{2 3 4 6}

      [[?e :name]
       (not [?e :name "Ivan"])
       (not [?e :age 10])]
      #{4}

      ;; full exclude
      [[?e :name]
       (not [?e :age])]
      #{}

      ;; not-intersecting rels
      [[?e :name "Ivan"]
       (not [?e :name "Oleg"])]
      #{1 2 5 6}

      ;; exclude empty set
      [[?e :name]
       (not [?e :name "Ivan"]
            [?e :name "Oleg"])]
      #{1 2 3 4 5 6}

      ;; nested excludes
      [[?e :name]
       (not [?e :name "Ivan"]
            (not [?e :age 10]))]
      #{1 3 4 5})))

(t/deftest datascript-test-not-join
  (populate-datascript-test-db)
  (let [db (xt/db *api*)]
    (t/is (= (xt/q db
                   '{:find [?e ?a]
                     :where [[?e :name]
                             [?e :age  ?a]
                             (not-join [?e]
                                       [?e :name "Oleg"]
                                       [?e :age ?a])]})
             #{[1 10] [2 20] [5 10] [6 20]}))

    (t/is (= (xt/q db
                   '{:find [?e ?a]
                     :where [[?e :name]
                             [?e :age  ?a]
                             [?e :age  10]
                             (not-join [?e]
                                       [?e :name "Oleg"]
                                       [?e :age  10]
                                       [?e :age ?a])]})
             #{[1 10] [5 10]}))))

(t/deftest datascript-test-not-impl-edge-cases
  (populate-datascript-test-db)
  (let [db (xt/db *api*)]
    (t/are [q res] (= (xt/q db {:find '[?e] :where (quote q)})
                      (into #{} (map vector) res))
      ;; const \ empty
      [[?e :name "Oleg"]
       [?e :age  10]
       (not [?e :age 20])]
      #{3}

      ;; const \ const
      [[?e :name "Oleg"]
       [?e :age  10]
       (not [?e :age 10])]
      #{}

      ;; rel \ const
      [[?e :name "Oleg"]
       (not [?e :age 10])]
      #{4})

    ;; 2 rels \ 2 rels
    (t/is (= (xt/q db
                   '{:find [?e ?e2]
                     :where [[?e  :name "Ivan"]
                             [?e2 :name "Ivan"]
                             (not [?e :age 10]
                                  [?e2 :age 20])]})
             #{[2 1] [6 5] [1 1] [2 2] [5 5] [6 6] [2 5] [1 5] [2 6] [6 1] [5 1] [6 2]}))

    ;; 2 rels \ rel + const
    (t/is (= (xt/q db
                   '{:find [?e ?e2]
                     :where [[?e  :name "Ivan"]
                             [?e2 :name "Oleg"]
                             (not [?e :age 10]
                                  [?e2 :age 20])]})
             #{[2 3] [1 3] [2 4] [6 3] [5 3] [6 4]}))

    ;; 2 rels \ 2 consts
    (t/is (= (xt/q db
                   '{:find [?e ?e2]
                     :where [[?e  :name "Oleg"]
                             [?e2 :name "Oleg"]
                             (not [?e :age 10]
                                  [?e2 :age 20])]})
             #{[4 3] [3 3] [4 4]}))))

(t/deftest datascript-test-or
  (populate-datascript-test-db)
  (let [db (xt/db *api*)]
    (t/are [q res]  (= (xt/q db {:find '[?e] :where (quote q)})
                       (into #{} (map vector) res))

      ;; intersecting results
      [(or [?e :name "Oleg"]
           [?e :age 10])]
      #{1 3 4 5}

      ;; one branch empty
      [(or [?e :name "Oleg"]
           [?e :age 30])]
      #{3 4}

      ;; both empty
      [(or [?e :name "Petr"]
           [?e :age 30])]
      #{}

      ;; join with 1 var
      [[?e :name "Ivan"]
       (or [?e :name "Oleg"]
           [?e :age 10])]
      #{1 5}

      ;; join with 2 vars
      [[?e :age ?a]
       (or (and [?e :name "Ivan"]
                [1  :age  ?a])
           (and [?e :name "Oleg"]
                [2  :age  ?a]))]
      #{1 5 4})))

(t/deftest datascript-test-or-join
  (populate-datascript-test-db)
  (let [db (xt/db *api*)]
    (t/are [q res] (= (xt/q db {:find '[?e] :where (quote q)})
                      (into #{} (map vector) res))
      [(or-join [?e]
                [?e :name ?n]
                (and [?e :age ?a]
                     [?e :name ?n]))]
      #{1 2 3 4 5 6}

      [(or-join [?e]
                [?e :name ?n]
                (and [?e :age ?a]
                     [?e :name ?n]))]
      #{1 2 3 4 5 6}

      [[(identity 1) ?e]
       (or-join [[?e]]
                [?e :name ?n]
                (and [?e :age ?a]
                     [?e :name ?n]))]
      #{1}

      [[?e  :name ?a]
       [?e2 :name ?a]
       (or-join [?e]
                (and [?e  :age ?a]
                     [?e2 :age ?a]))]
      #{1 2 3 4 5 6})))

(t/deftest test-rules
  (fix/transact! *api* [{:xt/id 5 :follow 3}
                        {:xt/id 1 :follow 2}
                        {:xt/id 2 :follow #{3 4}}
                        {:xt/id 3 :follow 4}
                        {:xt/id 4 :follow 6}])
  (let [db (xt/db *api*)]
    (t/is (= (xt/q db
                   '{:find  [?e1 ?e2]
                     :where [(follow ?e1 ?e2)]
                     :rules [[(follow ?x ?y)
                              [?x :follow ?y]]]})
             #{[1 2] [2 3] [3 4] [2 4] [5 3] [4 6]}))

    ;; NOTE: XTDB does not support vars in attribute position, so
    ;; :follow is explicit.
    (t/testing "Joining regular clauses with rule"
      (t/is (= (xt/q db
                     '{:find [?y ?x]
                       :where [[_ :follow ?x]
                               (rule ?x ?y)
                               [(even? ?x)]]
                       :rules [[(rule ?a ?b)
                                [?a :follow ?b]]]})
               #{[3 2] [6 4] [4 2]})))

    ;; NOTE: XTDB does not support vars in attribute position.
    #_(t/testing "Rule context is isolated from outer context"
        (t/is (= (xt/q db
                       '{:find [?x]
                         :where [[?e _ _]
                                 (rule ?x)]
                         :rules [[(rule ?e)
                                  [_ ?e _]]]})
                 #{[:follow]})))

    (t/testing "Rule with branches"
      (t/is (= (xt/q db
                     '{:find [?e2]
                       :where [(follow ?e1 ?e2)]
                       :args [{:?e1 1}]
                       :rules [[(follow ?e2 ?e1)
                                [?e2 :follow ?e1]]
                               [(follow ?e2 ?e1)
                                [?e2 :follow ?t]
                                [?t  :follow ?e1]]]})
               #{[2] [3] [4]})))


    (t/testing "Recursive rules"
      (t/is (= (xt/q db
                     '{:find  [?e2]
                       :where [(follow ?e1 ?e2)]
                       :args [{:?e1 1}]
                       :rules [[(follow ?e1 ?e2)
                                [?e1 :follow ?e2]]
                               [(follow ?e1 ?e2)
                                [?e1 :follow ?t]
                                (follow ?t ?e2)]]})
               #{[2] [3] [4] [6]})))

    (t/testing "Passing ins to rule"
      (t/is (= (xt/q db
                     {:find '[?x ?y]
                      :where '[(match ?even ?x ?y)]
                      :rules '[[(match ?pred ?e ?e2)
                                [?e :follow ?e2]
                                [(?pred ?e)]
                                [(?pred ?e2)]]]
                      :args [{:?even even?}]})
               #{[4 6] [2 4]})))

    (t/testing "Using built-ins inside rule"
      (t/is (= (xt/q db
                     '{:find [?x ?y]
                       :where [(match ?x ?y)]
                       :rules [[(match ?e ?e2)
                                [?e :follow ?e2]
                                [(even? ?e)]
                                [(even? ?e2)]]]})
               #{[4 6] [2 4]})))))

(t/deftest test-rules-with-recursion-1
  (fix/transact! *api* [{:xt/id 1 :follow 2}
                        {:xt/id 2 :follow 3}
                        {:xt/id 3 :follow 1}])
  (t/is (= (xt/q (xt/db *api*)
                 '{:find [?e1 ?e2]
                   :where [(follow ?e1 ?e2)]
                   :rules [[(follow ?e1 ?e2)
                            [?e1 :follow ?e2]]
                           [(follow ?e1 ?e2)
                            (follow ?e2 ?e1)]]})
           #{[1 2] [2 3] [3 1] [2 1] [3 2] [1 3]})))

(t/deftest test-rules-with-recursion-2
  (fix/transact! *api* [{:xt/id 1 :follow 2}
                        {:xt/id 2 :follow 3}])
  (t/is (= (xt/q (xt/db *api*)
                 '{:find [?e1 ?e2]
                   :where [(follow ?e1 ?e2)]
                   :rules [[(follow ?e1 ?e2)
                            [?e1 :follow ?e2]]
                           [(follow ?e1 ?e2)
                            (follow ?e2 ?e1)]]})
           #{[1 2] [2 3] [2 1] [3 2]})))

(t/deftest test-calling-rule-twice-44
  (fix/transact! *api* [{:xt/id 1 :attr "a"}])
  (let [db (xt/db *api*)]
    (t/is (xt/q db
                {:find '[?p]
                 :where '[(rule ?p ?fn "a")
                          (rule ?p ?fn "b")]
                 :rules '[[(rule ?p ?fn ?x)
                           [?p :attr ?x]
                           [(?fn ?x)]]]
                 :args [{:?fn (constantly true)}]}))))

(t/deftest test-mutually-recursive-rules
  (fix/transact! *api* [{:xt/id 0 :f1 1}
                        {:xt/id 1 :f2 2}
                        {:xt/id 2 :f1 3}
                        {:xt/id 3 :f2 4}
                        {:xt/id 4 :f1 5}
                        {:xt/id 5 :f2 6}])
  (let [db (xt/db *api*)]
    (t/is (= (xt/q db
                   '{:find [?e1 ?e2]
                     :where [(f1 ?e1 ?e2)]
                     :rules [[(f1 ?e1 ?e2)
                              [?e1 :f1 ?e2]]
                             [(f1 ?e1 ?e2)
                              [?t :f1 ?e2]
                              (f2 ?e1 ?t)]
                             [(f2 ?e1 ?e2)
                              [?e1 :f2 ?e2]]
                             [(f2 ?e1 ?e2)
                              [?t :f2 ?e2]
                              (f1 ?e1 ?t)]]})
             #{[0 1] [0 3] [0 5]
               [1 3] [1 5]
               [2 3] [2 5]
               [3 5]
               [4 5]}))))

;; https://github.com/tonsky/datascript/issues/218
(t/deftest datascript-test-rules-false-arguments
  (fix/transact! *api* [{:xt/id 1 :attr true}
                        {:xt/id 2 :attr false}])
  (let [db (xt/db *api*)
        rules '[[(is ?id ?val)
                 [?id :attr ?val]]]]
    (t/is (= (xt/q db
                   {:find '[?id]
                    :where '[(is ?id true)]
                    :rules rules})
             #{[1]}))
    (t/is (= (xt/q db
                   {:find '[?id]
                    :where '[(is ?id false)]
                    :rules rules})
             #{[2]}))))

(defn- even-or-nil? [x]
  (when (even? x)
    x))

(t/deftest data-script-test-query-fns
  (fix/transact! *api* [{:xt/id 1 :name "Ivan" :age 15}
                        {:xt/id 2 :name "Petr" :age 22 :height 240 :parent 1}
                        {:xt/id 3 :name "Slava" :age 37 :parent 2}])
  (let [db (xt/db *api*)]
    (t/testing "predicate without free variables"
      (t/is (= (xt/q db
                     '{:find [?x]
                       :args [{:?x :a}
                              {:?x :b}
                              {:?x :c}]
                       :where [[(> 2 1)]]})
               #{[:a] [:b] [:c]})))

    ;; NOTE: XTDB does not support these functions.
    #_(t/testing "ground"
        (t/is (= (d/q '[:find ?vowel
                        :where [(ground [:a :e :i :o :u]) [?vowel ...]]])
                 #{[:a] [:e] [:i] [:o] [:u]})))

    #_(t/testing "get-else"
        (t/is (= (d/q '[:find ?e ?age ?height
                        :where [?e :age ?age]
                        [(get-else $ ?e :height 300) ?height]] db)
                 #{[1 15 300] [2 22 240] [3 37 300]}))

        (t/is (thrown-with-msg? ExceptionInfo #"get-else: nil default value is not supported"
                                (d/q '[:find ?e ?height
                                       :where [?e :age]
                                       [(get-else $ ?e :height nil) ?height]] db))))

    #_(t/testing "get-some"
        (t/is (= (d/q '[:find ?e ?a ?v
                        :where [?e :name _]
                        [(get-some $ ?e :height :age) [?a ?v]]] db)
                 #{[1 :age 15]
                   [2 :height 240]
                   [3 :age 37]})))

    #_(t/testing "missing?"
        (t/is (= (xt/q '[:find ?e ?age
                         :in $
                         :where [?e :age ?age]
                         [(missing? $ ?e :height)]] db)
                 #{[1 15] [3 37]})))

    #_(t/testing "missing? back-ref"
        (t/is (= (xt/q '[:find ?e
                         :in $
                         :where [?e :age ?age]
                         [(missing? $ ?e :_parent)]] db)
                 #{[3]})))

    (t/testing "Built-ins"
      (t/is (= (xt/q db
                     '{:find [?e1 ?e2]
                       :where [[?e1 :age ?a1]
                               [?e2 :age ?a2]
                               [(< ?a1 18 ?a2)]]})
               #{[1 2] [1 3]}))

      (t/is (= (xt/q db
                     '{:find [?x ?c]
                       :args [{:?x "a"}
                              {:?x "abc"}]
                       :where [[(count ?x) ?c]]})
               #{["a" 1] ["abc" 3]})))

    (t/testing "Built-in vector, hashmap"
      (t/is (= (xt/q db
                     '{:find [?tx-data]
                       :where [[(identity :db/add) ?op]
                               [(vector ?op -1 :attr 12) ?tx-data]]})
               #{[[:db/add -1 :attr 12]]}))

      (t/is (= (xt/q db
                     '{:find [?tx-data]
                       :where
                       [[(hash-map :db/id -1 :age 92 :name "Aaron") ?tx-data]]})
               #{[{:db/id -1 :age 92 :name "Aaron"}]})))


    (t/testing "Passing predicate as source"
      (t/is (= (xt/q db
                     {:find '[?e]
                      :where '[[?e :age ?a]
                               [(?adult ?a)]]
                      :args [{:?adult #(> % 18)}]})
               #{[2] [3]})))

    (t/testing "Calling a function"
      (t/is (= (xt/q db
                     '{:find [?e1 ?e2 ?e3]
                       :where [[?e1 :age ?a1]
                               [?e2 :age ?a2]
                               [?e3 :age ?a3]
                               [(+ ?a1 ?a2) ?a12]
                               [(= ?a12 ?a3)]]})
               #{[1 2 3] [2 1 3]})))

    (t/testing "Two conflicting function values for one binding."
      (t/is (= (xt/q db
                     '{:find [?n]
                       :where [[(identity 1) ?n]
                               [(identity 2) ?n]]})
               #{})))

    (t/testing "Destructured conflicting function values for two bindings."
      (t/is (= (xt/q db
                     '[:find  ?n ?x
                       :where [(identity [3 4]) [?n ?x]]
                       [(identity [1 2]) [?n ?x]]])
               #{})))

    (t/testing "Rule bindings interacting with function binding. (fn, rule)"
      (t/is (= (xt/q db
                     '{:find [?n]
                       :where [[(identity 2) ?n]
                               (my-vals ?n)]
                       :rules [[(my-vals ?x)
                                [(identity 1) ?x]]
                               [(my-vals ?x)
                                [(identity 2) ?x]]
                               [(my-vals ?x)
                                [(identity 3) ?x]]]})
               #{[2]})))

    (t/testing "Rule bindings interacting with function binding. (rule, fn)"
      (t/is (= (xt/q db
                     '{:find [?n]
                       :where [(my-vals ?n)
                               [(identity 2) ?n]]
                       :rules [[(my-vals ?x)
                                [(identity 1) ?x]]
                               [(my-vals ?x)
                                [(identity 2) ?x]]
                               [(my-vals ?x)
                                [(identity 3) ?x]]]})
               #{[2]})))

    (t/testing "Conflicting relational bindings with function binding. (rel, fn)"
      (t/is (= (xt/q db
                     '{:find [?age]
                       :where [[_ :age ?age]
                               [(identity 100) ?age]]})
               #{})))

    (t/testing "Conflicting relational bindings with function binding. (fn, rel)"
      (t/is (= (xt/q db
                     '{:find [?age]
                       :where [[(identity 100) ?age]
                               [_ :age ?age]]})
               #{})))

    (t/testing "Function on empty rel"
      (t/is (= (xt/q db
                     '{:find [?e ?y]
                       :where [[?e :salary ?x]
                               [(+ ?x 100) ?y]
                               [0 :age 15]
                               [1 :age 35]]})
               #{})))

    ;; XTDB supports nil and false results.
    #_(t/testing "Returning nil from function filters out tuple from result"
        (t/is (= (xt/q db
                       {:find '[?x]
                        :where '[[(xtdb.query-test/even-or-nil? ?in) ?x]]
                        :args [{:?in 1}
                               {:?in 2}
                               {:?in 3}
                               {:?in 4}]})
                 #{[2] [4]})))

    (t/testing "Result bindings"
      (t/is (= (xt/q db '[:find ?a ?c
                          :args {?in [:a :b :c]}
                          :where [(identity ?in) [?a _ ?c]]])
               #{[:a :c]}))

      (t/is (= (xt/q db '[:find ?in
                          :args {?in :a}
                          :where [(identity ?in) _]])
               #{[:a]}))

      (t/is (= (xt/q db '[:find ?x ?z
                          :args {?in [[:a :b :c]
                                      [:d :e :f]]}
                          :where [(identity ?in) [[?x _ ?z]]]])
               #{[:a :c] [:d :f]}))

      (t/is (= (xt/q db '[:find ?in
                          :args {?in []}
                          :where [(identity ?in) [_ ...]]])
               #{})))))


(t/deftest datascript-test-predicates
  (fix/transact! *api* [{:xt/id 1 :name "Ivan" :age 10}
                        {:xt/id 2 :name "Ivan" :age 20}
                        {:xt/id 3 :name "Oleg" :age 10}
                        {:xt/id 4 :name "Oleg" :age 20}])
  (let [db (xt/db *api*)]
    (t/are [q res] (= (xt/q db (quote q)) res)
      ;; plain predicate
      {:find [?e ?a]
       :where [[?e :age ?a]
               [(> ?a 10)]]}
      #{[2 20] [4 20]}

      ;; join in predicate
      {:find [?e ?e2]
       :where [[?e  :name]
               [?e2 :name]
               [(< ?e ?e2)]]}
      #{[1 2] [1 3] [1 4] [2 3] [2 4] [3 4]}

      ;; join with extra symbols
      {:find [?e ?e2]
       :where [[?e  :age ?a]
               [?e2 :age ?a2]
               [(< ?e ?e2)]]}
      #{[1 2] [1 3] [1 4] [2 3] [2 4] [3 4]}

      ;; empty result
      {:find [?e ?e2]
       :where [[?e  :name "Ivan"]
               [?e2 :name "Oleg"]
               [(= ?e ?e2)]]}
      #{}

      ;; pred over const, true
      {:find [?e]
       :where [[?e :name "Ivan"]
               [?e :age 20]
               [(= ?e 2)]]}
      #{[2]}

      ;; pred over const, false
      {:find [?e]
       :where [[?e :name "Ivan"]
               [?e :age 20]
               [(= ?e 1)]]}
      #{})

    ;; NOTE: XTDB does not support source vars.
    #_(let [pred (fn [db e a]
                   (= a (:age (d/entity db e))))]
        (t/is (= (xt/q '[:find ?e
                         :in $ ?pred
                         :where [?e :age ?a]
                         [(?pred $ ?e 10)]]
                       db pred)
                 #{[1] [3]})))))


(t/deftest datascript-test-issue-180
  (fix/transact! *api* [{:xt/id 1 :age 20}])
  (let [db (xt/db *api*)]
    (t/is (= #{}
             (xt/q db
                   '{:find [?e ?a]
                     :where [[_ :pred ?pred]
                             [?e :age ?a]
                             [(?pred ?a)]]})))))

(defn sample-query-fn [] 42)

(t/deftest datascript-test-symbol-resolution
  (let [db (xt/db *api*)]
    (t/is (= #{[42]} (xt/q db
                           '{:find [?x]
                             :where [[(xtdb.query-test/sample-query-fn) ?x]]})))))


(defmethod xtdb.query/aggregate 'sort-reverse [_]
  (fn
    ([] [])
    ([acc] (vec (reverse (sort acc))))
    ([acc x] (conj acc x))))

(t/deftest datascript-test-aggregates
  (let [db (xt/db *api*)]
    #_(t/testing "with"
        (t/is (= (d/q '[:find ?heads
                        :with ?monster
                        :in   [[?monster ?heads]] ]
                      [["Medusa" 1]
                       ["Cyclops" 1]
                       ["Chimera" 1] ])
                 [[1] [1] [1]])))

    ;; This is solved as XTDB performs the aggregation before turning
    ;; it into a set.
    #_(t/testing "Wrong grouping without :with"
        (t/is (= (xt/q db '[:find (sum ?heads)
                            :where [(identity [["Cerberus" 3]
                                               ["Medusa" 1]
                                               ["Cyclops" 1]
                                               ["Chimera" 1]]) [[?monster ?heads]]]])
                 #{[4]})))

    (t/testing "Multiple aggregates, correct grouping with :with"
      (t/is (= (xt/q db '[:find (sum ?heads) (min ?heads) (max ?heads) (count ?heads) (count-distinct ?heads)
                          ;; :with ?monster
                          :where [(identity [["Cerberus" 3]
                                             ["Medusa" 1]
                                             ["Cyclops" 1]
                                             ["Chimera" 1]]) [[?monster ?heads]]]])
               #{[6 1 3 4 2]})))

    (t/testing "Min and max are using comparator instead of default compare"
      ;; Wrong: using js '<' operator
      ;; (apply min [:a/b :a-/b :a/c]) => :a-/b
      ;; (apply max [:a/b :a-/b :a/c]) => :a/c
      ;; Correct: use IComparable interface
      ;; (sort compare [:a/b :a-/b :a/c]) => (:a/b :a/c :a-/b)
      (t/is (= (xt/q db '[:find (min ?x) (max ?x)
                          :where [(identity [:a-/b :a/b]) [?x ...]]])
               #{[:a/b :a-/b]}))

      (t/is (= (xt/q db '[:find (min 2 ?x) (max 2 ?x)
                          :where [(identity [:a/b :a-/b :a/c]) [?x ...]]])
               #{[[:a/b :a/c] [:a/c :a-/b]]})))

    (t/testing "Grouping"
      (t/is (= (set (xt/q db '[:find ?color (max ?x) (min ?x)
                               :where [(identity [[:red 1]  [:red 2] [:red 3] [:red 4] [:red 5]
                                                  [:blue 7] [:blue 8]]) [[?color ?x]]]]))
               #{[:red  5 1]
                 [:blue 8 7]})))

    (t/testing "Grouping and parameter passing"
      (t/is (= (set (xt/q db '[:find ?color (max 3 ?x) (min 3 ?x)
                               :where [(identity [[:red 1]  [:red 2] [:red 3] [:red 4] [:red 5]
                                                  [:blue 7] [:blue 8]]) [[?color ?x]]]]))
               #{[:red  [3 4 5] [1 2 3]]
                 [:blue [7 8]   [7 8]]})))

    ;; NOTE: XTDB only support a single final logic var in aggregates.
    #_(t/testing "Grouping and parameter passing"
        (is (= (set (d/q '[:find ?color (max ?amount ?x) (min ?amount ?x)
                           :in   [[?color ?x]] ?amount ]
                         [[:red 1]  [:red 2] [:red 3] [:red 4] [:red 5]
                          [:blue 7] [:blue 8]]
                         3))
               #{[:red  [3 4 5] [1 2 3]]
                 [:blue [7 8]   [7 8]]})))

    (t/testing "avg aggregate"
      (t/is (= (ffirst (xt/q db '[:find (avg ?x)
                                  :where [(identity [10 15 20 35 75]) [?x ...]]]))
               31)))

    (t/testing "median aggregate"
      (t/is (= (ffirst (xt/q db '[:find (median ?x)
                                  :where [(identity [10 15 20 35 75]) [?x ...]]]))
               20)))

    (t/testing "variance aggregate"
      (t/is (= (ffirst (xt/q db '[:find (variance ?x)
                                  :where [(identity [10 15 20 35 75]) [?x ...]]]))
               554.0))) ;; double

    (t/testing "stddev aggregate"
      (t/is (= (ffirst (xt/q db '[:find (stddev ?x)
                                  :where [(identity [10 15 20 35 75]) [?x ...]]]))
               23.53720459187964)))

    (t/testing "distinct aggregate"
      (t/is (= (ffirst (xt/q db '[:find (distinct ?x)
                                  :where [(identity [:a :b :c :a :d]) [?x ...]]]))
               #{:a :b :c :d})))

    (t/testing "sample aggregate"
      (t/is (= (count (ffirst (xt/q db '[:find (sample 7 ?x)
                                         :where [(identity [:a :b :c :a :d]) [?x ...]]])))
               4)))

    (t/testing "rand aggregate"
      (t/is (= (count (ffirst (xt/q db '[:find (rand 7 ?x)
                                         :where [(identity [:a :b :c :a :d]) [?x ...]]])))
               7)))

    (t/testing "Custom aggregates"
      (t/is (= (set (xt/q db '[:find ?color (sort-reverse ?x)
                               :where [(identity [[:red 1]  [:red 2] [:red 3] [:red 4] [:red 5]
                                                  [:blue 7] [:blue 8]]) [[?color ?x]]]]))
               #{[:red [5 4 3 2 1]] [:blue [8 7]]})))))

(defn allowed-fn [e] e)
(defn banned-fn [e] e)

(t/deftest test-projection-exprs
  (t/is (= #{[0 0 0] [1 0 1] [1 1 3]
             [2 0 2] [2 1 4] [2 2 6]
             [3 0 3] [3 1 5] [3 2 7] [3 3 9]}
           (xt/q (xt/db *api*)
                 '{:find [?x ?y (+ ?x (* 2 ?y))]
                   :in [[[?x ?y]]]}
                 (for [x (range 4)
                       y (range (inc x))]
                   [x y]))))

  (t/is (= #{[0 0] [1 -1] [2 4] [3 -1] [4 8] [5 -1]}
           (xt/q (xt/db *api*)
                 '{:find [?x (if (even? ?x) (* 2 ?x) -1)]
                   :in [[?x ...]]}
                 (range 6)))
        "if")

  (t/is (= #{[{:a 1, :bs [2 3], :cs #{4 5}}]
             [{:a 2, :bs [4 6], :cs #{8 10}}]}
           (xt/q (xt/db *api*)
                 '{:find [{:a ?a, :bs [?b1 ?b2], :cs #{?c1 ?c2}}]
                   :in [[[?a ?b1 ?b2 ?c1 ?c2]]]}
                 [[1 2 3 4 5]
                  [2 4 6 8 10]]))
        "different collections")

  (with-open [node (xt/start-node {:xtdb/query-engine {:fn-allow-list #{'xtdb.query-test/allowed-fn}}})]
    (t/is (= #{[42]}
             (xt/q (xt/db node)
                   '{:find [(xtdb.query-test/allowed-fn ?e)]
                     :in [?e]}
                   42)))

    (t/is (thrown? IllegalArgumentException
                   (xt/q (xt/db node)
                         '{:find [(xtdb.query-test/banned-fn ?e)]
                           :in [?e]}
                         42)))))

(t/deftest test-aggregate-exprs
  (t/is (= #{[0 0 1] [1 1 5] [2 3 14] [3 6 30]}
           (xt/q (xt/db *api*)
                 '{:find [?x (sum ?y) (sum (+ (* ?y ?y) ?x 1))]
                   :in [[[?x ?y]]]}
                 (for [x (range 4)
                       y (range (inc x))]
                   [x y]))))

  (t/is (= #{[20]}
           (xt/q (xt/db *api*)
                 '{:find [(sum (if (even? ?x) ?x 0))]
                   :in [[?x ...]]}
                 (range 10)))
        "if")

  (t/is (= #{[28.5]}
           (xt/q (xt/db *api*)
                 '{:find [(/ (double (sum (* ?x ?x)))
                             (count ?x))]
                   :in [[?x ...]]}
                 (range 10)))
        "aggregates can be included in exprs")

  (t/is (thrown-with-msg? IllegalArgumentException
                          #"nested agg"
                          (xt/q (xt/db *api*)
                                '{:find [(sum (sum ?x))]
                                  :in [[?x ...]]}
                                (range 10)))
        "aggregates can't be nested")

  (t/testing "implicitly groups by variables present outside of aggregates"
    (t/is (= #{[1 2] [2 3] [2 5]}
             (xt/q (xt/db *api*)
                   '{:find [(/ ?x ?y) (sum ?z)]
                     :in [[[?x ?y ?z]]]}
                   [[1 1 2]
                    [2 1 3]
                    [4 2 5]]))
          "even though (/ x y) yields the same result in the latter two rows, we group by them individually")

    (t/is (= #{[1 3] [1 7] [4 -1]}
             (xt/q (xt/db *api*)
                   '{:find [?x (- (sum ?z) ?y)]
                     :in [[[?x ?y ?z]]]}
                   [[1 1 4]
                    [1 3 2]
                    [1 3 8]
                    [4 6 5]]))
          "groups by x and y in this case")))

(t/deftest test-can-bind-function-returns-to-falsy
  ;; Datomic does allow binding falsy values, DataScript doesn't
  ;; see "Returning nil from function filters out tuple from result"
  (t/is (= #{[false]}
           (xt/q (xt/db *api*)
                 '{:find [b]
                   :where [[(identity false) b]]})))

  (t/is (= #{[nil]}
           (xt/q (xt/db *api*)
                 '{:find [b]
                   :where [[(identity nil) b]]})))

  (t/is (= #{[true]}
           (xt/q (xt/db *api*)
                 '{:find [b]
                   :where [[(identity true) b]]}))))

(t/deftest test-can-use-any-value-as-entity-id
  (fix/transact! *api* [{:xt/id "ivan@example.com" :name "Ivan"}
                        {:xt/id 42 :name "Petr"}
                        {:xt/id true :name "Oleg" :friends ["ivan@example.com" 42 3.14]}
                        {:xt/id 3.14 :name "Pi" :boss "ivan@example.com"}])
  (t/is (= #{["Ivan"]}
           (xt/q (xt/db *api*)
                 '{:find [name]
                   :where [["ivan@example.com" :name name]]})))
  (t/is (= #{["Petr"]}
           (xt/q (xt/db *api*)
                 '{:find [name]
                   :where [[42 :name name]]})))
  (t/is (= #{["Oleg"]}
           (xt/q (xt/db *api*)
                 '{:find [name]
                   :where [[true :name name]]})))
  (t/is (= #{["Pi"]}
           (xt/q (xt/db *api*)
                 '{:find [name]
                   :where [[3.14 :name name]]})))

  (t/testing "join"
    (t/is (= #{["ivan@example.com" "Ivan"]}
             (xt/q (xt/db *api*)
                   '{:find [boss name]
                     :where [[boss :name name]
                             [pi :boss boss]
                             [pi :name "Pi"]]})))

    (t/is (= #{["Ivan"]
               ["Petr"]
               ["Pi"]}
             (xt/q (xt/db *api*)
                   '{:find [name]
                     :where [[true :friends f]
                             [f :name name]]})))))

;; Tests from Racket Datalog
;; https://github.com/racket/datalog/tree/master/tests/examples

(t/deftest test-racket-datalog-tutorial
  ;; parent(john,douglas).
  (fix/transact! *api* [{:xt/id :john :parent :douglas}])
  ;; parent(john,douglas)?
  (t/is (= #{[true]}
           (xt/q (xt/db *api*)
                 '{:find [found]
                   :where [[:john :parent :douglas]
                           [(identity true) found]]})))

  ;; parent(john,ebbon)?
  (t/is (= #{}
           (xt/q (xt/db *api*)
                 '{:find [found]
                   :where [[:john :parent :ebbon]
                           [(identity true) found]]})))

  ;; parent(bob,john).
  ;; parent(ebbon,bob).
  (fix/transact! *api* [{:xt/id :bob :parent :john}
                        {:xt/id :ebbon :parent :bob}])

  ;; parent(A,B)?
  (t/is (= #{[:john :douglas]
             [:bob :john]
             [:ebbon :bob]}
           (xt/q (xt/db *api*)
                 '{:find [a b]
                   :where [[a :parent b]]})))

  ;; parent(john,B)?
  (t/is (= #{[:douglas]}
           (xt/q (xt/db *api*)
                 '{:find [ b]
                   :where [[:john :parent b]]})))

  ;; parent(A,A)?
  (t/is (= #{}
           (xt/q (xt/db *api*)
                 '{:find [a]
                   :where [[a :parent a]]})))

  ;; ancestor(A,B) :- parent(A,B).
  ;; ancestor(A,B) :- parent(A,C), ancestor(C, B).
  ;; ancestor(A, B)?
  (t/is (= #{[:ebbon :bob]
             [:bob :john]
             [:john :douglas]
             [:bob :douglas]
             [:ebbon :john]
             [:ebbon :douglas]}
           (xt/q (xt/db *api*)
                 '{:find [a b]
                   :where [(ancestor a b)]
                   :rules [[(ancestor a b)
                            [a :parent b]]
                           [(ancestor a b)
                            [a :parent c]
                            (ancestor c b)]]})))

  ;; ancestor(X,john)?
  (t/is (= #{[:bob]
             [:ebbon]}
           (xt/q (xt/db *api*)
                 '{:find [x]
                   :where [(ancestor x :john)]
                   :rules [[(ancestor a b)
                            [a :parent b]]
                           [(ancestor a b)
                            [a :parent c]
                            (ancestor c b)]]})))

  (let [db-before (xt/db *api*)]
    ;; parent(bob, john)-
    (fix/submit+await-tx [[::xt/delete :bob]])
    ;; parent(A,B)?
    (t/is (= #{[:john :douglas]
               [:ebbon :bob]}
             (xt/q (xt/db *api*)
                   '{:find [a b]
                     :where [[a :parent b]]})))

    ;; ancestor(A,B)?
    (t/is (= #{[:ebbon :bob]
               [:john :douglas]}
             (xt/q (xt/db *api*)
                   '{:find [a b]
                     :where [(ancestor a b)]
                     :rules [[(ancestor a b)
                              [a :parent b]]
                             [(ancestor a b)
                              [a :parent c]
                              (ancestor c b)]]})))

    (t/testing "can query previous state"
      (t/is (= #{[:ebbon :bob]
                 [:bob :john]
                 [:john :douglas]
                 [:bob :douglas]
                 [:ebbon :john]
                 [:ebbon :douglas]}
               (xt/q db-before
                     '{:find [a b]
                       :where [(ancestor a b)]
                       :rules [[(ancestor a b)
                                [a :parent b]]
                               [(ancestor a b)
                                [a :parent c]
                                (ancestor c b)]]}))))))

(t/deftest test-racket-datalog-path
  ;; edge(a, b). edge(b, c). edge(c, d). edge(d, a).
  (fix/transact! *api* [{:xt/id :a :edge :b}
                        {:xt/id :b :edge :c}
                        {:xt/id :c :edge :d}
                        {:xt/id :d :edge :a}])

  ;; path(X, Y) :- edge(X, Y).
  ;; path(X, Y) :- edge(X, Z), path(Z, Y).
  ;; path(X, Y)?
  (t/is (= #{[:a :a]
             [:a :d]
             [:a :c]
             [:a :b]
             [:b :a]
             [:b :d]
             [:b :c]
             [:b :b]
             [:c :a]
             [:c :d]
             [:c :c]
             [:c :b]
             [:d :b]
             [:d :c]
             [:d :d]
             [:d :a]}
           (xt/q (xt/db *api*)
                 '{:find [x y]
                   :where [(path x y)]
                   :rules [[(path x y)
                            [x :edge y]]
                           [(path x y)
                            [x :edge z]
                            (path z y)]]}))))

(t/deftest test-racket-datalog-revpath
  ;; edge(a, b). edge(b, c). edge(c, d). edge(d, a).
  (fix/transact! *api* [{:xt/id :a :edge :b}
                        {:xt/id :b :edge :c}
                        {:xt/id :c :edge :d}
                        {:xt/id :d :edge :a}])
  ;; path(X, Y) :- edge(X, Y).
  ;; path(X, Y) :- path(X, Z), edge(Z, Y).
  ;; path(X, Y)?
  (t/is (= #{[:a :a]
             [:a :d]
             [:a :c]
             [:a :b]
             [:b :a]
             [:b :d]
             [:b :c]
             [:b :b]
             [:c :a]
             [:c :d]
             [:c :c]
             [:c :b]
             [:d :b]
             [:d :c]
             [:d :d]
             [:d :a]}
           (xt/q (xt/db *api*)
                 '{:find [x y]
                   :where [(path x y)]
                   :rules [[(path x y)
                            [x :edge y]]
                           [(path x y)
                            (path x z)
                            [z :edge y]]]}))))

(t/deftest test-racket-datalog-bidipath
  ;; edge(a, b). edge(b, c). edge(c, d). edge(d, a).
  (fix/transact! *api* [{:xt/id :a :edge :b}
                        {:xt/id :b :edge :c}
                        {:xt/id :c :edge :d}
                        {:xt/id :d :edge :a}])

  ;; path(X, Y) :- edge(X, Y).
  ;; path(X, Y) :- edge(X, Z), path(Z, Y).
  ;; path(X, Y) :- path(X, Z), edge(Z, Y).
  ;; path(X, Y)?
  (t/is (= #{[:a :a]
             [:a :d]
             [:a :c]
             [:a :b]
             [:b :a]
             [:b :d]
             [:b :c]
             [:b :b]
             [:c :a]
             [:c :d]
             [:c :c]
             [:c :b]
             [:d :b]
             [:d :c]
             [:d :d]
             [:d :a]}
           (xt/q (xt/db *api*)
                 '{:find [x y]
                   :where [(path x y)]
                   :rules [[(path x y)
                            [x :edge y]]
                           [(path x y)
                            (path x z)
                            [z :edge y]]
                           [(path x y)
                            (path x z)
                            [z :edge y]]]}))))

(t/deftest test-racket-datalog-sym
  ;; sym(a).
  ;; sym(b).
  ;; sym(c).
  (fix/transact! *api* [{:xt/id :a}
                        {:xt/id :b}
                        {:xt/id :c}])

  ;; perm(X,Y) :- sym(X), sym(Y), X != Y.
  ;; perm(X, Y)?
  (t/is (= #{[:a :c]
             [:a :b]
             [:c :a]
             [:b :a]
             [:b :c]
             [:c :b]}
           (xt/q (xt/db *api*)
                 '{:find [x y]
                   :where [(perm x y)]
                   :rules [[(perm x y)
                            [x :xt/id]
                            [y :xt/id]
                            [(!= x y)]]]}))))

(t/deftest test-failing-predicates-at-top-level-bug
  (t/testing "predicate order shouldn't matter"
    (t/is (= #{}
             (xt/q (xt/db *api*)
                   '{:find [f]
                     :where [[(identity 4) f]
                             [(identity false)]]})))

    (t/is (= #{}
             (xt/q (xt/db *api*)
                   '{:find [f]
                     :where [[(identity false)]
                             [(identity 4) f]]})))))

(t/deftest test-literal-rule-arguments-bug-507
  (t/testing "range clause in rule"
    ;; supplying 4 as the rule arg here is a necessary condition
    (t/is (= #{}
             (xt/q (xt/db *api*)
                   '{:find [f]
                     :where [(foo 4 f)]
                     :rules [[(foo n f)
                              [(<= 6 n)]
                              [(identity n) f]]]})))

    (t/testing "predicates work for non-numeric comparables too"
      (t/is (= #{}
               (xt/q (xt/db *api*)
                     '{:find [f]
                       :where [(foo #inst "2019" f)]
                       :rules [[(foo n f)
                                [(<= #inst "2020" n)]
                                [(identity n) f]]]}))))))

;; Test from Racket Datalog documentation:
;; https://docs.racket-lang.org/datalog/datalog.html
(t/deftest test-racket-datalog-fib
  ;; fib(0, 0).
  ;; fib(1, 1).

  ;;  fib(N, F) :- N != 1,
  ;;               N != 0,
  ;;               N1 :- -(N, 1),
  ;;               N2 :- -(N, 2),
  ;;               fib(N1, F1),
  ;;               fib(N2, F2),
  ;;               F :- +(F1, F2).

  ;;  fib(10, F)?
  (let [fib-rules '[[(fib n f)
                     [(<= n 1)]
                     [(identity n) f]]
                    [(fib n f)
                     [(> n 1)]
                     [(- n 1) n1]
                     [(- n 2) n2]
                     (fib n1 f1)
                     (fib n2 f2)
                     [(+ f1 f2) f]]]]
    (t/is (= #{[55]}
             (xt/q (xt/db *api*)
                   {:find '[f]
                    :where '[(fib 10 f)]
                    :rules fib-rules})))

    (t/is (= #{[55]}
             (xt/q (xt/db *api*)
                   {:find '[f]
                    :where '[(fib n f)]
                    :args '[{n 10}]
                    :rules fib-rules})))))

;; Tests from
;; https://pdfs.semanticscholar.org/9374/f0da312f3ba77fa840071d68935a28cba364.pdf

(t/deftest test-datalog-paper-sgc
  (fix/transact! *api* [{:xt/id :ann :parent #{:dorothy :hilary}}
                        {:xt/id :bertrand :parent :dorothy}
                        {:xt/id :charles :parent :evelyn}
                        {:xt/id :dorothy :parent :george}
                        {:xt/id :evelyn :parent :george}
                        {:xt/id :fred}
                        {:xt/id :george}
                        {:xt/id :hilary}])

  ;; rl: sgc(X, X) :- person(X).
  ;; r2: sgc(X, Y) :- par(X, X1), sgc(X1, Y1), par(Y, Y1).
  (t/is (= #{[:ann :ann]
             [:bertrand :bertrand]
             [:charles :charles]
             [:dorothy :dorothy]
             [:evelyn :evelyn]
             [:fred :fred]
             [:george :george]
             [:hilary :hilary]
             [:dorothy :evelyn]
             [:evelyn :dorothy]
             [:charles :ann]
             [:ann :charles]
             [:ann :bertrand]
             [:bertrand :ann]
             [:charles :bertrand]
             [:bertrand :charles]}
           (xt/q (xt/db *api*)
                 '{:find [x y]
                   :where [(sgc x y)]
                   :rules [[(sgc x y)
                            [x :xt/id y]]
                           [(sgc x y)
                            [x :parent x1]
                            (sgc x1 y1)
                            [y :parent y1]]]}))))

(t/deftest test-datalog-paper-stratified-datalog
  ;; d(a, b), d(b, c) d(e, e)
  (fix/transact! *api* [{:xt/id :a :d :b}
                        {:xt/id :b :d :c}
                        {:xt/id :e :d :e}])

  ;; rl: p(X, Y) :- not q(X, Y), s(X, Y).
  ;; r2: q(K, Y) :- q(X, Z), q(Z, Y).
  ;; r3: q(X, Y) :- d(X, Y), not r(X, Y).
  ;; r4: r(X, Y) :- d(Y, X).
  ;; r5: s(X, Y) :- q(X, Z), q(Y, T), X != Y
  (let [rules '[[(p x y)
                 (not (q x y))
                 (s x y)]
                [(q x y)
                 (q x z)
                 (q z y)]
                [(q x y)
                 [x :d y]
                 (not (r x y))]
                [(r x y)
                 [y :d x]]
                [(s x y)
                 (q x z)
                 (q y t)
                 [(!= x y)]]]]

    (t/testing "stratum 1"
      (t/is (= #{[:b :a]
                 [:c :b]
                 [:e :e]}
               (xt/q (xt/db *api*)
                     {:find '[x y]
                      :where '[(r x y)]
                      :rules rules}))))

    (t/testing "stratum 2"
      (t/is (= #{[:a :b]
                 [:b :c]
                 [:a :c]
                 [:b :a]}
               (xt/q (xt/db *api*)
                     {:find '[x y]
                      :where '[(or (q x y)
                                   (s x y))]
                      :rules rules}))))

    (t/testing "stratum 3"
      (t/is (= #{[:b :a]}
               (xt/q (xt/db *api*)
                     {:find '[x y]
                      :where '[(p x y)]
                      :rules rules}))))))

(t/deftest test-query-against-empty-database-376
  (let [db (xt/db *api*)
        _ (t/is (not (xt/entity db :a)))
        _ (fix/transact! *api* [{:xt/id :a
                                 :arbitrary-key ["an untyped value" 123]
                                 :nested-map {"and values" :can-be-arbitrarily-nested}}])]
    (t/is (not (xt/entity db :a)))
    (t/is (xt/entity (xt/db *api*) :a))

    (with-open [res (xt/open-q db '{:find [x] :where [[x :arbitrary-key _]]})]
      (t/is (empty? (iterator-seq res))))

    (let [db (xt/db *api*)]
      (with-open [res (xt/open-q db '{:find [x] :where [[x :xt/id _]]})]
        (t/is (first (iterator-seq res)))))))

(t/deftest test-can-use-cons-in-query-377
  (fix/transact! *api* [{:xt/id :issue-377-test :name "TestName"}])
  (t/is (= #{[:issue-377-test]}
           (xt/q (xt/db *api*)
                 {:find ['e]
                  :where [['e :name 'n]
                          [(cons '= '(n "TestName"))]]}))))

(t/deftest test-query-keyword-to-entity-tx-351
  (fix/transact! *api* [{:xt/id :se.id/ASE,
                         :se/currency :currency/usd}
                        {:xt/id :ids/ticker-1000 ;;ids/ticker
                         :ticker/price 67
                         :ticker/market :se.id/ASE
                         :ticker/foo :bar}])

  (t/is (seq (xt/q (xt/db *api*) '{:find [p]
                                   :where
                                   [[e :xt/id someid]
                                    [e :ticker/price p]
                                    [(= p 67)]
                                    [e :ticker/market m2]
                                    [m2 :se/currency :currency/usd]]}))))

(t/deftest test-order-by-when-not-specified-in-return-418
  (fix/transact! *api* [{:xt/id :one
                         :val 1}
                        {:xt/id :two
                         :val 2}
                        {:xt/id :three
                         :val 3}])

  (t/is (= [:three :two :one]
           (mapv first (xt/q (xt/db *api*) '{:find [e v]
                                             :where [[x :xt/id e]
                                                     [x :val v]]
                                             :order-by [[v :desc]]}))))

  (t/is (= [:one :two :three]
           (mapv first (xt/q (xt/db *api*) '{:find [e v]
                                             :where [[x :xt/id e]
                                                     [x :val v]]
                                             :order-by [[v :asc]]}))))
  (t/is (thrown-with-msg? IllegalArgumentException
                          #"Order by requires an element from :find\. unreturned element:"
                          (xt/q (xt/db *api*) '{:find [e]
                                                :where [[x :xt/id e]
                                                        [x :val v]]
                                                :order-by [[v :asc]]})))
  (t/is (thrown-with-msg? IllegalArgumentException
                          #"Order by requires an element from :find\. unreturned element:"
                          (xt/q (xt/db *api*) '{:find [e]
                                                :where [[x :xt/id e]
                                                        [x :val v]]
                                                :order-by [[v :desc]]}))))

(t/deftest test-query-with-timeout-419
  (fix/transact! *api* [{:xt/id :ivan :name "Ivan" :last-name "Ivanov"}
                        {:xt/id :petr :name "Petr" :last-name "Petrov"}])

  (t/is (= #{[:ivan] [:petr]}
           (xt/q (xt/db *api*) '{:find [e]
                                 :where [[e :xt/id _]]
                                 :timeout 100})))

  (with-redefs [idx/layered-idx->seq (let [f idx/layered-idx->seq]
                                       (fn [& args]
                                         (lazy-seq
                                          (Thread/sleep 500)
                                          (apply f args))))]
    (t/is (thrown? TimeoutException
                   (xt/q (xt/db *api*) '{:find [e]
                                         :where [[e :xt/id _]]
                                         :timeout 100})))))

(defn long-running-pred [] (Thread/sleep 100) :ivan)

(t/deftest test-query-with-predicate-with-timeout-1654
  (t/is (thrown? TimeoutException
                 (xt/q (xt/db *api*) '{:find [e]
                                       :where [[(xtdb.query-test/long-running-pred) e]]
                                       :timeout 10}))))

(t/deftest test-nil-query-attribute-453
  (fix/transact! *api* [{:xt/id :id :this :that :these :those}])
  (t/is (thrown-with-msg? IllegalArgumentException
                          #"Query didn't match expected structure"
                          (xt/q (xt/db *api*) {:find ['e] :where [['_ nil 'e]]}))))

;; TODO: Unsure why this test has started failing, or how much it
;; matters?
#_(t/deftest test-entity-snapshot-520
    (let [ivan {:xt/id :ivan}
          _ (fix/transact! *api* [ivan])
          db (xt/db *api*)]
      (with-open [shared-db (xt/open-db *api*)]
        (t/is (= ivan (xt/entity shared-db :ivan)))
        (let [n 1000
              ;; TODO: was 1.4, introduced a bug?
              acceptable-snapshot-speedup 1.1
              factors (->> #(let [db-hit-ns-start (System/nanoTime)
                                  _ (xt/entity db :ivan)
                                  db-hit-ns (- (System/nanoTime) db-hit-ns-start)

                                  snapshot-hit-ns-start (System/nanoTime)
                                  _ (xt/entity shared-db :ivan)
                                  snapshot-hit-ns (- (System/nanoTime) snapshot-hit-ns-start)]

                              (double (/ db-hit-ns snapshot-hit-ns)))

                           (repeatedly n))]
          (t/is (>= (/ (reduce + factors) n) acceptable-snapshot-speedup))))))

(t/deftest test-greater-than-range-predicate-545
  (t/is (empty?
         (xt/q (xt/db *api*)
               '{:find [offset]
                 :where [[e :offset offset]
                         [(> offset -9223372036854775808)]] ;; Long/MIN_VALUE
                 :limit 1})))

  (t/testing "checking that entity ranges don't have same bug"
    (t/is (empty?
           (xt/q (xt/db *api*)
                 '{:find [offset]
                   :where [[e :offset offset]
                           [(= e :foo)]]
                   :limit 1})))))

(t/deftest test-query-result-cardinality-972
  (fix/transact! *api* [{:xt/id :ii :name "Ivan" :last-name "Ivanov", :age 20}
                        {:xt/id :pp :name "Petr" :last-name "Petrov", :age 20}
                        {:xt/id :ip :name "Ivan" :last-name "Petrov", :age 25}
                        {:xt/id :pi :name "Petr" :last-name "Ivanov", :age 30}])

  (let [db (xt/db *api*)]
    (t/testing "eager query without order-by returns set of results"
      (t/is (= #{[30] [25] [20]}
               (xt/q db '{:find [a] :where [[_ :age a]]}))))

    (t/testing "eager query with order-by returns a vector of results"
      (t/is (= [[30] [25] [20] [20]]
               (xt/q db '{:find [a] :where [[e :age a]], :order-by [[a :desc]]}))))

    (t/testing "lazy query does not return distinct results"
      (with-open [res (xt/open-q db '{:find [a] :where [[_ :age a]]})]
        (t/is (= [[20] [20] [25] [30]]
                 (sort (iterator-seq res))))))))

(t/deftest test-unsorted-args-697
  (fix/transact! *api* [{:xt/id :foo-some-bar-nil, :bar nil, :foo true}
                        {:xt/id :foo-nil-bar-some, :bar true, :foo nil}
                        {:xt/id :foo-some-bar-some, :foo true, :bar true}])

  (t/is (= #{[:foo-some-bar-nil] [:foo-nil-bar-some] [:foo-some-bar-some]}
           (xt/q (xt/db *api*)
                 '{:find [e]
                   :where [[e :foo f]
                           [e :bar g]]

                   :args [{f true, g true}
                          {f true, g nil}
                          {f nil, g true}]}))))

(t/deftest test-binds-args-before-entities
  (fix/submit+await-tx [[::xt/put {:xt/id :foo, :foo/type "type", :foo/id 1}]])

  (let [db (xt/db *api*)]
    (t/is (= ['m 'e]
             (->> (q/query-plan-for db
                                    {:find '[e]
                                     :where '[[e :foo/type "type"]
                                              [e :foo/id m]]
                                     :args [{'m 1}]})
                  :vars-in-join-order
                  (filter #{'m 'e}))))))

(t/deftest prioritises-single-value-in-vars-like-literals-1556
  (fix/submit+await-tx (->> (for [x (range 1000)]
                              [[::xt/put {:xt/id (str "thing-" x), :type :thing, :v x}]
                               [::xt/put {:xt/id (str "other-thing-" x), :type :other-thing, :v x}]])
                            (apply concat)))

  (let [db (xt/db *api*)]
    (t/is (= '[v e]
             (->> (q/query-plan-for db
                                    '{:find [e]
                                      :in [v]
                                      :where [[e :type :thing]
                                              [e :v v]]}
                                    [590])
                  :vars-in-join-order
                  (filter #{'e 'v}))))))

(t/deftest test-binds-against-false-arg-885
  (fix/submit+await-tx [[::xt/put {:xt/id :foo, :name "foo", :flag? false}]
                        [::xt/put {:xt/id :bar, :name "bar", :flag? true}]
                        [::xt/put {:xt/id :baz, :name "baz", :flag? nil}]])

  (t/is (= #{["foo" false]}
           (xt/q (xt/db *api*)
                 '{:find [?name flag?], :where [[?id :name ?name] [?id :flag? flag?]],
                   :args [{flag? false}]})))

  (t/is (= #{["bar" true]}
           (xt/q (xt/db *api*)
                 '{:find [?name flag?], :where [[?id :name ?name] [?id :flag? flag?]],
                   :args [{flag? true}]})))

  (t/is (= #{["baz" nil]}
           (xt/q (xt/db *api*)
                 '{:find [?name flag?], :where [[?id :name ?name] [?id :flag? flag?]],
                   :args [{flag? nil}]}))))

(t/deftest test-unused-args-still-binding-882
  (fix/submit+await-tx [[::xt/put {:xt/id :foo, :name "foo"}]])

  (t/is (= #{["foo" false]}
           (xt/q (xt/db *api*)
                 '{:find [?name foo], :where [[?id :name ?name]],
                   :args [{foo false}]})))

  (t/is (= #{["foo" true]}
           (xt/q (xt/db *api*)
                 '{:find [?name foo], :where [[?id :name ?name]],
                   :args [{foo true}]})))

  (t/is (= #{["foo" nil]}
           (xt/q (xt/db *api*)
                 '{:find [?name foo], :where [[?id :name ?name]],
                   :args [{foo nil}]}))))

(t/deftest test-leaf-vars-and-ors
  (fix/submit+await-tx [[::xt/put {:xt/id :foo, :field1 1 :field2 2}]])
  (t/is (= #{[:foo]}
           (xt/q (xt/db *api*)'{:find [?id], :where [[?id :field1 ?field1]
                                                     [?id :field2 ?field2]
                                                     (or (and [(boolean ?field2)]))]
                                :args []}))))

(t/deftest test-bound-rule-vars-946
  (fix/submit+await-tx (for [[id child-id] (partition 2 1 (range 101))]
                         [::xt/put {:xt/id id, :child child-id :name (str id "-" child-id)}]))

  (let [parent-id 50
        expected (set (for [[id child-id] (partition 2 1 (range (inc parent-id) 101))]
                        [(str id "-" child-id)]))
        expected-speedup-factor 2.5 ;; speedup also benefits from warm-up (i.e. test could be cleaner)
        free-vars-ns (let [free-vars-ns-start (System/nanoTime)
                           result (xt/q (xt/db *api*)
                                        {:find '[child-name]
                                         :where '[[parent :xt/id]
                                                  (child-of parent child)
                                                  [child :name child-name]]
                                         :rules '[[(child-of p c)
                                                   [p :child c]]
                                                  [(child-of p c)
                                                   [p :child c1]
                                                   (child-of c1 c)]]
                                         :args [{:parent parent-id}]})
                           free-vars-ns (- (System/nanoTime) free-vars-ns-start)]
                       (t/is (= expected result))
                       free-vars-ns)

        bound-vars-ns (let [bound-vars-ns-start (System/nanoTime)
                            result (xt/q (xt/db *api*)
                                         {:find '[child-name]
                                          :where '[[parent :xt/id]
                                                   (child-of parent child)
                                                   [child :name child-name]]
                                          :rules '[[(child-of [p] c)
                                                    [p :child c]]
                                                   [(child-of [p] c)
                                                    [p :child c1]
                                                    (child-of c1 c)]]
                                          :args [{:parent parent-id}]})
                            bound-vars-ns (- (System/nanoTime) bound-vars-ns-start)]
                        (t/is (= expected result))
                        bound-vars-ns)]
    (t/is (> (double (/ free-vars-ns bound-vars-ns)) expected-speedup-factor)
          (pr-str free-vars-ns " " bound-vars-ns))))

(t/deftest test-cardinality-join-order-avoids-cross-product
  (fix/transact! *api* (fix/people
                        (apply concat
                               (for [n (range 100)]
                                 [{:xt/id (keyword (str "dummy-" n))
                                   :my-name (str n)}
                                  {:xt/id (keyword (str "ivan-" n))
                                   :my-name "Ivan"
                                   :my-number n}
                                  {:xt/id (keyword (str "oleg-" n))
                                   :my-name "Oleg"
                                   :my-number n}]))))

  (t/testing "join order avoids cross product"
    (t/is (= '["Oleg" "Ivan" e1 n e2]
             (:vars-in-join-order
              (q/query-plan-for (xt/db *api*)
                                '{:find [e1]
                                  :where [[e1 :my-name "Ivan"]
                                          [e2 :my-name "Oleg"]
                                          [e1 :my-number n]
                                          [e2 :my-number n]]}))))))

(comment
  ;; repro for https://github.com/xtdb/xtdb/issues/443, don't have a solution yet though
  ;; replacing x with 0..50 and y with 0..100 takes a long time.

  (t/deftest multiple-joins-bug-443
    (doseq [x (range 5)]
      (xt/submit-tx *api* (for [y (range 10)]
                            [::xt/put {:xt/id (keyword (str "id" (+ (* x 1000) y))), :x x, :y y}])))

    (let [last-tx (xt/submit-tx *api* [[::xt/put {:xt/id :match, :x 4, :y 8}]])]
      (xt/await-tx *api* last-tx))

    (t/is (= #{[:id4008 :match 4 8] [:match :id4008 4 8]}
             (xt/q (xt/db *api*) '{:find [e1 e2 x y]
                                   :where [[e1 :x x]
                                           [e1 :y y]
                                           [e2 :x x]
                                           [e2 :y y]
                                           [(!= e1 e2)]]})))))

(t/deftest resurrecting-doc-1127
  (let [query {:find '[n]
               :where '[[n :name "hello"]
                        [n :age 17]]}]

    (fix/submit+await-tx [[::xt/put {:xt/id :my-id, :name "hello", :age 17}]])

    (t/is (= #{[:my-id]} (xt/q (xt/db *api*) query)))

    (fix/submit+await-tx [[::xt/delete :my-id]])

    (t/is (= #{} (xt/q (xt/db *api*) query)))))

(t/deftest hashing-quoted-lists-1197
  (fix/submit+await-tx [[::xt/put {:xt/id :foo, :a-list '(1 2 3)}]])

  (t/is (= #{[:foo]}
           (xt/q (xt/db *api*)
                 '{:find [?e]
                   :where [[?e :a-list (1 2 3)]]}))))

(t/deftest falsey-values-bind-with-rules
  ;; resolved in 20.05-1.8.4-alpha
  (fix/submit+await-tx [[::xt/put {:xt/id :a :att nil}]
                        [::xt/put {:xt/id :b :att :foo}]
                        [::xt/put {:xt/id :c :att false}]
                        [::xt/put {:xt/id :d}]])
  (t/is (= #{[:a] [:b] [:c]}
           (xt/q (xt/db *api*)
                 '{:find [?e]
                   :where [[?e :att ?v]
                           [(any? ?v)]
                           (or [(nil? ?v)]
                               [(false? ?v)]
                               [(some? ?v)])
                           (or-join [?v]
                                    (is-truthy? ?v)
                                    [(!= :foo ?v)])]
                   :rules [[(is-false? [?v])
                            [(false? ?v)]]
                           [(is-nil? [?v])
                            [(nil? ?v)]]
                           [(is-truthy? [?v])
                            (not (is-nil? ?v))
                            (not (is-false? ?v))]]}))))

(t/deftest closing-node-interrupts-open-snapshots
  (t/testing "open-db left open"
    (let [node (xt/start-node {})]
      @(try
         (let [fut (future
                     (with-open [_db (xt/open-db node)]
                       (t/is (thrown? InterruptedException (Thread/sleep 2000)))))]
           (Thread/sleep 100)
           fut)
         (finally
           (.close node)))))

  (t/testing "long-running query"
    (let [node (xt/start-node {})]
      @(try
         (let [fut (future
                     (with-redefs [idx/layered-idx->seq (let [f idx/layered-idx->seq]
                                                          (fn [& args]
                                                            (Thread/sleep 1000)
                                                            (apply f args)))]
                       (t/is (thrown? InterruptedException
                                      (xt/q (xt/db node)
                                            '{:find [?e] :where [[?e :xt/id]]})))))]
           (Thread/sleep 100)
           fut)
         (finally
           (.close node))))))

(t/deftest nil-in-entity-position-shouldnt-yield-results-1486
  (fix/submit+await-tx [[::xt/put {:xt/id 1 :foo nil}]
                        [::xt/put {:xt/id 2 :foo nil}]])

  (t/is (= #{}
           (xt/q (xt/db *api*)
                 '{:find [?v]
                   :where [[nil :foo ?v]]})))

  (t/is (= #{}
           (xt/q (xt/db *api*)
                 '{:find [?v]
                   :where [[#{nil} :foo ?v]]}))))

(t/deftest literal-nil-value-in-triple-clause-should-only-match-nil-1487
  (fix/submit+await-tx [[::xt/put {:xt/id 1 :foo nil}]
                        [::xt/put {:xt/id 2 :foo 2}]])
  (t/is (= #{[1] [2]}
           (xt/q (xt/db *api*)
                 '{:find [?e]
                   :where [[?e :foo]]})))
  (t/is (= #{[1] [2]}
           (xt/q (xt/db *api*)
                 '{:find [?e]
                   :where [[?e :foo _]]})))
  (t/is (= #{[1]}
           (xt/q (xt/db *api*)
                 '{:find [?e]
                   :where [[?e :foo nil]]})))
  (t/is (= #{[1]}
           (xt/q (xt/db *api*)
                 '{:find [?e]
                   :where [[?e :foo #{nil}]]})))
  (t/is (= #{[1] [2]}
           (xt/q (xt/db *api*)
                 '{:find [?e]
                   :where [[?e :foo #{nil 2}]]})))
  (t/is (= #{}
           (xt/q (xt/db *api*)
                 '{:find [?e]
                   :where [[?e :foo #{}]]}))))

(t/deftest picks-more-selective-join-order
  (let [query '{:find [?e ?name]
                :in [?name ?type]
                :where [[?e :name ?name]
                        [?e :type ?type]]}]
    (fix/submit+await-tx (conj (for [idx (range 1000)]
                                 [::xt/put {:xt/id (UUID/randomUUID)
                                            :type :person
                                            :name (str "person-" idx)}])
                               [::xt/put {:xt/id (UUID/randomUUID)
                                          :type "extra type"}]))

    (t/is (= '[?type ?name ?e]
             (-> (q/query-plan-for (xt/db *api*) query
                                   ["person-104" :person])
                 :vars-in-join-order)))

    (fix/submit+await-tx [[::xt/put {:xt/id (UUID/randomUUID)
                                     :name "extra name"}]
                          [::xt/put {:xt/id (UUID/randomUUID)
                                     :name "another extra name"}]])

    (t/is (= '[?type ?name ?e]
             (-> (q/query-plan-for (xt/db *api*) query
                                   ["person-104" :person])
                 :vars-in-join-order)))))

(defn- date->inverted-long [^Date d]
  (* -1 (.getTime d)))

(defn- inverted-long->date [l]
  (Date. ^Long (* -1 l)))

(t/deftest range-constraint-ordering-behaviours
  (fix/transact!
   *api*
   [{:xt/id :a :i -7 :j 30 :t #inst "2021-05-17" :t2 (date->inverted-long #inst "2021-05-17")}
    {:xt/id :b :i 14 :j 25 :t #inst "2021-05-19" :t2 (date->inverted-long #inst "2021-05-19")}
    {:xt/id :c :i 14 :j 14 :t #inst "2021-05-19" :t2 (date->inverted-long #inst "2021-05-19")}
    {:xt/id :d :i 25 :j 14 :t #inst "2021-05-21" :t2 (date->inverted-long #inst "2021-05-21")}
    {:xt/id :e :i 30 :j -7 :t #inst "2021-05-22" :t2 (date->inverted-long #inst "2021-05-22")}])

  (let [db (xt/db *api*)]
    (t/testing "eager query returns an unsorted set"
      (t/is (= #{[:a] [:b] [:c] [:d] [:e]}
               (xt/q db '{:find [e]
                          :where [[e :i i]
                                  [(> i -10)]]}))))

    (t/testing "lazy query with range constraint predicate returns the stored index order"
      (with-open [res (xt/open-q db '{:find [e]
                                      :where [[e :i i]
                                              [(> i -10)]]})]
        (t/is (= '([:a] [:b] [:c] [:d] [:e])
                 (iterator-seq res)))))

    (t/testing "eager query with range constraint predicate and :limit returns a vector with the stored index order"
      (t/is (= [[:a] [:b] [:c]]
               (xt/q db '{:find [e]
                          :limit 3
                          :where [[e :i i]
                                  [(> i -10)]]}))))

    (t/testing "eager query without :limit always returns a (deduplicated) set"
      (t/is (= #{[-7] [14] [25] [30]}
               (xt/q db '{:find [i]
                          :where [[e :i i]
                                  [(> i -10)]]}))))

    (t/testing "lazy query returns duplicates as they exist in the index"
      (with-open [res (xt/open-q db '{:find [i]
                                      :where [[e :i i]
                                              [(> i -10)]]})]
        (t/is (= '([-7] [14] [14] [25] [30])
                 (iterator-seq res)))))

    (t/testing "eager query with :limit returns a vector with duplicates (i.e. a bag)"
      (t/is (= [[-7] [14] [14] [25] [30]]
               (xt/q db '{:find [i]
                          :limit 5
                          :where [[e :i i]
                                  [(> i -10)]]}))))

    (t/testing "all range constraint predicates produces the ascending order from the index"
      (with-open [res (xt/open-q db '{:find [e]
                                      :where [[e :i i]
                                              [(< i 100)]]})]
        (t/is (= '([:a] [:b] [:c] [:d] [:e])
                 (iterator-seq res)))))

    (t/testing "ordering of tuples based on duplicates from the index will rely on highly unpredictable aspects of the join order (e.g. ~arbitrary sorting of entity ID) as the tie-break"
      (with-open [res (xt/open-q db '{:find [j e]
                                      :where [[e :j j]
                                              [(> j -10)]]})]
        (t/is (= '([-7 :e] [14 :c] [14 :d] [25 :b] [30 :a])
                 (iterator-seq res)))))

    (t/testing "range constraint predicates combine sensibly"
      (with-open [res (xt/open-q db '{:find [e]
                                      :where [[e :i i]
                                              [(> i -6)]
                                              [(> i -10)]
                                              [(< i 24)]]})]
        (t/is (= '([:b] [:c])
                 (iterator-seq res)))))

    (t/testing "range constraint predicates work exactly the same with Dates and other first-class sorted value types (see xtdb.codec)"
      (with-open [res (xt/open-q db '{:find [e]
                                      :where [[e :t t]
                                              [(> t #inst "2021-05-08")]]})]
        (t/is (= '([:a] [:b] [:c] [:d] [:e])
                 (iterator-seq res)))))

    (t/testing "range constraints only ever consume values in the stored ascending order, invert the original values to achieve the descending order"
      (with-open [res (xt/open-q db {:find '[t2]
                                     :where ['[e :t2 t2]
                                             [(list '> 't2 (date->inverted-long #inst "2021-05-23"))]]})]
        (t/is (= '(#inst "2021-05-22T00:00:00.000-00:00"
                   #inst "2021-05-21T00:00:00.000-00:00"
                   #inst "2021-05-19T00:00:00.000-00:00"
                   #inst "2021-05-19T00:00:00.000-00:00"
                   #inst "2021-05-17T00:00:00.000-00:00")
                 (map (comp inverted-long->date first) (iterator-seq res))))))))

(t/deftest circular-deps-test-1523
  (fix/submit+await-tx [[::xt/put {:xt/id :ivan :name "Ivan", :foo :foo}]
                        [::xt/put {:xt/id :foo, :bar :bar}]])

  ;; failed with 'circular dependency between ?foo and ?a'
  (with-open [db (xt/open-db *api*)]
    (t/is (= #{["Ivan" :bar]}
             (xt/q db '{:find [?a-name ?bar]
                        :where [[?a :name ?a-name]
                                [?a :foo ?foo-val]
                                [(identity ?foo-val) ?foo]
                                [?foo :bar ?bar]]})))))

(t/deftest test-rules-binding-1569
  (fix/submit+await-tx [[::xt/put {:xt/id :a-1, :next :a-2}]
                        [::xt/put {:xt/id :a-2, :next :a-3}]
                        [::xt/put {:xt/id :a-3, :next :a-4}]
                        [::xt/put {:xt/id :a-4, :next :a-1}]

                        [::xt/put {:xt/id :b-1, :next :b-2}]
                        [::xt/put {:xt/id :b-2, :next :b-3}]
                        [::xt/put {:xt/id :b-3, :next :b-4}]
                        [::xt/put {:xt/id :b-4, :next :b-5}]
                        [::xt/put {:xt/id :b-5, :next :b-1}]])

  #_ ; FIXME this returns all the B's too
  (t/is (= #{[:a-3] [:a-2] [:a-1] [:a-4]}
           (xt/q (xt/db *api*)
                 '{:find [node]
                   :where [[end :xt/id :a-1]
                           (pointsTo node end)]
                   :rules [[(pointsTo start end)
                            [start :next end]]
                           [(pointsTo start end)
                            [start :next intermediate]
                            (pointsTo end intermediate)]]}))))

;; these aren't necessarily the best join orders, but just to give you a heads-up if something changes
;; only does the top-level queries, but again, hopefully enough to spot an unexpected change.

(defn- test-join-orders [queries
                         {:keys [join-order-file
                                 stats-file
                                 regen-join-order-file?
                                 ->query-plan]
                          :or {->query-plan q/query-plan-for}}]
  (let [db (xt/db *api*)
        attr-stats (fix/->attr-stats stats-file)
        expected-join-orders (mapv read-string (str/split-lines (slurp join-order-file)))]
    (with-redefs [q/->stats (constantly attr-stats)]
      (let [actual-join-orders (->> queries
                                    (mapv (comp :vars-in-join-order #(->query-plan db %))))]
        (when regen-join-order-file?
          (with-open [w (io/writer join-order-file)]
            (doseq [join-order actual-join-orders]
              (.write w (prn-str join-order)))))

        (doall
         (->> (map vector expected-join-orders actual-join-orders)
              (map-indexed (fn [q-idx [expected-order actual-order]]
                             (t/is (= expected-order actual-order)
                                   (str "Q" (inc q-idx)))))))))))

(t/deftest test-tpch-join-orders
  (test-join-orders tpch/tpch-queries
                    {;:regen-join-order-file? true
                     :join-order-file (io/resource "xtdb/fixtures/tpch/current-join-orders.edn")
                     :stats-file (io/resource "xtdb/fixtures/tpch/attr-stats.edn")
                     :->query-plan (fn [db q]
                                     (q/query-plan-for db q (::tpch/in-args (meta q))))}))

(t/deftest test-watdiv-join-orders
  (letfn [(sanitise-join-order-var [v]
            (cond-> v
              ;; assuming only one self-join per variable in each query
              (and (symbol? v) (re-find #"^self-join_" (str v)))
              (-> str (str/replace #"_\d+$" "_<gensym>") symbol)))]

    (test-join-orders (->> (slurp (io/resource "xtdb/fixtures/watdiv/100-queries.edn"))
                           (str/split-lines)
                           (map read-string))
                      {;:regen-join-order-file? true
                       :join-order-file (io/resource "xtdb/fixtures/watdiv/current-join-orders.edn")
                       :stats-file (io/resource "xtdb/fixtures/watdiv/attr-stats.edn")
                       :->query-plan (fn [db q]
                                       (-> (q/query-plan-for db q)
                                           (update :vars-in-join-order
                                                   #(mapv sanitise-join-order-var %))))})))

(t/deftest test-same-literal-treated-as-separate-variables-1695
  (fix/submit+await-tx [[::xt/put {:xt/id :x
                                   :value :v1
                                   :y :y}]
                        [::xt/put {:xt/id :y
                                   :value :v2}]])

  (t/is (= #{[:x :y]}
           (xt/q (xt/db *api*)
                 '{:find [x y]
                   :where [[x :y y]
                           [x :value #{:v1 :v2}]
                           [y :value #{:v1 :v2 :v3}]]})))

  #_ ; FIXME
  (t/is (= #{[:x :y]}
           (xt/q (xt/db *api*)
                 '{:find [x y]
                   :where [[x :y y]
                           [x :value #{:v1 :v2}]
                           [y :value #{:v1 :v2}]]}))))

(t/deftest test-pred-variable-resolution-error-1731
  (fix/submit+await-tx [[::xt/put {:xt/id :foo, :type :some-value}]])

  (t/is (= #{[:foo]}
           (xt/q (xt/db *api*)
                 '{:find [?e]
                   :where [[(get-attr ?e :type) [?type]]
                           [(= ?type :some-value)]

                           (or [?e :type :some-value]
                               [?e :type :other])]}))))

(t/deftest handles-start-tx-time-between-txs-1732
  (fix/submit+await-tx [[::xt/put {:xt/id :foo, :v 0}]])
  (Thread/sleep 10)
  (let [{::xt/keys [^Date tx-time]} (fix/submit+await-tx [[::xt/put {:xt/id :foo, :v 1}]])]
    (t/is (= [{::xt/tx-id 1,
               ::xt/doc {:v 1, :xt/id :foo}}]
             (->> (xt/entity-history (xt/db *api*) :foo :asc,
                                     {:with-docs? true, :with-corrections? true
                                      :start-tx {::xt/tx-time (Date. (dec (.getTime tx-time)))}})
                  (map #(select-keys % [::xt/tx-id ::xt/doc])))))))

(t/deftest external-sort-test
  (with-redefs [xio/default-external-sort-part-size 1024]
    (fix/submit+await-tx (for [i (range 1025)]
                           [::xt/put {:xt/id i, :some/data i}]))
    (t/is (= (mapv vector (range 1025))
             (xt/q (xt/db *api*)
                   '{:find [i]
                     :where [[?e :some/data i]]
                     :order-by [[i :asc]]})))))

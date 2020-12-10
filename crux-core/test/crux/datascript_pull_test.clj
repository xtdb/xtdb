(ns crux.datascript-pull-test
  (:require [clojure.test :as t]
            [crux.api :as crux]
            [crux.fixtures :as fix :refer [*api*]]))

;; adapted from https://github.com/tonsky/datascript/blob/1.0.1/test/datascript/test/pull_api.cljc

(t/use-fixtures :each fix/with-node)

(def people-docs
  [{:crux.db/id :petr, :name "Petr", :aka #{"Devil" "Tupen"}}
   {:crux.db/id :david, :name "David", :parent #{:petr}}
   {:crux.db/id :thomas, :name "Thomas", :parent #{:petr}}
   {:crux.db/id :lucy, :name "Lucy" :friend #{:elizabeth}, :enemy #{:matthew}}
   {:crux.db/id :elizabeth, :name "Elizabeth" :friend #{:matthew}, :enemy #{:eunan}}
   {:crux.db/id :matthew, :name "Matthew", :parent #{:thomas}, :friend #{:eunan}, :enemy #{:kerri}}
   {:crux.db/id :eunan, :name "Eunan", :friend #{:kerri}, :enemy #{:lucy}}
   {:crux.db/id :kerri, :name "Kerri"}
   {:crux.db/id :rebecca, :name "Rebecca"}])

(def part-docs
  [{:crux.db/id :a, :part-name "Part A"}
   {:crux.db/id :a.a, :part-name "Part A.A", :part-of :a}
   {:crux.db/id :a.a.a, :part-name "Part A.A.A", :part-of :a.a}
   {:crux.db/id :a.a.a.a, :part-name "Part A.A.A.A", :part-of :a.a.a}
   {:crux.db/id :a.a.a.b, :part-name "Part A.A.A.B", :part-of :a.a.a}
   {:crux.db/id :a.b, :part-name "Part A.B", :part-of :a}
   {:crux.db/id :a.b.a, :part-name "Part A.B.A", :part-of :a.b}
   {:crux.db/id :a.b.a.a, :part-name "Part A.B.A.A", :part-of :a.b.a}
   {:crux.db/id :a.b.a.b, :part-name "Part A.B.A.B", :part-of :a.b.a}])

(defn submit-test-docs [docs]
  (fix/submit+await-tx (for [doc docs]
                         [:crux.tx/put doc]))
  (crux/db *api*))

(t/deftest test-pull-attr-spec
  (let [db (submit-test-docs people-docs)]
    (t/is (= #{[{:name "Petr" :aka #{"Devil" "Tupen"}}]}
             (crux/q db '{:find [(eql/project ?e [:name :aka])]
                          :where [[?e :crux.db/id :petr]]})))

    (t/is (= #{[{:name "Matthew" :parent [{:crux.db/id :thomas}] :crux.db/id :matthew}]}
             (crux/q db '{:find [(eql/project ?e [:name :crux.db/id {:parent [:crux.db/id]}])]
                          :where [[?e :crux.db/id :matthew]]})))

    (t/is (= #{[{:name "Petr"}] [{:name "Elizabeth"}] [{:name "Eunan"}] [{:name "Rebecca"}]}
             (crux/q db '{:find [(eql/project ?e [:name])]
                          :where [[?e :crux.db/id #{:petr :elizabeth :eunan :rebecca}]]})))))

(t/deftest test-pull-reverse-attr-spec
  (let [db (submit-test-docs people-docs)]
    ;; TODO reverse props
    #_
    (t/is (= #{[{:name "Thomas" :_parent [:matthew]}]}
             (crux/q db '{:find [(eql/project ?e [:name :_parent])]
                          :where [[?e :crux.db/id :thomas]]})))

    #_
    (t/is (= {:name "Petr" :_parent [:david :thomas]}
             (d/pull test-db '[:name :_parent] 1)))

    (t/is (= #{[{:name "Thomas" :_parent [{:name "Matthew"}]}]}
             (crux/q db '{:find [(eql/project ?e [:name {:_parent [:name]}])]
                          :where [[?e :crux.db/id :thomas]]})))

    (t/is (= #{[{:name "Petr" :_parent [{:name "David"} {:name "Thomas"}]}]}
             (crux/q db '{:find [(eql/project ?e [:name {:_parent [:name]}])]
                          :where [[?e :crux.db/id :petr]]})))))

;; we don't have the concept of 'components'
#_
(t/deftest test-pull-component-attr
  (let [parts {:name "Part A",
               :part
               [{:db/id 11
                 :name "Part A.A",
                 :part
                 [{:db/id 12
                   :name "Part A.A.A",
                   :part
                   [{:db/id 13 :name "Part A.A.A.A"}
                    {:db/id 14 :name "Part A.A.A.B"}]}]}
                {:db/id 15
                 :name "Part A.B",
                 :part
                 [{:db/id 16
                   :name "Part A.B.A",
                   :part
                   [{:db/id 17 :name "Part A.B.A.A"}
                    {:db/id 18 :name "Part A.B.A.B"}]}]}]}
        rpart (update-in parts [:part 0 :part 0 :part]
                         (partial into [{:db/id 10}]))
        recdb (d/init-db
               (concat test-datoms [(d/datom 12 :part 10)])
               test-schema)

        mutdb (d/init-db
               (concat test-datoms [(d/datom 12 :part 10)
                                    (d/datom 12 :spec 10)
                                    (d/datom 10 :spec 13)
                                    (d/datom 13 :spec 12)])
               test-schema)]

    (t/testing "Component entities are expanded recursively"
      (t/is (= parts (d/pull test-db '[:name :part] 10))))

    (t/testing "Reverse component references yield a single result"
      (t/is (= {:name "Part A.A" :_part {:db/id 10}}
               (d/pull test-db [:name :_part] 11)))

      (t/is (= {:name "Part A.A" :_part {:name "Part A"}}
             (d/pull test-db [:name {:_part [:name]}] 11))))

    (t/testing "Like explicit recursion, expansion will not allow loops"
      (t/is (= rpart (d/pull recdb '[:name :part] 10))))))

(t/deftest test-pull-wildcard
  (let [db (submit-test-docs people-docs)]
    (t/is (= #{[{:crux.db/id :petr :name "Petr" :aka #{"Devil" "Tupen"}}]}
             (crux/q db '{:find [(eql/project ?e [*])]
                          :where [[?e :crux.db/id :petr]]})))

    (t/is (= #{[{:crux.db/id :david, :name "David", :parent [{:crux.db/id :petr}]}]}
             (crux/q db '{:find [(eql/project ?e [* {:parent [:crux.db/id]}])]
                          :where [[?e :crux.db/id :david]]})))))

;; TODO `:limit`
#_
(t/deftest test-pull-limit
  (let [db (d/init-db
            (concat
             test-datoms
             [(d/datom 4 :friend 5)
              (d/datom 4 :friend 6)
              (d/datom 4 :friend 7)
              (d/datom 4 :friend 8)]
             (for [idx (range 2000)]
               (d/datom 8 :aka (str "aka-" idx))))
            test-schema)]

    (t/testing "Without an explicit limit, the default is 1000"
      (t/is (= 1000 (->> (d/pull db '[:aka] 8) :aka count))))

    (t/testing "Explicit limit can reduce the default"
      (t/is (= 500 (->> (d/pull db '[(limit :aka 500)] 8) :aka count)))
      (t/is (= 500 (->> (d/pull db '[[:aka :limit 500]] 8) :aka count))))

    (t/testing "Explicit limit can increase the default"
      (t/is (= 1500 (->> (d/pull db '[(limit :aka 1500)] 8) :aka count))))

    (t/testing "A nil limit produces unlimited results"
      (t/is (= 2000 (->> (d/pull db '[(limit :aka nil)] 8) :aka count))))

    (t/testing "Limits can be used as map specification keys"
      (t/is (= {:name "Lucy"
                :friend [{:name "Elizabeth"} {:name "Matthew"}]}
               (d/pull db '[:name {(limit :friend 2) [:name]}] 4))))))

(t/deftest test-pull-default
  (let [db (submit-test-docs people-docs)]
    ;; Datascript returns nil here, because there's no matching datom
    (t/is (= #{[{}]}
             (crux/q db '{:find [(eql/project ?e [:foo])]
                          :where [[?e :crux.db/id :petr]]}))
          "Missing attrs return empty map")

    ;; TODO `:default`
    #_
    (t/is (= #{[{:foo "bar"}]}
             (crux/q db '{:find [(eql/project ?e [(:foo {:default "bar"})])]
                          :where [[?e :crux.db/id :petr]]}))
          "A default can be used to replace nil results")))

(t/deftest test-pull-as
  (let [db (submit-test-docs people-docs)]
    (t/is (= #{[{"Name" "Petr", :alias #{"Devil" "Tupen"}}]}
             (crux/q db '{:find [(eql/project ?e [(:name {:as "Name"})
                                                  (:aka {:as :alias})])]
                          :where [[?e :crux.db/id :petr]]})))))

;; TODO `:default`
#_
(t/deftest test-pull-attr-with-opts
  (let [db (submit-test-docs people-docs)]
    (t/is (= #{[{"Name" "Nothing"}]}
             (crux/q db '{:find [(eql/project ?e [(:x {:as "Name", :default "Nothing"})])]
                          :where [[?e :crux.db/id :petr]]})))))

(t/deftest test-pull-map
  (let [db (do
             (submit-test-docs people-docs)
             (submit-test-docs part-docs))]
    (t/is (= #{[{:name "Matthew" :parent [{:name "Thomas"}]}]}
             (crux/q db '{:find [(eql/project ?e [:name {:parent [:name]}])]
                          :where [[?e :crux.db/id :matthew]]}))
          "Single attrs yield a map")

    (t/is (= #{[{:name "Petr" :children [{:name "David"} {:name "Thomas"}]}]}
             (crux/q db '{:find [(eql/project ?e [:name {(:_parent {:as :children}) [:name]}])]
                          :where [[?e :crux.db/id :petr]]}))
          "Multi attrs yield a collection of maps")

    (t/is (= #{[{:name "Petr"}]}
             (crux/q db '{:find [(eql/project ?e [:name {:parent [:name]}])]
                          :where [[?e :crux.db/id :petr]]}))
          "Missing attrs are dropped")

    ;; Datascript returns `{:name "Petr", :children []}` for this, but we've matched documents,
    ;; so we can say 'there are two children, but they don't have `:foo`'
    (t/is (= #{[{:name "Petr" :children [{} {}]}]}
             (crux/q db '{:find [(eql/project ?e [:name {(:_parent {:as :children}) [:foo]}])]
                          :where [[?e :crux.db/id :petr]]}))
          "Non matching results are removed from collections")

    (t/testing "Map specs can override component expansion"
      (let [parts #{[{:part-name "Part A" :_part-of [{:part-name "Part A.A"}
                                                     {:part-name "Part A.B"}]}]}]
        (t/is (= parts
                 (crux/q db '{:find [(eql/project ?e [:part-name {:_part-of [:part-name]}])]
                              :where [[?e :crux.db/id :a]]})))

        ;; TODO temporarily feature flagging recursion until it passes Datascript tests, see #1220
        #_
        (t/is (= parts
                 (crux/q db '{:find [(eql/project ?e [:part-name {:_part-of 1}])]
                              :where [[?e :crux.db/id :a]]})))))))

;; TODO temporarily feature flagging recursion until it passes Datascript tests, see #1220
#_
(t/deftest test-pull-recursion
  (let [db (submit-test-docs people-docs)
        friends {:name "Lucy"
                 :friend [{:name "Elizabeth"
                           :friend [{:name "Matthew"
                                     :friend [{:name "Eunan"
                                               :friend [{:name "Kerri"}]}]}]}]}
        enemies {:name "Lucy"
                 :friend [{:name "Elizabeth"
                           :friend [{:name "Matthew", :enemy [{:name "Kerri"}]}]
                           :enemy [{:name "Eunan",
                                    :friend [{:name "Kerri"}]
                                    :enemy [{:name "Lucy" :friend [{:db/id 5}]}]}]}]
                 :enemy [{:name "Matthew"
                          :friend [{:name "Eunan"
                                    :friend [{:name "Kerri"}]
                                    :enemy [{:name "Lucy"
                                             :friend [{:name "Elizabeth"}]}]}]
                          :enemy [{:name "Kerri"}]}]}]

    (t/is (= #{[friends]}
             (crux/q db '{:find [(eql/project ?e [:name {:friend ...}])]
                          :where [[?e :crux.db/id :lucy]]}))
          "Infinite recursion")

    ;; EQL doesn't support this
    #_
    (t/is (= #{[enemies]}
             (crux/q db '{:find [(eql/project ?e [:name {:friend 2, :enemy 2}])]
                          :where [[?e :crux.db/id :lucy]]}))
          "Multiple recursion specs in one pattern")

    ;; TODO recursion cycles
    #_
    (let [db (crux/with-tx db [:crux.tx/put [{:crux.db/id :kerri, :name "Kerri", :friend #{:lucy}}]])]
      (t/is (= (update-in friends (take 8 (cycle [:friend 0]))
                          assoc :friend [{:name "Lucy" :friend [:elizabeth]}])
               (crux/q db '{:find [(eql/project ?e [:name {:friend ...}])]
                            :where [[?e :crux.db/id :lucy]]}))
            "Cycles are handled by returning only the :db/id of entities which have been seen before"))))

;; TODO recursion cycles
#_
(t/deftest test-dual-recursion
  (let [db (submit-test-docs [{:crux.db/id 1, :part 2, :spec 2}
                              {:crux.db/id 2, :part 3, :spec 1}
                              {:crux.db/id 3, :part 1}])]
    (t/is (= #{[{:crux.db/id 1,
                 :spec {:crux.db/id 2
                        :spec {:crux.db/id 1,
                               :spec {:crux.db/id 2}, :part {:crux.db/id 2}}
                        :part {:crux.db/id 3,
                               :part {:crux.db/id 1,
                                      :spec {:crux.db/id 2},
                                      :part {:crux.db/id 2}}}}
                 :part {:crux.db/id 2
                        :spec {:crux.db/id 1, :spec {:crux.db/id 2}, :part {:crux.db/id 2}}
                        :part {:crux.db/id 3,
                               :part {:crux.db/id 1,
                                      :spec {:crux.db/id 2},
                                      :part {:crux.db/id 2}}}}}]}
             (crux/q db '{:find [(eql/project ?e [:crux.db/id {:part ...} {:spec ...}])]
                          :where [[?e :crux.db/id 1]]})))))

#_
(t/deftest test-deep-recursion
  ;; TODO Datascript gets to depth 1500 here; we StackOverflowError after ~400
  (let [start 100
        depth 1500
        db (submit-test-docs (conj (for [idx (range start depth)]
                                     {:crux.db/id idx
                                      :name (str "Person-" idx)
                                      :friend [(inc idx)]})
                                   {:crux.db/id depth
                                    :name (str "Person-" depth)}))
        path (->> [:friend 0]
                  (repeat (dec (- (inc depth) start)))
                  (into [] cat))]

    (t/is (= (str "Person-" depth)
             (-> (crux/q db
                         '{:find [(eql/project ?e [:name {:friend ...}])]
                           :in [start]
                           :where [[?e :crux.db/id start]]}
                         start)
                 ffirst
                 (get-in path)
                 :name)))))

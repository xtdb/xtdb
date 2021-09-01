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
             (crux/q db '{:find [(pull ?e [:name :aka])]
                          :where [[?e :crux.db/id :petr]]})))

    (t/is (= #{[{:name "Matthew" :parent [{:crux.db/id :thomas}] :crux.db/id :matthew}]}
             (crux/q db '{:find [(pull ?e [:name :crux.db/id {:parent [:crux.db/id]}])]
                          :where [[?e :crux.db/id :matthew]]})))

    (t/is (= #{[{:name "Petr"}] [{:name "Elizabeth"}] [{:name "Eunan"}] [{:name "Rebecca"}]}
             (crux/q db '{:find [(pull ?e [:name])]
                          :where [[?e :crux.db/id #{:petr :elizabeth :eunan :rebecca}]]})))))

(t/deftest test-pull-reverse-attr-spec
  (let [db (submit-test-docs people-docs)]
    ;; TODO reverse props
    #_
    (t/is (= #{[{:name "Thomas" :_parent [:matthew]}]}
             (crux/q db '{:find [(pull ?e [:name :_parent])]
                          :where [[?e :crux.db/id :thomas]]})))

    #_
    (t/is (= {:name "Petr" :_parent [:david :thomas]}
             (d/pull test-db '[:name :_parent] 1)))

    (t/is (= #{[{:name "Thomas" :_parent [{:name "Matthew"}]}]}
             (crux/q db '{:find [(pull ?e [:name {:_parent [:name]}])]
                          :where [[?e :crux.db/id :thomas]]})))

    (t/is (= #{[{:name "Petr" :_parent [{:name "David"} {:name "Thomas"}]}]}
             (crux/q db '{:find [(pull ?e [:name {:_parent [:name]}])]
                          :where [[?e :crux.db/id :petr]]})))))

(t/deftest test-pull-wildcard
  (let [db (submit-test-docs people-docs)]
    (t/is (= #{[{:crux.db/id :petr :name "Petr" :aka #{"Devil" "Tupen"}}]}
             (crux/q db '{:find [(pull ?e [*])]
                          :where [[?e :crux.db/id :petr]]})))

    (t/is (= #{[{:crux.db/id :david, :name "David", :parent [{:crux.db/id :petr}]}]}
             (crux/q db '{:find [(pull ?e [* {:parent [:crux.db/id]}])]
                          :where [[?e :crux.db/id :david]]})))))

(t/deftest test-pull-limit
  (let [db (submit-test-docs (concat people-docs
                                     [{:crux.db/id :elizabeth,
                                       :name "Elizabeth"
                                       :friend #{:matthew :eunan :kerri :rebecca}
                                       :aka (into #{} (map #(str "liz-" %)) (range 2000))}]))]

    ;; We don't have a default limit, because our query engine doesn't
    #_
    (t/testing "Without an explicit limit, the default is 1000"
      (t/is (= 1000 (->> (d/pull db '[:aka] 8) :aka count))))

    #_
    (t/testing "A nil limit produces unlimited results"
      (t/is (= 2000 (->> (d/pull db '[(limit :aka nil)] 8) :aka count))))

    (t/testing "Explicit limit can reduce the default"
      (t/is (= 500 (->> (crux/q db '{:find [(pull ?e [(:aka {:limit 500})])]
                                     :where [[?e :crux.db/id :elizabeth]]})
                        ffirst
                        :aka
                        count))))

    (t/testing "Explicit limit can increase the default"
      (t/is (= 1500 (->> (crux/q db '{:find [(pull ?e [(:aka {:limit 1500})])]
                                      :where [[?e :crux.db/id :elizabeth]]})
                         ffirst
                         :aka
                         count))))

    (t/testing "Limits can be used as map specification keys"
      (t/is (= #{[{:name "Elizabeth", :friend #{{:name "Kerri"} {:name "Matthew"}}}]}
               (crux/q db '{:find [(pull ?e [:name {(:friend {:limit 2, :into #{}}) [:name]}])]
                            :where [[?e :crux.db/id :elizabeth]]}))))))

(t/deftest test-pull-default
  (let [db (submit-test-docs people-docs)]
    ;; Datascript returns nil here, because there's no matching datom
    (t/is (= #{[nil]}
             (crux/q db '{:find [(pull ?e [:foo])]
                          :where [[?e :crux.db/id :petr]]}))
          "Missing attrs return empty map")

    (t/is (= #{[{:foo "bar"}]}
             (crux/q db '{:find [(pull ?e [(:foo {:default "bar"})])]
                          :where [[?e :crux.db/id :petr]]}))
          "A default can be used to replace nil results")))

(t/deftest test-pull-as
  (let [db (submit-test-docs people-docs)]
    (t/is (= #{[{"Name" "Petr", :alias #{"Devil" "Tupen"}}]}
             (crux/q db '{:find [(pull ?e [(:name {:as "Name"})
                                                  (:aka {:as :alias})])]
                          :where [[?e :crux.db/id :petr]]})))))

(t/deftest test-pull-attr-with-opts
  (let [db (submit-test-docs people-docs)]
    (t/is (= #{[{"Name" "Nothing"}]}
             (crux/q db '{:find [(pull ?e [(:x {:as "Name", :default "Nothing"})])]
                          :where [[?e :crux.db/id :petr]]})))))

(t/deftest test-pull-map
  (let [db (do
             (submit-test-docs people-docs)
             (submit-test-docs part-docs))]
    (t/is (= #{[{:name "Matthew" :parent [{:name "Thomas"}]}]}
             (crux/q db '{:find [(pull ?e [:name {:parent [:name]}])]
                          :where [[?e :crux.db/id :matthew]]}))
          "Single attrs yield a map")

    (t/is (= #{[{:name "Petr" :children [{:name "David"} {:name "Thomas"}]}]}
             (crux/q db '{:find [(pull ?e [:name {(:_parent {:as :children}) [:name]}])]
                          :where [[?e :crux.db/id :petr]]}))
          "Multi attrs yield a collection of maps")

    (t/is (= #{[{:name "Petr"}]}
             (crux/q db '{:find [(pull ?e [:name {:parent [:name]}])]
                          :where [[?e :crux.db/id :petr]]}))
          "Missing attrs are dropped")

    ;; Datascript returns `{:name "Petr", :children []}` for this, but we've matched documents,
    ;; so we can say 'there are two children, but they don't have `:foo`'
    (t/is (= #{[{:name "Petr"}]}
             (crux/q db '{:find [(pull ?e [:name {(:_parent {:as :children}) [:foo]}])]
                          :where [[?e :crux.db/id :petr]]}))
          "Non matching results are removed from collections")

    (t/testing "Map specs can override component expansion"
      (let [parts #{[{:part-name "Part A" :_part-of [{:part-name "Part A.A"}
                                                     {:part-name "Part A.B"}]}]}]
        (t/is (= parts
                 (crux/q db '{:find [(pull ?e [:part-name {:_part-of [:part-name]}])]
                              :where [[?e :crux.db/id :a]]})))

        (t/is (= parts
                 (crux/q db '{:find [(pull ?e [:part-name {:_part-of 1}])]
                              :where [[?e :crux.db/id :a]]})))))))

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
             (crux/q db '{:find [(pull ?e [:name {:friend ...}])]
                          :where [[?e :crux.db/id :lucy]]}))
          "Infinite recursion")

    ;; EQL doesn't support this
    #_
    (t/is (= #{[enemies]}
             (crux/q db '{:find [(pull ?e [:name {:friend 2, :enemy 2}])]
                          :where [[?e :crux.db/id :lucy]]}))
          "Multiple recursion specs in one pattern")

    (let [db (crux/with-tx db [[:crux.tx/put {:crux.db/id :kerri, :name "Kerri", :friend #{:lucy}}]])]
      (t/is (= #{[(assoc-in friends (take 7 (cycle [:friend 0])) [{:name "Kerri" :friend [{:crux.db/id :lucy}]}])]}
               (crux/q db '{:find [(pull ?e [:name {:friend ...}])]
                            :where [[?e :crux.db/id :lucy]]}))
            "Cycles are handled by returning only the :crux.db/id of entities which have been seen before"))))

;; TODO
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
             (crux/q db '{:find [(pull ?e [:crux.db/id {:part ...} {:spec ...}])]
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
                         '{:find [(pull ?e [:name {:friend ...}])]
                           :in [start]
                           :where [[?e :crux.db/id start]]}
                         start)
                 ffirst
                 (get-in path)
                 :name)))))

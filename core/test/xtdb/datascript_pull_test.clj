(ns xtdb.datascript-pull-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.fixtures :as fix :refer [*api*]]))

;; adapted from https://github.com/tonsky/datascript/blob/1.0.1/test/datascript/test/pull_api.cljc

(t/use-fixtures :each fix/with-node)

(def people-docs
  [{:xt/id :petr, :name "Petr", :aka #{"Devil" "Tupen"}}
   {:xt/id :david, :name "David", :parent #{:petr}}
   {:xt/id :thomas, :name "Thomas", :parent #{:petr}}
   {:xt/id :lucy, :name "Lucy" :friend #{:elizabeth}, :enemy #{:matthew}}
   {:xt/id :elizabeth, :name "Elizabeth" :friend #{:matthew}, :enemy #{:eunan}}
   {:xt/id :matthew, :name "Matthew", :parent #{:thomas}, :friend #{:eunan}, :enemy #{:kerri}}
   {:xt/id :eunan, :name "Eunan", :friend #{:kerri}, :enemy #{:lucy}}
   {:xt/id :kerri, :name "Kerri"}
   {:xt/id :rebecca, :name "Rebecca"}])

(def part-docs
  [{:xt/id :a, :part-name "Part A"}
   {:xt/id :a.a, :part-name "Part A.A", :part-of :a}
   {:xt/id :a.a.a, :part-name "Part A.A.A", :part-of :a.a}
   {:xt/id :a.a.a.a, :part-name "Part A.A.A.A", :part-of :a.a.a}
   {:xt/id :a.a.a.b, :part-name "Part A.A.A.B", :part-of :a.a.a}
   {:xt/id :a.b, :part-name "Part A.B", :part-of :a}
   {:xt/id :a.b.a, :part-name "Part A.B.A", :part-of :a.b}
   {:xt/id :a.b.a.a, :part-name "Part A.B.A.A", :part-of :a.b.a}
   {:xt/id :a.b.a.b, :part-name "Part A.B.A.B", :part-of :a.b.a}])

(defn submit-test-docs [docs]
  (fix/submit+await-tx (for [doc docs]
                         [::xt/put doc]))
  (xt/db *api*))

(t/deftest test-pull-attr-spec
  (let [db (submit-test-docs people-docs)]
    (t/is (= #{[{:name "Petr" :aka #{"Devil" "Tupen"}}]}
             (xt/q db '{:find [(pull ?e [:name :aka])]
                          :where [[?e :xt/id :petr]]})))

    (t/is (= #{[{:name "Matthew" :parent [{:xt/id :thomas}] :xt/id :matthew}]}
             (xt/q db '{:find [(pull ?e [:name :xt/id {:parent [:xt/id]}])]
                          :where [[?e :xt/id :matthew]]})))

    (t/is (= #{[{:name "Petr"}] [{:name "Elizabeth"}] [{:name "Eunan"}] [{:name "Rebecca"}]}
             (xt/q db '{:find [(pull ?e [:name])]
                          :where [[?e :xt/id #{:petr :elizabeth :eunan :rebecca}]]})))))

(t/deftest test-pull-reverse-attr-spec
  (let [db (submit-test-docs people-docs)]
    ;; TODO reverse props
    #_
    (t/is (= #{[{:name "Thomas" :_parent [:matthew]}]}
             (xt/q db '{:find [(pull ?e [:name :_parent])]
                          :where [[?e :xt/id :thomas]]})))

    #_
    (t/is (= {:name "Petr" :_parent [:david :thomas]}
             (d/pull test-db '[:name :_parent] 1)))

    (t/is (= #{[{:name "Thomas" :_parent [{:name "Matthew"}]}]}
             (xt/q db '{:find [(pull ?e [:name {:_parent [:name]}])]
                          :where [[?e :xt/id :thomas]]})))

    (t/is (= #{[{:name "Petr" :_parent [{:name "David"} {:name "Thomas"}]}]}
             (xt/q db '{:find [(pull ?e [:name {:_parent [:name]}])]
                          :where [[?e :xt/id :petr]]})))))

(t/deftest test-pull-wildcard
  (let [db (submit-test-docs people-docs)]
    (t/is (= #{[{:xt/id :petr :name "Petr" :aka #{"Devil" "Tupen"}}]}
             (xt/q db '{:find [(pull ?e [*])]
                          :where [[?e :xt/id :petr]]})))

    (t/is (= #{[{:xt/id :david, :name "David", :parent [{:xt/id :petr}]}]}
             (xt/q db '{:find [(pull ?e [* {:parent [:xt/id]}])]
                          :where [[?e :xt/id :david]]})))))

(t/deftest test-pull-limit
  (let [db (submit-test-docs (concat people-docs
                                     [{:xt/id :elizabeth,
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
      (t/is (= 500 (->> (xt/q db '{:find [(pull ?e [(:aka {:limit 500})])]
                                     :where [[?e :xt/id :elizabeth]]})
                        ffirst
                        :aka
                        count))))

    (t/testing "Explicit limit can increase the default"
      (t/is (= 1500 (->> (xt/q db '{:find [(pull ?e [(:aka {:limit 1500})])]
                                      :where [[?e :xt/id :elizabeth]]})
                         ffirst
                         :aka
                         count))))

    (t/testing "Limits can be used as map specification keys"
      (t/is (= #{[{:name "Elizabeth", :friend #{{:name "Kerri"} {:name "Matthew"}}}]}
               (xt/q db '{:find [(pull ?e [:name {(:friend {:limit 2, :into #{}}) [:name]}])]
                            :where [[?e :xt/id :elizabeth]]}))))))

(t/deftest test-pull-default
  (let [db (submit-test-docs people-docs)]
    ;; Datascript returns nil here, because there's no matching datom
    (t/is (= #{[{}]}
             (xt/q db '{:find [(pull ?e [:foo])]
                          :where [[?e :xt/id :petr]]}))
          "Missing attrs return empty map")

    (t/is (= #{[{:foo "bar"}]}
             (xt/q db '{:find [(pull ?e [(:foo {:default "bar"})])]
                          :where [[?e :xt/id :petr]]}))
          "A default can be used to replace nil results")))

(t/deftest test-pull-as
  (let [db (submit-test-docs people-docs)]
    (t/is (= #{[{"Name" "Petr", :alias #{"Devil" "Tupen"}}]}
             (xt/q db '{:find [(pull ?e [(:name {:as "Name"})
                                                  (:aka {:as :alias})])]
                          :where [[?e :xt/id :petr]]})))))

(t/deftest test-pull-attr-with-opts
  (let [db (submit-test-docs people-docs)]
    (t/is (= #{[{"Name" "Nothing"}]}
             (xt/q db '{:find [(pull ?e [(:x {:as "Name", :default "Nothing"})])]
                          :where [[?e :xt/id :petr]]})))))

(t/deftest test-pull-map
  (let [db (do
             (submit-test-docs people-docs)
             (submit-test-docs part-docs))]
    (t/is (= #{[{:name "Matthew" :parent [{:name "Thomas"}]}]}
             (xt/q db '{:find [(pull ?e [:name {:parent [:name]}])]
                          :where [[?e :xt/id :matthew]]}))
          "Single attrs yield a map")

    (t/is (= #{[{:name "Petr" :children [{:name "David"} {:name "Thomas"}]}]}
             (xt/q db '{:find [(pull ?e [:name {(:_parent {:as :children}) [:name]}])]
                          :where [[?e :xt/id :petr]]}))
          "Multi attrs yield a collection of maps")

    (t/is (= #{[{:name "Petr"}]}
             (xt/q db '{:find [(pull ?e [:name {:parent [:name]}])]
                          :where [[?e :xt/id :petr]]}))
          "Missing attrs are dropped")

    ;; Datascript returns `{:name "Petr", :children []}` for this, but we've matched documents,
    ;; so we can say 'there are two children, but they don't have `:foo`'
    (t/is (= #{[{:name "Petr" :children [{} {}]}]}
             (xt/q db '{:find [(pull ?e [:name {(:_parent {:as :children}) [:foo]}])]
                          :where [[?e :xt/id :petr]]}))
          "Non matching results are removed from collections")

    (t/testing "Map specs can override component expansion"
      (let [parts #{[{:part-name "Part A" :_part-of [{:part-name "Part A.A"}
                                                     {:part-name "Part A.B"}]}]}]
        (t/is (= parts
                 (xt/q db '{:find [(pull ?e [:part-name {:_part-of [:part-name]}])]
                              :where [[?e :xt/id :a]]})))

        (t/is (= parts
                 (xt/q db '{:find [(pull ?e [:part-name {:_part-of 1}])]
                              :where [[?e :xt/id :a]]})))))))

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
             (xt/q db '{:find [(pull ?e [:name {:friend ...}])]
                          :where [[?e :xt/id :lucy]]}))
          "Infinite recursion")

    ;; EQL doesn't support this
    #_
    (t/is (= #{[enemies]}
             (xt/q db '{:find [(pull ?e [:name {:friend 2, :enemy 2}])]
                          :where [[?e :xt/id :lucy]]}))
          "Multiple recursion specs in one pattern")

    (let [db (xt/with-tx db [[::xt/put {:xt/id :kerri, :name "Kerri", :friend #{:lucy}}]])]
      (t/is (= #{[(assoc-in friends (take 7 (cycle [:friend 0])) [{:name "Kerri" :friend [{:xt/id :lucy}]}])]}
               (xt/q db '{:find [(pull ?e [:name {:friend ...}])]
                            :where [[?e :xt/id :lucy]]}))
            "Cycles are handled by returning only the :xt/id of entities which have been seen before"))))

;; TODO
#_
(t/deftest test-dual-recursion
  (let [db (submit-test-docs [{:xt/id 1, :part 2, :spec 2}
                              {:xt/id 2, :part 3, :spec 1}
                              {:xt/id 3, :part 1}])]
    (t/is (= #{[{:xt/id 1,
                 :spec {:xt/id 2
                        :spec {:xt/id 1,
                               :spec {:xt/id 2}, :part {:xt/id 2}}
                        :part {:xt/id 3,
                               :part {:xt/id 1,
                                      :spec {:xt/id 2},
                                      :part {:xt/id 2}}}}
                 :part {:xt/id 2
                        :spec {:xt/id 1, :spec {:xt/id 2}, :part {:xt/id 2}}
                        :part {:xt/id 3,
                               :part {:xt/id 1,
                                      :spec {:xt/id 2},
                                      :part {:xt/id 2}}}}}]}
             (xt/q db '{:find [(pull ?e [:xt/id {:part ...} {:spec ...}])]
                          :where [[?e :xt/id 1]]})))))

#_
(t/deftest test-deep-recursion
  ;; TODO Datascript gets to depth 1500 here; we StackOverflowError after ~400
  (let [start 100
        depth 1500
        db (submit-test-docs (conj (for [idx (range start depth)]
                                     {:xt/id idx
                                      :name (str "Person-" idx)
                                      :friend [(inc idx)]})
                                   {:xt/id depth
                                    :name (str "Person-" depth)}))
        path (->> [:friend 0]
                  (repeat (dec (- (inc depth) start)))
                  (into [] cat))]

    (t/is (= (str "Person-" depth)
             (-> (xt/q db
                         '{:find [(pull ?e [:name {:friend ...}])]
                           :in [start]
                           :where [[?e :xt/id start]]}
                         start)
                 ffirst
                 (get-in path)
                 :name)))))

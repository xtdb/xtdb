(ns crux.query-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [crux.db :as db]
            [crux.api :as api]
            [crux.fixtures :as f :refer [*kv*]])
  (:import java.util.UUID))

(t/use-fixtures :each f/with-kv-store)

(t/deftest test-basic-query
  (f/transact-people! *kv* 
;; tag::test-basic-query-d[]
[{:crux.db/id :ivan
  :name "Ivan"
  :last-name "Ivanov"}

 {:crux.db/id :petr
  :name "Petr"
  :last-name "Petrov"}

 {:crux.db/id :smith
  :name "Smith"
  :last-name "Smith"}]
;; end::test-basic-query-d[]
)

  (t/testing "Can query across fields for same value when value is passed in"
    (t/is (= 
;; tag::test-basic-query-r[]
#{[:smith]}
;; end::test-basic-query-r[]
             (api/q (api/db *kv*) (quote
;; tag::test-basic-query-q[]
{:find [p1]
 :where [[p1 :name name]
         [p1 :last-name name]
         [p1 :name "Smith"]]}
;; end::test-basic-query-q[]
))))))

(t/deftest test-query-with-arguments
  (let [[ivan petr] (f/transact-people! *kv* 
;; tag::query-with-arguments-d[]
[{:crux.db/id :ivan
  :name "Ivan"
  :last-name "Ivanov"}

 {:crux.db/id :petr
  :name "Petr"
  :last-name "Petrov"}

 {:crux.db/id :smith
  :name "Smith"
  :last-name "Smith"}]
;; end::query-with-arguments-d[]
                                        )]

    (t/testing "Can match on both entity and value position"
      (t/is (=
;; tag::query-with-arguments1-r[]
#{["Ivan"]}
;; end::query-with-arguments1-r[]
               (api/q
                 (api/db *kv*)
(quote
;; tag::query-with-arguments1-q[]
{:find [name]
 :where [[e :name name]]
 :args [{:e :ivan 
         :name "Ivan"}]}
;; end::query-with-arguments1-q[]
)))))

    (t/testing "Can query entity by single field with several arguments"
      (t/is (=
;; tag::query-with-arguments2-r[]
#{[:ivan] [:petr]}
;; end::query-with-arguments2-r[]
               (api/q
                 (api/db *kv*)
(quote
;; tag::query-with-arguments2-q[]
{:find [e]
  :where [[e :name name]]
  :args [{:name "Ivan"}
         {:name "Petr"}]}
;; end::query-with-arguments2-q[]
)))))

    (t/testing "Can query entity with tuple arguments"
      (t/is (= 
;; tag::query-with-arguments3-r[]
#{[:ivan] [:petr]}
;; end::query-with-arguments3-r[]
               (api/q
                 (api/db *kv*)
                 (quote
;; tag::query-with-arguments3-q[]
{:find [e]
 :where [[e :name name]
         [e :last-name last-name]]
 :args [{:name "Ivan" :last-name "Ivanov"}
        {:name "Petr" :last-name "Petrov"}]}
;; end::query-with-arguments3-q[]
                 )))))

    (t/testing "Can query predicates based on arguments alone"
      (t/is (= 
;; tag::query-with-arguments4-r[]
#{["Ivan"]}
;; end::query-with-arguments4-r[]
               (api/q
                 (api/db *kv*)
                 (quote
;; tag::query-with-arguments4-q[]
{:find [name]
 :where [[(re-find #"I" name)]
         [(= last-name "Ivanov")]]
 :args [{:name "Ivan" :last-name "Ivanov"}
        {:name "Petr" :last-name "Petrov"}]}
;; end::query-with-arguments4-q[]
                 ))))

      (t/testing "Can use range constraints on arguments"
        (t/is (= 
;; tag::query-with-arguments5-r[]
#{[22]}
;; end::query-with-arguments5-r[]
                 (api/q
                   (api/db *kv*)
                   (quote
;; tag::query-with-arguments5-q[]
{:find [age]
 :where [[(>= age 21)]]
 :args [{:age 22}]}
;; end::query-with-arguments5-q[]
))))))))

(t/deftest test-basic-query-at-t
  (let [[malcolm] (f/transact-people!
                    *kv*
;; tag::query-at-t-d1[]
[{:crux.db/id :malcolm :name "Malcolm" :last-name "Sparks"}]
#inst "1986-10-22"
;; end::query-at-t-d1[]
                    )
        [malcolm2] (f/transact-people!
                     *kv*
;; tag::query-at-t-d2[]
[{:crux.db/id :malcolm :name "Malcolma" :last-name "Sparks"}]
#inst "1986-10-24"
;; end::query-at-t-d2[]
                    )
        ]
    (let [q (quote
;; tag::query-at-t-q[]
{:find [e]
 :where [[e :name "Malcolma"]
         [e :last-name "Sparks"]]}
;; end::query-at-t-q[]
              )]
      (t/is (=
;; tag::query-at-t-q1-r[]
#{}
;; end::query-at-t-q1-r[]
;; tag::query-at-t-q1-q[]
; Using Clojure: `(api/q (api/db my-crux-system #inst "1986-10-23") q)`
;; end::query-at-t-q1-q[]
(api/q (api/db *kv* #inst "1986-10-23") q)
))
      (t/is (=
;; tag::query-at-t-q2-r[]
#{[:malcolm]}
;; end::query-at-t-q2-r[]
;; tag::query-at-t-q2-q[]
; Using Clojure: `(api/q (api/db my-crux-system) q)`
;; end::query-at-t-q2-q[]
(api/q (api/db *kv*) q)
)))))

;; tag::history-full[]
(api/submit-tx
  system
  [[:crux.tx/put :ids.persons/Jeff
    {:crux.db/id :ids.persons/Jeff
     :person/name "Jeff"
     :person/wealth 100}
    #inst "2018-05-18T09:20:27.966"]
   [:crux.tx/put :ids.persons/Jeff
    {:crux.db/id :ids.persons/Jeff
     :person/name "Jeff"
     :person/wealth 1000}
    #inst "2015-05-18T09:20:27.966"]])

;yields
{:crux.tx/tx-id 1555314836178,
 :crux.tx/tx-time #inst "2019-04-15T07:53:56.178-00:00"}


(api/history system :ids.persons/Jeff)

; yields
[{:crux.db/id ; sha1 hash of document id
  "c7e66f757f198e08a07a8ea6dfc84bc3ab1c6613",
  :crux.db/content-hash ; sha1 hash of document contents
  "6ca48d3bf05a16cd8d30e6b466f76d5cc281b561",
  :crux.db/valid-time #inst "2018-05-18T09:20:27.966-00:00",
  :crux.tx/tx-time #inst "2019-04-15T07:53:55.817-00:00",
  :crux.tx/tx-id 1555314835817}
 {:crux.db/id "c7e66f757f198e08a07a8ea6dfc84bc3ab1c6613",
  :crux.db/content-hash "a95f149636e0a10a78452298e2135791c0203529",
  :crux.db/valid-time #inst "2015-05-18T09:20:27.966-00:00",
  :crux.tx/tx-time #inst "2019-04-15T07:53:56.178-00:00",
  :crux.tx/tx-id 1555314836178}]
;; end::history-full[]

;; tag::history-range[]
(crux.api/history-range system :ids.persons/Jeff
  #inst "2015-05-18T09:20:27.966"  ; valid-time start or nil
  #inst "2015-05-18T09:20:27.966"  ; transaction-time start or nil
  #inst "2020-05-18T09:20:27.966"  ; valid-time end or nil, inclusive
  #inst "2020-05-18T09:20:27.966") ; transaction-time end or nil, inclusive.

; yields
({:crux.db/id ; sha1 hash of document id
  "c7e66f757f198e08a07a8ea6dfc84bc3ab1c6613",
  :crux.db/content-hash  ; sha1 hash of document contents
  "a95f149636e0a10a78452298e2135791c0203529",
  :crux.db/valid-time #inst "2015-05-18T09:20:27.966-00:00",
  :crux.tx/tx-time #inst "2019-04-15T07:53:56.178-00:00",
  :crux.tx/tx-id 1555314836178}
  {:crux.db/id "c7e66f757f198e08a07a8ea6dfc84bc3ab1c6613",
   :crux.db/content-hash "6ca48d3bf05a16cd8d30e6b466f76d5cc281b561",
   :crux.db/valid-time #inst "2018-05-18T09:20:27.966-00:00",
   :crux.tx/tx-time #inst "2019-04-15T07:53:55.817-00:00",
   :crux.tx/tx-id 1555314835817})


(crux.api/entity (crux.api/db system) "c7e66f757f198e08a07a8ea6dfc84bc3ab1c6613")

; yields
{:crux.db/id :ids.persons/Jeff,
 :d.person/name "Jeff",
 :d.person/wealth 100}
;; end::history-range[]



(t/deftest test-query-across-entities-using-join
  ;; Five people, two of which share the same name:
  (f/transact-people! *kv* 
;; tag::join-d[]
[{:crux.db/id :ivan :name "Ivan"}
 {:crux.db/id :petr :name "Petr"}
 {:crux.db/id :sergei :name "Sergei"}
 {:crux.db/id :denis-a :name "Denis"}
 {:crux.db/id :denis-b :name "Denis"}]
;; end::join-d[]
)

  (t/testing "Every person joins once, plus 2 more matches"
    (t/is (= 
;; tag::join-r[]
#{[:ivan :ivan]
  [:petr :petr]
  [:sergei :sergei]
  [:denis-a :denis-a]
  [:denis-b :denis-b]
  [:denis-a :denis-b]
  [:denis-b :denis-a]}
;; end::join-r[]
            (api/q (api/db *kv*) (quote
;; tag::join-q[]
{:find [p1 p2]
 :where [[p1 :name name]
         [p2 :name name]]}
;; end::join-q[]
))))))

(t/deftest test-join-over-two-attributes
  (f/transact-people! *kv* 
;; tag::join2-d[]
[{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}
 {:crux.db/id :petr :name "Petr" :follows #{"Ivanov"}}]
;; end::join2-d[]
)

  (t/is (= 
;; tag::join2-r[]
#{[:petr]}
;; end::join2-r[]
          (api/q (api/db *kv*) (quote
;; tag::join2-q[]
{:find [e2]
 :where [[e :last-name last-name]
         [e2 :follows last-name]
         [e :name "Ivan"]]}
;; end::join2-q[]
                             )))))

;; tag::blanks[]
(t/deftest test-blanks
  (f/transact-people! *kv* [{:name "Ivan"} {:name "Petr"} {:name "Sergei"}])

  (t/is (= #{["Ivan"] ["Petr"] ["Sergei"]}
           (api/q (api/db *kv*) '{:find [name]
                              :where [[_ :name name]]}))))
;; end::blanks[]

;; tag::not[]
(t/deftest test-not-query
  (f/transact-people! *kv* [{:crux.db/id :ivan-ivanov-1 :name "Ivan" :last-name "Ivanov"}
                            {:crux.db/id :ivan-ivanov-2 :name "Ivan" :last-name "Ivanov"}
                            {:crux.db/id :ivan-ivanovtov-1 :name "Ivan" :last-name "Ivannotov"}])

  (t/testing "literal v"
    (t/is (= 2 (count (api/q (api/db *kv*) '{:find [e]
                                         :where [[e :name name]
                                                 [e :name "Ivan"]
                                                 (not [e :last-name "Ivannotov"])]}))))

    (t/testing "multiple clauses in not"
      (t/is (= 2 (count (api/q (api/db *kv*) '{:find [e]
                                           :where [[e :name name]
                                                   [e :name "Ivan"]
                                                   (not [e :last-name "Ivannotov"]
                                                        [(string? name)])]}))))))

  (t/testing "variable v"
    (t/is (= 2 (count (api/q (api/db *kv*) '{:find [e]
                                         :where [[e :name name]
                                                 [:ivan-ivanovtov-1 :last-name i-name]
                                                 (not [e :last-name i-name])]}))))))
;; end::not[]

;; tag::or[]
(t/deftest test-or-query
  (f/transact-people! *kv* [{:name "Ivan" :last-name "Ivanov"}
                            {:name "Ivan" :last-name "Ivanov"}
                            {:name "Ivan" :last-name "Ivannotov"}
                            {:name "Bob" :last-name "Controlguy"}])

  (t/testing "Or works as expected"
    (t/is (= 3 (count (api/q (api/db *kv*) '{:find [e]
                                         :where [[e :name name]
                                                 [e :name "Ivan"]
                                                 (or [e :last-name "Ivanov"]
                                                     [e :last-name "Ivannotov"])]}))))))
;; end::or[]

;; tag::or-and[]
(t/deftest test-or-query-can-use-and
  (let [[ivan] (f/transact-people! *kv* [{:name "Ivan" :sex :male}
                                         {:name "Bob" :sex :male}
                                         {:name "Ivana" :sex :female}])]

    (t/is (= #{["Ivan"]
               ["Ivana"]}
             (api/q (api/db *kv*) '{:find [name]
                                :where [[e :name name]
                                        (or [e :sex :female]
                                            (and [e :sex :male]
                                                 [e :name "Ivan"]))]})))))
;; end::or-and[]

;; tag::or-and2[]
(t/deftest test-ors-can-introduce-new-bindings
  (let [[petr ivan ivanova] (f/transact-people! *kv* [{:name "Petr" :last-name "Smith" :sex :male}
                                                      {:name "Ivan" :last-name "Ivanov" :sex :male}
                                                      {:name "Ivanova" :last-name "Ivanov" :sex :female}])]

    (t/testing "?p2 introduced only inside of an Or"
      (t/is (= #{[(:crux.db/id ivan)]} (api/q (api/db *kv*) '{:find [?p2]
                                                          :where [(or (and [?p2 :name "Petr"]
                                                                           [?p2 :sex :female])
                                                                      (and [?p2 :last-name "Ivanov"]
                                                                           [?p2 :sex :male]))]}))))))
;; end::or-and2[]

;; tag::not-join[]
(t/deftest test-not-join
  (f/transact-people! *kv* [{:name "Ivan" :last-name "Ivanov"}
                            {:name "Malcolm" :last-name "Ofsparks"}
                            {:name "Dominic" :last-name "Monroe"}])

  (t/testing "Rudimentary not-join"
    (t/is (= #{["Ivan"] ["Malcolm"]}
             (api/q (api/db *kv*) '{:find [name]
                                :where [[e :name name]
                                        (not-join [e]
                                                  [e :last-name "Monroe"])]})))))
;; end::not-join[]

(t/deftest test-mixing-expressions
  (f/transact-people! *kv* [{:name "Ivan" :last-name "Ivanov"}
                            {:name "Derek" :last-name "Ivanov"}
                            {:name "Bob" :last-name "Ivannotov"}
                            {:name "Fred" :last-name "Ivannotov"}])

  (t/testing "Or can use not expression"
    (t/is (= #{["Ivan"] ["Derek"] ["Fred"]}
             (api/q (api/db *kv*) '{:find [name]
                                :where [[e :name name]
                                        (or [e :last-name "Ivanov"]
                                            (not [e :name "Bob"]))]}))))

  (t/testing "Not can use Or expression"
    (t/is (= #{["Fred"]} (api/q (api/db *kv*) '{:find [name]
                                            :where [[e :name name]
                                                    (not (or [e :last-name "Ivanov"]
                                                             [e :name "Bob"]))]})))))

(t/deftest test-predicate-expression
  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov" :age 30}
                            {:crux.db/id :bob :name "Bob" :last-name "Ivanov" :age 40}
                            {:crux.db/id :dominic :name "Dominic" :last-name "Monroe" :age 50}])

  (t/testing "range expressions"
    (t/is (= #{["Ivan"] ["Bob"]}
             (api/q (api/db *kv*) '{:find [name]
                                :where [[e :name name]
                                        [e :age age]
                                        [(< age 50)]]})))

    (t/is (= #{["Dominic"]}
             (api/q (api/db *kv*) '{:find [name]
                                :where [[e :name name]
                                        [e :age age]
                                        [(>= age 50)]]}))))

  (t/testing "clojure.core predicate"
    (t/is (= #{["Bob"] ["Dominic"]}
             (api/q (api/db *kv*) '{:find [name]
                                :where [[e :name name]
                                        [(re-find #"o" name)]]})))


    (t/testing "Several variables"
      (t/is (= #{[:bob "Ivanov"]}
               (api/q (api/db *kv*) '{:find [e last-name]
                                  :where [[e :last-name last-name]
                                          [e :age age]
                                          [(re-find #"ov$" last-name)]
                                          (not [(= age 30)])]}))))

    (t/testing "Bind result to var"
      (t/is (= #{["Dominic" 25] ["Ivan" 15] ["Bob" 20]}
               (api/q (api/db *kv*) '{:find [name half-age]
                                  :where [[e :name name]
                                          [e :age age]
                                          [(quot age 2) half-age]]})))

      (t/testing "Binding more than once intersects result"
        (t/is (= #{["Ivan" 15]}
                 (api/q (api/db *kv*) '{:find [name half-age]
                                    :where [[e :name name]
                                            [e :age real-age]
                                            [(quot real-age 2) half-age]
                                            [(- real-age 15) half-age]]}))))

      (t/testing "Binding can use range predicates"
        (t/is (= #{["Dominic" 25]}
                 (api/q (api/db *kv*) '{:find [name half-age]
                                    :where [[e :name name]
                                            [e :age real-age]
                                            [(quot real-age 2) half-age]
                                            [(> half-age 20)]]})))))))

(t/deftest test-attributes-with-multiple-values
  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov" :age 30 :friends #{:bob :dominic}}
                            {:crux.db/id :bob :name "Bob" :last-name "Ivanov" :age 40 :friends #{:ivan :dominic}}
                            {:crux.db/id :dominic :name "Dominic" :last-name "Monroe" :age 50 :friends #{:bob}}])

  (t/testing "can find multiple values"
    (t/is (= #{[:bob] [:dominic]}
             (api/q (api/db *kv*) '{:find [f]
                                :where [[i :name "Ivan"]
                                        [i :friends f]]})))))

(t/deftest test-queries-with-variables-only
  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :mentor :petr}
                            {:crux.db/id :petr :name "Petr" :mentor :oleg}
                            {:crux.db/id :oleg :name "Oleg" :mentor :ivan}])

  (t/is (= #{[:oleg "Oleg" :petr "Petr"]
             [:ivan "Ivan" :oleg "Oleg"]
             [:petr "Petr" :ivan "Ivan"]} (api/q (api/db *kv*) '{:find [e1 n1 e2 n2]
                                                             :where [[e1 :name n1]
                                                                     [e2 :mentor e1]
                                                                     [e2 :name n2]]}))))

(t/deftest test-index-unification
  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}
                            {:crux.db/id :petr :name "Petr" :last-name "Petrov" :mentor :ivan}])

  (t/is (= #{[:petr :petr]} (api/q (api/db *kv*) '{:find [p1 p2]
                                               :where [[p1 :name "Petr"]
                                                       [p2 :mentor i]
                                                       [(== p1 p2)]]})))

  (t/testing "multiple literals in set"
    (t/is (= #{[:petr] [:ivan]} (api/q (api/db *kv*) '{:find [p]
                                                   :where [[p :name n]
                                                           [(== n #{"Petr" "Ivan"})]]})))))

(t/deftest test-basic-rules
  (f/transact-people! *kv* [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov" :age 21}
                            {:crux.db/id :petr :name "Petr" :last-name "Petrov" :age 18}])

  (t/testing "without rule"
    (t/is (= #{[:ivan]} (api/q (api/db *kv*) '{:find [i]
                                           :where [[i :age age]
                                                   [(>= age 21)]]}))))

  (t/testing "rule using same variable name as body"
    (t/is (= #{[:ivan]} (api/q (api/db *kv*) '{:find [i]
                                           :where [[i :age age]
                                                   (over-twenty-one? age)]
                                           :rules [[(over-twenty-one? age)
                                                    [(>= age 21)]]]}))))

  (t/testing "rules directly on arguments"
    (t/is (= #{[21]} (api/q (api/db *kv*) '{:find [age]
                                        :where [(over-twenty-one? age)]
                                        :args [{:age 21}]
                                        :rules [[(over-twenty-one? age)
                                                 [(>= age 21)]]]}))))

  (t/testing "rule using multiple arguments"
    (t/is (= #{[:ivan]} (api/q (api/db *kv*) '{:find [i]
                                           :where [[i :age age]
                                                   (over-age? age 21)]
                                           :rules [[(over-age? [age] required-age)
                                                    [(>= age required-age)]]]}))))

  (t/testing "rule using multiple branches"
    (t/is (= #{[:ivan]} (api/q (api/db *kv*) '{:find [i]
                                           :where [(is-ivan-or-bob? i)]
                                           :rules [[(is-ivan-or-bob? i)
                                                    [i :name "Ivan"]
                                                    [i :last-name "Ivanov"]]
                                                   [(is-ivan-or-bob? i)
                                                    [i :name "Bob"]]]})))

    (t/is (= #{["Petr"]} (api/q (api/db *kv*) '{:find [name]
                                            :where [[i :name name]
                                                    (not (is-ivan-or-bob? i))]
                                            :rules [[(is-ivan-or-bob? i)
                                                     [i :name "Ivan"]]
                                                    [(is-ivan-or-bob? i)
                                                     [i :name "Bob"]]]}))))


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

(comment
;; tag::bitemp0[]
{:crux.db/id :p2
 :entry-pt :SFO
 :arrival-time #inst "2018-12-31"
 :departure-time :na}
 
{:crux.db/id :p3
 :entry-pt :LA
 :arrival-time #inst "2018-12-31"
 :departure-time :na}

#inst "2018-12-31"
;; end::bitemp0[]

;; tag::bitemp2[]
{:crux.db/id :p4
 :entry-pt :NY
 :arrival-time #inst "2019-01-02"
 :departure-time :na}

#inst "2019-01-02"
;; end::bitemp2[]

;; tag::bitemp3[]
{:crux.db/id :p4
 :entry-pt :NY
 :arrival-time #inst "2019-01-02"
 :departure-time #inst "2019-01-03"}

#inst "2019-01-03"
;; end::bitemp3[]

;; tag::bitemp4[]
{:crux.db/id :p1
 :entry-pt :NY
 :arrival-time #inst "2018-12-31"
 :departure-time :na}

#inst "2018-12-31"
;; end::bitemp4[]

;; tag::bitemp4b[]
{:crux.db/id :p1
 :entry-pt :NY
 :arrival-time #inst "2018-12-31"
 :departure-time #inst "2019-01-03"}

#inst "2019-01-03"
;; end::bitemp4b[]

;; tag::bitemp4c[]
{:crux.db/id :p1
 :entry-pt :LA
 :arrival-time #inst "2019-01-04"
 :departure-time :na}

{:crux.db/id :p3
 :entry-pt :LA
 :arrival-time #inst "2018-12-31"
 :departure-time #inst "2019-01-04"}

#inst "2019-01-04"
;; end::bitemp4c[]

;; tag::bitemp5[]
{:crux.db/id :p2
 :entry-pt :SFO
 :arrival-time #inst "2018-12-31"
 :departure-time #inst "2019-01-05"}

#inst "2019-01-05"
;; end::bitemp5[]

;; tag::bitemp7[]
{:crux.db/id :p3
 :entry-pt :LA
 :arrival-time #inst "2018-12-31"
 :departure-time :na}
#inst "2019-01-04"

{:crux.db/id :p3
 :entry-pt :LA
 :arrival-time #inst "2018-12-31"
 :departure-time #inst "2019-01-07"}
#inst "2019-01-07"
;; end::bitemp7[]

;; tag::bitemp8[]
{:crux.db/id :p3
 :entry-pt :SFO
 :arrival-time #inst "2019-01-08"
 :departure-time :na}
#inst "2019-01-08"

{:crux.db/id :p4
 :entry-pt :LA
 :arrival-time #inst "2019-01-08"
 :departure-time :na}
#inst "2019-01-08"
;; end::bitemp8[]

;; tag::bitemp9[]
{:crux.db/id :p3
 :entry-pt :SFO
 :arrival-time #inst "2019-01-08"
 :departure-time #inst "2019-01-08"}
#inst "2019-01-09"
;; end::bitemp9[]

;; tag::bitemp10[]
{:crux.db/id :p5
 :entry-pt :LA
 :arrival-time #inst "2019-01-10"
 :departure-time :na}
#inst "2019-01-10"
;; end::bitemp10[]

;; tag::bitemp11[]
{:crux.db/id :p7
 :entry-pt :NY
 :arrival-time #inst "2019-01-11"
 :departure-time :na}
#inst "2019-01-11"
;; end::bitemp11[]

;; tag::bitemp12[]
{:crux.db/id :p6
 :entry-pt :NY
 :arrival-time #inst "2019-01-12"
 :departure-time :na}
#inst "2019-01-12"
;; end::bitemp12[]
)

;; tag::bitempq[]
{:find [p entry-pt arrival-time departure-time]
 :where [[p :entry-pt entry-pt]
         [p :arrival-time arrival-time]
         [p :departure-time departure-time]]}
#inst "2019-01-02" ; `as of` valid time
#inst "2019-01-03" ; `as at` transaction time
;; end::bitempq[]

;; tag::bitempr[]
#{[:p2 :SFO #inst "2018-12-31" :na]
  [:p3 :LA #inst "2018-12-31" :na]
  [:p4 :NY #inst "2019-01-02" :na]}
;; end::bitempr[]

(t/deftest test-bitemp-query-from-indexing-temporal-data-using-existing-b+-trees-paper
  (let [tx-log (f/kv-tx-log-w-cache *kv*)]
    ;; Day 0, represented as #inst "2018-12-31"
    @(db/submit-tx tx-log [[:crux.tx/put :p2
                            {:crux.db/id :p2
                             :entry-pt :SFO
                             :arrival-time #inst "2018-12-31"
                             :departure-time :na}
                            #inst "2018-12-31"]
                           [:crux.tx/put :p3
                            {:crux.db/id :p3
                             :entry-pt :LA
                             :arrival-time #inst "2018-12-31"
                             :departure-time :na}
                            #inst "2018-12-31"]])
    ;; Day 1, nothing happens.
    @(db/submit-tx tx-log [])
    ;; Day 2
    @(db/submit-tx tx-log [[:crux.tx/put :p4
                            {:crux.db/id :p4
                             :entry-pt :NY
                             :arrival-time #inst "2019-01-02"
                             :departure-time :na}
                            #inst "2019-01-02"]])
    ;; Day 3
    (let [third-day-submitted-tx @(db/submit-tx tx-log [[:crux.tx/put :p4
                                                         {:crux.db/id :p4
                                                          :entry-pt :NY
                                                          :arrival-time #inst "2019-01-02"
                                                          :departure-time #inst "2019-01-03"}
                                                         #inst "2019-01-03"]])]
      ;; Day 4, correction, adding missing trip on new arrival.
      @(db/submit-tx tx-log [[:crux.tx/put :p1
                              {:crux.db/id :p1
                               :entry-pt :NY
                               :arrival-time #inst "2018-12-31"
                               :departure-time :na}
                              #inst "2018-12-31"]
                             [:crux.tx/put :p1
                              {:crux.db/id :p1
                               :entry-pt :NY
                               :arrival-time #inst "2018-12-31"
                               :departure-time #inst "2019-01-03"}
                              #inst "2019-01-03"]
                             [:crux.tx/put :p1
                              {:crux.db/id :p1
                               :entry-pt :LA
                               :arrival-time #inst "2019-01-04"
                               :departure-time :na}
                              #inst "2019-01-04"]
                             [:crux.tx/put :p3
                              {:crux.db/id :p3
                               :entry-pt :LA
                               :arrival-time #inst "2018-12-31"
                               :departure-time #inst "2019-01-04"}
                              #inst "2019-01-04"]])
      ;; Day 5
      @(db/submit-tx tx-log [[:crux.tx/put :p2
                              {:crux.db/id :p2
                               :entry-pt :SFO
                               :arrival-time #inst "2018-12-31"
                               :departure-time #inst "2019-01-05"}
                              #inst "2019-01-05"]])
      ;; Day 6, nothing happens.
      @(db/submit-tx tx-log [])
      ;; Day 7-12, correction of deletion/departure on day 4. Shows
      ;; how valid time cannot be the same as arrival time.
      @(db/submit-tx tx-log [[:crux.tx/put :p3
                              {:crux.db/id :p3
                               :entry-pt :LA
                               :arrival-time #inst "2018-12-31"
                               :departure-time :na}
                              #inst "2019-01-04"]
                             [:crux.tx/put :p3
                              {:crux.db/id :p3
                               :entry-pt :LA
                               :arrival-time #inst "2018-12-31"
                               :departure-time #inst "2019-01-07"}
                              #inst "2019-01-07"]])
      @(db/submit-tx tx-log [[:crux.tx/put :p3
                              {:crux.db/id :p3
                               :entry-pt :SFO
                               :arrival-time #inst "2019-01-08"
                               :departure-time :na}
                              #inst "2019-01-08"]
                             [:crux.tx/put :p4
                              {:crux.db/id :p4
                               :entry-pt :LA
                               :arrival-time #inst "2019-01-08"
                               :departure-time :na}
                              #inst "2019-01-08"]])
      @(db/submit-tx tx-log [[:crux.tx/put :p3
                              {:crux.db/id :p3
                               :entry-pt :SFO
                               :arrival-time #inst "2019-01-08"
                               :departure-time #inst "2019-01-08"}
                              #inst "2019-01-09"]])
      @(db/submit-tx tx-log [[:crux.tx/put :p5
                              {:crux.db/id :p5
                               :entry-pt :LA
                               :arrival-time #inst "2019-01-10"
                               :departure-time :na}
                              #inst "2019-01-10"]])
      @(db/submit-tx tx-log [[:crux.tx/put :p7
                              {:crux.db/id :p7
                               :entry-pt :NY
                               :arrival-time #inst "2019-01-11"
                               :departure-time :na}
                              #inst "2019-01-11"]])
      @(db/submit-tx tx-log [[:crux.tx/put :p6
                              {:crux.db/id :p6
                               :entry-pt :NY
                               :arrival-time #inst "2019-01-12"
                               :departure-time :na}
                              #inst "2019-01-12"]])

      (t/is (= #{[:p2 :SFO #inst "2018-12-31" :na]
                 [:p3 :LA #inst "2018-12-31" :na]
                 [:p4 :NY #inst "2019-01-02" :na]}
               (api/q (api/db f/*kv* #inst "2019-01-02" (:crux.tx/tx-time third-day-submitted-tx))
                    '{:find [p entry-pt arrival-time departure-time]
                      :where [[p :entry-pt entry-pt]
                              [p :arrival-time arrival-time]
                              [p :departure-time departure-time]]}))))))

;; Tests borrowed from Datascript:
;; https://github.com/tonsky/datascript/tree/master/test/datascript/test

(defn populate-datascript-test-db []
  (f/transact-entity-maps! *kv* [{:crux.db/id :1 :name "Ivan" :age 10}
                                 {:crux.db/id :2 :name "Ivan" :age 20}
                                 {:crux.db/id :3 :name "Oleg" :age 10}
                                 {:crux.db/id :4 :name "Oleg" :age 20}
                                 {:crux.db/id :5 :name "Ivan" :age 10}
                                 {:crux.db/id :6 :name "Ivan" :age 20} ]))

(t/deftest test-rules
  (f/transact-entity-maps! f/*kv* [{:crux.db/id :5 :follow :3}
                                   {:crux.db/id :1 :follow :2}
                                   {:crux.db/id :2 :follow #{:3 :4}}
                                   {:crux.db/id :3 :follow :4}
                                   {:crux.db/id :4 :follow :6}])
  (let [db (api/db *kv*)]
    (t/is (= (api/q db
                  '{:find  [?e1 ?e2]
                    :where [(follow ?e1 ?e2)]
                    :rules [[(follow ?x ?y)
                             [?x :follow ?y]]]})
             #{[:1 :2] [:2 :3] [:3 :4] [:2 :4] [:5 :3] [:4 :6]}))

    ;; NOTE: Crux does not support vars in attribute position, so
    ;; :follow is explicit.
    (t/testing "Joining regular clauses with rule"
      (t/is (= (api/q db
                    '{:find [?y ?x]
                      :where [[_ :follow ?x]
                              (rule ?x ?y)
                              [(crux.query-test/even-kw? ?x)]]
                      :rules [[(rule ?a ?b)
                               [?a :follow ?b]]]})
               #{[:3 :2] [:6 :4] [:4 :2]})))

    ;; NOTE: Crux does not support vars in attribute position.
    #_(t/testing "Rule context is isolated from outer context"
        (t/is (= (api/q db
                      '{:find [?x]
                        :where [[?e _ _]
                                (rule ?x)]
                        :rules [[(rule ?e)
                                 [_ ?e _]]]})
                 #{[:follow]})))

    (t/testing "Rule with branches"
      (t/is (= (api/q db
                    '{:find [?e2]
                      :where [(follow ?e1 ?e2)]
                      :args [{:?e1 :1}]
                      :rules [[(follow ?e2 ?e1)
                               [?e2 :follow ?e1]]
                              [(follow ?e2 ?e1)
                               [?e2 :follow ?t]
                               [?t  :follow ?e1]]]})
               #{[:2] [:3] [:4]})))


    (t/testing "Recursive rules"
      (t/is (= (api/q db
                    '{:find  [?e2]
                      :where [(follow ?e1 ?e2)]
                      :args [{:?e1 :1}]
                      :rules [[(follow ?e1 ?e2)
                               [?e1 :follow ?e2]]
                              [(follow ?e1 ?e2)
                               [?e1 :follow ?t]
                               (follow ?t ?e2)]]})
               #{[:2] [:3] [:4] [:6]}))

      (f/with-kv-store
        (fn []
          (f/transact-entity-maps! f/*kv* [{:crux.db/id :1 :follow :2}
                                           {:crux.db/id :2 :follow :3}])
          (let [db (api/db *kv*)]
            (t/is (= (api/q db
                          '{:find [?e1 ?e2]
                            :where [(follow ?e1 ?e2)]
                            :rules [[(follow ?e1 ?e2)
                                     [?e1 :follow ?e2]]
                                    [(follow ?e1 ?e2)
                                     (follow ?e2 ?e1)]]})
                     #{[:1 :2] [:2 :3] [:2 :1] [:3 :2]})))))

      (f/with-kv-store
        (fn []
          (f/transact-entity-maps! f/*kv* [{:crux.db/id :1 :follow :2}
                                           {:crux.db/id :2 :follow :3}
                                           {:crux.db/id :3 :follow :1}])
          (let [db (api/db *kv*)]
            (t/is (= (api/q db
                          '{:find [?e1 ?e2]
                            :where [(follow ?e1 ?e2)]
                            :rules [[(follow ?e1 ?e2)
                                     [?e1 :follow ?e2]]
                                    [(follow ?e1 ?e2)
                                     (follow ?e2 ?e1)]]})
                     #{[:1 :2] [:2 :3] [:3 :1] [:2 :1] [:3 :2] [:1 :3]}))))))

    (t/testing "Mutually recursive rules"
      (f/with-kv-store
        (fn []
          (f/transact-entity-maps! f/*kv* [{:crux.db/id :0 :f1 :1}
                                           {:crux.db/id :1 :f2 :2}
                                           {:crux.db/id :2 :f1 :3}
                                           {:crux.db/id :3 :f2 :4}
                                           {:crux.db/id :4 :f1 :5}
                                           {:crux.db/id :5 :f2 :6}])
          (let [db (api/db *kv*)]
            (t/is (= (api/q db
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
                     #{[:0 :1] [:0 :3] [:0 :5]
                       [:1 :3] [:1 :5]
                       [:2 :3] [:2 :5]
                       [:3 :5]
                       [:4 :5]}))))))

    (t/testing "Passing ins to rule"
      (t/is (= (api/q db
                    {:find '[?x ?y]
                     :where '[(match ?even ?x ?y)]
                     :rules '[[(match ?pred ?e ?e2)
                               [?e :follow ?e2]
                               [(?pred ?e)]
                               [(?pred ?e2)]]]
                     :args [{:?even even-kw?}]})
               #{[:4 :6] [:2 :4]})))))

(t/deftest data-script-test-query-fns
    ;; NOTE: Crux does not support these functions.
    #_(t/testing "ground")
    #_(t/testing "get-else")
    #_(t/testing "get-some")
    #_(t/testing "missing?")
    #_(t/testing "missing? back-ref")

    ;; NOTE: Crux does not currently support destructuring.
    #_(t/testing "Destructured conflicting function values for two bindings.")
    #_(t/testing "Result bindings"))

(defn kw-less-than? [x y]
  (< (Long/parseLong (name x))
     (Long/parseLong (name y))))

(t/deftest datascript-test-predicates
  (f/transact-entity-maps! f/*kv* [{:crux.db/id :1 :name "Ivan" :age 10}
                                   {:crux.db/id :2 :name "Ivan" :age 20}
                                   {:crux.db/id :3 :name "Oleg" :age 10}
                                   {:crux.db/id :4 :name "Oleg" :age 20}])
  (let [db (api/db *kv*)]
    ;; NOTE: Crux does not support source vars.
    #_(let [pred (fn [db e a]
                   (= a (:age (d/entity db e))))]
        (t/is (= (api/q '[:find ?e
                        :in $ ?pred
                        :where [?e :age ?a]
                        [(?pred $ ?e 10)]]
                      db pred)
                 #{[1] [3]})))))


;; Tests from Racket Datalog
;; https://github.com/racket/datalog/tree/master/tests/examples

;; Tests from
;; https://pdfs.semanticscholar.org/9374/f0da312f3ba77fa840071d68935a28cba364.pdf

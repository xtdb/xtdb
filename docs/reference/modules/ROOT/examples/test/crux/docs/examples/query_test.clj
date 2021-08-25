(ns crux.docs.examples.query-test
  (:require [clojure.test :as t]
            [crux.api :as crux]
            [crux.fixtures :as fix :refer [*api*]]))

(def ^:dynamic *storage-dir*)

(defn with-storage-dir [f]
  (fix/with-tmp-dirs [storage-dir]
    (binding [*storage-dir* storage-dir]
      (f))))

(t/use-fixtures :each fix/with-node with-storage-dir)

(comment
  ;; tag::query-at-t-q1[]
  (def q
    '{:find [e]
      :where [[e :name "Malcolma"]
              [e :last-name "Sparks"]]})
  ;; end::query-at-t-q1[]
  )

(t/deftest test-queries-at-time
  (let [node *api*]
    ;; tag::query-at-t-d1[]
    (crux/submit-tx
     node
     [[:xt/put
       {:xt/id :malcolm :name "Malcolm" :last-name "Sparks"}
       #inst "1986-10-22"]])
    ;; end::query-at-t-d1[]

    ;; tag::query-at-t-d2[]
    (crux/submit-tx
     node
     [[:xt/put
       {:xt/id :malcolm :name "Malcolma" :last-name "Sparks"}
       #inst "1986-10-24"]])
    ;; end::query-at-t-d2[]

    (crux/sync node)

    (let [q '{:find [e]
              :where [[e :name "Malcolma"]
                      [e :last-name "Sparks"]]}]
      (t/is (= #{}
               ;; tag::query-at-t-q1-q[]
               (crux/q (crux/db node #inst "1986-10-23") q)
               ;; end::query-at-t-q1-q[]
               ))

      (t/is (= #{[:malcolm]}
               ;; tag::query-at-t-q2-q[]
               (crux/q (crux/db node) q)
               ;; end::query-at-t-q2-q[]
               )))))

(t/deftest test-streaming-queries
  (fix/submit+await-tx
   (fix/maps->tx-ops
    ;; tag::query-input[]
    [{:xt/id :ivan
      :name "Ivan"
      :last-name "Ivanov"}

     {:xt/id :petr
      :name "Petr"
      :last-name "Petrov"}

     {:xt/id :smith
      :name "Smith"
      :last-name "Smith"}])
   ;; end::query-input[]
   )

  (let [node *api*
        !results (atom [])
        ;; so that it's 'prn' in the example but we can still test it.
        ;; cheeky.
        prn #(swap! !results conj %)]

    ;; tag::streaming-query[]
    (with-open [res (crux/open-q (crux/db node)
                                 '{:find [p1]
                                   :where [[p1 :name n]
                                           [p1 :last-name n]
                                           [p1 :name "Smith"]]})]
      (doseq [tuple (iterator-seq res)]
        (prn tuple)))
    ;; end::streaming-query[]

    (t/is (= [[:smith]] @!results))))

(t/deftest test-basic-queries
  (fix/submit+await-tx
   (fix/maps->tx-ops
    ;; tag::query-input[]
    [{:xt/id :ivan
      :name "Ivan"
      :last-name "Ivanov"}

     {:xt/id :petr
      :name "Petr"
      :last-name "Petrov"}

     {:xt/id :smith
      :name "Smith"
      :last-name "Smith"}]
    ;; end::query-input[]
    ))

  (let [node *api*]
    (t/is (=
           ;; tag::basic-query-r[]
           #{[:smith]}
           ;; end::basic-query-r[]

           ;; tag::basic-query[]
           (crux/q
            (crux/db node)
            '{:find [p1]
              :where [[p1 :name n]
                      [p1 :last-name n]
                      [p1 :name "Smith"]]})
           ;; end::basic-query[]
           ))

    (t/is (=
           ;; tag::query-with-arguments1-r[]
           #{[:ivan]}
           ;; end::query-with-arguments1-r[]

           ;; tag::query-with-arguments1[]
           (crux/q
            (crux/db node)
            '{:find [e]
              :in [first-name]
              :where [[e :name first-name]]}
            "Ivan")
           ;; end::query-with-arguments1[]
           ))

    (t/is (=
           ;; tag::query-with-arguments2-r[]
           #{[:ivan] [:petr]}
           ;; end::query-with-arguments2-r[]

           ;; tag::query-with-arguments2[]
           (crux/q
            (crux/db node)
            '{:find [e]
              :in [[first-name ...]]
              :where [[e :name first-name]]}
            ["Ivan" "Petr"])
           ;; end::query-with-arguments2[]
           ))

    (t/is (=
           ;; tag::query-with-arguments3-r[]
           #{[:ivan]}
           ;; end::query-with-arguments3-r[]

           ;; tag::query-with-arguments3[]
           (crux/q
            (crux/db node)
            '{:find [e]
              :in [[first-name last-name]]
              :where [[e :name first-name]
                      [e :last-name last-name]]}
            ["Ivan" "Ivanov"])
           ;; end::query-with-arguments3[]
           ))

    (t/is (=
           ;; tag::query-with-arguments4-r[]
           #{[:petr] [:smith]}
           ;; end::query-with-arguments4-r[]

           ;; tag::query-with-arguments4[]
           (crux/q
            (crux/db node)
            '{:find [e]
              :in [[[first-name last-name]]]
              :where [[e :name first-name]
                      [e :last-name last-name]]}
            [["Petr" "Petrov"]
             ["Smith" "Smith"]])
           ;; end::query-with-arguments4[]
           ))

    (t/is (=
           ;; tag::query-with-arguments5-r[]
           #{[22]}
           ;; end::query-with-arguments5-r[]

           ;; tag::query-with-arguments5[]
           (crux/q
            (crux/db node)
            '{:find [age]
              :in [[age ...]]
              :where [[(> age 21)]]}
            [21 22])
           ;; end::query-with-arguments5[]
           ))

    (t/is (=
           ;; tag::sub-query-example-1-r[]
           #{[[[4]]]}
           ;; end::sub-query-example-1-r[]

           ;; tag::sub-query-example-1[]
           (crux/q
            (crux/db node)
            '{:find [x]
              :where [[(q {:find [y]
                           :where [[(identity 2) x]
                                   [(+ x 2) y]]})
                       x]]})
           ;; end::sub-query-example-1[]
           ))

    (t/is (=
           ;; tag::sub-query-example-2-r[]
           #{[4]}
           ;; end::sub-query-example-2-r[]

           ;; tag::sub-query-example-2[]
           (crux/q
            (crux/db node)
            '{:find [x]
              :where [[(q {:find [y]
                           :where [[(identity 2) x]
                                   [(+ x 2) y]]})
                       [[x]]]]})
           ;; end::sub-query-example-2[]
           ))

    (t/is (=
           ;; tag::sub-query-example-3-r[]
           #{[2 4 8]}
           ;; end::sub-query-example-3-r[]

           ;; tag::sub-query-example-3[]
           (crux/q
            (crux/db node)
            '{:find [x y z]
              :where [[(q {:find [x y]
                           :where [[(identity 2) x]
                                   [(+ x 2) y]]})
                       [[x y]]]
                      [(* x y) z]]})
           ;; end::sub-query-example-3[]
           ))))

(t/deftest test-aggregates
  (let [node *api*]
    (t/is (=
           ;; tag::query-aggregates-r[]
           #{[6 1 3 4 2]}
           ;; end::query-aggregates-r[]

           ;; tag::query-aggregates[]
           (crux/q
            (crux/db node)
            '{:find [(sum ?heads)
                     (min ?heads)
                     (max ?heads)
                     (count ?heads)
                     (count-distinct ?heads)]
              :where [[(identity [["Cerberus" 3]
                                  ["Medusa" 1]
                                  ["Cyclops" 1]
                                  ["Chimera" 1]])
                       [[?monster ?heads]]]]})
           ;; end::query-aggregates[]
           ))))

(t/deftest test-join-1
  (fix/submit+await-tx
   (fix/maps->tx-ops
    ;; tag::join-d[]
    [{:xt/id :ivan :name "Ivan"}
     {:xt/id :petr :name "Petr"}
     {:xt/id :sergei :name "Sergei"}
     {:xt/id :denis-a :name "Denis"}
     {:xt/id :denis-b :name "Denis"}]
    ;; end::join-d[]
    ))

  (let [node *api*]
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

           ;; tag::join-q[]
           (crux/q
            (crux/db node)
            '{:find [p1 p2]
              :where [[p1 :name n]
                      [p2 :name n]]})
           ;; end::join-q[]
           ))))

(t/deftest test-join-2
  (let [node *api*]
    (fix/submit+await-tx
     (fix/maps->tx-ops
      ;; tag::join2-d[]
      [{:xt/id :ivan :name "Ivan" :last-name "Ivanov"}
       {:xt/id :petr :name "Petr" :follows #{"Ivanov"}}]
      ;; end::join2-d[]
      ))

    (t/is (=
           ;; tag::join2-r[]
           #{[:petr]}
           ;; end::join2-r[]

           ;; tag::join2-q[]
           (crux/q
            (crux/db node)
            '{:find [e2]
              :where [[e :last-name l]
                      [e2 :follows l]
                      [e :name "Ivan"]]})
           ;; end::join2-q[]
           ))))

(t/deftest test-eql
  (fix/submit+await-tx
   (fix/maps->tx-ops
    [{:xt/id :ivan :user/name "Ivan" :user/id 1 :user/profession :doctor}
     {:xt/id :sergei :user/name "Sergei" :user/id 2 :user/profession :lawyer}
     {:xt/id :petr :user/name "Petr" :user/id 3 :user/profession :doctor}
     {:xt/id :doctor :profession/name "Doctor"}
     {:xt/id :lawyer :profession/name "Lawyer"}]))

  (let [node *api*]
    (t/is (=
           ;; tag::pull-query-1-r[]
           #{[1 "Ivan" :doctor] [2 "Sergei" :lawyer], [3 "Petr" :doctor]}
           ;; end::pull-query-1-r[]

           ;; tag::pull-query-1[]
           ;; with just 'query':
           (crux/q
            (crux/db node)
            '{:find [?uid ?name ?profession]
              :where [[?user :user/id ?uid]
                      [?user :user/name ?name]
                      [?user :user/profession ?profession]]})
           ;; end::pull-query-1[]
           ))

    (t/is (=
           ;; tag::pull-query-2-r[]
           #{[{:user/name "Ivan" :user/profession :doctor}]
             [{:user/name "Sergei" :user/profession :lawyer}]
             [{:user/name "Petr" :user/profession :doctor}]}
           ;; end::pull-query-2-r[]

           ;; tag::pull-query-2[]
           ;; using `pull`:
           (crux/q
            (crux/db node)
            '{:find [(pull ?user [:user/name :user/profession])]
              :where [[?user :user/id ?uid]]})
           ;; end::pull-query-2[]
           ))

    (t/is (=
           ;; tag::pull-query-3-r[]
           #{[1 "Ivan" "Doctor"] [2 "Sergei" "Lawyer"] [3 "Petr" "Doctor"]}
           ;; end::pull-query-3-r[]

           ;; tag::pull-query-3[]
           ;; with just 'query':
           (crux/q
            (crux/db node)
            '{:find [?uid ?name ?profession-name]
              :where [[?user :user/id ?uid]
                      [?user :user/name ?name]
                      [?user :user/profession ?profession]
                      [?profession :profession/name ?profession-name]]})
           ;; end::pull-query-3[]
           ))

    (t/is (=
           ;; tag::pull-query-4-r[]
           #{[{:user/name "Ivan" :user/profession {:profession/name "Doctor"}}]
             [{:user/name "Sergei" :user/profession {:profession/name "Lawyer"}}]
             [{:user/name "Petr" :user/profession {:profession/name "Doctor"}}]}
           ;; end::pull-query-4-r[]

           ;; tag::pull-query-4[]
           ;; using `pull`:
           (crux/q
            (crux/db node)
            '{:find [(pull ?user [:user/name {:user/profession [:profession/name]}])]
              :where [[?user :user/id ?uid]]})
           ;; end::pull-query-4[]
           ))

    (t/is (=
           ;; tag::pull-query-5-r[]
           #{[{:profession/name "Doctor"
               :user/_profession [{:user/id 1 :user/name "Ivan"},
                                  {:user/id 3 :user/name "Petr"}]}]
             [{:profession/name "Lawyer"
               :user/_profession [{:user/id 2 :user/name "Sergei"}]}]}
           ;; end::pull-query-5-r[]

           ;; tag::pull-query-5[]
           (crux/q
            (crux/db node)
            '{:find [(pull ?profession [:profession/name {:user/_profession [:user/id :user/name]}])]
              :where [[?profession :profession/name]]})
           ;; end::pull-query-5[]
           ))

    (t/is (=
           ;; tag::pull-query-6-r[]
           #{[{:xt/id :ivan :user/id 1, :user/name "Ivan", :user/profession :doctor}]}
           ;; end::pull-query-6-r[]

           ;; tag::pull-query-6[]
           (crux/q
            (crux/db node)
            '{:find [(pull ?user [*])]
              :where [[?user :user/id 1]]})
           ;; end::pull-query-6[]
           ))

    (t/is (=
           ;; tag::pull-many-query-1-r[]
           {:user/name "Ivan", :user/profession :doctor}
           ;; end::pull-many-query-1-r[]

           ;; tag::pull-many-query-1[]
           ;; using `pull`:
           (crux/pull
            (crux/db node)
            [:user/name :user/profession]
            :ivan)
           ;; end::pull-many-query-1[]
           ))

    (t/is (=
           ;; tag::pull-many-query-2-r[]
           [{:user/name "Ivan", :user/profession :doctor},
            {:user/name "Sergei", :user/profession :lawyer}]
           ;; end::pull-many-query-2-r[]

           ;; tag::pull-many-query-2[]
           ;; using `pull-many`:
           (crux/pull-many
            (crux/db node)
            [:user/name :user/profession]
            [:ivan :sergei])
           ;; end::pull-many-query-2[]
           ))))

(t/deftest test-return-maps
  (fix/submit+await-tx
   (fix/maps->tx-ops
    [{:xt/id :ivan :user/name "Ivan" :user/id 1 :user/profession :doctor}
     {:xt/id :sergei :user/name "Sergei" :user/id 2 :user/profession :lawyer}
     {:xt/id :petr :user/name "Petr" :user/id 3 :user/profession :doctor}
     {:xt/id :doctor :profession/name "Doctor"}
     {:xt/id :lawyer :profession/name "Lawyer"}]))

  (let [node *api*]
    (t/is (=
           ;; tag::return-maps-r[]
           #{{:name "Ivan", :profession "Doctor"}}
           ;; end::return-maps-r[]

           ;; tag::return-maps[]
           (crux/q
            (crux/db node)
            '{:find [?name ?profession-name]
              :keys [name profession]
              :where [[?user :user/id 1]
                      [?user :user/name ?name]
                      [?user :user/profession ?profession]
                      [?profession :profession/name ?profession-name]]})
           ;; end::return-maps[]
           ))))

(t/deftest test-order-and-pagination
  (let [node *api*
        data (for [i (range 200)]
               {:xt/id i
                :condition/time (if (even? i) (quot i 4) (- (quot i 4)))
                :condition/device-id i
                :condition/temperature :temp
                :condition/humidity :hum})
        conv (partial map #(vec [(:condition/time %) (:condition/device-id %) :temp :hum]))]

    (fix/submit+await-tx (fix/maps->tx-ops data))

    (t/is (=
           (->> data
                (sort-by :condition/device-id <)
                (sort-by :condition/time >)
                conv)

           ;; tag::order-and-pagination-1[]
           (crux/q
            (crux/db node)
            '{:find [time device-id temperature humidity]
              :where [[c :condition/time time]
                      [c :condition/device-id device-id]
                      [c :condition/temperature temperature]
                      [c :condition/humidity humidity]]
              :order-by [[time :desc] [device-id :asc]]})
           ;; end::order-and-pagination-1[]
           ))

    (t/is (=
           (->> data
                (sort-by :condition/device-id <)
                (drop 90)
                (take 10)
                conv)

           ;; tag::order-and-pagination-2[]
           (crux/q
            (crux/db node)
            '{:find [time device-id temperature humidity]
              :where [[c :condition/time time]
                      [c :condition/device-id device-id]
                      [c :condition/temperature temperature]
                      [c :condition/humidity humidity]]
              :order-by [[device-id :asc]]
              :limit 10
              :offset 90})
           ;; end::order-and-pagination-2[]
           ))))

(t/deftest test-rules
  (fix/submit+await-tx
   (fix/maps->tx-ops
    [{:xt/id :ivan :follow :petr :age 19}
     {:xt/id :petr :follow :sergei :age 25}
     {:xt/id :sergei :age 3}]))

  (let [node *api*]
    (t/is (= #{[:ivan] [:petr]}
             ;; tag::rules-1[]
             (crux/q
              (crux/db node)
              '{:find [p]
                :where [(adult? p)] ;;<1>
                :rules [[(adult? p) ;;<2>
                         [p :age a] ;;<3>
                         [(>= a 18)]]]})
             ;; end::rules-1[]
             ))

    (t/is (= #{[:petr] [:sergei]}
             ;; tag::rules-2[]
             (crux/q
              (crux/db node)
              '{:find [?e2]
                :in [?e1]
                :where [(follow ?e1 ?e2)]
                :rules [[(follow ?e1 ?e2)
                         [?e1 :follow ?e2]]
                        [(follow ?e1 ?e2)
                         [?e1 :follow ?t]
                         (follow ?t ?e2)]]}
              :ivan)
             ;; end::rules-2[]
             ))))

(t/deftest test-bound-rule-vars-946
  (fix/submit+await-tx (for [[id child-id] (partition 2 1 (range 101))]
                         [:xt/put {:xt/id id, :child child-id :name (str id "-" child-id)}]))

  (let [node *api*
        parent-id 50
        expected (set (for [[id child-id] (partition 2 1 (range (inc parent-id) 101))]
                        [(str id "-" child-id)]))]
    (t/is (= expected
             ;; tag::rules-3[]
             (crux/q
              (crux/db node)
              '{:find [child-name]
                :in [parent]
                :where [[parent :xt/id]
                        (child-of parent child)
                        [child :name child-name]]
                :rules [[(child-of p c)
                         [p :child c]]
                        [(child-of p c)
                         [p :child c1]
                         (child-of c1 c)]]}
              parent-id)
             ;; end::rules-3[]
             ))
    (t/is (= expected
             ;; tag::rules-4[]
             (crux/q
              (crux/db node)
              '{:find [child-name]
                :in [parent]
                :where [[parent :xt/id]
                        (child-of parent child)
                        [child :name child-name]]
                :rules [[(child-of [p] c)
                         [p :child c]]
                        [(child-of [p] c)
                         [p :child c1]
                         (child-of c1 c)]]}
              parent-id)
             ;; end::rules-4[]
             ))))

(t/deftest test-not
  (fix/submit+await-tx
   (fix/maps->tx-ops
    ;; tag::not-data[]
    [{:xt/id :petr-ivanov :name "Petr" :last-name "Ivanov"} ;; <1>
     {:xt/id :ivan-ivanov :name "Ivan" :last-name "Ivanov"}
     {:xt/id :ivan-petrov :name "Ivan" :last-name "Petrov"}
     {:xt/id :petr-petrov :name "Petr" :last-name "Petrov"}]
    ;; end::not-data[]
    ))

  (let [node *api*]
    (t/is (= #{[:ivan-petrov]}
             (crux/q
              (crux/db node)
              '{:find [e]
                :where [[e :name name]
                        [e :name "Ivan"]
                        (not [e :last-name "Ivanov"])]})
             ))

    (t/is (=
           ;; tag::not-2-r[]
           #{[:petr-ivanov] [:petr-petrov] [:ivan-petrov]} ;;<3>
           ;; end::not-2-r[]

           ;; tag::not-2[]
           (crux/q
            (crux/db node)
            '{:find [e]
              :where [[e :xt/id]
                      (not [e :last-name "Ivanov"] ;;<2>
                           [e :name "Ivan"])]})
           ;; end::not-2[]
           ))

    (t/is (=
           #{[:ivan-ivanov] [:petr-ivanov]}

           (crux/q
            (crux/db node)
            '{:find [e]
              :where [[e :name name]
                      [:ivan-petrov :last-name i-name]
                      (not [e :last-name i-name])]})
           ))))

(t/deftest test-or
  (fix/submit+await-tx
   (fix/maps->tx-ops
    ;; tag::or-data[]
    [{:xt/id :ivan-ivanov-1 :name "Ivan" :last-name "Ivanov" :sex :male} ;;<1>
     {:xt/id :ivan-ivanov-2 :name "Ivan" :last-name "Ivanov" :sex :male}
     {:xt/id :ivan-ivanovtov-1 :name "Ivan" :last-name "Ivannotov" :sex :male}
     {:xt/id :ivanova :name "Ivanova" :last-name "Ivanov" :sex :female}
     {:xt/id :bob :name "Bob" :last-name "Controlguy"}]
    ;; end::or-data[]
    ))

  (let [node *api*]
    (t/is (=
           ;; tag::or-1-r[]
           #{[:ivan-ivanov-1] [:ivan-ivanov-2] [:ivan-ivanovtov-1]} ;;<3>
           ;; end::or-1-r[]

           ;; tag::or-1[]
           (crux/q
            (crux/db node)
            '{:find [e] ;;<2>
              :where [[e :name name]
                      [e :name "Ivan"]
                      (or [e :last-name "Ivanov"]
                          [e :last-name "Ivannotov"])]})
           ;; end::or-1[]
           ))

    (t/is (=
           #{["Ivan"] ["Ivanova"]}
           ;; tag::or-2[]
           (crux/q
            (crux/db node)
            '{:find [name]
              :where [[e :name name]
                      (or [e :sex :female]
                          (and [e :sex :male]
                               [e :name "Ivan"]))]})
           ;; end::or-2[]
           ))

    (t/is (=
           #{["Ivan"] ["Ivanova"]}
           ;; tag::or-3[]
           (crux/q
            (crux/db node)
            '{:find [name]
              :where [[e :name name]
                      (or (and [e :sex :female]
                               [(= name "Ivanova")])
                          (and [e :sex :male]
                               [(any? name)]))]})
           ;; end::or-3[]
           ))

    (t/is (=
           #{[:ivan-ivanov-1] [:ivan-ivanov-2]}
           (crux/q
            (crux/db node)
            '{:find [?p2]
              :where [(or (and [?p2 :name "Petr"]
                               [?p2 :sex :female])
                          (and [?p2 :last-name "Ivanov"]
                               [?p2 :sex :male]))]})
           ))))

(t/deftest test-blanks
  (fix/submit+await-tx
   (fix/maps->tx-ops [{:xt/id :ivan :name "Ivan"}
                      {:xt/id :petr :name "Petr"}
                      {:xt/id :sergei :name "Sergei"}]))

  (let [node *api*]
    (t/is (= #{["Ivan"] ["Petr"] ["Sergei"]}
             (crux/q
              (crux/db node)
              '{:find [name]
                :where [[_ :name name]]})))))

(t/deftest test-not-join
  (fix/submit+await-tx
   (fix/maps->tx-ops
    ;; tag::not-join-data[]
    [{:xt/id :ivan :name "Ivan" :last-name "Ivanov"} ;;<1>
     {:xt/id :petr :name "Petr" :last-name "Petrov"}
     {:xt/id :sergei :name "Sergei" :last-name "Sergei"}]
    ;; end::not-join-data[]
    ))

  (let [node *api*]
    (t/is (=
           ;; tag::not-join-r[]
           #{[:ivan] [:petr]} ;;<4>
           ;; end::not-join-r[]
           ;; tag::not-join[]
           (crux/q
            (crux/db node)
            '{:find [e]
              :where [[e :xt/id]
                      (not-join [e] ;;<2>
                                [e :last-name n] ;;<3>
                                [e :name n])]})
           ;; end::not-join[]
           ))))

(t/deftest test-generic
  (fix/submit+await-tx
   (fix/maps->tx-ops
    [{:xt/id :ivan :name "Ivan" :last-name "Ivan"}]))

  (let [node *api*]
    (t/is (= #{[:ivan]}
             ;; tag::anatomy[]
             (crux/q
              (crux/db node) ;;<1>
              '{:find [p1] ;;<2>
                :where [[p1 :name n]
                        [p1 :last-name n]
                        [p1 :name name]]
                :in [name]}
              "Ivan") ;;<3>
             ;; end::anatomy[]
             ))

    (t/is (= #{["Ivan"]}
             ;; tag::triple[]
             (crux/q
              (crux/db node)
              '{:find [n]
                :where [[p :last-name n]]})
             ;; end::triple[]
             ))

    (t/is (=
           #{[:ivan]}
           ;; tag::double[]
           (crux/q
            (crux/db node)
            '{:find [p]
              :where [[p :name]]}) ;;<1>
           ;; end::double[]
           ))

    (t/is (=
           #{[:ivan]}
           ;; tag::triple-2[]
           (crux/q
            (crux/db node)
            '{:find [p]
              :where [[p :name "Ivan"]]}) ;;<2>
           ;; end::triple-2[]
           ))

    (t/is (=
           #{[:ivan]}
           ;; tag::triple-3[]
           (crux/q
            (crux/db node)
            '{:find [p]
              :where [[q :name n]
                      [p :last-name n]]}) ;;<3>
           ;; end::triple-3[]
           ))))

(t/deftest test-or-join
  (fix/submit+await-tx
   (fix/maps->tx-ops
    ;; tag::or-join-data[]
    [{:xt/id :ivan :name "Ivan" :age 12} ;;<1>
     {:xt/id :petr :name "Petr" :age 15}
     {:xt/id :sergei :name "Sergei" :age 19}]
    ;; end::or-join-data[]
    ))

  (let [node *api*]
    (t/is (=
           ;; tag::or-join-r[]
           #{[:ivan] [:sergei]} ;;<4>
           ;; end::or-join-r[]

           ;; tag::or-join[]
           (crux/q
            (crux/db node)
            '{:find [p]
              :where [[p :xt/id]
                      (or-join [p] ;;<2>
                               (and [p :age a] ;;<3>
                                    [(>= a 18)])
                               [p :name "Ivan"])]})
           ;; end::or-join[]
           ))))

(t/deftest test-range
  (fix/submit+await-tx
   (fix/maps->tx-ops
    ;; tag::range-data[]
    [{:xt/id :ivan :name "Ivan" :age 12} ;;<1>
     {:xt/id :petr :name "Petr" :age 15}
     {:xt/id :sergei :name "Sergei" :age 19}]
    ;; end::range-data[]
    ))

  (let [node *api*]
    (t/is (=
           #{[:sergei]}
           ;; tag::range-1[]
           (crux/q
            (crux/db node)
            '{:find [p] ;;<1>
              :where [[p :age a]
                      [(> a 18)]]})
           ;; end::range-1[]
           ))

    (t/is (=
           #{[:petr] [:sergei]}
           ;; tag::range-2[]
           (crux/q
            (crux/db node)
            '{:find [p] ;;<2>
              :where [[p :age a]
                      [q :age b]
                      [(> a b)]]})
           ;; end::range-2[]
           ))

    (t/is (=
           #{[:ivan] [:petr]}
           ;; tag::range-3[]
           (crux/q
            (crux/db node)
            '{:find [p] ;;<3>
              :where [[p :age a]
                      [(> 18 a)]]})
           ;; end::range-3[]
           ))

    (t/is (=
           ;; tag::query-with-pred-1-r[]
           #{[:petr] [:sergei]}
           ;; end::query-with-pred-1-r[]

           ;; tag::query-with-pred-1[]
           (crux/q
            (crux/db node)
            '{:find [p]
              :where [[p :age age]
                      [(odd? age)]]})
           ;; end::query-with-pred-1[]
           ))

    ))

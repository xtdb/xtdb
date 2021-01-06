(ns docs.examples
  (:require [clojure.java.io :as io]
            [crux.api :as crux]))

;; tag::require-ek[]
(require '[crux.kafka.embedded :as ek])
;; end::require-ek[]

;; tag::ek-example[]
(defn start-embedded-kafka [kafka-port storage-dir]
  (ek/start-embedded-kafka {:crux.kafka.embedded/zookeeper-data-dir (io/file storage-dir "zk-data")
                            :crux.kafka.embedded/kafka-log-dir (io/file storage-dir "kafka-log")
                            :crux.kafka.embedded/kafka-port kafka-port}))
;; end::ek-example[]

(defn stop-embedded-kafka [^java.io.Closeable embedded-kafka]
;; tag::ek-close[]
(.close embedded-kafka)
;; end::ek-close[]
)

;; tag::start-http-client[]
(defn start-http-client [port]
  (crux/new-api-client (str "http://localhost:" port)))
;; end::start-http-client[]

(defn example-query-entity [node]
;; tag::query-entity[]
(crux/entity (crux/db node) :dbpedia.resource/Pablo-Picasso)
;; end::query-entity[]
)

(defn example-query-valid-time [node]
;; tag::query-valid-time[]
(crux/q (crux/db node #inst "2018-05-19T09:20:27.966-00:00")
        '{:find [e]
          :where [[e :name "Pablo"]]})
;; end::query-valid-time[]
)

(defn query-example-setup [node]
  (let [maps
        ;; tag::query-input[]
        [{:crux.db/id :ivan
          :name "Ivan"
          :last-name "Ivanov"}

         {:crux.db/id :petr
          :name "Petr"
          :last-name "Petrov"}

         {:crux.db/id :smith
          :name "Smith"
          :last-name "Smith"}]
        ;; end::query-input[]
        ]

    (crux/submit-tx node
                   (vec (for [m maps]
                          [:crux.tx/put m])))))

(defn query-example-basic-query [node]
  ;; tag::basic-query[]
  (crux/q
   (crux/db node)
   '{:find [p1]
     :where [[p1 :name n]
             [p1 :last-name n]
             [p1 :name "Smith"]]})
  ;; end::basic-query[]
  )

;; tag::basic-query-r[]
#{[:smith]}
;; end::basic-query-r[]

(defn query-example-with-arguments-1 [node]
  ;; tag::query-with-arguments1[]
  (crux/q
   (crux/db node)
   '{:find [e]
     :in [first-name]
     :where [[e :name first-name]]}
   "Ivan")
  ;; end::query-with-arguments1[]
  )

;; tag::query-with-arguments1-r[]
#{[:ivan]}
;; end::query-with-arguments1-r[]

(defn query-example-with-arguments-2 [node]
  ;; tag::query-with-arguments2[]
  (crux/q
   (crux/db node)
   '{:find [e]
     :in [[first-name ...]]
     :where [[e :name first-name]]}
   ["Ivan" "Petr"])
  ;; end::query-with-arguments2[]
  )

;; tag::query-with-arguments2-r[]
#{[:ivan] [:petr]}
;; end::query-with-arguments2-r[]

(defn query-example-with-arguments-3 [node]
  ;; tag::query-with-arguments3[]
  (crux/q
   (crux/db node)
   '{:find [e]
     :in [[first-name last-name]]
     :where [[e :name first-name]
             [e :last-name last-name]]}
   ["Ivan" "Ivanov"])
  ;; end::query-with-arguments3[]
  )

;; tag::query-with-arguments3-r[]
#{[:ivan]}
;; end::query-with-arguments3-r[]

(defn query-example-with-arguments-4 [node]
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
  )

;; tag::query-with-arguments4-r[]
#{[:petr] [:smith]}
;; end::query-with-arguments4-r[]

(defn query-example-with-arguments-5 [node]
  ;; tag::query-with-arguments5[]
  (crux/q
   (crux/db node)
   '{:find [age]
     :in [[age ...]]
     :where [[(> age 21)]]}
   [21 22])
  ;; end::query-with-arguments5[]
  )

;; tag::query-with-arguments5-r[]
#{[22]}
;; end::query-with-arguments5-r[]

(defn query-example-with-predicate-1 [node]
  ;; tag::query-with-pred-1[]
  (crux/q
   (crux/db node)
   '{:find [age]
     :in [[age ...]]
     :where [[(odd? age)]]}
   [21 22])
  ;; end::query-with-pred-1[]
  )

;; tag::query-with-pred-1-r[]
#{[21]}
;; end::query-with-pred-1-r[]

(defn query-example-subquery-1 [node]
  ;; tag::sub-query-example-1[]
  (crux/q
   (crux/db node)
   '{:find [x]
     :where [[(q {:find [y]
                  :where [[(identity 2) x]
                          [(+ x 2) y]]})
              x]]})
  ;; end::sub-query-example-1[]
  )

;; tag::sub-query-example-1-r[]
#{[[[4]]]}
;; end::sub-query-example-1-r[]

(defn query-example-subquery-2 [node]
  ;; tag::sub-query-example-2[]
  (crux/q
   (crux/db node)
   '{:find [x]
     :where [[(q {:find [y]
                  :where [[(identity 2) x]
                          [(+ x 2) y]]})
              [[x]]]]})
  ;; end::sub-query-example-2[]
  )

;; tag::sub-query-example-2-r[]
#{[4]}
;; end::sub-query-example-2-r[]

(defn query-example-subquery-3 [node]
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
  )

;; tag::sub-query-example-3-r[]
#{[2 4 8]}
;; end::sub-query-example-3-r[]

(defn query-example-streaming [node prn]
  ;; tag::streaming-query[]
  (with-open [res (crux/open-q (crux/db node)
                               '{:find [p1]
                                 :where [[p1 :name n]
                                         [p1 :last-name n]
                                         [p1 :name "Smith"]]})]
    (doseq [tuple (iterator-seq res)]
      (prn tuple)))
  ;; end::streaming-query[]
  )

(defn query-example-at-time-setup [node]
  ;; tag::query-at-t-d1[]
  (crux/submit-tx
   node
   [[:crux.tx/put
     {:crux.db/id :malcolm :name "Malcolm" :last-name "Sparks"}
     #inst "1986-10-22"]])
  ;; end::query-at-t-d1[]

  ;; tag::query-at-t-d2[]
  (crux/submit-tx
   node
   [[:crux.tx/put
     {:crux.db/id :malcolm :name "Malcolma" :last-name "Sparks"}
     #inst "1986-10-24"]])
  ;; end::query-at-t-d2[]
  )

;; tag::query-at-t-q1[]
(def q
  '{:find [e]
    :where [[e :name "Malcolma"]
            [e :last-name "Sparks"]]})
;; end::query-at-t-q1[]

(defn query-example-at-time-q1 [node]
  ;; tag::query-at-t-q1-q[]
  (crux/q
   (crux/db
    node #inst "1986-10-23")
   q)
  ;; end::query-at-t-q1-q[]
  )

;; tag::query-at-t-q1-r[]
#{}
;; end::query-at-t-q1-r[]

(defn query-example-at-time-q2 [node]
  ;; tag::query-at-t-q2-q[]
  (crux/q
   (crux/db node)
   q)
  ;; end::query-at-t-q2-q[]
  )

;; tag::query-at-t-q2-r[]
#{[:malcolm]}
;; end::query-at-t-q2-r[]

#_(comment
;; tag::history-full[]
(api/submit-tx
  node
  [[:crux.tx/put
    {:crux.db/id :ids.persons/Jeff
     :person/name "Jeff"
     :person/wealth 100}
    #inst "2018-05-18T09:20:27.966"]
   [:crux.tx/put
    {:crux.db/id :ids.persons/Jeff
     :person/name "Jeff"
     :person/wealth 1000}
    #inst "2015-05-18T09:20:27.966"]])

; yields
{:crux.tx/tx-id 1555314836178,
 :crux.tx/tx-time #inst "2019-04-15T07:53:56.178-00:00"}

; Returning the history in descending order
; To return in ascending order, use :asc in place of :desc
(api/entity-history (api/db node) :ids.persons/Jeff :desc)

; yields
[{:crux.tx/tx-time #inst "2019-04-15T07:53:55.817-00:00",
  :crux.tx/tx-id 1555314835817,
  :crux.db/valid-time #inst "2018-05-18T09:20:27.966-00:00",
  :crux.db/content-hash ; sha1 hash of document contents
  "6ca48d3bf05a16cd8d30e6b466f76d5cc281b561"}
 {:crux.tx/tx-time #inst "2019-04-15T07:53:56.178-00:00",
  :crux.tx/tx-id 1555314836178,
  :crux.db/valid-time #inst "2015-05-18T09:20:27.966-00:00",
  :crux.db/content-hash "a95f149636e0a10a78452298e2135791c0203529"}]
;; end::history-full[]

;; tag::history-with-docs[]
(api/entity-history (api/db node) :ids.persons/Jeff :desc {:with-docs? true})

; yields
[{:crux.tx/tx-time #inst "2019-04-15T07:53:55.817-00:00",
  :crux.tx/tx-id 1555314835817,
  :crux.db/valid-time #inst "2018-05-18T09:20:27.966-00:00",
  :crux.db/content-hash
  "6ca48d3bf05a16cd8d30e6b466f76d5cc281b561"
  :crux.db/doc
  {:crux.db/id :ids.persons/Jeff
   :person/name "Jeff"
   :person/wealth 100}}
 {:crux.tx/tx-time #inst "2019-04-15T07:53:56.178-00:00",
  :crux.tx/tx-id 1555314836178,
  :crux.db/valid-time #inst "2015-05-18T09:20:27.966-00:00",
  :crux.db/content-hash "a95f149636e0a10a78452298e2135791c0203529"
  :crux.db/doc
  {:crux.db/id :ids.persons/Jeff
   :person/name "Jeff"
   :person/wealth 1000}}]
;; end::history-with-docs[]

;; tag::history-range[]

; Passing the aditional 'opts' map with the start/end bounds.
; As we are returning results in :asc order, the :start map contains the earlier co-ordinates -
; If returning history range in descending order, we pass the later co-ordinates to the :start map
(api/entity-history
 (api/db node)
 :ids.persons/Jeff
 :asc
 {:start {:crux.db/valid-time #inst "2015-05-18T09:20:27.966" ; valid-time-start
          :crux.tx/tx-time #inst "2015-05-18T09:20:27.966"} ; tx-time-start
  :end {:crux.db/valid-time #inst "2020-05-18T09:20:27.966" ; valid-time-end
        :crux.tx/tx-time #inst "2020-05-18T09:20:27.966"} ; tx-time-end
  })

; yields
[{:crux.tx/tx-time #inst "2019-04-15T07:53:56.178-00:00",
  :crux.tx/tx-id 1555314836178,
  :crux.db/valid-time #inst "2015-05-18T09:20:27.966-00:00",
  :crux.db/content-hash
  "a95f149636e0a10a78452298e2135791c0203529"}
 {:crux.tx/tx-time #inst "2019-04-15T07:53:55.817-00:00",
  :crux.tx/tx-id 1555314835817
  :crux.db/valid-time #inst "2018-05-18T09:20:27.966-00:00",
  :crux.db/content-hash "6ca48d3bf05a16cd8d30e6b466f76d5cc281b561"}]

;; end::history-range[]
)

(defn query-example-join-q1-setup [node]
  ;; Five people, two of which share the same name:
  (let [maps
        ;; tag::join-d[]
        [{:crux.db/id :ivan :name "Ivan"}
         {:crux.db/id :petr :name "Petr"}
         {:crux.db/id :sergei :name "Sergei"}
         {:crux.db/id :denis-a :name "Denis"}
         {:crux.db/id :denis-b :name "Denis"}]
        ;; end::join-d[]
        ]
    (crux/submit-tx node
                    (vec (for [m maps]
                           [:crux.tx/put m])))))

(defn query-example-join-q1 [node]
  ;; tag::join-q[]
  (crux/q
   (crux/db node)
   '{:find [p1 p2]
     :where [[p1 :name n]
             [p2 :name n]]})
  ;; end::join-q[]
  )

;; tag::join-r[]
#{[:ivan :ivan]
  [:petr :petr]
  [:sergei :sergei]
  [:denis-a :denis-a]
  [:denis-b :denis-b]
  [:denis-a :denis-b]
  [:denis-b :denis-a]}
;; end::join-r[]

(defn query-example-join-q2-setup [node]
  (let [maps
      ;; tag::join2-d[]
      [{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"}
       {:crux.db/id :petr :name "Petr" :follows #{"Ivanov"}}]
      ;; end::join2-d[]
      ]
  (crux/submit-tx node
                  (vec (for [m maps]
                         [:crux.tx/put m])))))


(defn query-example-join-q2 [node]
  ;; tag::join2-q[]
  (crux/q
   (crux/db node)
   '{:find [e2]
     :where [[e :last-name l]
             [e2 :follows l]
             [e :name "Ivan"]]})
  ;; end::join2-q[]
  )

;; tag::join2-r[]
#{[:petr]}
;; end::join2-r[]

(defn query-example-aggregates [node]
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
  )

;; tag::query-aggregates-r[]
  ;;=> #{[6 1 3 4 2]}
;; end::query-aggregates-r[]

(comment
  ;; following queries / result sets are in here but not tested within examples_test
  ;; tag::eql-query-1[]
  ;; with just 'query':
  (crux/q
   (crux/db node)
   '{:find [?uid ?name ?profession]
     :where [[?user :user/id ?uid]
             [?user :user/name ?name]
             [?user :user/profession ?profession]]})
  ;; end::eql-query-1[]

  ;; tag::eql-query-1-r[]
  ;; => [[1 "Ivan" :doctor] [2 "Sergei" :lawyer], [3 "Petr" :doctor]]
  ;; end::eql-query-1-r[]

  ;; tag::eql-query-2[]
  ;; using `eql/project`:
  (crux/q
   (crux/db node)
   '{:find [(eql/project ?user [:user/name :user/profession])]
     :where [[?user :user/id ?uid]]})
  ;; end::eql-query-2[]

  ;; tag::eql-query-2-r[]
  ;; => [{:user/name "Ivan", :user/profession :doctor},
  ;;     {:user/name "Sergei", :user/profession :lawyer},
  ;;     {:user/name "Petr", :user/profession :doctor}]
  ;; end::eql-query-2-r[]

  ;; tag::eql-project[]
  ;; using `project`:
  (crux/project
   (crux/db node)
   [:user/name :user/profession]
   :ivan)
  ;; end::eql-project[]

  ;; tag::eql-project-r[]
  ;; => {:user/name "Ivan", :user/profession :doctor}
  ;; end::eql-project-r[]

  ;; tag::eql-project-many[]
  ;; using `project-many`:
  (crux/project-many
   (crux/db node)
   [:user/name :user/profession]
   [:ivan :sergei])
  ;; end::eql-project-many[]

  ;; tag::eql-project-many-r[]
  ;; => [{:user/name "Ivan", :user/profession :doctor},
  ;;     {:user/name "Sergei", :user/profession :lawyer}]
  ;; end::eql-project-many-r[]

  ;; tag::eql-query-3[]
  ;; with just 'query':
  (crux/q
   (crux/db node)
   '{:find [?uid ?name ?profession-name]
     :where [[?user :user/id ?uid]
             [?user :user/name ?name]
             [?user :user/profession ?profession]
             [?profession :profession/name ?profession-name]]})
  ;; end::eql-query-3[]

  ;; tag::eql-query-3-r[]
  ;; => [[1 "Ivan" "Doctor"] [2 "Sergei" "Lawyer"], [3 "Petr" "Doctor"]]
  ;; end::eql-query-3-r[]

  ;; tag::eql-query-4[]
  ;; using `eql/project`:
  (crux/q
   (crux/db node)
   '{:find [(eql/project ?user [:user/name {:user/profession [:profession/name]}])]
     :where [[?user :user/id ?uid]]})
  ;; end::eql-query-4[]

  ;; tag::eql-query-4-r[]
  ;; => [{:user/id 1, :user/name "Ivan", :user/profession {:profession/name "Doctor"}},
  ;;     {:user/id 2, :user/name "Sergei", :user/profession {:profession/name "Lawyer"}}
  ;;     {:user/id 3, :user/name "Petr", :user/profession {:profession/name "Doctor"}}]
  ;; end::eql-query-4-r[]

  ;; tag::eql-query-5[]
  (crux/q
   (crux/db node)
   '{:find [(eql/project ?profession [:profession/name {:user/_profession [:user/id :user/name]}])]
     :where [[?profession :profession/name]]})
  ;; end::eql-query-5[]

  ;; tag::eql-query-5-r[]
  ;; => [{:profession/name "Doctor",
  ;;      :user/_profession [{:user/id 1, :user/name "Ivan"},
  ;;                         {:user/id 3, :user/name "Petr"}]},
  ;;     {:profession/name "Lawyer",
  ;;      :user/_profession [{:user/id 2, :user/name "Sergei"}]}]
  ;; end::eql-query-5-r[]

  ;; tag::eql-query-6[]
  (crux/q
   (crux/db node)
   '{:find [(eql/project ?user [*])]
     :where [[?user :user/id 1]]})
  ;; end::eql-query-6[]

  ;; tag::eql-query-6-r[]
  ;; => [{:user/id 1, :user/name "Ivan", :user/profession :doctor, ...}]
  ;; end::eql-query-6-r[]

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

  ;; tag::order-and-pagination-3[]
  (crux/q
   (crux/db node)
   '{:find '[time device-id temperature humidity]
     :where '[[c :condition/time time]
              [c :condition/device-id device-id]
              [(>= device-id my-offset)]
              [c :condition/temperature temperature]
              [c :condition/humidity humidity]]
     :order-by '[[device-id :asc]]
     :limit 10
     :args [{'my-offset 990}]})
  ;; end::order-and-pagination-3[]

  ;; tag::rules[]
  (crux/q
   (crux/db node)
   '{:find [?e2]
     :where [(follow ?e1 ?e2)]
     :args [{?e1 :1}]
     :rules [[(follow ?e1 ?e2)
              [?e1 :follow ?e2]]
             [(follow ?e1 ?e2)
              [?e1 :follow ?t]
              (follow ?t ?e2)]]})
  ;; end::rules[]

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

  ;; tag::bitempq[]
  {:find [p entry-pt arrival-time departure-time]
   :where [[p :entry-pt entry-pt]
           [p :arrival-time arrival-time]
           [p :departure-time departure-time]]}
  #inst "2019-01-03"                    ; `as of` transaction time
  #inst "2019-01-02"                    ; `as at` valid time
  ;; end::bitempq[]

  ;; tag::bitempr[]
  #{[:p2 :SFO #inst "2018-12-31" :na]
    [:p3 :LA #inst "2018-12-31" :na]
    [:p4 :NY #inst "2019-01-02" :na]}
  ;; end::bitempr[]
  )

(comment ;; Not currently used, but could be useful after some reworking.
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

  )

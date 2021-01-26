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

(def query-example-basic-query-result
;; tag::basic-query-r[]
#{[:smith]}
;; end::basic-query-r[]
)

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

(def query-example-with-arguments-1-result
;; tag::query-with-arguments1-r[]
#{[:ivan]}
;; end::query-with-arguments1-r[]
)

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

(def query-example-with-arguments-2-result
;; tag::query-with-arguments2-r[]
#{[:ivan] [:petr]}
;; end::query-with-arguments2-r[]
)

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

(def query-example-with-arguments-3-result
;; tag::query-with-arguments3-r[]
#{[:ivan]}
;; end::query-with-arguments3-r[]
)

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

(def query-example-with-arguments-4-result
;; tag::query-with-arguments4-r[]
#{[:petr] [:smith]}
;; end::query-with-arguments4-r[]
)

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

(def query-example-with-arguments-5-result
;; tag::query-with-arguments5-r[]
#{[22]}
;; end::query-with-arguments5-r[]
)

(defn query-example-with-predicate-1 [node]
;; tag::query-with-pred-1[]
(crux/q
  (crux/db node)
  '{:find [p]
    :where [[p :age age]
            [(odd? age)]]})
;; end::query-with-pred-1[]
)

(def query-example-with-predicate-1-result
;; tag::query-with-pred-1-r[]
#{[:petr] [:sergei]}
;; end::query-with-pred-1-r[]
)

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

(def query-example-subquery-1-result
;; tag::sub-query-example-1-r[]
#{[[[4]]]}
;; end::sub-query-example-1-r[]
)

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

(def query-example-subquery-2-result
;; tag::sub-query-example-2-r[]
#{[4]}
;; end::sub-query-example-2-r[]
)

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


(def query-example-subquery-3-result
;; tag::sub-query-example-3-r[]
#{[2 4 8]}
;; end::sub-query-example-3-r[]
)

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

(def query-example-at-time-q1-result
;; tag::query-at-t-q1-r[]
#{}
;; end::query-at-t-q1-r[]
)

(defn query-example-at-time-q2 [node]
;; tag::query-at-t-q2-q[]
(crux/q
 (crux/db node)
 q)
;; end::query-at-t-q2-q[]
)

(def query-example-at-time-q2-result
;; tag::query-at-t-q2-r[]
#{[:malcolm]}
;; end::query-at-t-q2-r[]
)

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

(def query-example-join-q1-result
;; tag::join-r[]
#{[:ivan :ivan]
  [:petr :petr]
  [:sergei :sergei]
  [:denis-a :denis-a]
  [:denis-b :denis-b]
  [:denis-a :denis-b]
  [:denis-b :denis-a]}
;; end::join-r[]
)

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

(def query-example-join-q2-result
;; tag::join2-r[]
#{[:petr]}
;; end::join2-r[]
)

(defn query-example-aggregates-setup [node]
      (crux/submit-tx
        node
        [])
      )

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

(def query-example-aggregates-result
;; tag::query-aggregates-r[]
  #{[6 1 3 4 2]}
;; end::query-aggregates-r[]
)

(defn query-example-eql-setup [node]
  (let [maps
        [{:crux.db/id :ivan :user/name "Ivan" :user/id 1 :user/profession :doctor}
         {:crux.db/id :sergei :user/name "Sergei" :user/id 2 :user/profession :lawyer}
         {:crux.db/id :petr :user/name "Petr" :user/id 3 :user/profession :doctor}
         {:crux.db/id :doctor :profession/name "Doctor"}
         {:crux.db/id :lawyer :profession/name "Lawyer"}]
        ]
       (crux/submit-tx node
                       (vec (for [m maps]
                                 [:crux.tx/put m])))))

(defn query-example-eql-q1 [node]
;; tag::eql-query-1[]
;; with just 'query':
(crux/q
  (crux/db node)
  '{:find [?uid ?name ?profession]
    :where [[?user :user/id ?uid]
            [?user :user/name ?name]
            [?user :user/profession ?profession]]})
;; end::eql-query-1[]
)

(def query-example-eql-q1-result
;; tag::eql-query-1-r[]
#{[1 "Ivan" :doctor] [2 "Sergei" :lawyer], [3 "Petr" :doctor]}
;; end::eql-query-1-r[]
)

(defn query-example-eql-project [node]
  ;; tag::eql-project[]
  ;; using `project`:
  (crux/project
   (crux/db node)
   [:user/name :user/profession]
   :ivan)
  ;; end::eql-project[]
  )

(def query-example-eql-project-result
  ;; tag::eql-project-r[]
  {:user/name "Ivan", :user/profession :doctor}
  ;; end::eql-project-r[]
  )

(defn query-example-eql-project-many [node]
  ;; tag::eql-project-many[]
  ;; using `project-many`:
  (crux/project-many
   (crux/db node)
   [:user/name :user/profession]
   [:ivan :sergei])
  ;; end::eql-project-many[]
  )

(def query-example-eql-project-many-result
  ;; tag::eql-project-many-r[]
  [{:user/name "Ivan", :user/profession :doctor},
   {:user/name "Sergei", :user/profession :lawyer}]
  ;; end::eql-project-many-r[]
  )

(defn query-example-eql-q2 [node]
;; tag::eql-query-2[]
;; using `eql/project`:
(crux/q
  (crux/db node)
  '{:find [(eql/project ?user [:user/name :user/profession])]
   :where [[?user :user/id ?uid]]})
;; end::eql-query-2[]
)

(def query-example-eql-q2-result
;; tag::eql-query-2-r[]
#{[{:user/name "Ivan" :user/profession :doctor}]
  [{:user/name "Sergei" :user/profession :lawyer}]
  [{:user/name "Petr" :user/profession :doctor}]}
;; end::eql-query-2-r[]
)

(defn query-example-eql-q3 [node]
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
)

(def query-example-eql-q3-result
;; tag::eql-query-3-r[]
#{[1 "Ivan" "Doctor"] [2 "Sergei" "Lawyer"] [3 "Petr" "Doctor"]}
;; end::eql-query-3-r[]
)

(defn query-example-eql-q4 [node]
;; tag::eql-query-4[]
;; using `eql/project`:
(crux/q
  (crux/db node)
  '{:find [(eql/project ?user [:user/name {:user/profession [:profession/name]}])]
    :where [[?user :user/id ?uid]]})
;; end::eql-query-4[]
)

(def query-example-eql-q4-result
;; tag::eql-query-4-r[]
#{[{:user/name "Ivan" :user/profession {:profession/name "Doctor"}}]
  [{:user/name "Sergei" :user/profession {:profession/name "Lawyer"}}]
  [{:user/name "Petr" :user/profession {:profession/name "Doctor"}}]}
;; end::eql-query-4-r[]
)

(defn query-example-eql-q5 [node]
;; tag::eql-query-5[]
(crux/q
  (crux/db node)
  '{:find [(eql/project ?profession [:profession/name {:user/_profession [:user/id :user/name]}])]
    :where [[?profession :profession/name]]})
;; end::eql-query-5[]
)

(def query-example-eql-q5-result
;; tag::eql-query-5-r[]
#{[{:profession/name "Doctor"
    :user/_profession [{:user/id 1 :user/name "Ivan"},
                       {:user/id 3 :user/name "Petr"}]}]
  [{:profession/name "Lawyer"
    :user/_profession [{:user/id 2 :user/name "Sergei"}]}]}
;; end::eql-query-5-r[]
)

(defn query-example-eql-q6 [node]
;; tag::eql-query-6[]
(crux/q
  (crux/db node)
  '{:find [(eql/project ?user [*])]
    :where [[?user :user/id 1]]})
;; end::eql-query-6[]
)

(def query-example-eql-q6-result
;; tag::eql-query-6-r[]
#{[{:crux.db/id :ivan :user/id 1, :user/name "Ivan", :user/profession :doctor}]}
;; end::eql-query-6-r[]
)

(def query-example-order-data
  (for [i (range 200)]
       {:crux.db/id i
        :condition/time (if (even? i) (quot i 4) (- (quot i 4)))
        :condition/device-id i
        :condition/temperature :temp
        :condition/humidity :hum}))

(defn query-example-order-setup [node]
  (crux/submit-tx node
                 (vec (for [m query-example-order-data]
                           [:crux.tx/put m]))))

(defn query-example-order-q1 [node]
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
)

(defn query-example-order-conversion [m]
      [(:condition/time m) (:condition/device-id m) :temp :hum])

(def query-example-order-q1-result
  (->> query-example-order-data
       (sort-by :condition/device-id <)
       (sort-by :condition/time >)
       (map query-example-order-conversion)))

(defn query-example-order-q2 [node]
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
)

(def query-example-order-q2-result
  (->> query-example-order-data
       (sort-by :condition/device-id <)
       (drop 90)
       (take 10)
       (map query-example-order-conversion)))

(defn query-example-order-q3 [node]
;; tag::order-and-pagination-3[]
(crux/q
  (crux/db node)
  '{:find [time device-id temperature humidity]
    :where [[c :condition/time time]
            [c :condition/device-id device-id]
            [(>= device-id my-offset)]
            [c :condition/temperature temperature]
            [c :condition/humidity humidity]]
    :order-by [[device-id :asc]]
    :limit 10
    :args [{my-offset 150}]})
;; end::order-and-pagination-3[]
)

(def query-example-order-q3-result
  (->> query-example-order-data
       (sort-by :condition/device-id <)
       (drop 150)
       (take 10)
       (map query-example-order-conversion)))

(defn query-example-rules-setup [node]
      (let [maps
            [{:crux.db/id :ivan :follow :petr :age 19}
             {:crux.db/id :petr :follow :sergei :age 25}
             {:crux.db/id :sergei :age 3}]
            ]
           (crux/submit-tx node
                           (vec (for [m maps]
                                     [:crux.tx/put m])))))

(defn query-example-rules-1 [node]
;; tag::rules-1[]
(crux/q
  (crux/db node)
  '{:find [p]
    :where [(adult? p)] ;;<1>
    :rules [[(adult? p) ;;<2>
             [p :age a] ;;<3>
             [(>= a 18)]]]})
;; end::rules-1[]
)

(def query-example-rules-1-result
  #{[:ivan] [:petr]}
)

(defn query-example-rules-2 [node]
;; tag::rules-2[]
(crux/q
  (crux/db node)
  '{:find [?e2]
    :where [(follow ?e1 ?e2)]
    :args [{?e1 :ivan}]
    :rules [[(follow ?e1 ?e2)
             [?e1 :follow ?e2]]
            [(follow ?e1 ?e2)
             [?e1 :follow ?t]
             (follow ?t ?e2)]]}
  )
;; end::rules-2[]
)

(def query-example-rules-2-result
#{[:petr] [:sergei]}
)

(defn query-example-not-setup [node]
(let [maps
;; tag::not-data[]
[{:crux.db/id :petr-ivanov :name "Petr" :last-name "Ivanov"} ;; <1>
 {:crux.db/id :ivan-ivanov :name "Ivan" :last-name "Ivanov"}
 {:crux.db/id :ivan-petrov :name "Ivan" :last-name "Petrov"}
 {:crux.db/id :petr-petrov :name "Petr" :last-name "Petrov"}]
;; end::not-data[]
]
(crux/submit-tx node
               (vec (for [m maps]
                         [:crux.tx/put m])))))

(defn query-example-not-q1 [node]

(crux/q
  (crux/db node)
  '{:find [e]
    :where [[e :name name]
            [e :name "Ivan"]
            (not [e :last-name "Ivanov"])]})

)

(def query-example-not-q1-result
  #{[:ivan-petrov]}
)

(defn query-example-not-q2 [node]
;; tag::not-2[]
(crux/q
  (crux/db node)
  '{:find [e]
    :where [[e :crux.db/id]
            (not [e :last-name "Ivanov"] ;;<2>
                 [e :name "Ivan"])]})
;; end::not-2[]
)

(def query-example-not-q2-result
;; tag::not-2-r[]
#{[:petr-ivanov] [:petr-petrov] [:ivan-petrov]} ;;<3>
;; end::not-2-r[]
)

(defn query-example-not-q3 [node]
      (crux/q
        (crux/db node)
        '{:find [e]
          :where [[e :name name]
                  [:ivan-petrov :last-name i-name]
                  (not [e :last-name i-name])]}))

(def query-example-not-q3-result
  #{[:ivan-ivanov] [:petr-ivanov]}
)

(defn query-example-or-setup [node]
(let [maps
;; tag::or-data[]
[{:crux.db/id :ivan-ivanov-1 :name "Ivan" :last-name "Ivanov" :sex :male} ;;<1>
 {:crux.db/id :ivan-ivanov-2 :name "Ivan" :last-name "Ivanov" :sex :male}
 {:crux.db/id :ivan-ivanovtov-1 :name "Ivan" :last-name "Ivannotov" :sex :male}
 {:crux.db/id :ivanova :name "Ivanova" :last-name "Ivanov" :sex :female}
 {:crux.db/id :bob :name "Bob" :last-name "Controlguy"}]
;; end::or-data[]
]
(crux/submit-tx node
               (vec (for [m maps]
                         [:crux.tx/put m])))))

(defn query-example-or-q1 [node]
;; tag::or-1[]
(crux/q
  (crux/db node)
  '{:find [e] ;;<2>
    :where [[e :name name]
            [e :name "Ivan"]
            (or [e :last-name "Ivanov"]
                [e :last-name "Ivannotov"])]})
;; end::or-1[]
)

(def query-example-or-q1-result
;; tag::or-1-r[]
#{[:ivan-ivanov-1] [:ivan-ivanov-2] [:ivan-ivanovtov-1]} ;;<3>
;; end::or-1-r[]
)

(defn query-example-or-q2 [node]
;; tag::or-2[]
(crux/q
  (crux/db node)
  '{:find [name]
    :where [[e :name name]
            (or [e :sex :female]
                (and [e :sex :male]
                     [e :name "Ivan"]))]})
;; end::or-2[]
)

(def query-example-or-q2-result
  #{["Ivan"] ["Ivanova"]}
)

(defn query-example-or-q3 [node]
      (crux/q
        (crux/db node)
        '{:find [?p2]
          :where [(or (and [?p2 :name "Petr"]
                           [?p2 :sex :female])
                      (and [?p2 :last-name "Ivanov"]
                           [?p2 :sex :male]))]}))

(def query-example-or-q3-result
  #{[:ivan-ivanov-1] [:ivan-ivanov-2]}
)

(defn query-example-blanks-setup [node]
  (let [maps
        [{:crux.db/id :ivan :name "Ivan"}
         {:crux.db/id :petr :name "Petr"}
         {:crux.db/id :sergei :name "Sergei"}]
        ]
       (crux/submit-tx node
                       (vec (for [m maps]
                                 [:crux.tx/put m])))))

(defn query-example-blanks [node]
  (crux/q
    (crux/db node)
    '{:find [name]
      :where [[_ :name name]]}))

(def query-example-blanks-result
  #{["Ivan"] ["Petr"] ["Sergei"]}
)

(defn query-example-not-join-setup [node]
(let [maps
;; tag::not-join-data[]
[{:crux.db/id :ivan :name "Ivan" :last-name "Ivanov"} ;;<1>
 {:crux.db/id :petr :name "Petr" :last-name "Petrov"}
 {:crux.db/id :sergei :name "Sergei" :last-name "Sergei"}]
;; end::not-join-data[]
]
(crux/submit-tx node
               (vec (for [m maps]
                         [:crux.tx/put m])))))

(defn query-example-not-join [node]
;; tag::not-join[]
(crux/q
  (crux/db node)
  '{:find [e]
    :where [[e :crux.db/id]
            (not-join [e] ;;<2>
                      [e :last-name n] ;;<3>
                      [e :name n])]})
;; end::not-join[]
)

(def query-example-not-join-result
;; tag::not-join-r[]
#{[:ivan] [:petr]} ;;<4>
;; end::not-join-r[]
)

(defn query-example-anatomy-setup [node]
      (crux/submit-tx
        node
        [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :last-name "Ivan"}]]))

(defn query-example-anatomy [node]
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
)

(def query-example-anatomy-result
  #{[:ivan]}
)

(defn query-example-triple [node]
;; tag::triple[]
(crux/q
  (crux/db node)
  '{:find [n]
    :where [[p :last-name n]]})
;; end::triple[]
)

(def query-example-triple-result
  #{["Ivan"]}
)

(defn query-example-double [node]
;; tag::double[]
(crux/q
  (crux/db node)
  '{:find [p]
    :where [[p :name]]}) ;;<1>
;; end::double[]
)

(def query-example-double-result
  #{[:ivan]}
)

(defn query-example-triple-2 [node]
;; tag::triple-2[]
(crux/q
  (crux/db node)
  '{:find [p]
    :where [[p :name "Ivan"]]}) ;;<2>
;; end::triple-2[]
)

(def query-example-triple-2-result
  #{[:ivan]}
)

(defn query-example-triple-3 [node]
;; tag::triple-3[]
(crux/q
  (crux/db node)
  '{:find [p]
    :where [[q :name n]
            [p :last-name n]]}) ;;<3>
;; end::triple-3[]
)

(def query-example-triple-3-result
  #{[:ivan]}
)

(defn query-example-or-join-setup [node]
(let [maps
;; tag::or-join-data[]
[{:crux.db/id :ivan :name "Ivan" :age 12} ;;<1>
 {:crux.db/id :petr :name "Petr" :age 15}
 {:crux.db/id :sergei :name "Sergei" :age 19}]
;; end::or-join-data[]
]
(crux/submit-tx node
               (vec (for [m maps]
                         [:crux.tx/put m])))))

(defn query-example-or-join [node]
;; tag::or-join[]
(crux/q
  (crux/db node)
  '{:find [p]
    :where [[p :crux.db/id]
            (or-join [p] ;;<2>
                     (and [p :age a] ;;<3>
                          [(>= a 18)])
                     [p :name "Ivan"])]})
;; end::or-join[]
)

(def query-example-or-join-result
;; tag::or-join-r[]
#{[:ivan] [:sergei]} ;;<4>
;; end::or-join-r[]
)

(defn query-example-range-setup [node]
(let [maps
;; tag::range-data[]
[{:crux.db/id :ivan :name "Ivan" :age 12} ;;<1>
 {:crux.db/id :petr :name "Petr" :age 15}
 {:crux.db/id :sergei :name "Sergei" :age 19}]
;; end::range-data[]
]
(crux/submit-tx node
               (vec (for [m maps]
                         [:crux.tx/put m])))))

(defn query-example-range-1 [node]
;; tag::range-1[]
(crux/q
  (crux/db node)
  '{:find [p] ;;<1>
          :where [[p :age a]
                  [(> a 18)]]})
;; end::range-1[]
)

(def query-example-range-1-result
  #{[:sergei]}
  )

(defn query-example-range-2 [node]
;; tag::range-2[]
(crux/q
  (crux/db node)
  '{:find [p] ;;<2>
          :where [[p :age a]
                  [q :age b]
                  [(> a b)]]})
;; end::range-2[]
)

(def query-example-range-2-result
  #{[:petr] [:sergei]}
  )

(defn query-example-range-3 [node]
;; tag::range-3[]
(crux/q
  (crux/db node)
  '{:find [p] ;;<3>
          :where [[p :age a]
                  [(> 18 a)]]})
;; end::range-3[]
)

(def query-example-range-3-result
  #{[:ivan] [:petr]}
  )

(defn query-example-full-results-setup [node]
;; tag::full-results-data[]
(crux/submit-tx node [[:crux.tx/put {:crux.db/id :foo :bar :baz}]])
;; end::full-results-data[]
)

(defn query-example-full-results-1 [node]
;; tag::full-results-1[]
(crux/q
  (crux/db node)
  '{:find [p]
    :where [[p :bar :baz]]})
;; end::full-results-1[]
)

(def query-example-full-results-1-result
;; tag::full-results-1-r[]
#{[:foo]}
;; end::full-results-1-r[]
)

(defn query-example-full-results-2 [node]
;; tag::full-results-2[]
(crux/q
  (crux/db node)
    '{:find [p]
      :where [[p :bar :baz]]
      :full-results? true})
;; end::full-results-2[]
)

(def query-example-full-results-2-result
;; tag::full-results-2-r[]
#{[{:crux.db/id :foo :bar :baz}]}
;; end::full-results-2-r[]
)

;; These will all be run as part of crux.examples-test in crux-test
(def basic-tests
  [{:n "Basic Examples"
    :s query-example-setup
    :t [{:d "Query example"
         :e query-example-basic-query-result
         :q query-example-basic-query}
        {:d "Arguments 1"
         :e query-example-with-arguments-1-result
         :q query-example-with-arguments-1}
        {:d "Arguments 2"
         :e query-example-with-arguments-2-result
         :q query-example-with-arguments-2}
        {:d "Arguments 3"
         :e query-example-with-arguments-3-result
         :q query-example-with-arguments-3}
        {:d "Arguments 4"
         :e query-example-with-arguments-4-result
         :q query-example-with-arguments-4}
        {:d "Arguments 5"
         :e query-example-with-arguments-5-result
         :q query-example-with-arguments-5}
        {:d "Subquery 1"
         :e query-example-subquery-1-result
         :q query-example-subquery-1}
        {:d "Subquery 2"
         :e query-example-subquery-2-result
         :q query-example-subquery-2}
        {:d "Subquery 3"
         :e query-example-subquery-3-result
         :q query-example-subquery-3}]}
   {:n "Queries in time"
    :s query-example-at-time-setup
    :t [{:d "Query 1"
         :e query-example-at-time-q1-result
         :q query-example-at-time-q1}
        {:d "Query 2"
         :e query-example-at-time-q2-result
         :q query-example-at-time-q2}]}
   {:n "Aggregates"
    :s query-example-aggregates-setup
    :t [{:d ""
         :e query-example-aggregates-result
         :q query-example-aggregates}]}
   {:n "Join Queries 1"
    :s query-example-join-q1-setup
    :t [{:d ""
         :e query-example-join-q1-result
         :q query-example-join-q1}]}
   {:n "Join Queries 2"
    :s query-example-join-q2-setup
    :t [{:d ""
         :e query-example-join-q2-result
         :q query-example-join-q2}]}
   {:n "EQL Projection"
    :s query-example-eql-setup
    :t [{:d "Simple without projection"
         :e query-example-eql-q1-result
         :q query-example-eql-q1}
        {:d "Simple with projection"
         :e query-example-eql-q2-result
         :q query-example-eql-q2}
        {:d "Nested without projection"
         :e query-example-eql-q3-result
         :q query-example-eql-q3}
        {:d "Nested with projection"
         :e query-example-eql-q4-result
         :q query-example-eql-q4}
        {:d "Inverse projection"
         :e query-example-eql-q5-result
         :q query-example-eql-q5}
        {:d "Using *"
         :e query-example-eql-q6-result
         :q query-example-eql-q6}
        ]}
   {:n "Not"
    :s query-example-not-setup
    :t [{:d "Literal v"
         :e query-example-not-q1-result
         :q query-example-not-q1}
        {:d "Multiple terms"
         :e query-example-not-q2-result
         :q query-example-not-q2}
        {:d "Variable v"
         :e query-example-not-q3-result
         :q query-example-not-q3}
        ]}
   {:n "Or"
    :s query-example-or-setup
    :t [{:d "Simple"
         :e query-example-or-q1-result
         :q query-example-or-q1}
        {:d "With And"
         :e query-example-or-q2-result
         :q query-example-or-q2}
        {:d "Introducing new variables"
         :e query-example-or-q3-result
         :q query-example-or-q3}
        ]}
   {:n "Blanks"
    :s query-example-blanks-setup
    :t [{:d ""
         :e query-example-blanks-result
         :q query-example-blanks}]}
   {:n "Not Joins"
    :s query-example-not-join-setup
    :t [{:d ""
         :e query-example-not-join-result
         :q query-example-not-join}]}
   {:n "Order and Pagination"
    :s query-example-order-setup
    :t [{:d "Sorting"
         :e query-example-order-q1-result
         :q query-example-order-q1}
        {:d "Limits and Offsets"
         :e query-example-order-q2-result
         :q query-example-order-q2}
        {:d "Custom offset"
         :e query-example-order-q3-result
         :q query-example-order-q3}
        ]}
   {:n "Rules"
    :s query-example-rules-setup
    :t [{:d "Standard"
         :e query-example-rules-1-result
         :q query-example-rules-1}
        {:d "Recursive"
         :e query-example-rules-2-result
         :q query-example-rules-2}]}
   {:n "Generic Examples"
    :s query-example-anatomy-setup
    :t [{:d "Anatomy"
         :e query-example-anatomy-result
         :q query-example-anatomy}
        {:d "Find"
         :e query-example-triple-result
         :q query-example-triple}
        {:d "Double"
         :e query-example-double-result
         :q query-example-double}
        {:d "Triple literal"
         :e query-example-triple-2-result
         :q query-example-triple-2}
        ]}
   {:n "Or Join"
    :s query-example-or-join-setup
    :t [{:d ""
         :e query-example-or-join-result
         :q query-example-or-join}]}
   {:n "Range check"
    :s query-example-range-setup
    :t [{:d "variable constant"
         :e query-example-range-1-result
         :q query-example-range-1}
        {:d "variable variable"
         :e query-example-range-2-result
         :q query-example-range-2}
        {:d "constant variable"
         :e query-example-range-3-result
         :q query-example-range-3}
        {:d "Predicate"
         :e query-example-with-predicate-1-result
         :q query-example-with-predicate-1}]}
   {:n "Full results"
    :s query-example-full-results-setup
    :t [{:d "Without"
         :e query-example-full-results-1-result
         :q query-example-full-results-1}
        {:d "With"
         :e query-example-full-results-2-result
         :q query-example-full-results-2}]}
   ])

(comment
;; following examples are in here but not tested within examples_test

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

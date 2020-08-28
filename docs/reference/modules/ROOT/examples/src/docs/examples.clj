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
 (crux/q
  (crux/db node)
 ;; tag::basic-query[]
 '{:find [p1]
   :where [[p1 :name n]
           [p1 :last-name n]
           [p1 :name "Smith"]]}
 ;; end::basic-query[]
))

#_(comment
;; tag::basic-query-r[]
#{[:smith]}
;; end::basic-query-r[]
)

(defn query-example-with-arguments-1 [node]
 (crux/q
  (crux/db node)
 ;; tag::query-with-arguments1[]
 {:find '[n]
  :where '[[e :name n]]
  :args [{'e :ivan
          'n "Ivan"}]}
 ;; end::query-with-arguments1[]
))

#_(comment
;; tag::query-with-arguments1-r[]
#{["Ivan"]}
;; end::query-with-arguments1-r[]
)

(defn query-example-with-arguments-2 [node]
 (crux/q
  (crux/db node)
  ;; tag::query-with-arguments2[]
 {:find '[e]
  :where '[[e :name n]]
  :args [{'n "Ivan"}
         {'n "Petr"}]}
  ;; end::query-with-arguments2[]
))

#_(comment
;; tag::query-with-arguments2-r[]
#{[:petr] [:ivan]}
;; end::query-with-arguments2-r[]
)

(defn query-example-with-arguments-3 [node]
 (crux/q
  (crux/db node)
  ;; tag::query-with-arguments3[]
 {:find '[e]
  :where '[[e :name n]
           [e :last-name l]]
  :args [{'n "Ivan" 'l "Ivanov"}
         {'n "Petr" 'l "Petrov"
          }]}
 ;; end::query-with-arguments3[]
))

#_(comment
;; tag::query-with-arguments3-r[]
#{[:petr] [:ivan]}
;; end::query-with-arguments3-r[]
)

(defn query-example-with-arguments-4 [node]
 (crux/q
  (crux/db node)
 ;; tag::query-with-arguments4[]
 {:find '[n]
  :where '[[(re-find #"I" n)]
           [(= l "Ivanov")]]
  :args [{'n "Ivan" 'l "Ivanov"}
         {'n "Petr" 'l "Petrov"}]}
 ;; end::query-with-arguments4[]
 ))

#_(comment
 ;; tag::query-with-arguments4-r[]
 #{["Ivan"]}
 ;; end::query-with-arguments4-r[]
 )

(defn query-example-with-arguments-5 [node]
 (crux/q
  (crux/db node)
 ;; tag::query-with-arguments5[]
 {:find '[age]
  :where '[[(>= age 21)]]
  :args [{'age 22}]}
 ;; end::query-with-arguments5[]
 ))

#_(comment
;; tag::query-with-arguments5-r[]
#{[22]}
;; end::query-with-arguments5-r[]
)

(defn query-example-with-predicate-1 [node]
  (crux/q
   (crux/db node)
   ;; tag::query-with-pred-1[]
   {:find '[age]
    :where '[[(odd? age)]]
    :args [{'age 22} {'age 21}]}
   ;; end::query-with-pred-1[]
 ))

#_(comment
;; tag::query-with-pred-1-r[]
#{[21]}
;; end::query-with-pred-1-r[]
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
 (crux/submit-tx
  node
  [[:crux.tx/put
    ;; tag::query-at-t-d1[]
    {:crux.db/id :malcolm :name "Malcolm" :last-name "Sparks"}
    #inst "1986-10-22"
    ;; end::query-at-t-d1[]
    ]])

 (crux/submit-tx
  node
  [[:crux.tx/put
    ;; tag::query-at-t-d2[]
    {:crux.db/id :malcolm :name "Malcolma" :last-name "Sparks"}
    #inst "1986-10-24"
    ;; end::query-at-t-d2[]
    ]]))

(defn query-example-at-time-q1 [node]
 (crux/q
  (crux/db
   node #inst "1986-10-23")
  ;; tag::query-at-t-q1[]
  '{:find [e]
    :where [[e :name "Malcolma"]
            [e :last-name "Sparks"]]}
  ;; end::query-at-t-q1[]
))

;; tag::query-at-t-q1-q[]
; Using Clojure: `(api/q (api/db my-crux-system #inst "1986-10-23") q)`
;; end::query-at-t-q1-q[]

#_(comment
;; tag::query-at-t-q1-r[]
#{}
;; end::query-at-t-q1-r[]
)

(defn query-example-at-time-q2 [node]
  (crux/q
   (crux/db node)
   '{:find [e]
     :where [[e :name "Malcolma"]
             [e :last-name "Sparks"]]}))

;; tag::query-at-t-q2-q[]
; Using Clojure: `(api/q (api/db my-crux-system) q)`
;; end::query-at-t-q2-q[]

#_(comment
;; tag::query-at-t-q2-r[]
#{[:malcolm]}
;; end::query-at-t-q2-r[]
)

#_(comment
;; tag::history-full[]
(api/submit-tx
  system
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
(api/entity-history (api/db system) :ids.persons/Jeff :desc)

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
(api/entity-history (api/db system) :ids.persons/Jeff :desc {:with-docs? true})

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
 (api/db system)
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
 (crux/q
  (crux/db node)
 ;; tag::join-q[]
 '{:find [p1 p2]
   :where [[p1 :name n]
           [p2 :name n]]}
 ;; end::join-q[]
))

#_(comment
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
 (crux/q
  (crux/db node)
 ;; tag::join2-q[]
 '{:find [e2]
   :where [[e :last-name l]
           [e2 :follows l]
           [e :name "Ivan"]]}
 ;; end::join2-q[]
))

(comment
;; tag::join2-r[]
#{[:petr]}
;; end::join2-r[]
)

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

(ns docs.examples)

;; tag::include-crux-api[]
(require '[crux.api :as crux])
(import (crux.api ICruxAPI))
;; end::include-crux-api[]

;; tag::require-ek[]
(require '[crux.kafka.embedded :as ek])
;; end::require-ek[]

(defn example-start-standalone []
;; tag::start-standalone-node[]
(def ^crux.api.ICruxAPI node
  (crux/start-node {:crux.node/topology :crux.standalone/topology
                    :crux.node/kv-store "crux.kv.memdb/kv"
                    :crux.kv/db-dir "data/db-dir-1"
                    :crux.standalone/event-log-dir "data/eventlog-1"
                    :crux.standalone/event-log-kv-store "crux.kv.memdb/kv"}))
;; end::start-standalone-node[]
  node)

(defn example-start-in-memory-standalone []
;; tag::start-in-memory-standalone-node[]
(def ^crux.api.ICruxAPI node
  (crux/start-node {:crux.node/topology :crux.standalone/topology
                    :crux.node/kv-store "crux.kv.memdb/kv"
                    :crux.standalone/event-log-kv "crux.kv.memdb/kv"}))
;; end::start-in-memory-standalone-node[]
  node)

(defn example-close-node [^java.io.Closeable node]
;; tag::close-node[]
(.close node)
;; end::close-node[]
)

(defn example-start-embedded-kafka []
;; tag::ek-example[]
(def storage-dir "dev-storage")
(def embedded-kafka-options
  {:crux.kafka.embedded/zookeeper-data-dir (str storage-dir "/zookeeper")
   :crux.kafka.embedded/kafka-log-dir (str storage-dir "/kafka-log")
   :crux.kafka.embedded/kafka-port 9092})

(def embedded-kafka (ek/start-embedded-kafka embedded-kafka-options))
;; end::ek-example[]
embedded-kafka)

(defn example-stop-embedded-kafka [^java.io.Closeable embedded-kafka]
;; tag::ek-close[]
(.close embedded-kafka)
;; end::ek-close[]
)

(defn example-start-cluster []
;; tag::start-cluster-node[]
(def ^crux.api.ICruxAPI node
  (crux/start-node {:crux.node/topology :crux.kafka/topology
                    :crux.node/kv-store "crux.kv.memdb/kv"
                    :crux.kafka/bootstrap-servers "localhost:9092"}))
;; end::start-cluster-node[]
node)

(defn example-start-rocks []
;; tag::start-standalone-with-rocks[]
(def ^crux.api.ICruxAPI node
  (crux/start-node {:crux.node/topology :crux.standalone/topology
                    :crux.node/kv-store "crux.kv.rocksdb/kv"
                    :crux.kv/db-dir "data/db-dir-1"
                    :crux.standalone/event-log-dir "data/eventlog-1"}))
;; end::start-standalone-with-rocks[]
node)

(defn example-start-lmdb []
;; tag::start-standalone-with-lmdb[]
(def ^crux.api.ICruxAPI node
  (crux/start-node {:crux.node/topology :crux.standalone/topology
                    :crux.node/kv-store "crux.kv.lmdb/kv"
                    :crux.kv/db-dir "data/db-dir-1"
                    :crux.standalone/event-log-dir "data/eventlog-1"
                    :crux.standalone/event-log-kv-store "crux.kv.lmdb/kv"}))
;; end::start-standalone-with-lmdb[]
node)

(defn example-start-jdbc []
;; tag::start-jdbc-node[]
(def ^crux.api.ICruxAPI node
  (crux/start-node {:crux.node/topology :crux.jdbc/topology
                    :crux.jdbc/dbtype "postgresql"
                    :crux.jdbc/dbname "cruxdb"
                    :crux.jdbc/host "<host>"
                    :crux.jdbc/user "<user>"
                    :crux.jdbc/password "<password>"}))
  ;; end::start-jdbc-node[]
  )

(defn example-submit-tx [node]
;; tag::submit-tx[]
(crux/submit-tx
 node
 [[:crux.tx/put
   {:crux.db/id :dbpedia.resource/Pablo-Picasso ; id
    :name "Pablo"
    :last-name "Picasso"}
   #inst "2018-05-18T09:20:27.966-00:00"]]) ; valid time
;; end::submit-tx[]
)

(defn example-query [node]
;; tag::query[]
(crux/q (crux/db node)
        '{:find [e]
          :where [[e :name "Pablo"]]})
;; end::query[]
)

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

#_(comment
;; tag::should-get[]
#{[:dbpedia.resource/Pablo-Picasso]}
;; end::should-get[]

;; tag::should-get-entity[]
{:crux.db/id :dbpedia.resource/Pablo-Picasso
:name "Pablo"
:last-name "Picasso"}
;; end::should-get-entity[]
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
(api/history-range system :ids.persons/Jeff
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


(api/entity (api/db system) "c7e66f757f198e08a07a8ea6dfc84bc3ab1c6613")

; yields
{:crux.db/id :ids.persons/Jeff,
 :d.person/name "Jeff",
 :d.person/wealth 100}
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

(comment ;; Used in decorators.adoc - will not work in this namespace

  ;; tag::aggr1[]
  (t/deftest test-count-aggregation
    (f/transact-entity-maps!
     *kv*
     [{:crux.db/id :a1 :user/name "patrik" :user/post 1 :post/cost 30}
      {:crux.db/id :a2 :user/name "patrik" :user/post 2 :post/cost 35}
      {:crux.db/id :a3 :user/name "patrik" :user/post 3 :post/cost 5}
      {:crux.db/id :a4 :user/name "niclas" :user/post 1 :post/cost 8}])

    (t/testing "with vector syntax"
      (t/is (= [{:user-name "niclas" :post-count 1 :cost-sum 8}
                {:user-name "patrik" :post-count 3 :cost-sum 70}]
               (aggr/q
                (api/db *api*)
                '{:aggr {:partition-by [?user-name]
                         :select
                         {?cost-sum [0 (+ acc ?post-cost)]
                          ?post-count [0 (inc acc) ?e]}}
                  :where [[?e :user/name ?user-name]
                          [?e :post/cost ?post-cost]]})))))
  ;; end::aggr1[]
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

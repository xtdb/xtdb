(ns crux.bitemp-test
  (:require [crux.api :as crux]
            [clojure.test :as t]
            [crux.fixtures :as fix :refer [*api*]]))

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

(t/use-fixtures :each
  fix/with-standalone-topology

  (fn [f]
    (t/testing "without z-curve index"
      (f))

    (t/testing "with z-curve index"
      (fix/with-opts {:crux.kv-indexer/z-curve-index? true} f)))

  fix/with-kv-dir

  fix/with-node)

(t/deftest test-bitemp-query-from-indexing-temporal-data-using-existing-b+-trees-paper
  ;; Day 0, represented as #inst "2018-12-31"
  (crux/submit-tx *api* [[:crux.tx/put
                         {:crux.db/id :p2
                          :entry-pt :SFO
                          :arrival-time #inst "2018-12-31"
                          :departure-time :na}
                         #inst "2018-12-31"]
                        [:crux.tx/put
                         {:crux.db/id :p3
                          :entry-pt :LA
                          :arrival-time #inst "2018-12-31"
                          :departure-time :na}
                         #inst "2018-12-31"]])
  ;; Day 1, nothing happens.
  (crux/submit-tx *api* [])
  ;; Day 2
  (crux/submit-tx *api* [[:crux.tx/put
                         {:crux.db/id :p4
                          :entry-pt :NY
                          :arrival-time #inst "2019-01-02"
                          :departure-time :na}
                         #inst "2019-01-02"]])
  ;; Day 3
  (let [third-day-submitted-tx (crux/submit-tx *api* [[:crux.tx/put
                                                       {:crux.db/id :p4
                                                        :entry-pt :NY
                                                        :arrival-time #inst "2019-01-02"
                                                        :departure-time #inst "2019-01-03"}
                                                       #inst "2019-01-03"]])]
    ;; this introduces enough delay s.t. tx3 and tx4 don't happen in the same millisecond
    ;; (see test further down)
    ;; because we only ask for the DB at the tx-time, currently, not the tx-id
    ;; see #421
    (crux/await-tx *api* third-day-submitted-tx)

    ;; Day 4, correction, adding missing trip on new arrival.
    (crux/submit-tx *api* [[:crux.tx/put
                            {:crux.db/id :p1
                             :entry-pt :NY
                             :arrival-time #inst "2018-12-31"
                             :departure-time :na}
                            #inst "2018-12-31"]
                           [:crux.tx/put
                            {:crux.db/id :p1
                             :entry-pt :NY
                             :arrival-time #inst "2018-12-31"
                             :departure-time #inst "2019-01-03"}
                            #inst "2019-01-03"]
                           [:crux.tx/put
                            {:crux.db/id :p1
                             :entry-pt :LA
                             :arrival-time #inst "2019-01-04"
                             :departure-time :na}
                            #inst "2019-01-04"]
                           [:crux.tx/put
                            {:crux.db/id :p3
                             :entry-pt :LA
                             :arrival-time #inst "2018-12-31"
                             :departure-time #inst "2019-01-04"}
                            #inst "2019-01-04"]])
    ;; Day 5
    (crux/submit-tx *api* [[:crux.tx/put
                            {:crux.db/id :p2
                             :entry-pt :SFO
                             :arrival-time #inst "2018-12-31"
                             :departure-time #inst "2018-12-31"}
                            #inst "2019-01-05"]])
    ;; Day 6, nothing happens.
    (crux/submit-tx *api* [])
    ;; Day 7-12, correction of deletion/departure on day 4. Shows
    ;; how valid time cannot be the same as arrival time.
    (crux/submit-tx *api* [[:crux.tx/put
                            {:crux.db/id :p3
                             :entry-pt :LA
                             :arrival-time #inst "2018-12-31"
                             :departure-time :na}
                            #inst "2019-01-04"]
                           [:crux.tx/put
                            {:crux.db/id :p3
                             :entry-pt :LA
                             :arrival-time #inst "2018-12-31"
                             :departure-time #inst "2019-01-07"}
                            #inst "2019-01-07"]])
    (crux/submit-tx *api* [[:crux.tx/put
                            {:crux.db/id :p3
                             :entry-pt :SFO
                             :arrival-time #inst "2019-01-08"
                             :departure-time :na}
                            #inst "2019-01-08"]
                           [:crux.tx/put
                            {:crux.db/id :p4
                             :entry-pt :LA
                             :arrival-time #inst "2019-01-08"
                             :departure-time :na}
                            #inst "2019-01-08"]])
    (crux/submit-tx *api* [[:crux.tx/put
                            {:crux.db/id :p3
                             :entry-pt :SFO
                             :arrival-time #inst "2019-01-08"
                             :departure-time #inst "2019-01-08"}
                            #inst "2019-01-09"]])
    (crux/submit-tx *api* [[:crux.tx/put
                            {:crux.db/id :p5
                             :entry-pt :LA
                             :arrival-time #inst "2019-01-10"
                             :departure-time :na}
                            #inst "2019-01-10"]])
    (crux/submit-tx *api* [[:crux.tx/put
                            {:crux.db/id :p7
                             :entry-pt :NY
                             :arrival-time #inst "2019-01-11"
                             :departure-time :na}
                            #inst "2019-01-11"]])

    (doto (crux/submit-tx *api* [[:crux.tx/put
                                  {:crux.db/id :p6
                                   :entry-pt :NY
                                   :arrival-time #inst "2019-01-12"
                                   :departure-time :na}
                                  #inst "2019-01-12"]])
      (->> (crux/await-tx *api*)))

    (t/is (= #{[:p2 :SFO #inst "2018-12-31" :na]
               [:p3 :LA #inst "2018-12-31" :na]
               [:p4 :NY #inst "2019-01-02" :na]}
             (crux/q (crux/db *api* #inst "2019-01-02" (:crux.tx/tx-time third-day-submitted-tx))
                     '{:find [p entry-pt arrival-time departure-time]
                       :where [[p :entry-pt entry-pt]
                               [p :arrival-time arrival-time]
                               [p :departure-time departure-time]]})))))

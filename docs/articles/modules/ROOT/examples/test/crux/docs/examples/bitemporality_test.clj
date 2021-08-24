(ns crux.docs.examples.bitemporality-test
    (:require [clojure.test :as t]
              [crux.api :as crux]
              [crux.fixtures :as fix :refer [*api*]]))
(def ^:dynamic *storage-dir*)

(defn with-storage-dir [f]
      (fix/with-tmp-dir "storage" [storage-dir]
                        (binding [*storage-dir* storage-dir]
                                 (f))))

(t/use-fixtures :each fix/with-node with-storage-dir)

(defn put-at-vt [node doc vt]
      (crux/submit-tx node [[:crux.tx/put doc vt]]))

(t/deftest test-bitemp
  (let [node *api*
        put (partial put-at-vt node)]
       (put
         ;; tag::bitemp0[]
         {:xt/id :p2
          :entry-pt :SFO
          :arrival-time #inst "2018-12-31"
          :departure-time :na}
         #inst "2018-12-31"
         ;; end::bitemp0[]
         )

       (put
         ;; tag::bitemp1[]
         {:xt/id :p3
          :entry-pt :LA
          :arrival-time #inst "2018-12-31"
          :departure-time :na}
         #inst "2018-12-31"
         ;; end::bitemp1[]
         )

       (put
         ;; tag::bitemp2[]
         {:xt/id :p4
          :entry-pt :NY
          :arrival-time #inst "2019-01-02"
          :departure-time :na}
         #inst "2019-01-02"
         ;; end::bitemp2[]
         )

       ;; So we get a distinct transaction time around our day 3 put
       (Thread/sleep 20)

       (let [transaction
             (put
               ;; tag::bitemp3[]
               {:xt/id :p4
                :entry-pt :NY
                :arrival-time #inst "2019-01-02"
                :departure-time #inst "2019-01-03"}
               #inst "2019-01-03"
               ;; end::bitemp3[]
               )]

       ;; So we get a distinct transaction time around our day 3 put
       (Thread/sleep 20)

       (put
         ;; tag::bitemp4[]
         {:xt/id :p1
          :entry-pt :NY
          :arrival-time #inst "2018-12-31"
          :departure-time :na}
         #inst "2018-12-31"
         ;; end::bitemp4[]
         )

       (put
         ;; tag::bitemp4b[]
         {:xt/id :p1
          :entry-pt :NY
          :arrival-time #inst "2018-12-31"
          :departure-time #inst "2019-01-03"}
         #inst "2019-01-03"
         ;; end::bitemp4b[]
         )

       (put
         ;; tag::bitemp4c[]
         {:xt/id :p1
          :entry-pt :LA
          :arrival-time #inst "2019-01-04"
          :departure-time :na}
         #inst "2019-01-04"
         ;; end::bitemp4c[]
         )

       (put
         ;; tag::bitemp4d[]
         {:xt/id :p3
          :entry-pt :LA
          :arrival-time #inst "2018-12-31"
          :departure-time #inst "2019-01-04"}
         #inst "2019-01-04"
         ;; end::bitemp4d[]
         )

       (put
         ;; tag::bitemp5[]
         {:xt/id :p2
          :entry-pt :SFO
          :arrival-time #inst "2018-12-31"
          :departure-time #inst "2019-01-05"}
         #inst "2019-01-05"
         ;; end::bitemp5[]
         )

       (put
         ;; tag::bitemp7a[]
         {:xt/id :p3
          :entry-pt :LA
          :arrival-time #inst "2018-12-31"
          :departure-time :na}
         #inst "2019-01-04"
         ;; end::bitemp7a[]
         )

       (put
         ;; tag::bitemp7b[]
         {:xt/id :p3
          :entry-pt :LA
          :arrival-time #inst "2018-12-31"
          :departure-time #inst "2019-01-07"}
         #inst "2019-01-07"
         ;; end::bitemp7b[]
         )

       (put
         ;; tag::bitemp8a[]
         {:xt/id :p3
          :entry-pt :SFO
          :arrival-time #inst "2019-01-08"
          :departure-time :na}
         #inst "2019-01-08"
         ;; end::bitemp8a[]
         )

       (put
         ;; tag::bitemp8b[]
         {:xt/id :p4
          :entry-pt :LA
          :arrival-time #inst "2019-01-08"
          :departure-time :na}
         #inst "2019-01-08"
         ;; end::bitemp8b[]
         )

       (put
         ;; tag::bitemp9[]
         {:xt/id :p3
          :entry-pt :SFO
          :arrival-time #inst "2019-01-08"
          :departure-time #inst "2019-01-08"}
         #inst "2019-01-09"
         ;; end::bitemp9[]
         )

        (put
          ;; tag::bitemp10[]
          {:xt/id :p5
           :entry-pt :LA
           :arrival-time #inst "2019-01-10"
           :departure-time :na}
          #inst "2019-01-10"
          ;; end::bitemp10[]
          )

       (put
         ;; tag::bitemp11[]
         {:xt/id :p7
          :entry-pt :NY
          :arrival-time #inst "2019-01-11"
          :departure-time :na}
         #inst "2019-01-11"
         ;; end::bitemp11[]
         )

       (put
         ;; tag::bitemp12[]
         {:xt/id :p6
          :entry-pt :NY
          :arrival-time #inst "2019-01-12"
          :departure-time :na}
         #inst "2019-01-12"
         ;; end::bitemp12[]
         )

       (crux/sync node)

       (t/is (=
               ;; tag::bitempr[]
               #{[:p2 :SFO #inst "2018-12-31" :na]
                 [:p3 :LA #inst "2018-12-31" :na]
                 [:p4 :NY #inst "2019-01-02" :na]}
               ;; end::bitempr[]

               ;; tag::bitempq-a[]
               (crux/q
                 (crux/db node
                          {
                           :crux.db/valid-time #inst "2019-01-02" ; `as at` valid time
               ;; end::bitempq-a[]
                          ;; Fudging this so we can use the real transaction time
                           :crux.tx/tx transaction
                          ;; tag::bitempq-c[]
                          })

                 '{:find [p entry-pt arrival-time departure-time]
                   :where [[p :entry-pt entry-pt]
                           [p :arrival-time arrival-time]
                           [p :departure-time departure-time]]}
               ;; end::bitempq-c[]
               ))))))

;; Fudging this bit in the actual test
(comment
  ;; tag::bitempq-b[]
  :crux.tx/tx #inst "2019-01-03" ; `as of` transaction time
  ;; end::bitempq-b[]
  )

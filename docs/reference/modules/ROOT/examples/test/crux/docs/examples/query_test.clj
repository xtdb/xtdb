(ns crux.docs.examples.query-test
  (:require [clojure.test :as t]
            [crux.api :as crux]
            [crux.fixtures :as fix :refer [*api*]]))

(t/use-fixtures :each fix/with-node)

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

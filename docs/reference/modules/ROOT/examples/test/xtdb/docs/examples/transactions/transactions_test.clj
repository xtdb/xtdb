(ns xtdb.docs.examples.transactions.transactions-test
  (:require [clojure.test :as t]
            [crux.api :as xt]
            [crux.fixtures :as fix :refer [*api*]]))

;; Note: Not using submit-and-await so that the snippets for the doc can include the submit

(t/use-fixtures :each fix/with-node)

(defn- put-pablo []
  (fix/submit+await-tx
   (fix/maps->tx-ops
    [{:xt/id :dbpedia.resource/Pablo-Picasso :first-name :Pablo}]
    #inst "2017")))

(defn- pablo-db [db]
  (xt/entity db :dbpedia.resource/Pablo-Picasso))

(defn- pablo
  ([] (pablo-db (xt/db *api*)))
  ([valid-time] (pablo-db (xt/db *api* valid-time))))

(t/deftest test-put
  (let [node *api*]
    ;; tag::put[]
    (xt/submit-tx node [[:xt/put
                           {:xt/id :dbpedia.resource/Pablo-Picasso :first-name :Pablo} ;; <1>
                           #inst "2018-05-18T09:20:27.966-00:00" ;; <2>
                           #inst "2018-05-19T08:31:15.966-00:00"]] ) ;; <3>
    ;; end::put[]
    (xt/sync node)
    (t/is (nil? (pablo)))
    (t/is (some? (pablo #inst "2018-05-18T10:20:27.966-00:00")))
    (t/is (nil? (pablo #inst "2018-05-17T10:20:27.966-00:00")))))

(t/deftest test-delete
  (let [node *api*]
    (put-pablo)

    ;; tag::delete[]
    (xt/submit-tx node [[:xt/delete
                           :dbpedia.resource/Pablo-Picasso  ;; <1>
                           #inst "2018-05-18T09:20:27.966-00:00" ;; <2>
                           #inst "2018-05-19T08:31:15.966-00:00"]]) ;; <3>
    ;; end::delete[]
    (xt/sync node)
    (t/is (some? (pablo)))
    (t/is (nil? (pablo #inst "2018-05-18T10:20:27.966-00:00")))
    (t/is (some? (pablo #inst "2018-05-17T10:20:27.966-00:00")))))

(t/deftest test-match
  (let [node *api*]
    (put-pablo)

    ;; tag::match[]
    (xt/submit-tx node [[:xt/match
                           :dbpedia.resource/Pablo-Picasso ;; <1>
                           {:xt/id :dbpedia.resource/Pablo-Picasso :first-name :Pablo} ;; <2>
                           #inst "2018-05-18T09:21:31.846-00:00"] ;; <3>
                          [:xt/delete :dbpedia.resource/Pablo-Picasso]]) ;; <4>
    ;; end::match[]
    (xt/sync node)
    (t/is (nil? (pablo)))
    (t/is (some? (pablo #inst "2018-05-18T10:20:27.966-00:00")))
    (t/is (some? (pablo #inst "2018-05-17T10:20:27.966-00:00")))))

(t/deftest test-evict
  (let [node *api*]
    (put-pablo)

    ;; tag::evict[]
    (xt/submit-tx node [[:xt/evict :dbpedia.resource/Pablo-Picasso]])
    ;; end::evict[]

    (xt/sync node)
    (t/is (nil? (pablo)))
    (t/is (nil? (pablo #inst "2018-05-18T10:20:27.966-00:00")))
    (t/is (nil? (pablo #inst "2018-05-17T10:20:27.966-00:00")))))

(t/deftest test-function-anatomy
  (fix/submit+await-tx
   [[:xt/put {:xt/id :increment-age
              :crux.db/fn '
              ;; tag::fn-anatomy[]
              (fn [ctx eid]  ;;<1>
                (let [db (crux.api/db ctx) ;;<2>
                      entity (crux.api/entity db eid)]
                  [[:xt/put (update entity :age inc)]])) ;;<3>
              ;; end::fn-anatomy[]
              }]
    [:xt/put {:xt/id :ivan
              :age 0}]])

  (fix/submit+await-tx
   [[:xt/fn
     :increment-age
     :ivan]])

  (t/is (= {:xt/id :ivan :age 1} (xt/entity (xt/db *api*) :ivan))))

(t/deftest test-function
  (let [node *api*]
    (fix/submit+await-tx
     (fix/maps->tx-ops
      [{:xt/id :ivan :age 0}]))

    ;; tag::fn-put[]
    (xt/submit-tx node [[:xt/put {:xt/id :increment-age
                                    :crux.db/fn '(fn [ctx eid] ;;<1>
                                                   (let [db (crux.api/db ctx)
                                                         entity (crux.api/entity db eid)]
                                                     [[:xt/put (update entity :age inc)]]))}]])
    ;; end::fn-put[]

    (xt/sync node)

    ;; tag::fn-use[]
    (xt/submit-tx node [[:xt/fn
                           :increment-age ;; <1>
                           :ivan]]) ;; <2>
    ;; end::fn-use[]

    (xt/sync node)

    (t/is (= {:xt/id :ivan :age 1} (xt/entity (xt/db *api*) :ivan)))))

(t/deftest speculative-transactions
  (let [node *api*]
    ;; tag::speculative-0[]
    (let [real-tx (xt/submit-tx node [[:xt/put {:xt/id :ivan, :name "Ivan"}]])
          _ (xt/await-tx node real-tx)
          all-names '{:find [?name], :where [[?e :name ?name]]}
          db (xt/db node)]

      (xt/q db all-names) ; => #{["Ivan"]}
      ;; end::speculative-0[]
      (t/is #{["Ivan"]}, (xt/q db all-names))

      ;; tag::speculative-1[]
      (let [speculative-db (xt/with-tx db
                             [[:xt/put {:xt/id :petr, :name "Petr"}]])]
        (xt/q speculative-db all-names) ; => #{["Petr"] ["Ivan"]}
        ;; end::speculative-1[]
        (t/is #{["Petr"] ["Ivan"]} (xt/q speculative-db all-names))
        ;; tag::speculative-2[]
        )

      ;; we haven't impacted the original db value, nor the node
      (xt/q db all-names) ; => #{["Ivan"]}
      (xt/q (xt/db node) all-names) ; => #{["Ivan"]}
      ;; end::speculative-2[]
      (t/is (= #{["Ivan"]}) (xt/q db all-names))
      (t/is (= #{["Ivan"]} (xt/q (xt/db node) all-names)))
      ;; tag::speculative-3[]
      )
    ;; end::speculative-3[]
    ))

(t/deftest awaiting
  (let [node *api*]
    ;; tag::ti[]
    (let [tx (xt/submit-tx node [[:xt/put {:xt/id :ivan}]])]
      ;; The transaction won't have indexed yet so :ivan won't exist in a snapshot
      (xt/entity (xt/db node) :ivan) ;; => nil

      ;; Wait for the transaction to be indexed
      (xt/await-tx node tx)

      ;; Now :ivan will exist in a snapshot
      (xt/entity (xt/db node) :ivan)) ;; => {:xt/id :ivan}
    ;; end::ti[]
    ))

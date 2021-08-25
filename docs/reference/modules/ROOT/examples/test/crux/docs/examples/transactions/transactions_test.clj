(ns crux.docs.examples.transactions.transactions-test
  (:require [clojure.test :as t]
            [crux.api :as crux]
            [crux.fixtures :as fix :refer [*api*]]))

;; Note: Not using submit-and-await so that the snippets for the doc can include the submit

(t/use-fixtures :each fix/with-node)

(defn- put-pablo []
  (fix/submit+await-tx
   (fix/maps->tx-ops
    [{:xt/id :dbpedia.resource/Pablo-Picasso :first-name :Pablo}]
    #inst "2017")))

(defn- pablo-db [db]
  (crux/entity db :dbpedia.resource/Pablo-Picasso))

(defn- pablo
  ([] (pablo-db (crux/db *api*)))
  ([valid-time] (pablo-db (crux/db *api* valid-time))))

(t/deftest test-put
  (let [node *api*]
    ;; tag::put[]
    (crux/submit-tx node [[:xt/put
                           {:xt/id :dbpedia.resource/Pablo-Picasso :first-name :Pablo} ;; <1>
                           #inst "2018-05-18T09:20:27.966-00:00" ;; <2>
                           #inst "2018-05-19T08:31:15.966-00:00"]] ) ;; <3>
    ;; end::put[]
    (crux/sync node)
    (t/is (nil? (pablo)))
    (t/is (some? (pablo #inst "2018-05-18T10:20:27.966-00:00")))
    (t/is (nil? (pablo #inst "2018-05-17T10:20:27.966-00:00")))))

(t/deftest test-delete
  (let [node *api*]
    (put-pablo)

    ;; tag::delete[]
    (crux/submit-tx node [[:xt/delete
                           :dbpedia.resource/Pablo-Picasso  ;; <1>
                           #inst "2018-05-18T09:20:27.966-00:00" ;; <2>
                           #inst "2018-05-19T08:31:15.966-00:00"]]) ;; <3>
    ;; end::delete[]
    (crux/sync node)
    (t/is (some? (pablo)))
    (t/is (nil? (pablo #inst "2018-05-18T10:20:27.966-00:00")))
    (t/is (some? (pablo #inst "2018-05-17T10:20:27.966-00:00")))))

(t/deftest test-match
  (let [node *api*]
    (put-pablo)

    ;; tag::match[]
    (crux/submit-tx node [[:xt/match
                           :dbpedia.resource/Pablo-Picasso ;; <1>
                           {:xt/id :dbpedia.resource/Pablo-Picasso :first-name :Pablo} ;; <2>
                           #inst "2018-05-18T09:21:31.846-00:00"] ;; <3>
                          [:xt/delete :dbpedia.resource/Pablo-Picasso]]) ;; <4>
    ;; end::match[]
    (crux/sync node)
    (t/is (nil? (pablo)))
    (t/is (some? (pablo #inst "2018-05-18T10:20:27.966-00:00")))
    (t/is (some? (pablo #inst "2018-05-17T10:20:27.966-00:00")))))

(t/deftest test-evict
  (let [node *api*]
    (put-pablo)

    ;; tag::evict[]
    (crux/submit-tx node [[:xt/evict :dbpedia.resource/Pablo-Picasso]])
    ;; end::evict[]

    (crux/sync node)
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

  (t/is (= {:xt/id :ivan :age 1} (crux/entity (crux/db *api*) :ivan))))

(t/deftest test-function
  (let [node *api*]
    (fix/submit+await-tx
     (fix/maps->tx-ops
      [{:xt/id :ivan :age 0}]))

    ;; tag::fn-put[]
    (crux/submit-tx node [[:xt/put {:xt/id :increment-age
                                    :crux.db/fn '(fn [ctx eid] ;;<1>
                                                   (let [db (crux.api/db ctx)
                                                         entity (crux.api/entity db eid)]
                                                     [[:xt/put (update entity :age inc)]]))}]])
    ;; end::fn-put[]

    (crux/sync node)

    ;; tag::fn-use[]
    (crux/submit-tx node [[:xt/fn
                           :increment-age ;; <1>
                           :ivan]]) ;; <2>
    ;; end::fn-use[]

    (crux/sync node)

    (t/is (= {:xt/id :ivan :age 1} (crux/entity (crux/db *api*) :ivan)))))

(t/deftest speculative-transactions
  (let [node *api*]
    ;; tag::speculative-0[]
    (let [real-tx (crux/submit-tx node [[:xt/put {:xt/id :ivan, :name "Ivan"}]])
          _ (crux/await-tx node real-tx)
          all-names '{:find [?name], :where [[?e :name ?name]]}
          db (crux/db node)]

      (crux/q db all-names) ; => #{["Ivan"]}
      ;; end::speculative-0[]
      (t/is #{["Ivan"]}, (crux/q db all-names))

      ;; tag::speculative-1[]
      (let [speculative-db (crux/with-tx db
                             [[:xt/put {:xt/id :petr, :name "Petr"}]])]
        (crux/q speculative-db all-names) ; => #{["Petr"] ["Ivan"]}
        ;; end::speculative-1[]
        (t/is #{["Petr"] ["Ivan"]} (crux/q speculative-db all-names))
        ;; tag::speculative-2[]
        )

      ;; we haven't impacted the original db value, nor the node
      (crux/q db all-names) ; => #{["Ivan"]}
      (crux/q (crux/db node) all-names) ; => #{["Ivan"]}
      ;; end::speculative-2[]
      (t/is (= #{["Ivan"]}) (crux/q db all-names))
      (t/is (= #{["Ivan"]} (crux/q (crux/db node) all-names)))
      ;; tag::speculative-3[]
      )
    ;; end::speculative-3[]
    ))

(t/deftest awaiting
  (let [node *api*]
    ;; tag::ti[]
    (let [tx (crux/submit-tx node [[:xt/put {:xt/id :ivan}]])]
      ;; The transaction won't have indexed yet so :ivan won't exist in a snapshot
      (crux/entity (crux/db node) :ivan) ;; => nil

      ;; Wait for the transaction to be indexed
      (crux/await-tx node tx)

      ;; Now :ivan will exist in a snapshot
      (crux/entity (crux/db node) :ivan)) ;; => {:xt/id :ivan}
    ;; end::ti[]
    ))

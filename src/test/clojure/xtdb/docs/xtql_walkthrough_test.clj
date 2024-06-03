(ns xtdb.docs.xtql-walkthrough-test
  (:require [clj-yaml.core :as yaml]
            [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest testing]]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            [xtdb.test-util :as tu]
            [xtdb.xtql.edn :as x-edn])
  (:import (xtdb JsonSerde)
           (xtdb.api.query XtqlQuery)
           (xtdb.api.tx TxOp)))

(t/use-fixtures :each tu/with-node)

(def examples
  (-> (slurp (io/resource "docs/xtql_tutorial_examples.yaml"))
      (yaml/parse-string :keywords false)))

(defn json-example-query [name]
  (-> (get examples name)
      JsonSerde/jsonRemoveComments
      (JsonSerde/decode XtqlQuery)
      x-edn/unparse-query))

(defn json-example-dml [name]
  (-> (get examples name)
      JsonSerde/jsonRemoveComments
      (JsonSerde/decode TxOp)))

(defn sql-example [name]
  (get examples name))

(def users
  [[:put-docs :users {:xt/id "ivan", :first-name "Ivan", :last-name "Ivanov", :age 25}]
   [:put-docs {:into :users, :valid-from #inst "2018"}
    {:xt/id "petr", :first-name "Petr", :last-name "Petrov", :age 25}]])

(def old-users
  [[:put-docs {:into :old-users, :valid-from #inst "2017"}
    {:xt/id "ivan", :given-name "Ivan", :surname "Ivanov"}]
   [:put-docs {:into :old-users, :valid-from #inst "2018"}
    {:xt/id "petr", :given-name "Petr", :surname "Petrov"}]])

(def articles
  [[:put-docs :articles {:xt/id 1, :author-id "ivan", :title "First" :content "My first blog"}]
   [:put-docs :articles {:xt/id 2, :author-id "ivan", :title "Second" :content "My second blog"}]])

(def posts
  [[:put-docs :posts {:xt/id 1, :author-id "ivan", :title "First" :content "My first blog"}]
   [:put-docs :posts {:xt/id 2, :author-id "ivan", :title "Second" :content "My second blog"}]])

(def comments
  [[:put-docs :comments {:xt/id 1, :article-id 1, :post-id 1, :created-at #inst "2020", :comment "1"}]
   [:put-docs :comments {:xt/id 2, :article-id 1, :post-id 1, :created-at #inst "2021", :comment "2"}]
   [:put-docs :comments {:xt/id 3, :article-id 2, :post-id 2, :created-at #inst "2022", :comment "1"}]])

(def customers
  [[:put-docs :customers {:xt/id "ivan"}]
   [:put-docs :customers {:xt/id "petr"}]])

(def orders
  [[:put-docs :orders {:xt/id 1, :customer-id "ivan", :currency :gbp, :order-value 100}]
   [:put-docs :orders {:xt/id 2, :customer-id "ivan", :currency :gbp, :order-value 150}]])

(def promotions
  [[:put-docs :promotions {:xt/id 1, :promotion-type "christmas"}]
   [:put-docs :promotions {:xt/id 2, :promotion-type "general"}]])

(deftest basic-operations

  (xt/submit-tx tu/*node* users)

  (t/is (= #{{:user-id "ivan", :first-name "Ivan", :last-name "Ivanov"}
             {:user-id "petr", :first-name "Petr", :last-name "Petrov"}}
           (set
            (xt/q tu/*node*
                  ';; tag::bo-xtql-1[]
                  (from :users [{:xt/id user-id} first-name last-name])
                  ;; end::bo-xtql-1[]
                  ,))
           (set (xt/q tu/*node* (json-example-query "bo-json-1")))
           (set (xt/q tu/*node* (sql-example "bo-sql-1")))))

  (t/is (= [{:first-name "Ivan", :last-name "Ivanov"}]
           (xt/q tu/*node*
                 '
                 ;; tag::bo-xtql-2[]
                 (from :users [{:xt/id "ivan"} first-name last-name])
                 ;; end::bo-xtql-2[]
                 ,)
           (xt/q tu/*node* (json-example-query "bo-json-2"))
           (xt/q tu/*node* (sql-example "bo-sql-2"))))

  (t/is (= [{:user-id "ivan", :first-name "Ivan", :last-name "Ivanov"}
            {:user-id "petr", :first-name "Petr", :last-name "Petrov"}]
           (xt/q tu/*node*
                 '
                 ;; tag::bo-xtql-3[]
                 (-> (from :users [{:xt/id user-id} first-name last-name])
                     (order-by last-name first-name)
                     (limit 10))
                 ;; end::bo-xtql-3[]
                 ,)
           (xt/q tu/*node* (json-example-query "bo-json-3"))
           (xt/q tu/*node* (sql-example "bo-sql-3")))))

(deftest joins

  (xt/submit-tx tu/*node*
                (concat users articles
                        customers orders))

  (t/is (= #{{:user-id "ivan", :first-name "Ivan", :last-name "Ivanov", :content "My first blog", :title "First"}
             {:user-id "ivan", :first-name "Ivan", :last-name "Ivanov", :content "My second blog", :title "Second"}}
           (set
            (xt/q tu/*node*
                  '
                  ;; tag::joins-xtql-1[]
                  (unify (from :users [{:xt/id user-id} first-name last-name])
                         (from :articles [{:author-id user-id} title content]))
                  ;; end::joins-xtql-1[]
                  ,))
           (set (xt/q tu/*node* (json-example-query "joins-json-1")))
           (set (xt/q tu/*node* (sql-example "joins-sql-1")))))

  (t/is (= #{{:age 25, :uid1 "ivan", :uid2 "petr"}
             {:age 25, :uid1 "petr", :uid2 "ivan"}}
           (set
            (xt/q tu/*node*
                  '
                  ;; tag::joins-xtql-2[]
                  (unify (from :users [{:xt/id uid1} age])
                         (from :users [{:xt/id uid2} age])
                         (where (<> uid1 uid2)))
                  ;; end::joins-xtql-2[]
                  ,))
           (set (xt/q tu/*node* (json-example-query "joins-json-2")))
           (set (xt/q tu/*node* (sql-example "joins-sql-2")))))

  (t/is (= #{{:cid "ivan", :order-value 150, :currency :gbp}
             {:cid "ivan", :order-value 100, :currency :gbp}
             {:cid "petr"}}
           (set
            (xt/q tu/*node*
                  '
                  ;; tag::joins-xtql-3[]
                  (-> (unify (from :customers [{:xt/id cid}])
                             (left-join (from :orders [{:xt/id oid, :customer-id cid} currency order-value])
                                        [cid currency order-value]))
                      (limit 100))
                  ;; end::joins-xtql-3[]
                  ,))
           (set (xt/q tu/*node* (json-example-query "joins-json-3")))
           (set (xt/q tu/*node* (sql-example "joins-sql-3")))))

  (t/is (= [{:cid "petr"}]
           (xt/q tu/*node*
                 '
                 ;; tag::joins-xtql-4[]
                 (-> (unify (from :customers [{:xt/id cid}])
                            (where (not (exists? (from :orders [{:customer-id $cid}])
                                                 {:args [cid]}))))
                     (limit 100))
                 ;; end::joins-xtql-4[]
                 ,)
           (xt/q tu/*node* (json-example-query "joins-json-4"))
           (xt/q tu/*node* (sql-example "joins-sql-4")))))

(deftest projections

  (xt/submit-tx tu/*node* (concat users articles))

  (t/is (= #{{:first-name "Ivan", :last-name "Ivanov", :full-name "Ivan Ivanov"}
             {:first-name "Petr", :last-name "Petrov", :full-name "Petr Petrov"}}
           (set
            (xt/q tu/*node*
                  '
                  ;; tag::proj-xtql-1[]
                  (-> (from :users [first-name last-name])
                      (with {:full-name (concat first-name " " last-name)}))
                  ;; end::proj-xtql-1[]
                  ,))
           (set (xt/q tu/*node* (json-example-query "proj-json-1")))
           (set (xt/q tu/*node* (sql-example "proj-sql-1")))))

  (t/is (= #{{:full-name "Ivan Ivanov", :title "First", :content "My first blog"}
             {:full-name "Ivan Ivanov", :title "Second", :content "My second blog"}}
           (set
            (xt/q tu/*node*
                  '
                  ;; tag::proj-xtql-2[]
                  (-> (unify (from :users [{:xt/id user-id} first-name last-name])
                             (from :articles [{:author-id user-id} title content]))
                      (return {:full-name (concat first-name " " last-name)} title content))
                  ;; end::proj-xtql-2[]
                  ,))
           (set (xt/q tu/*node* (json-example-query "proj-json-2")))
           (set (xt/q tu/*node* (sql-example "proj-sql-2")))))

  (t/is (= #{{:first-name "Ivan", :last-name "Ivanov", :title "Second", :content "My second blog"}
             {:first-name "Ivan", :last-name "Ivanov", :title "First", :content "My first blog"}}
           (set
            (xt/q tu/*node*
                  '
                  ;; tag::proj-xtql-3[]
                  (-> (unify (from :users [{:xt/id user-id} first-name last-name])
                             (from :articles [{:author-id user-id} title content]))
                      (without :user-id))
                  ;; end::proj-xtql-3[]
                  ,))
           (set (xt/q tu/*node* (json-example-query "proj-json-3")))
           (set (xt/q tu/*node* (sql-example "proj-sql-3"))))))

(deftest aggregations

  (xt/submit-tx tu/*node* (concat customers orders))

  (testing "To count/sum/average values, we use `aggregate`:"

    (t/is (= #{{:cid "ivan", :currency :gbp, :order-count 2, :total-value 250}
               {:cid "petr", :order-count 0, :total-value 0}}
             (set
               (xt/q tu/*node*
                 '
                 ;; tag::aggr-xtql-1[]
                 (-> (unify (from :customers [{:xt/id cid}])
                            (left-join (from :orders [{:xt/id oid :customer-id cid} currency order-value])
                                       [oid cid currency order-value]))
                     (aggregate cid currency
                                {:order-count (count oid)
                                 :total-value (sum order-value)})
                     (with {:total-value (coalesce total-value 0)})
                     (order-by {:val total-value :dir :desc})
                     (limit 100))
                 ;; end::aggr-xtql-1[]
                 ,))
             (set (xt/q tu/*node* (json-example-query "aggr-json-1")))
             (set (xt/q tu/*node* (sql-example "aggr-sql-1")))))))


(deftest pull

  (xt/submit-tx tu/*node*
    (concat
      articles comments
      [[:put-docs :authors {:xt/id "ivan", :first-name "Ivan", :last-name "Ivanov"}]
       [:put-docs :authors {:xt/id "petr", :first-name "Petr", :last-name "Petrov"}]]))

  (testing "For example, if a user is reading an article, we might also want to show them details about the author as well as any comments."

    (t/is (= #{{:article-id 1
                :title "First",
                :content "My first blog",
                :author-id "ivan",
                :author {:last-name "Ivanov", :first-name "Ivan"},
                :comments [{:created-at #time/zoned-date-time "2021-01-01T00:00Z[UTC]", :comment "2"}
                           {:created-at #time/zoned-date-time "2020-01-01T00:00Z[UTC]", :comment "1"}],}
               {:article-id 2
                :title "Second",
                :content "My second blog",
                :author-id "ivan",
                :author {:last-name "Ivanov", :first-name "Ivan"},
                :comments [{:created-at #time/zoned-date-time "2022-01-01T00:00Z[UTC]", :comment "1"}],}}
             (set
               (xt/q tu/*node*
                 '
                 ;; tag::pull-xtql-1[]
                 (-> (from :articles [{:xt/id article-id} title content author-id])

                     (with {:author (pull (from :authors [{:xt/id $author-id} first-name last-name])
                                          {:args [author-id]})


                            :comments (pull* (-> (from :comments [{:article-id $article-id} created-at comment])
                                                 (order-by {:val created-at :dir :desc})
                                                 (limit 10))
                                             {:args [article-id]})}))
                 ;; end::pull-xtql-1[]
                 ,))
             (set (xt/q tu/*node* (json-example-query "pull-json-1")))))))
            ;; No SQL for this one

(deftest bitemporality

  (xt/submit-tx tu/*node* users)

  (t/is (= [{:first-name "Petr", :last-name "Petrov"}]
           (xt/q tu/*node*
                 '
                 ;; tag::bitemp-xtql-1[]
                 (from :users {:for-valid-time (at #inst "2020-01-01")
                               :bind [first-name last-name]})
                 ;; end::bitemp-xtql-1[]
                 ,)
           (xt/q tu/*node* (json-example-query "bitemp-json-1"))
           (xt/q tu/*node* (sql-example "bitemp-sql-1"))))

  (t/is (= #{{:first-name "Ivan", :last-name "Ivanov"}
             {:first-name "Petr", :last-name "Petrov"}}
           (set
            (xt/q tu/*node*
                  '
                  ;; tag::bitemp-xtql-2[]
                  (from :users {:for-valid-time :all-time
                                :bind [first-name last-name]})
                  ;; end::bitemp-xtql-2[]
                  ,))
           (set (xt/q tu/*node* (json-example-query "bitemp-json-2")))
           (set (xt/q tu/*node* (sql-example "bitemp-sql-2")))))

  (t/is (= [{:user-id "petr"}]
           (xt/q tu/*node*
                 '
                 ;; tag::bitemp-xtql-3[]
                 (unify (from :users {:for-valid-time (at #inst "2018")
                                      :bind [{:xt/id user-id}]})

                        (from :users {:for-valid-time (at #inst "2023")
                                      :bind [{:xt/id user-id}]}))
                 ;; end::bitemp-xtql-3[]
                 ,)
           (xt/q tu/*node* (json-example-query "bitemp-json-3")))))

(deftest DML-Insert-xtql
  (xt/submit-tx tu/*node* old-users)

  (t/is (= []
           (xt/q tu/*node*
                 '(from :users [first-name last-name]))))

  (let [node tu/*node*]
    ;; tag::DML-Insert-xtql[]
    (xt/submit-tx node
      [[:insert-into :users
        '(from :old-users [xt/id {:given-name first-name, :surname last-name}
                           xt/valid-from xt/valid-to])]])
    ;; end::DML-Insert-xtql[]
    ,)

  (t/is (= #{{:first-name "Ivan"
              :last-name "Ivanov"
              :xt/valid-from #time/zoned-date-time "2017-01-01T00:00Z[UTC]"}
             {:first-name "Petr"
              :last-name "Petrov"
              :xt/valid-from #time/zoned-date-time "2018-01-01T00:00Z[UTC]"}}
           (set
            (xt/q tu/*node*
                  '(from :users [first-name last-name
                                 xt/valid-from xt/valid-to]))))))

(deftest DML-Insert-xtql-json
  (xt/submit-tx tu/*node* old-users)

  (t/is (= []
           (xt/q tu/*node*
                 '(from :users [first-name last-name]))))

  (xt/submit-tx tu/*node* [(json-example-dml "DML-Insert-json")])

  (t/is (= #{{:first-name "Ivan"
              :last-name "Ivanov"
              :xt/valid-from #time/zoned-date-time "2017-01-01T00:00Z[UTC]"}
             {:first-name "Petr"
              :last-name "Petrov"
              :xt/valid-from #time/zoned-date-time "2018-01-01T00:00Z[UTC]"}}
           (set
            (xt/q tu/*node*
                  '(from :users [first-name last-name
                                 xt/valid-from xt/valid-to]))))))


(deftest DML-Insert-sql
  (xt/submit-tx tu/*node* old-users)

  (t/is (= []
           (xt/q tu/*node*
                 '(from :users [first-name last-name]))))

  (xt/submit-tx tu/*node*
                [[:sql (sql-example "DML-Insert-sql")]])

  (t/is (= #{{:first-name "Ivan"
              :last-name "Ivanov"
              :xt/valid-from #time/zoned-date-time "2017-01-01T00:00Z[UTC]"}
             {:first-name "Petr"
              :last-name "Petrov"
              :xt/valid-from #time/zoned-date-time "2018-01-01T00:00Z[UTC]"}}
           (set
            (xt/q tu/*node*
                  '(from :users [first-name last-name
                                 xt/valid-from xt/valid-to]))))))

;; tag::DML-Delete-xtql[]
(defn delete-a-post [node the-post-id]
  (xt/submit-tx node
    [[:delete {:from :comments, :bind '[{:post-id $post-id}]}
      {:post-id the-post-id}]]))
;; end::DML-Delete-xtql[]

(deftest DML-Delete-xtql
  (xt/submit-tx tu/*node* comments)

  (delete-a-post tu/*node* 1)

  (t/is (empty? (xt/q tu/*node* '(from :comments [{:post-id $post-id}])
                      {:args {:post-id 1}})))

  (t/is (not (empty?
              (xt/q tu/*node* '(from :comments {:bind [{:post-id $post-id}]
                                                :for-valid-time :all-time})
                    {:args {:post-id 1}})))))

(deftest DML-Delete-xtql-json
  (xt/submit-tx tu/*node* comments)

  (xt/submit-tx tu/*node*
                [(json-example-dml "DML-Delete-json")])

  (t/is (= [] (xt/q tu/*node* '(from :comments [xt/id {:post-id $post-id}])
                    {:args {:post-id 1}})))

  (t/is (not (empty?
              (xt/q tu/*node* '(from :comments {:bind [{:post-id $post-id}]
                                                :for-valid-time :all-time})
                    {:args {:post-id 1}})))))

(deftest DML-Delete-sql
  (xt/submit-tx tu/*node* comments)

  (xt/submit-tx tu/*node*
    [[:sql (sql-example "DML-Delete-sql")
      [1]]])

  (t/is (empty? (xt/q tu/*node* '(from :comments [{:post-id $post-id}])
                      {:args {:post-id 1}})))

  (t/is (not (empty?
              (xt/q tu/*node* '(from :comments {:bind [{:post-id $post-id}]
                                                :for-valid-time :all-time})
                    {:args {:post-id 1}})))))

(deftest DML-Delete-additional-unify-clauses-xtql
  (xt/submit-tx tu/*node* (concat posts comments))

  (let [node tu/*node*]
    ;; tag::DML-Delete-additional-unify-clauses-xtql[]
    (xt/submit-tx node
      [[:delete '{:from :comments
                  :bind [{:post-id pid}]
                  :unify [(from :posts [{:xt/id pid, :author-id $author}])]}
        {:author "ivan"}]])
    ;; end::DML-Delete-additional-unify-clauses-xtql[]
    )

  (t/is (empty? (xt/q tu/*node*
                      '(unify (from :comments [{:post-id pid}])
                              (from :posts [{:xt/id pid, :author-id $author}]))
                      {:args {:author "ivan"}})))

  (t/is (not (empty?
               (xt/q tu/*node*
                     '(unify (from :comments {:bind [{:post-id pid}]
                                              :for-valid-time :all-time})
                             (from :posts [{:xt/id pid, :author-id $author}]))
                     {:args {:author "ivan"}})))))

(deftest DML-Delete-additional-unify-clauses-sql
  (xt/submit-tx tu/*node* (concat posts comments))

  (t/is (:committed? (xt/execute-tx tu/*node*
                                    [[:sql (sql-example "DML-Delete-additional-unify-clauses-sql")
                                      ["ivan"]]])))

  (t/is (empty? (xt/q tu/*node*
                      '(unify (from :comments [{:post-id pid}])
                              (from :posts [{:xt/id pid, :author-id $author}]))
                      {:args {:author "ivan"}})))

  (t/is (not (empty?
               (xt/q tu/*node*
                     '(unify (from :comments {:bind [{:post-id pid}]
                                              :for-valid-time :all-time})
                             (from :posts [{:xt/id pid, :author-id $author}]))
                     {:args {:author "ivan"}})))))

(deftest DML-Delete-bitemporal-xtql
  (xt/submit-tx tu/*node* promotions)

  (t/is (= #{{:promotion-type "christmas"}
             {:promotion-type "general"}}
           (set
            (xt/q tu/*node*
                  '(unify (from :promotions {:bind [promotion-type]
                                             :for-valid-time (from #inst "2023-12-25")})
                          (from :promotions {:bind [promotion-type]
                                             :for-valid-time (from #inst "2023-12-26")}))))))

  (let [node tu/*node*]
    ;; tag::DML-Delete-bitemporal-xtql[]
    (xt/submit-tx node
      [[:delete '{:from :promotions
                  :for-valid-time (from #inst "2023-12-26")
                  :bind [{:promotion-type "christmas"}]}]])
    ;; end::DML-Delete-bitemporal-xtql[]
    ,)

  (t/is (= #{{:promotion-type "general"}}
           (set
            (xt/q tu/*node*
                  '(unify (from :promotions {:bind [promotion-type]
                                             :for-valid-time (from #inst "2023-12-25")})
                          (from :promotions {:bind [promotion-type]
                                             :for-valid-time (from #inst "2023-12-26")})))))))

(deftest DML-Delete-bitemporal-xtql-json
  (xt/submit-tx tu/*node* promotions)

  (t/is (= #{{:promotion-type "christmas"}
             {:promotion-type "general"}}
           (set
            (xt/q tu/*node*
                  '(unify (from :promotions {:bind [promotion-type]
                                             :for-valid-time (from #inst "2023-12-25")})
                          (from :promotions {:bind [promotion-type]
                                             :for-valid-time (from #inst "2023-12-26")}))))))

  (xt/submit-tx tu/*node* [(json-example-dml "DML-Delete-bitemporal-json")])

  (t/is (= #{{:promotion-type "general"}}
           (set
            (xt/q tu/*node*
                  '(unify (from :promotions {:bind [promotion-type]
                                             :for-valid-time (from #inst "2023-12-25")})
                          (from :promotions {:bind [promotion-type]
                                             :for-valid-time (from #inst "2023-12-26")})))))))

(deftest DML-Delete-bitemporal-sql
  (xt/submit-tx tu/*node* promotions)

  (t/is (= #{{:promotion-type "christmas"}
             {:promotion-type "general"}}
           (set
            (xt/q tu/*node*
                  '(unify (from :promotions {:bind [promotion-type]
                                             :for-valid-time (from #inst "2023-12-25")})
                          (from :promotions {:bind [promotion-type]
                                             :for-valid-time (from #inst "2023-12-26")}))))))

  (t/is (:committed? (xt/execute-tx tu/*node*
                                    [[:sql (sql-example "DML-Delete-bitemporal-sql")]])))

  (t/is (= #{{:promotion-type "general"}}
           (set
            (xt/q tu/*node*
                  '(unify (from :promotions {:bind [promotion-type]
                                             :for-valid-time (from #inst "2023-12-25")})
                          (from :promotions {:bind [promotion-type]
                                             :for-valid-time (from #inst "2023-12-26")})))))))

(deftest DML-Delete-everything-xtql
  (xt/submit-tx tu/*node* comments)

  (t/is (not (empty? (xt/q tu/*node* '(from :comments [])))))

  (let [node tu/*node*]
    ;; tag::DML-Delete-everything-xtql[]
    (xt/submit-tx node
      [[:delete {:from :comments}]])
    ;; end::DML-Delete-everything-xtql[]
    ,)

  (t/is (empty? (xt/q tu/*node* '(from :comments []))))

  (t/is (not (empty? (xt/q tu/*node* '(from :comments {:bind [] :for-valid-time :all-time}))))))

(deftest DML-Delete-everything-sql
  (xt/submit-tx tu/*node* comments)

  (t/is (not (empty? (xt/q tu/*node* '(from :comments [])))))

  (xt/submit-tx tu/*node*
    [[:sql (sql-example "DML-Delete-everything-sql")]])

  (t/is (empty? (xt/q tu/*node* '(from :comments []))))

  (t/is (not (empty? (xt/q tu/*node* '(from :comments {:bind [] :for-valid-time :all-time}))))))


(deftest DML-Delete-everything-json
  (xt/submit-tx tu/*node* comments)

  (t/is (not (empty? (xt/q tu/*node* '(from :comments [])))))

  (xt/submit-tx tu/*node*
                [(json-example-dml "DML-Delete-everything-json")])

  (t/is (empty? (xt/q tu/*node* '(from :comments []))))

  (t/is (not (empty? (xt/q tu/*node* '(from :comments {:bind [] :for-valid-time :all-time}))))))


(deftest DML-Update-xtql
  (xt/submit-tx tu/*node*
    [[:put-docs :documents {:xt/id "doc-id", :version 1}]])

  (t/is (= [{:version 1}]
           (xt/q tu/*node* '(from :documents [version]))))

  (let [node tu/*node*]
    ;; tag::DML-Update-xtql[]
    (xt/submit-tx node
      [[:update '{:table :documents
                  :bind [{:xt/id $doc-id, :version v}]
                  :set {:version (+ v 1)}}
        {:doc-id "doc-id"}]])
    ;; end::DML-Update-xtql[]
    ,)

  (t/is (= [{:version 2}]
           (xt/q tu/*node* '(from :documents [version])))))

(deftest DML-Update-json
  (xt/submit-tx tu/*node*
                [[:put-docs :documents {:xt/id "doc-id", :version 1}]])

  (t/is (= [{:version 1}]
           (xt/q tu/*node* '(from :documents [version]))))

  (let [node tu/*node*]
    (xt/submit-tx node [(json-example-dml "DML-Update-json")]))

  (t/is (= [{:version 2}]
           (xt/q tu/*node* '(from :documents [version])))))

(deftest DML-Update-sql
  (xt/submit-tx tu/*node*
                [[:put-docs :documents {:xt/id "doc-id", :version 1}]])

  (t/is (= [{:version 1}]
           (xt/q tu/*node* '(from :documents [version]))))

  (t/is (:committed? (xt/execute-tx tu/*node*
                                    [[:sql (sql-example "DML-Update-sql")
                                      ["doc-id"]]])))

  (t/is (= [{:version 2}]
           (xt/q tu/*node* '(from :documents [version])))))

(deftest DML-Update-bitemporal-xtql
  (xt/submit-tx tu/*node*
                [[:put-docs :posts {:xt/id "my-post-id" :comment-count 1}]
                 [:put-docs :comments {:xt/id 1, :post-id "my-post-id"}]])

  (t/is (= [{:comment-count 1}]
           (xt/q tu/*node* '(from :posts [comment-count]))))

  (let [node tu/*node*]
    ;; tag::DML-Update-bitemporal-xtql[]
    (xt/submit-tx node
                  [[:put-docs :comments {:xt/id (random-uuid), :post-id "my-post-id"}]
                   [:update '{:table :posts
                              :bind [{:xt/id $post-id}]
                              :unify [(with {cc (q (-> (from :comments [{:post-id $post-id}])
                                                       (aggregate {:cc (row-count)})))})]
                              :set {:comment-count cc}}
                    {:post-id "my-post-id"}]])
    ;; end::DML-Update-bitemporal-xtql[]
    ,)

  (t/is (= [{:comment-count 2}]
           (xt/q tu/*node* '(from :posts [comment-count])))))

(deftest DML-Update-bitemporal-json
  (xt/submit-tx tu/*node*
                [[:put-docs :posts {:xt/id "my-post-id" :comment-count 1}]
                 [:put-docs :comments {:xt/id 1, :post-id "my-post-id"}]])

  (t/is (= [{:comment-count 1}]
           (xt/q tu/*node* '(from :posts [comment-count]))))

  (let [node tu/*node*]
    (xt/submit-tx node
                  [[:put-docs :comments {:xt/id (random-uuid), :post-id "my-post-id"}]
                   (json-example-dml "DML-Update-bitemporal-json")]))

  (t/is (= [{:comment-count 2}]
           (xt/q tu/*node* '(from :posts [comment-count])))))

#_ ;; TODO: Uncomment when supported: https://github.com/xtdb/xtdb/issues/3050
(deftest DML-Update-bitemporal-sql
  (xt/submit-tx tu/*node*

    [[:put-docs :posts {:xt/id "my-post-id" :comment-count 1}]
     [:put-docs :comments {:xt/id 1, :post-id "my-post-id"}]])

  (t/is (= [{:comment-count 1}]
           (xt/q tu/*node* '(from :posts [comment-count]))))

  (xt/submit-tx tu/*node*
    [[:sql (sql-example "DML-Update-bitemporal-sql-1")
      [(random-uuid), "my-post-id"]]
     [:sql (sql-example "DML-Update-bitemporal-sql-2")
      ["my-post-id" "my-post-id"]]])

  (t/is (= [{:comment-count 2}]
           (xt/q tu/*node* '(from :posts [comment-count])))))


(deftest DML-Erase-xtql
  (xt/submit-tx tu/*node*
    [[:put-docs :users {:xt/id "user-id", :email "jms@example.com"}]])

  (t/is (not (empty? (xt/q tu/*node* '(from :users [])))))

  (let [node tu/*node*]
    ;; tag::DML-Erase-xtql[]
    (xt/submit-tx node
      [[:erase {:from :users, :bind '[{:email "jms@example.com"}]}]])
    ;; end::DML-Erase-xtql[]
    ,)

  (t/is (empty? (xt/q tu/*node* '(from :users []))))
  (t/is (empty? (xt/q tu/*node* '(from :users {:bind [] :for-valid-time :all-time})))))

(deftest DML-Erase-json
  (xt/submit-tx tu/*node* [[:put-docs :users {:xt/id "user-id", :email "jms@example.com"}]])

  (t/is (not (empty? (xt/q tu/*node* '(from :users [])))))

  (xt/submit-tx tu/*node*
                [(json-example-dml "DML-Erase-json")])

  (t/is (empty? (xt/q tu/*node* '(from :users []))))
  (t/is (empty? (xt/q tu/*node* '(from :users {:bind [] :for-valid-time :all-time})))))

(deftest DML-Erase-sql
  (xt/submit-tx tu/*node* [[:put-docs :users {:xt/id :james, :email "jms@example.com"}]])

  (t/is (not (empty? (xt/q tu/*node* '(from :users [])))))

  (xt/submit-tx tu/*node* [[:sql (sql-example "DML-Erase-sql")]])

  (t/is (empty? (xt/q tu/*node* '(from :users []))))
  (t/is (empty? (xt/q tu/*node* '(from :users {:bind [] :for-valid-time :all-time})))))

#_ ;; TODO see https://github.com/xtdb/xtdb/issues/3110
(deftest DML-Assert
  (let [node tu/*node*
        {my-tx-id :tx-id} (xt/submit-tx node
                                        [(xt/assert-exists '(from :users [{:xt/id :james}]))
                                         (xt/put :orders {:xt/id :james, :item "less bugs"})])]
    (t/is (= [{:xt/committed? false
               :xt/error (err/runtime-err :xtdb/assert-failed
                                          {::err/message "Precondition failed: assert-exists"
                                           :row-count 1})}]
             (-> (xt/q node '(from :xt/txs [{:xt/id $tx-id} xt/committed? xt/error])
                       {:args {:tx-id my-tx-id}}))))))


(deftest DML-Assert-Not
  (xt/submit-tx tu/*node* [[:put-docs :users {:xt/id :james, :email "james@example.com"}]])

  (let [node tu/*node*
        {my-tx-id :tx-id}
        ;; tag::DML-Assert-xtql[]
        (xt/submit-tx node
                      [[:assert-not-exists '(from :users [{:email $email}])
                        {:email "james@example.com"}]

                       [:put-docs :users {:xt/id :james, :email "james@example.com"}]])
        ;; end::DML-Assert-xtql[]
        ,]
    (t/is (= ;; tag::DML-Assert-query-result[]
           [{:xt/committed? false
             :xt/error (err/runtime-err :xtdb/assert-failed
                                        {::err/message "Precondition failed: assert-not-exists"
                                         :row-count 1})}]
           ;; end::DML-Assert-query-result[]
           (-> ;; tag::DML-Assert-query[]
            (xt/q node '(from :xt/txs [{:xt/id $tx-id} xt/committed? xt/error])
                  {:args {:tx-id my-tx-id}})
            ;; end::DML-Assert-query[]
            ))))

  ;; TODO: Once implemented
  #_
  (let [{:keys [tx-id]}
        (xt/submit-tx tu/*node*
                      [[:sql (sql-example "DML-Assert-sql")]])]
    (t/is (= [{:xt/committed? false}]
             (xt/q tu/*node* '(from :xt/txs [{:xt/id $tx-id} xt/committed?])
                   {:args {:tx-id tx-id}})))))

(deftest DML-Assert-Not-json
  (xt/submit-tx tu/*node* [[:put-docs :users {:xt/id :james, :email "james@example.com"}]])

  (let [{my-tx-id :tx-id}
        (xt/submit-tx tu/*node*
                      [(json-example-dml "DML-Assert-Not-json")
                       [:put-docs :users {:xt/id :james, :email "james@example.com"}]])]
    (t/is (=
           [{:xt/committed? false
             :xt/error (err/runtime-err :xtdb/assert-failed
                                        {::err/message "Precondition failed: assert-not-exists"
                                         :row-count 1})}]
           (-> (xt/q tu/*node* '(from :xt/txs [{:xt/id $tx-id} xt/committed? xt/error])
                     {:args {:tx-id my-tx-id}})
               )))))

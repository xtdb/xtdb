(ns xtdb.docs.xtql-walkthrough-test
  (:require [clj-yaml.core :as yaml]
            [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest testing]]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-node)

(def examples
  (-> (slurp (io/resource "docs/xtql_tutorial_examples.yaml"))
      (yaml/parse-string :keywords false)))

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
           (set (xt/q tu/*node* (sql-example "bo-sql-1")))))

  (t/is (= [{:first-name "Ivan", :last-name "Ivanov"}]
           (xt/q tu/*node*
                 '
                 ;; tag::bo-xtql-2[]
                 (from :users [{:xt/id "ivan"} first-name last-name])
                 ;; end::bo-xtql-2[]
                 ,)
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
           (set (xt/q tu/*node* (sql-example "joins-sql-3")))))

  (t/is (= [{:cid "petr"}]
           (xt/q tu/*node*
                 '
                 ;; tag::joins-xtql-4[]
                 (-> (unify (from :customers [{:xt/id cid}])
                            (where (not (exists? (fn [cid]
                                                   (from :orders [{:customer-id cid}]))))))
                     (limit 100))
                 ;; end::joins-xtql-4[]
                 )
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
                :comments [{:created-at #xt/zoned-date-time "2021-01-01T00:00Z[UTC]", :comment "2"}
                           {:created-at #xt/zoned-date-time "2020-01-01T00:00Z[UTC]", :comment "1"}],}
               {:article-id 2
                :title "Second",
                :content "My second blog",
                :author-id "ivan",
                :author {:last-name "Ivanov", :first-name "Ivan"},
                :comments [{:created-at #xt/zoned-date-time "2022-01-01T00:00Z[UTC]", :comment "1"}],}}
             (set
               (xt/q tu/*node*
                 '
                 ;; tag::pull-xtql-1[]
                 (-> (from :articles [{:xt/id article-id} title content author-id])

                     (with {:author (pull (fn [author-id]
                                            (from :authors [{:xt/id author-id} first-name last-name])))

                            :comments (pull* (fn [article-id]
                                               (-> (from :comments [{:article-id article-id} created-at comment])
                                                   (order-by {:val created-at :dir :desc})
                                                   (limit 10))))}))
                 ;; end::pull-xtql-1[]
                 ))

             (set (xt/q tu/*node* (sql-example "pull-sql-1")))))))

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
                 ))))

(deftest DML-Insert-sql
  (xt/submit-tx tu/*node* old-users)

  (t/is (= []
           (xt/q tu/*node*
                 '(from :users [first-name last-name]))))

  (xt/submit-tx tu/*node*
                [[:sql (sql-example "DML-Insert-sql")]])

  (t/is (= #{{:first-name "Ivan"
              :last-name "Ivanov"
              :xt/valid-from #xt/zoned-date-time "2017-01-01T00:00Z[UTC]"}
             {:first-name "Petr"
              :last-name "Petrov"
              :xt/valid-from #xt/zoned-date-time "2018-01-01T00:00Z[UTC]"}}
           (set
            (xt/q tu/*node*
                  '(from :users [first-name last-name
                                 xt/valid-from xt/valid-to]))))))

(deftest DML-Delete-sql
  (xt/submit-tx tu/*node* comments)

  (xt/submit-tx tu/*node*
    [[:sql (sql-example "DML-Delete-sql")
      [1]]])

  (t/is (empty? (xt/q tu/*node* ['#(from :comments [{:post-id %}]) 1])))

  (t/is (not (empty?
              (xt/q tu/*node* ['#(from :comments {:bind [{:post-id %}]
                                                  :for-valid-time :all-time})
                               1])))))

(deftest DML-Delete-additional-unify-clauses-sql
  (xt/submit-tx tu/*node* (concat posts comments))

  (t/is (xt/execute-tx tu/*node*
                       [[:sql (sql-example "DML-Delete-additional-unify-clauses-sql")
                         ["ivan"]]]))

  (t/is (empty? (xt/q tu/*node*
                      ['(fn [author]
                          (unify (from :comments [{:post-id pid}])
                                 (from :posts [{:xt/id pid, :author-id author}])))
                       "ivan"])))

  (t/is (not (empty?
              (xt/q tu/*node*
                    ['(fn [author]
                        (unify (from :comments {:bind [{:post-id pid}]
                                                :for-valid-time :all-time})
                               (from :posts [{:xt/id pid, :author-id author}])))
                     "ivan"])))))

(deftest DML-Delete-bitemporal-sql
  (xt/execute-tx tu/*node* promotions)

  (t/is (= #{{:promotion-type "christmas"}
             {:promotion-type "general"}}
           (set
            (xt/q tu/*node*
                  '(unify (from :promotions {:bind [promotion-type]
                                             :for-valid-time (from #inst "2023-12-25")})
                          (from :promotions {:bind [promotion-type]
                                             :for-valid-time (from #inst "2023-12-26")}))))))

  (t/is (xt/execute-tx tu/*node*
                       [[:sql (sql-example "DML-Delete-bitemporal-sql")]]))

  (t/is (= #{{:promotion-type "general"}}
           (set
            (xt/q tu/*node*
                  '(unify (from :promotions {:bind [promotion-type]
                                             :for-valid-time (from #inst "2023-12-25")})
                          (from :promotions {:bind [promotion-type]
                                             :for-valid-time (from #inst "2023-12-26")})))))))

(deftest DML-Delete-everything-sql
  (xt/execute-tx tu/*node* comments)

  (t/is (not (empty? (xt/q tu/*node* '(from :comments [])))))

  (xt/execute-tx tu/*node*
    [[:sql (sql-example "DML-Delete-everything-sql")]])

  (t/is (empty? (xt/q tu/*node* '(from :comments []))))

  (t/is (not (empty? (xt/q tu/*node* '(from :comments {:bind [] :for-valid-time :all-time}))))))

(deftest DML-Update-sql
  (xt/submit-tx tu/*node*
                [[:put-docs :documents {:xt/id "doc-id", :version 1}]])

  (t/is (= [{:version 1}]
           (xt/q tu/*node* '(from :documents [version]))))

  (t/is (xt/execute-tx tu/*node*
                       [[:sql (sql-example "DML-Update-sql")
                         ["doc-id"]]]))

  (t/is (= [{:version 2}]
           (xt/q tu/*node* '(from :documents [version])))))

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

(deftest DML-Erase-sql
  (xt/submit-tx tu/*node* [[:put-docs :users {:xt/id :james, :email "jms@example.com"}]])

  (t/is (not (empty? (xt/q tu/*node* '(from :users [])))))

  (xt/submit-tx tu/*node* [[:sql (sql-example "DML-Erase-sql")]])

  (t/is (empty? (xt/q tu/*node* '(from :users []))))
  (t/is (empty? (xt/q tu/*node* '(from :users {:bind [] :for-valid-time :all-time})))))

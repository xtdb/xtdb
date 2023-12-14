(ns xtdb.tutorial-test
  (:require [clojure.data.json :as json]
            [clojure.test :as t :refer [deftest testing]]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]
            [xtdb.xtql.edn :as x-edn]
            [xtdb.xtql.json :as x-json]
            [xtdb.error :as err]))

(t/use-fixtures :each tu/with-node)

(defn str-json-query->edn [str-query]
  (-> str-query
      json/read-str
      x-json/parse-query
      x-edn/unparse))

;; TODO: Inline some
(def users
  [(xt/put :users {:xt/id "ivan", :first-name "Ivan", :last-name "Ivanov", :age 25})
   (-> (xt/put :users {:xt/id "petr", :first-name "Petr", :last-name "Petrov", :age 25})
       (xt/starting-from #inst "2018"))])

(def old-users
  [(xt/put :old-users {:xt/id "ivan", :given-name "Ivan", :surname "Ivanov"})
   (-> (xt/put :old-users {:xt/id "petr", :given-name "Petr", :surname "Petrov"})
       (xt/starting-from #inst "2018"))])

(def authors
  [(xt/put :authors {:xt/id "ivan", :first-name "Ivan", :last-name "Ivanov"})
   (xt/put :authors {:xt/id "petr", :first-name "Petr", :last-name "Petrov"})])

(def articles
  [(xt/put :articles {:xt/id 1, :author-id "ivan", :title "First" :content "My first blog"})
   (xt/put :articles {:xt/id 2, :author-id "ivan", :title "Second" :content "My second blog"})])

(def posts
  [(xt/put :posts {:xt/id 1, :author-id "ivan", :title "First" :content "My first blog"})
   (xt/put :posts {:xt/id 2, :author-id "ivan", :title "Second" :content "My second blog"})])

(def comments
  [(xt/put :comments {:xt/id 1, :article-id 1, :post-id 1, :created-at #inst "2020", :comment "1"})
   (xt/put :comments {:xt/id 2, :article-id 1, :post-id 1, :created-at #inst "2021", :comment "2"})
   (xt/put :comments {:xt/id 3, :article-id 2, :post-id 2, :created-at #inst "2022", :comment "1"})])

(def customers
  [(xt/put :customers {:xt/id "ivan"})
   (xt/put :customers {:xt/id "petr"})])

(def orders
  [(xt/put :orders {:xt/id 1, :customer-id "ivan", :currency :gbp, :order-value 100})
   (xt/put :orders {:xt/id 2, :customer-id "ivan", :currency :gbp, :order-value 150})])

(def promotions
  [(xt/put :promotions {:xt/id 1, :promotion-type "christmas"})
   (xt/put :promotions {:xt/id 2, :promotion-type "general"})])

#_
(ns-unmap *ns* 'test-issue)

#_
(deftest test-issue
  (xt/submit-tx tu/*node*
    [(xt/put :users {:xt/id "ivan"})])

  #_
  (xt/q tu/*node* '(-> (from :users [xt/id])
                       (where true)))
  #_
  (t/is (= (xt/q tu/*node* '(from :users [xt/id]))
           (xt/q tu/*node* '(from :users [xt/id])
                 {:key-fn :clojure}))))

(deftest basic-operations

  (xt/submit-tx tu/*node* users)

  (t/is (= #{{:user-id "ivan", :first-name "Ivan", :last-name "Ivanov"}
             {:user-id "petr", :first-name "Petr", :last-name "Petrov"}}
           (set
             (xt/q tu/*node*
                   ';; tag::bo-xtql-1[]
                   (from :users [{:xt/id user-id} first-name last-name])
                   ;; end::bo-xtql-1[]
                   ,))))

  (t/is (= #{{:user-id "ivan", :first-name "Ivan", :last-name "Ivanov"}
             {:user-id "petr", :first-name "Petr", :last-name "Petrov"}}
           (set
             (xt/q tu/*node*
                      (str-json-query->edn
                       ;; tag::bo-json-1[]
                       "{
                          \"from\": \"users\",
                          \"bind\": [ { \"xt/id\": \"user-id\" }, \"first-name\", \"last-name\" ]
                        }"
                       ;; end::bo-json-1[]
                        ,)))))

  (t/is (= #{{:user_id "ivan", :first_name "Ivan", :last_name "Ivanov"}
             {:user_id "petr", :first_name "Petr", :last_name "Petrov"}}
           (set
             (xt/q tu/*node*
                   ;; tag::bo-sql-1[]
                   "SELECT users.xt$id AS user_id, users.first_name, users.last_name FROM users"
                   ;; end::bo-sql-1[]
                   ,))))

  (t/is (= [{:first-name "Ivan", :last-name "Ivanov"}]
           (xt/q tu/*node*
                 '
                 ;; tag::bo-xtql-2[]
                 (from :users [{:xt/id "ivan"} first-name last-name])
                 ;; end::bo-xtql-2[]
                 ,)))

  (t/is (= [{:first-name "Ivan", :last-name "Ivanov"}]
           (xt/q tu/*node*
                 (str-json-query->edn
                   ;; tag::bo-json-2[]
                   "{
                      \"from\": \"users\",
                      \"bind\": [ { \"xt/id\": { \"@value\": \"ivan\" } }, \"first-name\", \"last-name\" ]
                    }"
                   ;; end::bo-json-2[]
                   ,))))

  (t/is (= [{:first_name "Ivan", :last_name "Ivanov"}]
           (xt/q tu/*node*
                 ;; tag::bo-sql-2[]
                 "SELECT users.first_name, users.last_name FROM users WHERE users.xt$id = 'ivan'"
                 ;; end::bo-sql-2[]
                 ,)))

  (t/is (= [{:user-id "ivan", :first-name "Ivan", :last-name "Ivanov"}
            {:user-id "petr", :first-name "Petr", :last-name "Petrov"}]
           (xt/q tu/*node*
                 '
                 ;; tag::bo-xtql-3[]
                 (-> (from :users [{:xt/id user-id} first-name last-name])
                     (order-by last-name first-name)
                     (limit 10))
                 ;; end::bo-xtql-3[]
                 ,)))

  (t/is (= [{:user-id "ivan", :first-name "Ivan", :last-name "Ivanov"}
            {:user-id "petr", :first-name "Petr", :last-name "Petrov"}]
           (xt/q tu/*node*
                 (str-json-query->edn
                   ;; tag::bo-json-3[]
                   "[
                      {
                        \"from\": \"users\",
                        \"bind\": [{ \"xt/id\": \"user-id\" }, \"first-name\", \"last-name\"]
                      },
                      { \"orderBy\": [\"last-name\", \"first-name\"] },
                      { \"limit\": 10 }
                    ]"
                   ;; end::bo-json-3[]
                   ,))))

  (t/is (= [{:user_id "ivan", :first_name "Ivan", :last_name "Ivanov"}
            {:user_id "petr", :first_name "Petr", :last_name "Petrov"}]
           (xt/q tu/*node*
                 ;; tag::bo-sql-3[]
                 "SELECT users.xt$id AS user_id, users.first_name, users.last_name
                  FROM users
                  ORDER BY last_name, first_name
                  LIMIT 10"
                 ;; end::bo-sql-3[]
                 ,))))

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
                   ,))))

  (t/is (= #{{:user-id "ivan", :first-name "Ivan", :last-name "Ivanov", :content "My first blog", :title "First"}
             {:user-id "ivan", :first-name "Ivan", :last-name "Ivanov", , :content "My second blog", :title "Second"}}
           (set
             (xt/q tu/*node*
                     (str-json-query->edn
                       ;; tag::joins-json-1[]
                       "{
                          \"unify\": [
                            {
                              \"from\": \"users\",
                              \"bind\": [ { \"xt/id\": \"user-id\" }, \"first-name\", \"last-name\" ]
                            },
                            {
                              \"from\": \"articles\",
                              \"bind\": [ { \"author-id\": \"user-id\" }, \"title\", \"content\" ]
                            }
                          ]
                        }"
                       ;; end::joins-json-1[]
                       ,)))))

  (t/is (= #{{:user_id "ivan", :first_name "Ivan", :last_name "Ivanov", :content "My first blog", :title "First"}
             {:user_id "ivan", :first_name "Ivan", :last_name "Ivanov", , :content "My second blog", :title "Second"}}
           (set
             (xt/q tu/*node*
                   ;; tag::joins-sql-1[]
                   "SELECT u.xt$id AS user_id, u.first_name, u.last_name, a.title, a.content
                    FROM users u JOIN articles a ON u.xt$id = a.author_id"
                    ;; end::joins-sql-1[]
                    ,))))

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
                   ,))))

  (t/is (= #{{:age 25, :uid1 "ivan", :uid2 "petr"}
             {:age 25, :uid1 "petr", :uid2 "ivan"}}
           (set
             (xt/q tu/*node*
                   (str-json-query->edn
                     ;; tag::joins-json-2[]
                     "{
                        \"unify\": [
                          {
                            \"from\": \"users\",
                            \"bind\": [ { \"xt/id\": \"uid1\" }, \"age\" ]
                          },
                          {
                            \"from\": \"users\",
                            \"bind\": [ { \"xt/id\": \"uid2\" }, \"age\" ]
                          },
                          {
                            \"where\": [ { \"<>\": [ \"uid1\", \"uid2\" ] } ]
                          }
                        ]
                      }"
                     ;; end::joins-json-2[]
                     ,)))))

  (t/is (= #{{:age 25, :uid1 "ivan", :uid2 "petr"}
             {:age 25, :uid1 "petr", :uid2 "ivan"}}
           (set
             (xt/q tu/*node*
                   ;; tag::joins-sql-2[]
                   "SELECT u1.xt$id AS uid1, u2.xt$id AS uid2, u1.age
                    FROM users u1
                      JOIN users u2 ON (u1.age = u2.age)
                    WHERE u1.xt$id <> u2.xt$id"
                   ;; end::joins-sql-2[]
                   ,))))

  (t/is (= #{{:cid "ivan", :order-value 150, :currency :gbp}
             {:cid "ivan", :order-value 100, :currency :gbp}
             {:cid "petr", :order-value nil, :currency nil}}
           (set
             (xt/q tu/*node*
                   '
                   ;; tag::joins-xtql-3[]
                   (-> (unify (from :customers [{:xt/id cid}])
                              (left-join (from :orders [{:xt/id oid, :customer-id cid} currency order-value])
                                         [cid currency order-value]))
                       (limit 100))
                   ;; end::joins-xtql-3[]
                   ,))))

  (t/is (= #{{:cid "ivan", :order-value 150, :currency :gbp}
             {:cid "ivan", :order-value 100, :currency :gbp}
             {:cid "petr", :order-value nil, :currency nil}}
           (set
             (xt/q tu/*node*
                     (str-json-query->edn
                       ;; tag::joins-json-3[]
                       "[
                          {
                            \"unify\": [
                              {
                                \"from\": \"customers\",
                                \"bind\": [{ \"xt/id\": \"cid\" }]
                              },
                              {
                                \"leftJoin\": {
                                  \"from\": \"orders\",
                                  \"bind\": [{ \"xt/id\": \"oid\", \"customer-id\": \"cid\" }, \"currency\", \"order-value\"]
                                },
                                \"bind\": [\"cid\", \"currency\", \"order-value\"]
                              }
                            ]
                          },
                          { \"limit\": 100 }
                        ]"
                       ;; end::joins-json-3[]
                       ,)))))

  (t/is (= #{{:cid "ivan", :order_value 150, :currency :gbp}
             {:cid "ivan", :order_value 100, :currency :gbp}
             {:cid "petr", :order_value nil, :currency nil}}
           (set
             (xt/q tu/*node*
                   ;; tag::joins-sql-3[]
                   "SELECT c.xt$id AS cid, o.currency, o.order_value
                    FROM customers c
                      LEFT JOIN orders o ON (c.xt$id = o.customer_id)
                    LIMIT 100"
                   ;; end::joins-sql-3[]
                   ,))))

  (t/is (= [{:cid "petr"}]
           (xt/q tu/*node*
                 '
                 ;; tag::joins-xtql-4[]
                 (-> (unify (from :customers [{:xt/id cid}])
                            (where (not (exists? (from :orders [{:customer-id $cid}])
                                                 {:args [cid]}))))
                     (limit 100))
                 ;; end::joins-xtql-4[]
                 ,)))

  (t/is (= [{:cid "petr"}]
           (xt/q tu/*node*
                 (str-json-query->edn
                   ;; tag::joins-json-4[]
                   "[
                      {
                        \"unify\": [
                          {
                            \"from\": \"customers\",
                            \"bind\": [{ \"xt/id\": \"cid\" }]
                          },
                          {
                            \"where\": [
                              {
                                \"not\": [
                                  {
                                    \"exists\": {
                                      \"from\": \"orders\",
                                      \"bind\": [ { \"customer-id\": \"$cid\" } ]
                                    },
                                    \"args\": [ \"cid\" ]
                                  }
                                ]
                              }
                            ]
                          }
                        ]
                      },
                      { \"limit\": 100 }
                    ]"
                   ;; end::joins-json-4[]
                   ,))))

  (t/is (= [{:cid "petr"}]
           (xt/q tu/*node*
                 ;; tag::joins-sql-4[]
                 "SELECT c.xt$id AS cid
                  FROM customers c
                  WHERE c.xt$id NOT IN (SELECT orders.customer_id FROM orders)
                  LIMIT 100"
                 ;; end::joins-sql-4[]
                 ,))))

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
                   ,))))

  (t/is (= #{{:first-name "Ivan", :last-name "Ivanov", :full-name "Ivan Ivanov"}
             {:first-name "Petr", :last-name "Petrov", :full-name "Petr Petrov"}}
           (set
             (xt/q tu/*node*
                   (str-json-query->edn
                     ;; tag::proj-json-1[]
                     "[
                        {
                          \"from\": \"users\",
                          \"bind\": [\"first-name\", \"last-name\"]
                        },
                        {
                          \"with\": [
                            {
                              \"full-name\": {
                                \"concat\": [\"first-name\", { \"@value\": \" \" }, \"last-name\"]
                              }
                            }
                          ]
                        }
                      ]"
                     ;; end::proj-json-1[]
                     ,)))))

  (t/is (= #{{:first_name "Ivan", :last_name "Ivanov", :full_name "Ivan Ivanov"}
             {:first_name "Petr", :last_name "Petrov", :full_name "Petr Petrov"}}
           (set
             (xt/q tu/*node*
                   ;; tag::proj-sql-1[]
                   "SELECT users.first_name, users.last_name, (users.first_name || ' ' || users.last_name) AS full_name
                    FROM users"
                   ;; end::proj-sql-1[]
                   ,))))

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
                   ,))))

  (t/is (= #{{:full-name "Ivan Ivanov", :title "First", :content "My first blog"}
             {:full-name "Ivan Ivanov", :title "Second", :content "My second blog"}}
           (set
             (xt/q tu/*node*
                   (str-json-query->edn
                     ;; tag::proj-json-2[]
                     "[
                        {
                          \"unify\": [
                            {
                              \"from\": \"users\",
                              \"bind\": [ { \"xt/id\": \"user-id\" }, \"first-name\", \"last-name\" ]
                            },
                            {
                              \"from\": \"articles\",
                              \"bind\": [ { \"author-id\": \"user-id\" }, \"title\", \"content\" ]
                            }
                          ]
                        },
                        {
                          \"return\": [
                            {
                              \"full-name\": {
                                \"concat\": [ \"first-name\", { \"@value\": \" \" }, \"last-name\" ]
                              }
                            },
                            \"title\",
                            \"content\"
                          ]
                        }
                      ]"
                     ;; end::proj-json-2[]
                     ,)))))

  (t/is (= #{{:full_name "Ivan Ivanov", :title "First", :content "My first blog"}
             {:full_name "Ivan Ivanov", :title "Second", :content "My second blog"}}
           (set
             (xt/q tu/*node*
                   ;; tag::proj-sql-2[]
                   "SELECT (u.first_name || ' ' || u.last_name) AS full_name, a.title, a.content
                    FROM users u
                      JOIN articles a ON u.xt$id = a.author_id"
                   ;; end::proj-sql-2[]
                   ,))))

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
                   ,))))

  (t/is (= #{{:first-name "Ivan", :last-name "Ivanov", :title "Second", :content "My second blog"}
             {:first-name "Ivan", :last-name "Ivanov", :title "First", :content "My first blog"}}
           (set
             (xt/q tu/*node*
                   (str-json-query->edn
                     ;; tag::proj-json-3[]
                     "[
                        {
                          \"unify\": [
                            {
                              \"from\": \"users\",
                              \"bind\": [ { \"xt/id\": \"user-id\" }, \"first-name\", \"last-name\" ]
                            },
                            {
                              \"from\": \"articles\",
                              \"bind\": [ { \"author-id\": \"user-id\" }, \"title\", \"content\" ]
                            }
                          ]
                        },
                        { \"without\": [ \"user-id\" ] }
                      ]"
                     ;; end::proj-json-3[]
                     ,)))))

  (t/is (= #{{:first_name "Ivan", :last_name "Ivanov", :title "Second", :content "My second blog"}
             {:first_name "Ivan", :last_name "Ivanov", :title "First", :content "My first blog"}}
           (set
             (xt/q tu/*node*
                   ;; tag::proj-sql-3[]
                   "SELECT u.first_name, u.last_name, a.title, a.content
                    FROM users u
                      JOIN articles a ON u.xt$id = a.author_id"
                   ;; end::proj-sql-3[]
                   ,)))))

(deftest aggregations

  (xt/submit-tx tu/*node* (concat customers orders))

  (testing "To count/sum/average values, we use `aggregate`:"

    (t/is (= #{{:cid "ivan", :currency :gbp, :order-count 2, :total-value 250}
               {:cid "petr", :currency nil, :order-count 0, :total-value 0}}
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
                 ,))))


    (t/is (= #{{:cid "ivan", :currency :gbp, :order-count 2, :total-value 250}
               {:cid "petr", :currency nil, :order-count 0, :total-value 0}}
             (set
               (xt/q tu/*node*
                 (str-json-query->edn
                   ;; tag::aggr-json-1[]
                   "[
                      {
                        \"unify\": [
                          {
                            \"from\": \"customers\",
                            \"bind\": [ { \"xt/id\": \"cid\" } ]
                          },
                          {
                            \"leftJoin\": {
                              \"from\": \"orders\",
                              \"bind\": [ { \"xt/id\": \"oid\", \"customer-id\": \"cid\" }, \"currency\", \"order-value\" ]
                            },
                            \"bind\": [ \"oid\", \"cid\", \"currency\", \"order-value\" ]
                          }
                        ]
                      },
                      {
                        \"aggregate\": [
                          { \"cid\": \"cid\" },
                          { \"currency\": \"currency\" },
                          { \"order-count\": { \"count\": [\"oid\"] } },
                          { \"total-value\": { \"sum\": [ \"order-value\" ] } }
                        ]
                      },
                      { 
                         \"with\": [
                           {
                             \"total-value\": {
                               \"coalesce\": [\"total-value\", 0]
                             }
                           }
                         ]
                      },
                      { \"orderBy\": [ { \"val\": \"total-value\", \"dir\": \"desc\" } ] },
                      { \"limit\": 100 }
                    ]"
                   ;; end::aggr-json-1[]
                   ,)))))

    (t/is (= #{{:cid "ivan", :currency :gbp, :order_count 2, :total_value 250}
               {:cid "petr", :currency nil, :order_count 0, :total_value 0}}
             (set
               (xt/q tu/*node*
                 ;; tag::aggr-sql-1[]
                 "SELECT c.xt$id AS cid, o.currency, COUNT(o.xt$id) AS order_count, COALESCE(SUM(o.order_value), 0) AS total_value
                  FROM customers c
                    LEFT JOIN orders o ON (c.xt$id = o.customer_id)
                  GROUP BY c.xt$id, o.currency
                  ORDER BY total_value DESC
                  LIMIT 100"
                 ;; end::aggr-sql-1[]
                 ,))))))



(deftest pull

  (xt/submit-tx tu/*node* (concat authors articles comments))

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
                 ,))))

    ;; TODO: Waiting for `pull` to be implemented:
    #_
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
                 (str-json-query->edn
                   ;; tag::pull-json-1[]
                   "[
                      {
                        \"from\": \"articles\",
                        \"bind\": [{ \"xt/id\": \"article-id\" }, \"title\", \"content\", \"author-id\"]
                      },
                      {
                        \"with\": [
                          {
                            \"author\": {
                              \"pull\": [
                                {
                                  \"from\": \"authors\",
                                  \"bind\": [{ \"xt/id\": \"$author-id\" }, \"first-name\", \"last-name\"]
                                }
                              ],
                              \"args\": [\"author-id\"]
                            },
                            \"comments\": {
                              \"pullMany\": [
                                {
                                  \"from\": \"comments\",
                                  \"bind\": [{\"article-id\": \"$article-id\"}, \"created-at\", \"comment\"]
                                },
                                { \"orderBy\": [{ \"val\": \"created-at\", \"dir\": \"desc\" }] },
                                { \"limit\": 10 }
                              ],
                              \"args\": [\"article-id\"]
                            }
                          }
                        ]
                      }
                    ]"
                   ;; end::pull-json-1[]
                   ,)))))

    ;; NO SQL for this one
    ,))

(deftest bitemporality

  (xt/submit-tx tu/*node* users)

  (t/is (= [{:first-name "Petr", :last-name "Petrov"}]
           (xt/q tu/*node*
             '
             ;; tag::bitemp-xtql-1[]
             (from :users {:for-valid-time (at #inst "2020-01-01")
                           :bind [first-name last-name]})
             ;; end::bitemp-xtql-1[]
             ,)))

  (t/is (= #{{:first-name "Ivan", :last-name "Ivanov"}
             {:first-name "Petr", :last-name "Petrov"}}
           (set
             (xt/q tu/*node*
               '
               ;; tag::bitemp-xtql-2[]
               (from :users {:for-valid-time :all-time
                             :bind [first-name last-name]})
               ;; end::bitemp-xtql-2[]
               ,))))


  (t/is (= [{:first-name "Petr", :last-name "Petrov"}]
           (xt/q tu/*node*
             (str-json-query->edn
               ;; tag::bitemp-json-1[]
               "{
                  \"from\": \"users\",
                  \"forValidTime\": { \"at\": { \"@value\": \"2020-01-01\", \"@type\": \"xt:date\" } },
                  \"bind\": [ \"first-name\", \"last-name\" ]
                }"
               ;; end::bitemp-json-1[]
               ,))))

  (t/is (= #{{:first-name "Ivan", :last-name "Ivanov"}
             {:first-name "Petr", :last-name "Petrov"}}
           (set
             (xt/q tu/*node*
               (str-json-query->edn
                 ;; tag::bitemp-json-2[]
                 "{
                    \"from\": \"users\",
                    \"forValidTime\": \"allTime\",
                    \"bind\": [ \"first-name\", \"last-name\" ]
                  }"
                 ;; end::bitemp-json-2[]
                 ,)))))


  (t/is (= [{:first_name "Petr", :last_name "Petrov"}]
           (xt/q tu/*node*
                 ;; tag::bitemp-sql-1[]
                 "SELECT users.first_name, users.last_name FROM users FOR VALID_TIME AS OF DATE '2020-01-01'"
                 ;; end::bitemp-sql-1[]
                 ,)))

  (t/is (= #{{:first_name "Ivan", :last_name "Ivanov"}
             {:first_name "Petr", :last_name "Petrov"}}
           (set
             (xt/q tu/*node*
                   ;; tag::bitemp-sql-2[]
                   "SELECT users.first_name, users.last_name FROM users FOR ALL VALID_TIME"
                   ;; end::bitemp-sql-2[]
                   ,))))

  (t/is (= [{:user-id "petr"}]
           (xt/q tu/*node*
                 '
                 ;; tag::bitemp-xtql-3[]
                 (unify (from :users {:for-valid-time (at #inst "2018")
                                      :bind [{:xt/id user-id}]})

                        (from :users {:for-valid-time (at #inst "2023")
                                      :bind [{:xt/id user-id}]}))
                 ;; end::bitemp-xtql-3[]
                 ,)))

  (t/is (= [{:user-id "petr"}]
           (xt/q tu/*node*
             (str-json-query->edn
               ;; tag::bitemp-json-3[]
               "{
                  \"unify\": [
                    {
                      \"from\": \"users\",
                      \"forValidTime\": { \"at\": { \"@value\": \"2018-01-01\", \"@type\": \"xt:date\" } },
                      \"bind\": [ { \"xt/id\": \"user-id\"} ]
                    },
                    {
                      \"from\": \"users\",
                      \"forValidTime\": { \"at\": { \"@value\": \"2023-01-01\", \"@type\": \"xt:date\" } },
                      \"bind\": [ { \"xt/id\": \"user-id\" } ]
                    }
                  ]
                }"
               ;; end::bitemp-json-3[]
               ,)))))

(deftest DML-Insert-xtql
  (xt/submit-tx tu/*node* old-users)

  (t/is (= []
           (xt/q tu/*node*
                 '(from :users [first-name last-name]))))

  (let [node tu/*node*]
    ;; tag::DML-Insert-xtql[]
    (xt/submit-tx node
      [(xt/insert-into :users
         '(from :old-users [xt/id {:given-name first-name, :surname last-name}
                            xt/valid-from xt/valid-to]))])
    ;; end::DML-Insert-xtql[]
    ,)

  ;; TODO: Test valid time
  (t/is (= #{{:first-name "Ivan", :last-name "Ivanov"}
             {:first-name "Petr", :last-name "Petrov"}}
           (set
             (xt/q tu/*node*
                   '(from :users [first-name last-name]))))))

(deftest DML-Insert-sql
  (xt/submit-tx tu/*node* old-users)

  (t/is (= []
           (xt/q tu/*node*
                 '(from :users [first-name last-name]))))

  (xt/submit-tx tu/*node*
    [(xt/sql-op
       ;; tag::DML-Insert-sql[]
      "INSERT INTO users
      SELECT ou.xt$id, ou.given_name AS first_name, ou.surname AS last_name,
             ou.xt$valid_from, ou.xt$valid_to
      FROM old_users AS ou"
       ;; end::DML-Insert-sql[]
       ,)])
  
  ;; TODO: Test valid time
  (t/is (= #{{:first-name "Ivan", :last-name "Ivanov"}
             {:first-name "Petr", :last-name "Petrov"}}
           (set
             (xt/q tu/*node*
                   '(from :users [first-name last-name]))))))

;; tag::DML-Delete-xtql[]
(defn delete-a-post [node the-post-id]
  (xt/submit-tx node
    [(-> (xt/delete-from :comments '[{:post-id $post-id}])
         (xt/with-op-args {:post-id the-post-id}))]))
;; end::DML-Delete-xtql[]

(deftest DML-Delete-xtql
  (xt/submit-tx tu/*node* comments)

  (delete-a-post tu/*node* 1)

  (t/is (= []
           (xt/q tu/*node*
                 '(from :comments [{:post-id $post-id}])
                 {:args {:post-id 1}}))))

(deftest DML-Delete-sql
  (xt/submit-tx tu/*node* comments)

  (xt/submit-tx tu/*node*
                [(-> (xt/sql-op
                       ;; tag::DML-Delete-sql[]
                       "DELETE FROM comments WHERE comments.post_id = ?"
                       ;; end::DML-Delete-sql[]
                       ,)
                     (xt/with-op-args [1]))])

  (t/is (= []
           (xt/q tu/*node*
                 '(from :comments [{:post-id $post-id}])
                 {:args {:post-id 1}}))))

(deftest DML-Delete-additional-unify-clauses-xtql
  (xt/submit-tx tu/*node* (concat posts comments))

  (let [node tu/*node*]
    ;; tag::DML-Delete-additional-unify-clauses-xtql[]
    (xt/submit-tx node
      [(-> (xt/delete-from :comments '[{:post-id pid}]
                           '(from :posts [{:xt/id pid, :author-id $author}]))
           (xt/with-op-args {:author "ivan"}))])
    ;; end::DML-Delete-additional-unify-clauses-xtql[]
    ,)

  (t/is (= []
           (xt/q tu/*node*
                 '(unify (from :comments [{:post-id pid}])
                         (from :posts [{:xt/id pid, :author-id $author}]))
                 {:args {:author "ivan"}}))))

(deftest DML-Delete-additional-unify-clauses-sql
  (xt/submit-tx tu/*node* (concat posts comments))

  (xt/submit-tx tu/*node*
                [(-> (xt/sql-op
                       ;; tag::DML-Delete-additional-unify-clauses-sql[]
                       "DELETE FROM comments
                        WHERE comments.post_id IN (SELECT posts.xt$id FROM posts WHERE posts.author_id = ?)"
                       ;; end::DML-Delete-additional-unify-clauses-sql[]
                       ,)
                     (xt/with-op-args ["ivan"]))])

  (t/is (= []
           (xt/q tu/*node*
                 '(unify (from :comments [{:post-id pid}])
                         (from :posts [{:xt/id pid, :author-id $author}]))
                 {:args {:author "ivan"}}))))

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
      [(xt/delete-from :promotions '{:bind [{:promotion-type "christmas"}]
                                     :for-valid-time (from #inst "2023-12-26")})])
    ;; end::DML-Delete-bitemporal-xtql[]
    ,)

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

  (xt/submit-tx tu/*node*
                [(xt/sql-op
                   ;; tag::DML-Delete-bitemporal-sql[]
                   "DELETE FROM promotions
                    FOR PORTION OF VALID_TIME FROM DATE '2023-12-26' TO END_OF_TIME
                    WHERE promotions.promotion_type = 'christmas'"
                   ;; end::DML-Delete-bitemporal-sql[]
                   ,)])

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
      [(xt/delete-from :comments '{})])
    ;; end::DML-Delete-everything-xtql[]
    ,)

  (t/is (empty? (xt/q tu/*node* '(from :comments [])))))

(deftest DML-Delete-everything-sql
  (xt/submit-tx tu/*node* comments)

  (t/is (not (empty? (xt/q tu/*node* '(from :comments [])))))

  (xt/submit-tx tu/*node*
    [(xt/sql-op
       ;; tag::DML-Delete-everything-sql[]
       "DELETE FROM comments"
       ;; end::DML-Delete-everything-sql[]
       ,)])

  (t/is (empty? (xt/q tu/*node* '(from :comments [])))))

(deftest DML-Update-xtql
  (xt/submit-tx tu/*node*
    [(xt/put :documents {:xt/id "doc-id", :version 1})])

  (t/is (= [{:version 1}]
           (xt/q tu/*node* '(from :documents [version]))))

  (let [node tu/*node*]
    ;; tag::DML-Update-xtql[]
    (xt/submit-tx node
      [(-> (xt/update-table :documents '{:bind [{:xt/id $doc-id, :version v}]
                                         :set {:version (+ v 1)}})
           (xt/with-op-args {:doc-id "doc-id"}))])
    ;; end::DML-Update-xtql[]
    ,)

  (t/is (= [{:version 2}]
           (xt/q tu/*node* '(from :documents [version])))))

(deftest DML-Update-sql
  (xt/submit-tx tu/*node*
    [(xt/put :documents {:xt/id "doc-id", :version 1})])

  (t/is (= [{:version 1}]
           (xt/q tu/*node* '(from :documents [version]))))

  (xt/submit-tx tu/*node*
                [(-> (xt/sql-op
                       ;; tag::DML-Update-sql[]
                       "UPDATE documents
                        SET version = documents.version + 1
                        WHERE documents.xt$id = ?"
                       ;; end::DML-Update-sql[]
                       ,)
                     (xt/with-op-args ["doc-id"]))])

  (t/is (= [{:version 2}]
           (xt/q tu/*node* '(from :documents [version])))))

(deftest DML-Update-bitemporal-xtql
  (xt/submit-tx tu/*node*
    [(xt/put :posts {:xt/id "my-post-id" :comment-count 1})
     (xt/put :comments {:xt/id 1, :post-id "my-post-id"})])

  (t/is (= [{:comment-count 1}]
           (xt/q tu/*node* '(from :posts [comment-count]))))
  
  (let [node tu/*node*]
    ;; tag::DML-Update-bitemporal-xtql[]
    (xt/submit-tx node
      [(xt/put :comments {:xt/id (random-uuid), :post-id "my-post-id"})
       (-> (xt/update-table :posts '{:bind [{:xt/id $post-id}], :set {:comment-count cc}}
                            '(with {cc (q (-> (from :comments [{:post-id $post-id}])
                                              (aggregate {:cc (row-count)})))}))
           (xt/with-op-args {:post-id "my-post-id"}))])
    ;; end::DML-Update-bitemporal-xtql[]
    ,)

  (t/is (= [{:comment-count 2}]
           (xt/q tu/*node* '(from :posts [comment-count])))))

(deftest DML-Update-bitemporal-sql
  (xt/submit-tx tu/*node*
    [(xt/put :posts {:xt/id "my-post-id" :comment-count 1})
     (xt/put :comments {:xt/id 1, :post-id "my-post-id"})])

  (t/is (= [{:comment-count 1}]
           (xt/q tu/*node* '(from :posts [comment-count]))))
  
  (xt/submit-tx tu/*node*
    [(-> (xt/sql-op
           ;; tag::DML-Update-bitemporal-sql-1[]
           "INSERT INTO comments (xt$id, post_id) VALUES (?, ?)"
           ;; end::DML-Update-bitemporal-sql-1[]
           ,)
         (xt/with-op-args [(random-uuid), "my-post-id"]))
     (-> (xt/sql-op
           ;; tag::DML-Update-bitemporal-sql-2[]
           "UPDATE posts AS p
            SET comment_count = (SELECT COUNT(*) FROM comments WHERE comments.post_id = ?)
            WHERE p.post_id = ?" 
           ;; end::DML-Update-bitemporal-sql-2[]
           ,)
         (xt/with-op-args ["my-post-id" "my-post-id"]))])

  (t/is (= [{:comment-count 2}]
           (xt/q tu/*node* '(from :posts [comment-count])))))

(deftest DML-Erase-xtql
  (xt/submit-tx tu/*node*
    [(xt/put :users {:xt/id "user-id", :email "jms@example.com"})])

  (t/is (not (empty? (xt/q tu/*node* '(from :users [])))))

  (let [node tu/*node*]
    ;; tag::DML-Erase-xtql[]
    (xt/submit-tx node
      [(xt/erase-from :users '[{:email "jms@example.com"}])])
    ;; end::DML-Erase-xtql[]
    ,)

  ;; TODO: Test valid time
  (t/is (empty? (xt/q tu/*node* '(from :users [])))))

(deftest DML-Erase-sql
  (xt/submit-tx tu/*node*
    [(xt/put :users {:xt/id "user-id", :email "jms@example.com"})])

  (t/is (not (empty? (xt/q tu/*node* '(from :users [])))))

  (xt/submit-tx tu/*node*
    [(xt/sql-op
       ;; tag::DML-Erase-sql[]
       "ERASE FROM users WHERE users.email = 'jms@example.com'"
       ;; end::DML-Erase-sql[]
       ,)])

  ;; TODO: Test valid time
  (t/is (empty? (xt/q tu/*node* '(from :users [])))))

(deftest DML-Assert
  (xt/submit-tx tu/*node*
    [(xt/put :users {:xt/id :james, :email "james@example.com"})])

  (let [node tu/*node*
        {my-tx-id :tx-id}
        ;; tag::DML-Assert-xtql[]
        (xt/submit-tx node
          [(-> (xt/assert-not-exists '(from :users [{:email $email}]))
               (xt/with-op-args {:email "james@example.com"}))

           (xt/put :users {:xt/id :james, :email "james@example.com"})])
        ;; end::DML-Assert-xtql[]
        ,]
    (t/is (= ;; tag::DML-Assert-query-result[]
             [{:xt/committed? false
               :xt/error {::err/error-type :runtime-error
                          ::err/error-key :xtdb/assert-failed
                          ::err/message "Precondition failed: assert-not-exists"
                          :row-count 1}}]
             ;; end::DML-Assert-query-result[]
             (-> ;; tag::DML-Assert-query[]
                 (xt/q node '(from :xt/txs [{:xt/id $tx-id} xt/committed? xt/error])
                      {:args {:tx-id my-tx-id}})
                 ;; end::DML-Assert-query[]
                 (update-in [0 :xt/error] ex-data)))))

  ;; TODO: Once implemented
  #_
  (let [{:keys [tx-id]}
        (xt/submit-tx tu/*node*
          [(xt/sql-op
             ;; tag::DML-Assert-sql[]
             "ASSERT NOT EXISTS (SELECT 1 FROM users WHERE email = 'james@juxt.pro')"
             ;; end::DML-Assert-sql[]
             ,)])]
    (t/is (= [{:xt/committed? false}]
             (xt/q tu/*node* '(from :xt/txs [{:xt/id $tx-id} xt/committed?])
                   {:args {:tx-id tx-id}})))))

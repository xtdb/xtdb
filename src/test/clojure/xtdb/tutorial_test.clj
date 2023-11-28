(ns xtdb.tutorial-test
  (:require [clojure.data.json :as json]
            [clojure.test :as t :refer [deftest testing]]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]
            [xtdb.xtql.edn :as x-edn]
            [xtdb.xtql.json :as x-json]))

(t/use-fixtures :each tu/with-node)

(defn str-json-query->edn [str-query]
  (-> str-query
      json/read-str
      x-json/parse-query
      x-edn/unparse))

(deftest basic-operations

  (testing "To read a table, we use from."

    (xt/q tu/*node*
          '(from :users [{:xt/id user-id} first-name last-name]))

    (xt/q tu/*node*
          (str-json-query->edn
           "{
           \"from\": \"users\",
           \"bind\": [ { \"xt/id\": \"user-id\" }, \"first-name\", \"last-name\" ]
         }"))

    (xt/q tu/*node*
          "SELECT users.xt$id AS user_id, users.first_name, users.last_name FROM users"))



  (testing "We can look up a single user-id by binding it in the from clause:"

    (xt/q tu/*node*
          '(from :users [{:xt/id "james"} first-name last-name]))

    (xt/q tu/*node*
          (str-json-query->edn
           "{
           \"from\": \"users\",
           \"bind\": [ { \"xt/id\": { \"@value\": \"james\" } }, \"first-name\", \"last-name\" ]
         }"))

    (xt/q tu/*node*
          "SELECT users.first_name, users.last_name FROM users WHERE users.xt$id = 'james'"))



  (testing "from is a valid query in isolation but, for anything more powerful, we’ll need a 'pipeline':"

    (xt/q tu/*node*
          '(-> (from :users [{:xt/id user-id} first-name last-name])
               (order-by last-name first-name)
               (limit 10)))

    (xt/q tu/*node*
          (str-json-query->edn
           "[
           {
             \"from\": \"users\",
             \"bind\": [{ \"xt/id\": \"user-id\" }, \"first-name\", \"last-name\"]
           },
           { \"orderBy\": [\"last-name\", \"first-name\"] },
           { \"limit\": 10 }
         ]"))

    ;; TODO: Interesting to note that ORDER BY doesn't require fully qualified columns
    (xt/q tu/*node*
          "SELECT users.xt$id AS user_id, users.first_name, users.last_name
       FROM users
       ORDER BY last_name, first_name
       LIMIT 10")))

(deftest joins

  (testing "Within a unify, we use 'logic variables' (e.g. user-id, first-name etc in the above example) to specify how the inputs should be joined together.
            In this case, we re-use the user-id logic variable to indicate that the :xt/id from the :users table should be matched with the :author-id of the :articles table."

    (xt/q tu/*node*
          '(unify (from :users [{:xt/id user-id} first-name last-name])
                  (from :articles [{:author-id user-id} title content])))

    (xt/q tu/*node*
          (str-json-query->edn
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
         }"))

    (xt/q tu/*node*
          "SELECT u.xt$id AS user_id, u.first_name, u.last_name, a.title, a.content
       FROM users u JOIN articles a ON u.xt$id = a.author_id"))



  (testing "For non-equality cases, we can use a where clause (where we have a full SQL-inspired expression standard library at our disposal)"

    (xt/q tu/*node*
          '(unify (from :users [{:xt/id uid1} age])
                  (from :users [{:xt/id uid2} age])
                  (where (<> uid1 uid2))))

    (xt/q tu/*node*
          (str-json-query->edn
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
         }"))

    (xt/q tu/*node*
          "SELECT u1.xt$id AS uid1, u2.xt$id AS u2, u1.age
       FROM users u1
         JOIN users u2 ON (u1.age = u2.age)
       WHERE u1.xt$id <> u2.xt$id"))



  (testing "We can specify that a certain match is optional using left-join:"

    (xt/q tu/*node*
          '(-> (unify (from :customers [{:xt/id cid}])
                      (left-join (from :orders [{:xt/id oid, :customer-id cid} currency order-value])
                                 [cid currency order-value]))
               (limit 100)))

    (xt/q tu/*node*
          (str-json-query->edn
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
         ]"))

    (xt/q tu/*node*
          "SELECT c.xt$id AS cid, o.xt$id AS oid, o.currency, o.order_value
       FROM customers c
         LEFT JOIN orders o ON (c.xt$id = o.customer_id)
       LIMIT 100"))



  (testing "Or, we can specify that we only want to return customers who don’t have any orders, using not-exists?:"

    (xt/q tu/*node*
          '(-> (unify (from :customers [{:xt/id cid}])
                      (where (not (exists? (from :orders [{:customer-id $cid}])
                                           {:args [cid]}))))
               (limit 100)))

    (xt/q tu/*node*
          (str-json-query->edn
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
         ]"))

    (xt/q tu/*node*
          "SELECT c.xt$id AS cid
       FROM customers c
       WHERE c.xt$id NOT IN (SELECT orders.customer_id FROM orders)
       LIMIT 100")))

(deftest projections

  (testing "We can create new columns from old ones using with:"

    (xt/q tu/*node*
          '(-> (from :users [first-name last-name])
               (with {:full-name (concat first-name " " last-name)})))

    (xt/q tu/*node*
          (str-json-query->edn
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
         ]"))

    (xt/q tu/*node*
          "SELECT users.first_name, users.last_name, (users.first_name || ' ' || users.last_name) AS full_name
       FROM users"))


  (testing "Where `with` adds to the available columns, `return` _only_ yields the specified columns to the next operation:"

    (xt/q tu/*node*
          '(-> (unify (from :users [{:xt/id user-id} first-name last-name])
                      (from :articles [{:author-id user-id} title content]))
               (return {:full-name (concat first-name " " last-name)} :title :content)))

    (xt/q tu/*node*
          (str-json-query->edn
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
         ]"))




    (xt/q tu/*node*
          "SELECT (u.first_name || ' ' || u.last_name) AS full_name, a.title, a.content
       FROM users u
         JOIN articles a ON u.xt$id = a.author_id"))



  (testing "Where we don't need any additional projections, we can use `without`:"

    (xt/q tu/*node*
          '(-> (unify (from :users [{:xt/id user-id} first-name last-name])
                      (from :articles [{:author-id user-id} title content]))
               (without :user-id)))


    (xt/q tu/*node*
          (str-json-query->edn
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
         ]"))

    (xt/q tu/*node*
          "SELECT u.first_name, u.last_name, a.title, a.content
       FROM users u
         JOIN articles a ON u.xt$id = a.author_id")))



(deftest aggregations

  (testing "To count/sum/average values, we use `aggregate`:"

    (xt/q tu/*node*
      '(-> (unify (from :customers [{:xt/id cid}])
                  (left-join (from :orders [{:customer-id cid} currency order-value])
                             [cid currency order-value]))
           (aggregate :cid :currency
                      {:order-count (count-star)
                       :total-value (sum order-value)})
           (order-by {:val total-value :dir :desc})
           (limit 100)))


    (xt/q tu/*node*
      (str-json-query->edn
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
                   \"bind\": [ { \"customer-id\": \"cid\" }, \"currency\", \"order-value\" ]
                 },
                 \"bind\": [ \"cid\", \"currency\", \"order-value\" ]
               }
             ]
           },
           {
             \"aggregate\": [
               { \"order-count\": { \"count-star\": [] } },
               { \"total-value\": { \"sum\": [ \"order-value\" ] } }
             ]
           },
           { \"orderBy\": [ { \"val\": \"total-value\", \"dir\": \"desc\" } ] },
           { \"limit\": 100 }
         ]"))

    (xt/q tu/*node*
      "SELECT c.xt$id AS cid, o.currency, COUNT(*) AS order_count, SUM(o.order_value) AS total_value
       FROM customers c
         LEFT JOIN orders o ON (c.xt$id = o.customer_id)
       GROUP BY c.xt$id, o.currency
       ORDER BY total_value DESC
       LIMIT 100")))



(deftest pull

  (testing "For example, if a user is reading an article, we might also want to show them details about the author as well as any comments."

    (xt/q tu/*node*
      '(-> (from :articles [{:xt/id article-id} title content author-id])

           (with {:author (pull (from :authors [{:xt/id $author-id} first-name last-name])
                                {:args [author-id]})


                  :comments (pull* (-> (from :comments [{:article-id $article-id} created-at comment])
                                       (order-by {:val created-at :dir :desc})
                                       (limit 10))
                                   {:args [article-id]})})))


    ;; TODO: Waiting for `pull` to be implemented:
    #_
    (xt/q tu/*node*
      (str-json-query->edn
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
         ]"))))

      ;; NO SQL for this one



(deftest bitemporality

  (testing "In XTQL, while there are sensible defaults set for the whole query, you can override this on a per-`from` basis by wrapping the table name in a vector and providing temporal parameters:"

    (xt/q tu/*node*
      '(from :users {:for-valid-time (at #inst "2020-01-01")
                     :bind [first-name last-name]}))

    (xt/q tu/*node*
      '(from :users {:for-valid-time :all-time
                     :bind [first-name last-name]}))


    (xt/q tu/*node*
      (str-json-query->edn
        "{
           \"from\": \"users\",
           \"forValidTime\": { \"at\": { \"@value\": \"2020-01-01\", \"@type\": \"xt:date\" } },
           \"bind\": [ \"first-name\", \"last-name\" ]
         }"))

    (xt/q tu/*node*
      (str-json-query->edn
        "{
           \"from\": \"users\",
           \"forValidTime\": \"allTime\",
           \"bind\": [ \"first-name\", \"last-name\" ]
         }"))


    (xt/q tu/*node*
      "SELECT users.first_name, users.last_name FROM users FOR VALID_TIME AS OF DATE '2020-01-01'")

    (xt/q tu/*node*
      "SELECT users.first_name, users.last_name FROM users FOR ALL VALID_TIME"))



  (testing "who worked here in both 2018 and 2023"
    (xt/q tu/*node*
      '(unify (from :users {:for-valid-time (at #inst "2018")
                            :bind [{:xt/id user-id}]})
              (from :users {:for-valid-time (at #inst "2023")
                            :bind [{:xt/id user-id}]})))

    (xt/q tu/*node*
      (str-json-query->edn
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
         }"))))



(deftest DML

  (testing "Insert"

    (xt/submit-tx tu/*node*
                  [[:xtql '(insert :users (from :old-users [xt/id {:first-name given-name, :last-name surname}
                                                            xt/valid-from xt/valid-to]))]])

    (xt/submit-tx tu/*node*
                  [[:sql
                    "INSERT INTO users
         SELECT xt$id, first_name AS given_name, last_name AS surname,
                xt$valid_from, xt$valid_to
         FROM old_users"]]))



  (testing "Delete"

    (xt/submit-tx tu/*node*
                  [[:xtql '(delete :comments [{:post-id $post-id}])
                    {:post-id 1}]])

    (xt/submit-tx tu/*node*
                  [[:sql
                    "DELETE FROM comments WHERE post_id = ?"
                    1]])

    (testing "additional unify clauses"
      (xt/submit-tx tu/*node*
                    [[:xtql '(delete :comments [{:post-id pid}]
                                     (from :posts [{:xt/id pid, :author-id $author}]))
                      {:author "james"}]])

      (xt/submit-tx tu/*node*
                    [[:sql
                      "DELETE FROM comments
         WHERE post_id IN (SELECT xt$id FROM posts WHERE author_id = ?)"
                      "james"]]))

    (testing "bitemporal"
      (xt/submit-tx tu/*node*
                    [[:xtql '(delete :promotions {:bind [{:promotion-type "christmas"}]
                                                  :for-valid-time (from #inst "2023-12-26")})]])

      (xt/submit-tx tu/*node*
                    [[:sql
                      "DELETE FROM promotions
           FOR PORTION OF VALID_TIME FROM DATE '2023-12-26' TO END_OF_TIME
           WHERE promotion_type = 'christmas'"]]))

    (testing "everything"
      (xt/submit-tx tu/*node*
                    [[:xtql '(delete :comments {})]])

      (xt/submit-tx tu/*node*
                    [[:sql
                      "DELETE FROM comments"]])))



  (testing "Update"

    (xt/submit-tx tu/*node*
                  [[:xtql '(update :documents {:bind [{:xt/id $doc-id, :version v}]
                                               :set {:version (inc v)}})
                    {:doc-id "doc-id"}]])

    (xt/submit-tx tu/*node*
                  [[:sql
                    "UPDATE documents
         SET version = version + 1
         WHERE xt$id = ?"
                    "doc-id"]])

    (testing "bitemporal"

      (xt/submit-tx tu/*node*
                    [[:put :comments {:xt/id (random-uuid), :post-id "my-post-id"}]
                     [:xtql '(update :posts {:bind [{:xt/id $post-id}], :set {:comment-count cc}}

                                     (with {cc (q (-> (from :comments [{:post-id $post-id}])
                                                      (aggregate {cc (count)}))
                                                  [cc])}))
                      {:post-id "my-post-id"}]])

      (xt/submit-tx tu/*node*
                    [[:sql
                      "INSERT INTO comments (xt$id, post_id) VALUES (?, ?)"
                      (random-uuid) "my-post-id"]
                     [:sql
                      "UPDATE posts AS p
           SET comment_count = (SELECT COUNT(*) FROM comments WHERE post_id = ?)
           WHERE p.post_id = ?"
                      "my-post-id" "my-post-id"]])))



  (testing "Erase"

    (xt/submit-tx tu/*node*
                  [[:xtql '(erase :users [{:email "jms@example.com"}])]])

    (xt/submit-tx tu/*node*
                  [[:sql "ERASE FROM users WHERE email = 'jms@example.com'"]]))



  (testing "Assert"

    (xt/submit-tx tu/*node*
                  [[:xtql '(assert-not-exists (from :users [{:email $email}]))
                    {:email "james@example.com"}]

                   [:put :users {:xt/id :james, :email "james@example.com"}]])

    ;; TODO: Update when implemented
    #_
    (xt/submit-tx tu/*node*
                  [[:sql "ASSERT NOT EXISTS (SELECT 1 FROM users WHERE email = 'james@juxt.pro')"]])


    (testing ":xt/txs table"

      (xt/q tu/*node*
            '(from :xt/txs [{:xt/id $tx-id} xt/committed? xt/error])
            {:args {:tx-id 0}}))))

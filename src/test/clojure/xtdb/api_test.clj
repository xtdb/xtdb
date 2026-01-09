(ns xtdb.api-test
  (:require [clojure.test :as t :refer [deftest]]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.basis :as basis]
            [xtdb.compactor :as c]
            [xtdb.serde :as serde]
            [xtdb.test-util :as tu :refer [*node*]]
            [xtdb.time :as time]
            [xtdb.util :as util])
  (:import (java.lang AutoCloseable)
           (java.time Duration ZoneId)))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(t/deftest test-status
  (t/is (map? (xt/status *node*))))

(t/deftest test-simple-query
  (let [tx (xt/execute-tx *node* [[:put-docs :docs {:xt/id :foo, :inst #inst "2021"}]])]
    (t/is (= (serde/->TxKey 0 (time/->instant #inst "2020-01-01")) tx))

    (t/is (= [{:e :foo, :inst (time/->zdt #inst "2021")}]
             (xt/q *node* '(from :docs [{:xt/id e} inst]))))))

(t/deftest test-validation-errors
  (t/is (anomalous? [:incorrect nil]
                    (-> (xt/submit-tx *node* [[:pot :docs {:xt/id :foo}]])
                        (util/rethrowing-cause))))

  (t/is (anomalous? [:incorrect nil]
                    (-> (xt/submit-tx *node* [[:put-docs :docs {}]])
                        (util/rethrowing-cause)))))

(t/deftest round-trips-lists
  (let [tx (xt/execute-tx *node* [[:put-docs :docs {:xt/id :foo, :list [1 2 ["foo" "bar"]]}]
                                 [:sql "INSERT INTO docs (_id, list) VALUES ('bar', ARRAY[?, 2, 3 + 5])"
                                  [4]]])]
    (t/is (= (serde/->TxKey 0 (time/->instant #inst "2020-01-01")) tx))

    (t/is (= [{:id :foo, :list [1 2 ["foo" "bar"]]}
              {:id "bar", :list [4 2 8]}]
             (xt/q *node* '(from :docs [{:xt/id id} list])
                   {:tx-timeout (Duration/ofSeconds 1)})))

    (t/is (= [{:xt/id :foo, :list [1 2 ["foo" "bar"]]}
              {:xt/id "bar", :list [4 2 8]}]
             (xt/q *node*
                   "SELECT b._id, b.list FROM docs b"
                   {:tx-timeout (Duration/ofSeconds 1)})))))

(t/deftest round-trips-sets
  (let [tx (xt/execute-tx *node* [[:put-docs :docs {:xt/id :foo, :v #{1 2 #{"foo" "bar"}}}]])]
    (t/is (= (serde/->TxKey 0 (time/->instant #inst "2020-01-01")) tx))

    (t/is (= [{:id :foo, :v #{1 2 #{"foo" "bar"}}}]
             (xt/q *node* '(from :docs [{:xt/id id} v]))))

    (t/is (= [{:xt/id :foo, :v #{1 2 #{"foo" "bar"}}}]
             (xt/q *node* "SELECT b._id, b.v FROM docs b")))))

(t/deftest round-trips-structs
  (let [tx (xt/execute-tx *node* [[:put-docs :docs {:xt/id :foo, :struct {:a 1, :b {:c "bar"}}}]
                                 [:put-docs :docs {:xt/id :bar, :struct {:a true, :d 42.0}}]])]
    (t/is (= (serde/->TxKey 0 (time/->instant #inst "2020-01-01")) tx))

    (t/is (= #{{:id :foo, :struct {:a 1, :b {:c "bar"}}}
               {:id :bar, :struct {:a true, :d 42.0}}}
             (set (xt/q *node* '(from :docs [{:xt/id id} struct])))))))

(deftest round-trips-temporal
  (let [vs {:dt #xt/date "2022-08-01"
            :ts #xt/date-time "2022-08-01T14:34"
            :tstz #xt/zoned-date-time "2022-08-01T14:34+01:00"
            :tm #xt/time "13:21:14.932254"
            ;; :tmtz #xt/offset-time "11:21:14.932254-08:00" ; TODO #323
            }]

    (xt/execute-tx *node* [[:sql "INSERT INTO foo (_id, dt, ts, tstz, tm) VALUES ('foo', ?, ?, ?, ?)"
                            (mapv vs [:dt :ts :tstz :tm])]])

    (t/is (= [(assoc vs :xt/id "foo")]
             (xt/q *node* "SELECT f._id, f.dt, f.ts, f.tstz, f.tm FROM foo f"
                   {:default-tz (ZoneId/of "Europe/London")})))

    (let [lits [[:dt "DATE '2022-08-01'"]
                [:ts "TIMESTAMP '2022-08-01 14:34:00'"]
                [:tstz "TIMESTAMP '2022-08-01 14:34:00+01:00'"]
                [:tm "TIME '13:21:14.932254'"]

                #_ ; FIXME #323
                [:tmtz "TIME '11:21:14.932254-08:00'"]]]

      (xt/execute-tx *node* (vec (for [[t lit] lits]
                                   [:sql (format "INSERT INTO bar (_id, v) VALUES (?, %s)" lit)
                                    [(name t)]])))
      (t/is (= (set (for [[t _lit] lits]
                      {:xt/id (name t), :v (get vs t)}))
               (set (xt/q *node* "SELECT b._id, b.v FROM bar b"
                          {:default-tz (ZoneId/of "Europe/London")})))))))

(t/deftest can-manually-specify-system-time-47
  (let [tx1 (xt/execute-tx *node* [[:put-docs :docs {:xt/id :foo}]]
                           {:system-time #inst "2012"})

        _ (t/is (anomalous? [:incorrect nil]
                            (xt/execute-tx *node* [[:put-docs :docs {:xt/id :bar}]]
                                           {:system-time #inst "2011"})))

        tx3 (xt/execute-tx *node* [[:put-docs :docs {:xt/id :baz}]])]

    (t/is (= (serde/->TxKey 0 (time/->instant #inst "2012"))
             tx1))

    (letfn [(q-at [tx]
              (->> (xt/q *node* '(from :docs [{:xt/id id}])
                         {:snapshot-time (:system-time tx)
                          :tx-timeout (Duration/ofSeconds 1)})
                   (into #{} (map :id))))]

      (t/is (= #{:foo} (q-at tx1)))
      (t/is (= #{:foo :baz} (q-at tx3))))

    (t/is (= #{{:tx-id 0,
                :system-time #xt/zoned-date-time "2012-01-01T00:00Z[UTC]",
                :committed? true}
               {:tx-id 1,
                :system-time #xt/zoned-date-time "2020-01-02T00:00Z[UTC]",
                :committed? false,
                :error #xt/error [:incorrect :invalid-system-time
                                  "specified system-time older than current tx"
                                  {:tx-key #xt/tx-key {:tx-id 1, :system-time #xt/instant "2011-01-01T00:00:00Z"}
                                   :latest-completed-tx #xt/tx-key {:tx-id 0, :system-time #xt/instant "2012-01-01T00:00:00Z"}}]}
               {:tx-id 2,
                :system-time #xt/zoned-date-time "2020-01-03T00:00Z[UTC]",
                :committed? true}}
             (set (xt/q *node*
                        '(from :xt/txs [{:xt/id tx-id, :committed committed?} system-time error])))))))

(def ^:private devs
  [[:put-docs :users {:xt/id :jms, :name "James"}]
   [:put-docs :users {:xt/id :hak, :name "Håkan"}]
   [:put-docs :users {:xt/id :mat, :name "Matt"}]
   [:put-docs :users {:xt/id :wot, :name "Dan"}]])

(t/deftest test-sql-roundtrip
  (let [tx (xt/submit-tx *node* devs)]

    (t/is (= {:tx-id 0} tx))

    (t/is (= [{:name "James"}]
             (xt/q *node* "SELECT u.name FROM users u WHERE u.name = 'James'")))))

(t/deftest test-sql-dynamic-params-103
  (xt/submit-tx *node* devs)

  (t/is (= #{{:name "James"} {:name "Matt"}}
           (set (xt/q *node* ["SELECT u.name FROM users u WHERE u.name IN (?, ?)"
                              "James" "Matt"])))))

(t/deftest start-and-query-empty-node-re-231-test
  (t/is (= [] (xt/q tu/*node* "select a.a from a a" {}))))

(t/deftest test-duration-jdbc-roundtrip
  (t/is (= [{:d #xt/duration "PT1H30M"}]
           (jdbc/execute! tu/*node* ["SELECT ? AS d" #xt/duration "PT1H30M"]))))

(t/deftest test-basic-sql-dml
  (letfn [(all-users [{:keys [system-time]}]
            (->> (xt/q *node* "SELECT u.first_name, u.last_name, u._valid_from, u._valid_to FROM users FOR ALL VALID_TIME u"
                       {:snapshot-token (basis/->time-basis-str {"xtdb" [system-time]})})
                 (into #{} (map (juxt :first-name :last-name :xt/valid-from :xt/valid-to)))))]

    (let [tx1 (xt/execute-tx *node* [[:sql "INSERT INTO users (_id, first_name, last_name, _valid_from) VALUES (?, ?, ?, ?)"
                                      ["dave", "Dave", "Davis", #inst "2018"]
                                      ["claire", "Claire", "Cooper", #inst "2019"]
                                      ["alan", "Alan", "Andrews", #inst "2020"]
                                      ["susan", "Susan", "Smith", #inst "2021"]]])
          tx1-expected #{["Dave" "Davis", (time/->zdt #inst "2018"), nil]
                         ["Claire" "Cooper", (time/->zdt #inst "2019"), nil]
                         ["Alan" "Andrews", (time/->zdt #inst "2020"), nil]
                         ["Susan" "Smith", (time/->zdt #inst "2021") nil]}]

      (t/is (= (serde/->TxKey 0 (time/->instant #inst "2020-01-01")) tx1))

      (t/is (= tx1-expected (all-users tx1)))

      (let [tx2 (xt/execute-tx *node* [["DELETE FROM users FOR PORTION OF VALID_TIME FROM DATE '2020-05-01' TO NULL AS u WHERE u._id = ?" "dave"]]
                               {:default-tz #xt/zone "UTC"})
            tx2-expected #{["Dave" "Davis", (time/->zdt #inst "2018"), (time/->zdt #inst "2020-05-01")]
                           ["Claire" "Cooper", (time/->zdt #inst "2019"), nil]
                           ["Alan" "Andrews", (time/->zdt #inst "2020"), nil]
                           ["Susan" "Smith", (time/->zdt #inst "2021") nil]}]

        (t/is (= tx2-expected (all-users tx2)))
        (t/is (= tx1-expected (all-users tx1)))

        (let [tx3 (xt/execute-tx *node* [["UPDATE users FOR PORTION OF VALID_TIME FROM DATE '2021-07-01' TO NULL AS u SET first_name = 'Sue' WHERE u._id = ?"
                                          "susan"]]
                                 {:default-tz #xt/zone "UTC"})

              tx3-expected #{["Dave" "Davis", (time/->zdt #inst "2018"), (time/->zdt #inst "2020-05-01")]
                             ["Claire" "Cooper", (time/->zdt #inst "2019"), nil]
                             ["Alan" "Andrews", (time/->zdt #inst "2020"), nil]
                             ["Susan" "Smith", (time/->zdt #inst "2021") (time/->zdt #inst "2021-07-01")]
                             ["Sue" "Smith", (time/->zdt #inst "2021-07-01") nil]}]

          (t/is (= tx3-expected (all-users tx3)))
          (t/is (= tx2-expected (all-users tx2)))
          (t/is (= tx1-expected (all-users tx1))))))))

(deftest test-sql-insert
  (t/is (= (serde/->TxKey 0 (time/->instant #inst "2020-01-01"))
           (xt/execute-tx *node*
                          [[:sql "INSERT INTO users (_id, name, _valid_from) VALUES (?, ?, ?)"
                            ["dave", "Dave", #inst "2018"]
                            ["claire", "Claire", #inst "2019"]]])))


  (t/is (= (serde/->TxKey 1 (time/->instant #inst "2020-01-02"))
           (xt/execute-tx *node*
                          ["INSERT INTO people (_id, renamed_name)
                            SELECT users._id, users.name
                            FROM users
                            WHERE users.name = 'Claire'"])))

  (t/is (= [{:renamed-name "Claire"}]
           (xt/q *node* "SELECT renamed_name FROM people")))

  (t/is (= (serde/->TxKey 2 (time/->instant #inst "2020-01-03"))
           (xt/execute-tx *node*
                          ["INSERT INTO people (_id, renamed_name, _valid_from)
                            SELECT users._id, users.name, users._valid_from
                            FROM users FOR VALID_TIME AS OF DATE '2019-06-01'
                            WHERE users.name = 'Dave'"])))

  (t/is (= [{:renamed-name "Dave"}]
           (xt/q *node* "SELECT renamed_name FROM people FOR VALID_TIME AS OF DATE '2019-06-01'"))))

(deftest test-sql-insert-app-time-date-398
  (let [tx (xt/submit-tx *node*
                         ["INSERT INTO foo (_id, _valid_from) VALUES ('foo', DATE '2018-01-01')"])]

    (t/is (= {:tx-id 0} tx))

    (t/is (= [{:xt/id "foo", :xt/valid-from (-> (time/->zdt #inst "2018")
                                                (.withZoneSameLocal (ZoneId/systemDefault))
                                                (.withZoneSameInstant (ZoneId/of "UTC")))}]
             (xt/q *node* "SELECT foo._id, foo._valid_from, foo._valid_to FROM foo")))))

(deftest test-dql-as-of-now-flag-339
  (let [tt1 (time/->zdt #inst "2020-01-01")
        tt2 (time/->zdt #inst "2020-01-02")]
    (xt/submit-tx *node*
                  [["INSERT INTO foo (_id, version) VALUES (?, ?)"
                    "foo" 0]])

    (t/is (= [{:version 0, :xt/valid-from tt1}]
             (xt/q *node*
                   "SELECT foo.version, foo._valid_from, foo._valid_to FROM foo")))

    (t/is (= [{:version 0, :xt/valid-from tt1}]
             (xt/q *node*
                   "SELECT foo.version, foo._valid_from, foo._valid_to FROM foo")))

    (xt/submit-tx *node*
                  ["UPDATE foo SET version = 1 WHERE foo._id = 'foo'"])

    (t/is (= [{:version 1, :xt/valid-from tt2}]
             (xt/q *node*
                   "SELECT foo.version, foo._valid_from, foo._valid_to FROM foo"))
          "without flag it returns as of now")

    (t/is (= #{{:version 0, :xt/valid-from tt1, :xt/valid-to tt2}
               {:version 1, :xt/valid-from tt2}}
             (set (xt/q *node*
                        "SELECT foo.version, foo._valid_from, foo._valid_to FROM foo FOR ALL VALID_TIME"))))))

(t/deftest test-erase
  (letfn [(q [{:keys [system-time]}]
            (set (xt/q *node*
                       "SELECT foo._id, foo.version, foo._valid_from, foo._valid_to FROM foo FOR ALL VALID_TIME"
                       {:snapshot-token (basis/->time-basis-str {"xtdb" [system-time]})})))]
    (let [tx1 (xt/execute-tx *node*
                             [[:sql "INSERT INTO foo (_id, version) VALUES (?, ?)"
                               ["foo", 0]
                               ["bar", 0]]])
          tx2 (xt/execute-tx *node* ["UPDATE foo SET version = 1"])
          v0 {:version 0,
              :xt/valid-from (time/->zdt #inst "2020-01-01"),
              :xt/valid-to (time/->zdt #inst "2020-01-02")}

          v1 {:version 1,
              :xt/valid-from (time/->zdt #inst "2020-01-02")}]

      (t/is (= #{{:xt/id "foo", :version 0,
                  :xt/valid-from (time/->zdt #inst "2020-01-01")}
                 {:xt/id "bar", :version 0,
                  :xt/valid-from (time/->zdt #inst "2020-01-01")}}
               (q tx1)))

      (t/is (= #{(assoc v0 :xt/id "foo")
                 (assoc v0 :xt/id "bar")
                 (assoc v1 :xt/id "foo")
                 (assoc v1 :xt/id "bar")}
               (q tx2)))

      (let [tx3 (xt/execute-tx *node*
                               ["ERASE FROM foo WHERE foo._id = 'foo'"])]
        (t/is (= #{(assoc v0 :xt/id "bar") (assoc v1 :xt/id "bar")} (q tx3)))
        (t/is (= #{(assoc v0 :xt/id "bar") (assoc v1 :xt/id "bar")} (q tx2)))

        (t/is (= #{{:xt/id "bar", :version 0,
                    :xt/valid-from (time/->zdt #inst "2020-01-01")}}
                 (q tx1)))))))

(t/deftest execute-tx-throws-dml-errors
  (t/is (anomalous? [:incorrect nil #"Errors parsing SQL statement"]
                    (xt/execute-tx tu/*node* ["INSERT INTO foo (_id, dt) VALUES ('id', DATE \"2020-01-01\")"])))

  (t/testing "still an active node"
    (xt/submit-tx tu/*node* ["INSERT INTO users (_id, name) VALUES ('dave', 'Dave')"])

    (t/is (= [{:name "Dave"}]
             (xt/q tu/*node* "SELECT users.name FROM users")))))

(t/deftest aborts-insert-if-end-lt-start-401-425
  (letfn [(q-all []
            (->> (xt/q tu/*node* "SELECT foo._id, foo._valid_from, foo._valid_to FROM foo")
                 (into {} (map (juxt :xt/id (juxt :xt/valid-from :xt/valid-to))))))]

    (xt/submit-tx tu/*node* ["INSERT INTO foo (_id) VALUES (1)"])

    (xt/submit-tx tu/*node* ["
INSERT INTO foo (_id, _valid_from, _valid_to)
VALUES (2, DATE '2022-01-01', DATE '2021-01-01')"])

    (t/is (= {1 [(time/->zdt #inst "2020-01-01") nil]}
             (q-all)))

    (t/testing "continues indexing after abort"
      (xt/submit-tx tu/*node* ["INSERT INTO foo (_id) VALUES (3)"])

      (t/is (= {1 [(time/->zdt #inst "2020-01-01") nil]
                3 [(time/->zdt #inst "2020-01-03") nil]}
               (q-all))))))

(deftest test-insert-from-other-table-with-as-of-now
  (xt/submit-tx *node*
                ["INSERT INTO posts (_id, user_id, body, _valid_from)
                  VALUES (9012, 5678, 'Happy 2050!', DATE '2050-01-01')"])

  (t/is (= [{:body "Happy 2050!"}]
           (xt/q *node* "SELECT posts.body FROM posts FOR VALID_TIME AS OF DATE '2050-01-02'")))

  (t/is (= []
           (xt/q *node* "SELECT posts.body FROM posts")))

  (xt/submit-tx *node*
                ["INSERT INTO t1 SELECT posts._id, posts.body FROM posts"])

  (t/is (= []
           (xt/q *node* "SELECT t1.body FROM t1 FOR ALL VALID_TIME"))))

(deftest test-submit-tx-system-time-opt
  (t/is (anomalous? [:incorrect nil]
                    (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1}]]
                                  {:system-time "foo"}))))

(t/deftest test-xtql-with-param-2933
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id :petr :name "Petr"}]])

  (t/is (= [{:name "Petr"}]
           (xt/q tu/*node* ['#(from :docs [{:xt/id %} name]) :petr]))))

(t/deftest test-close-node-multiple-times
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id :petr :name "Petr"}]])
  (let [^AutoCloseable node tu/*node*]
    (.close node)
    (.close node)))

(t/deftest test-query-with-errors
  (t/is (anomalous? [:incorrect :xtql/malformed-from]
                    #_{:clj-kondo/ignore [:xtql/type-mismatch]}
                    (xt/q tu/*node* '(from docs [name]))))

  (t/is (anomalous? [:incorrect nil #"data exception - division by zero"]
                    (xt/q tu/*node* '(-> (rel [{}] [])
                                         (with {:foo (/ 1 0)})))))

  ;; Might need to get updated if this kind of error gets handled differently.
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1 :name 2}]])
  (t/is (anomalous? [:incorrect nil #"upper not applicable to types i64"]
                    (xt/q tu/*node* "SELECT UPPER(docs.name) AS name FROM docs"))))

(def ivan+petr
  [[:put-docs :docs {:xt/id :ivan, :first-name "Ivan", :last-name "Ivanov"}]
   [:put-docs :docs {:xt/id :petr, :first-name "Petr", :last-name "Petrov"}]])

(t/deftest normalisation-option
  (xt/submit-tx tu/*node* ivan+petr)

  (t/is (= [{:xt/id :petr :first-name "Petr", :last-name "Petrov"}
            {:xt/id :ivan :first-name "Ivan", :last-name "Ivanov"}]
           (xt/q tu/*node* '(from :docs [xt/id first-name last-name])
                 {:key-fn :kebab-case-keyword})))

  (t/is (= [{"_id" :petr "first-name" "Petr", "last-name" "Petrov"}
            {"_id" :ivan "first-name" "Ivan", "last-name" "Ivanov"}]
           (xt/q tu/*node* '(from :docs [xt/id first-name last-name])
                 {:key-fn :kebab-case-string})))

  (t/is (= [{:xt/id :petr, :first_name "Petr", :last_name "Petrov"}
            {:xt/id :ivan, :first_name "Ivan", :last_name "Ivanov"}]
           (xt/q tu/*node* '(from :docs [xt/id first-name last-name])
                 {:key-fn :snake-case-keyword})))

  (t/is (= [{"_id" :petr, "first_name" "Petr", "last_name" "Petrov"}
            {"_id" :ivan, "first_name" "Ivan", "last_name" "Ivanov"}]
           (xt/q tu/*node* '(from :docs [xt/id first-name last-name])
                 {:key-fn :snake-case-string})))

  (t/is (anomalous? [:incorrect nil #"Invalid key-fn"]
                    (xt/q tu/*node* '(from :docs [first-name last-name])
                          {:key-fn :foo-bar}))))

(t/deftest dynamic-xtql-queries
  (xt/submit-tx tu/*node* [[:put-docs :posts {:xt/id :uk, :text "Hello from England!", :likes 68, :author-name "James"}]
                           [:put-docs :posts {:xt/id :de, :text "Hallo aus Deutschland!", :likes 127, :author-name "Finn"}]])

  (letfn [(build-posts-query [{:keys [with-author? popular?]}]
            (xt/template (-> (from :posts [{:xt/id id} text
                                           ~@(when with-author?
                                               '[author-name])
                                           ~@(when popular?
                                               '[likes])])
                             ~@(when popular?
                                 ['(where (> likes 100))]))))]

    (t/is (= #{{:id :uk, :text "Hello from England!"}
               {:id :de, :text "Hallo aus Deutschland!"}}
             (set (xt/q tu/*node* (build-posts-query {})))))

    (t/is (= #{{:id :uk, :text "Hello from England!", :author-name "James"}
               {:id :de, :text "Hallo aus Deutschland!", :author-name "Finn"}}
             (set (xt/q tu/*node* (build-posts-query {:with-author? true})))))

    (t/is (= #{{:id :de, :text "Hallo aus Deutschland!", :likes 127}}
             (set (xt/q tu/*node* (build-posts-query {:popular? true})))))

    (t/is (= #{{:id :de, :text "Hallo aus Deutschland!", :likes 127, :author-name "Finn"}}
             (set (xt/q tu/*node* (build-posts-query {:popular? true, :with-author? true})))))))

(t/deftest test-explain-plan-2383
  (xt/submit-tx tu/*node* [[:put-docs :people {:xt/id 1 :name "Dave" :age 30}]
                           [:put-docs :people {:xt/id 2 :name "John" :age 40}]])

  (t/is (= '[{:depth "->", :op :project,
              :explain {:append? false, :project "[{name name} {age age} {_valid_from _valid_from} {_valid_to _valid_to}]"}}
             {:depth "  ->", :op :scan,
              :explain {:table "xtdb.public.people",
                        :columns ["_valid_from" "age" "name" "_valid_to" "_id"],
                        :predicates ["(== _id ?_0)"]}}]
           (xt/q tu/*node*
                 [(format "EXPLAIN XTQL ($$ %s $$, ?)"
                          (pr-str '#(from :people [{:xt/id %} name age xt/valid-from xt/valid-to])))
                  1])))

  (t/is (= '[{:depth "->", :op :project,
              :explain {:append? false, :project "[{name people.1/name} {age people.1/age} {_valid_from people.1/_valid_from} {_valid_to people.1/_valid_to}]"}}
             {:depth "  ->", :op :rename, :explain {:prefix "people.1"}}
             {:depth "    ->", :op :scan,
              :explain {:table "xtdb.public.people"
                        :columns ["_valid_from" "age" "name" "_valid_to" "_id"]
                        :predicates ["(== _id ?_0)"]}}]
           (xt/q tu/*node*
                 ["EXPLAIN SELECT name, age, _valid_from, _valid_to FROM people WHERE _id = ?"
                  "dummy-arg-because-pgjdbc-expects-one"]))))

(t/deftest test-explain-analyze
  (xt/submit-tx tu/*node* [[:put-docs :people {:xt/id 1 :name "Dave" :age 30}]
                           [:put-docs :people {:xt/id 2 :name "John" :age 40}]])

  (letfn [(elide-durations [row]
            (-> row
                (update :total-time class)
                (update :time-to-first-page class)))]

    (t/is (= [{:depth "->", :op :project,
               :page-count 1, :row-count 1
               :total-time Duration, :time-to-first-page Duration}
              {:depth "  ->", :op :scan,
               :page-count 1, :row-count 1
               :total-time Duration, :time-to-first-page Duration}]
             (->> (xt/q tu/*node*
                        [(format "EXPLAIN ANALYZE XTQL ($$ %s $$, ?)"
                                 (pr-str '#(from :people [{:xt/id %} name age xt/valid-from xt/valid-to])))
                         1])
                  (mapv elide-durations))))

    (t/is (= [{:depth "->", :op :project,
               :page-count 1, :row-count 1
               :total-time Duration, :time-to-first-page Duration}
              {:depth "  ->", :op :rename,
               :page-count 1, :row-count 1
               :total-time Duration, :time-to-first-page Duration}
              {:depth "    ->", :op :scan,
               :page-count 1, :row-count 1
               :total-time Duration, :time-to-first-page Duration}]
             (->> (xt/q tu/*node*
                        ["EXPLAIN ANALYZE SELECT name, age, _valid_from, _valid_to FROM people WHERE _id = ?" 1])
                  (mapv elide-durations))))))

(t/deftest test-transit-encoding-of-ast-objects-3019
  (t/is (anomalous? [:incorrect nil #"Not all variables in expression are in scope"]
                    (xt/q tu/*node*
                          '(-> (from :album [xt/id])
                               (return foo)))))
  (t/is (anomalous? [:incorrect nil #"Scalar subquery must only return a single column"]
                    (xt/q tu/*node*
                          '(-> (from :album [xt/id])
                               (with {:foo (q (from :artist [xt/id name]))})))))
  (t/is (anomalous? [:incorrect nil #"\* is not a valid in from when inside a unify context"]
                    (xt/q tu/*node*
                          '(unify (from :album [*])
                                  (from :album [*]))))))

(deftest test-plan-q
  (doseq [batch (->> (range 2000)
                     (map #(hash-map :xt/id % :num %))
                     (partition-all 1000))]
    (xt/execute-tx tu/*node* [(into [:put-docs :docs] batch)]))

  (t/is (= 10 (->> (xt/plan-q tu/*node* "SELECT docs.num FROM docs LIMIT 10")
                   (reduce (fn [acc _] (inc acc)) 0))))

  (t/is (= 10 (->> (xt/plan-q tu/*node* "SELECT docs.num FROM docs")
                   (reduce (fn [acc _] (let [acc (inc acc)] (if (== 10 acc) (reduced acc) acc))) 0))))

  #_ ; FIXME pgwire batching
  (t/is (= 10 (->> (xt/plan-q tu/*node* "(SELECT docs.num FROM docs)
                                         UNION ALL
                                         (SELECT 1 / 0)")
                   (reduce (fn [acc _] (let [acc (inc acc)] (if (== 10 acc) (reduced acc) acc))) 0)))))

(t/deftest first-class-tstz-ranges
  (xt/submit-tx tu/*node* [[:put-docs :users {:xt/id :dave, :name "Dave"}]])
  (xt/submit-tx tu/*node* [[:put-docs :users {:xt/id :claire, :name "Claire"}]])
  (xt/submit-tx tu/*node* [[:delete-docs :users :dave]])

  (t/is (= [{:name "Dave", :xt/valid-time #xt/tstz-range [#xt/zoned-date-time "2020-01-01T00:00Z"
                                                          #xt/zoned-date-time "2020-01-03T00:00Z"]}
            {:name "Claire", :xt/valid-time #xt/tstz-range [#xt/zoned-date-time "2020-01-02T00:00Z" nil]}]
           (xt/q tu/*node* "SELECT name, _valid_time FROM users FOR ALL VALID_TIME")))

  (xt/submit-tx tu/*node* [[:put-docs :foo
                            {:xt/id 1, :for-range (tu/->tstz-range #inst "2020-01-01" nil)}]

                           ["INSERT INTO foo (_id, for_range)
                             VALUES (2, ?)"
                            (tu/->tstz-range #inst "2020-03-01" #inst "2021-01-01")]

                           "INSERT INTO foo (_id, for_range)
                            VALUES (3, PERIOD(DATE '2021-08-01'::timestamptz, DATE '2022-01-01'::timestamptz))"]
                {:default-tz #xt/zone "UTC"})

  (let [expected #{{:xt/id 1,
                    :for-range #xt/tstz-range [#xt/zoned-date-time "2020-01-01T00:00Z" nil]}
                   {:xt/id 2,
                    :for-range #xt/tstz-range [#xt/zoned-date-time "2020-03-01T00:00Z" #xt/zoned-date-time "2021-01-01T00:00Z"]}
                   {:xt/id 3,
                    :for-range #xt/tstz-range [#xt/zoned-date-time "2021-08-01T00:00Z" #xt/zoned-date-time "2022-01-01T00:00Z"]}}]

    (t/is (= expected (set (xt/q tu/*node* "SELECT * FROM foo"))))

    (tu/finish-block! tu/*node*)
    (c/compact-all! tu/*node* #xt/duration "PT2S")

    (t/is (= expected (set (xt/q tu/*node* "SELECT * FROM foo"))))))

(t/deftest test-remote-client
  (let [client (xt/client {:port (.getServerPort tu/*node*)})]
    (xt/execute-tx client [[:put-docs :docs {:xt/id :foo, :name "Foo"}]])

    (t/is (= [{:xt/id :foo, :name "Foo"}]
             (xt/q client "SELECT * FROM docs"))
          "through the client")

    (t/is (= [{:xt/id :foo, :name "Foo"}]
             (xt/q tu/*node* "SELECT * FROM docs"))
          "original node")

    (with-open [conn (jdbc/get-connection client)]
      (t/is (= {:tx-id 1} (xt/submit-tx conn [[:put-docs :docs {:xt/id :bar, :name "Bar"}]])))

      (t/is (= [{:name "Bar", :xt/id :bar}
                {:xt/id :foo, :name "Foo"}]
               (xt/q client "SELECT * FROM docs ORDER BY _id"))

            "submit-tx through a connection awaits the previous transaction"))))

(t/deftest execute-two-transactions-4809
  (let [client (xt/client {:port (.getServerPort tu/*node*)})]
    (xt/execute-tx client [[:put-docs :docs {:xt/id :foo, :name "Foo"}]])
    (xt/execute-tx client [[:put-docs :docs {:xt/id :bar, :name "Bar"}]])))

(t/deftest processed-msg-ids-equal-to-submitted-msg-ids-4813
  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id :foo, :name "Foo"}]])
  (tu/flush-block! tu/*node*)

  (let [{:keys [latest-submitted-tx-ids latest-processed-tx-ids]} (xt/status tu/*node*)]
    (t/is (= latest-submitted-tx-ids latest-processed-tx-ids))))

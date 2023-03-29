(ns xtdb.api-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.datalog :as xt.d]
            [xtdb.sql :as xt.sql]
            [xtdb.node :as node]
            [xtdb.test-util :as tu :refer [*node*]]
            [xtdb.util :as util])
  (:import (java.time Duration ZoneId)
           java.util.concurrent.ExecutionException))

(t/use-fixtures :each
  (tu/with-each-api-implementation
    (-> {:in-memory (t/join-fixtures [tu/with-mock-clock tu/with-node]),
         :remote (t/join-fixtures [tu/with-mock-clock tu/with-http-client-node])}
        #_(select-keys [:in-memory])
        #_(select-keys [:remote]))))

(t/deftest test-status
  (t/is (map? (xt.d/status *node*)))
  (t/is (map? (xt.sql/status *node*))))

(t/deftest test-simple-query
  (let [tx (xt.d/submit-tx *node* '[[:put :xt_docs {:id :foo, :inst #inst "2021"}]])]
    (t/is (= (xt/map->TransactionInstant {:tx-id 0, :sys-time (util/->instant #inst "2020-01-01")}) tx))

    (t/is (= [{:e :foo, :inst (util/->zdt #inst "2021")}]
             (xt.d/q *node*
                     '{:find [e inst]
                       :where [(match :xt_docs {:id e})
                               [e :inst inst]]})))))

(t/deftest test-validation-errors
  (t/is (thrown? IllegalArgumentException
                 (try
                   (xt.d/submit-tx *node* [[:pot :xt_docs {:id :foo}]])
                   (catch ExecutionException e
                     (throw (.getCause e))))))

  (t/is (thrown? IllegalArgumentException
                 (try
                   (xt.d/submit-tx *node* [[:put :xt_docs {}]])
                   (catch ExecutionException e
                     (throw (.getCause e)))))))

(t/deftest round-trips-lists
  (let [tx (xt.d/submit-tx *node* '[[:put :xt_docs {:id :foo, :list [1 2 ["foo" "bar"]]}]
                                    [:sql "INSERT INTO xt_docs (id, list) VALUES ('bar', ARRAY[?, 2, 3 + 5])"
                                     [[4]]]])]
    (t/is (= (xt/map->TransactionInstant {:tx-id 0, :sys-time (util/->instant #inst "2020-01-01")}) tx))

    (t/is (= [{:id :foo, :list [1 2 ["foo" "bar"]]}
              {:id "bar", :list [4 2 8]}]
             (xt.d/q *node*
                     (-> '{:find [id list]
                           :where [(match :xt_docs [id])
                                   [id :list list]]}
                         (assoc :basis-timeout (Duration/ofSeconds 1))))))

    (t/is (= [{:id :foo, :list [1 2 ["foo" "bar"]]}
              {:id "bar", :list [4 2 8]}]
             (xt.sql/q *node*
                       "SELECT b.id, b.list FROM xt_docs b"
                       {:basis-timeout (Duration/ofSeconds 1)})))))

(t/deftest round-trips-sets
  (let [tx (xt.d/submit-tx *node* '[[:put :xt_docs {:id :foo, :v #{1 2 #{"foo" "bar"}}}]])]
    (t/is (= (xt/map->TransactionInstant {:tx-id 0, :sys-time (util/->instant #inst "2020-01-01")}) tx))

    (t/is (= [{:id :foo, :v #{1 2 #{"foo" "bar"}}}]
             (xt.d/q *node*
                     '{:find [id v]
                       :where [(match :xt_docs [id])
                               [id :v v]]})))

    (t/is (= [{:id :foo, :v #{1 2 #{"foo" "bar"}}}]
             (xt.sql/q *node* "SELECT b.id, b.v FROM xt_docs b")))))

(t/deftest round-trips-structs
  (let [tx (xt.d/submit-tx *node* '[[:put :xt_docs {:id :foo, :struct {:a 1, :b {:c "bar"}}}]
                                    [:put :xt_docs {:id :bar, :struct {:a true, :d 42.0}}]])]
    (t/is (= (xt/map->TransactionInstant {:tx-id 0, :sys-time (util/->instant #inst "2020-01-01")}) tx))

    (t/is (= #{{:id :foo, :struct {:a 1, :b {:c "bar"}}}
               {:id :bar, :struct {:a true, :d 42.0}}}
             (set (xt.d/q *node*
                          '{:find [id struct]
                            :where [(match :xt_docs [id])
                                    [id :struct struct]]}))))))

(t/deftest round-trips-temporal
  (let [vs {:dt #time/date "2022-08-01"
            :ts #time/date-time "2022-08-01T14:34"
            :tstz #time/zoned-date-time "2022-08-01T14:34+01:00"
            :tm #time/time "13:21:14.932254"
            ;; :tmtz #time/offset-time "11:21:14.932254-08:00" ; TODO #323
            }]

    (xt.sql/submit-tx *node* [[:sql "INSERT INTO foo (id, dt, ts, tstz, tm) VALUES ('foo', ?, ?, ?, ?)"
                               [(mapv vs [:dt :ts :tstz :tm])]]])

    (t/is (= [(assoc vs :id "foo")]
             (xt.sql/q *node* "SELECT f.id, f.dt, f.ts, f.tstz, f.tm FROM foo f"
                       {:basis-timeout (Duration/ofMillis 100)
                        :default-tz (ZoneId/of "Europe/London")})))

    (let [lits [[:dt "DATE '2022-08-01'"]
                [:ts "TIMESTAMP '2022-08-01 14:34:00'"]
                [:tstz "TIMESTAMP '2022-08-01 14:34:00+01:00'"]
                [:tm "TIME '13:21:14.932254'"]

                #_ ; FIXME #323
                [:tmtz "TIME '11:21:14.932254-08:00'"]]]

      (xt.sql/submit-tx *node* (vec (for [[t lit] lits]
                                      [:sql (format "INSERT INTO bar (id, v) VALUES (?, %s)" lit)
                                       [[(name t)]]])))
      (t/is (= (set (for [[t _lit] lits]
                      {:id (name t), :v (get vs t)}))
               (set (xt.sql/q *node* "SELECT b.id, b.v FROM bar b"
                              {:basis-timeout (Duration/ofMillis 100)
                               :default-tz (ZoneId/of "Europe/London")})))))))

(t/deftest can-manually-specify-sys-time-47
  (let [tx1 (xt.d/submit-tx *node* '[[:put :xt_docs {:id :foo}]]
                            {:sys-time #inst "2012"})

        _invalid-tx (xt.d/submit-tx *node* '[[:put :xt_docs {:id :bar}]]
                                    {:sys-time #inst "2011"})

        tx3 (xt.d/submit-tx *node* '[[:put :xt_docs {:id :baz}]])]

    (t/is (= (xt/map->TransactionInstant {:tx-id 0, :sys-time (util/->instant #inst "2012")})
             tx1))

    (letfn [(q-at [tx]
              (->> (xt.d/q *node*
                           (-> '{:find [id]
                                 :where [(match :xt_docs {:id e})
                                         [e :id id]]}
                               (assoc :basis {:tx tx}
                                      :basis-timeout (Duration/ofSeconds 1))))
                   (into #{} (map :id))))]

      (t/is (= #{:foo} (q-at tx1)))
      (t/is (= #{:foo :baz} (q-at tx3))))))

(def ^:private devs
  '[[:put :users {:id :jms, :name "James"}]
    [:put :users {:id :hak, :name "Håkan"}]
    [:put :users {:id :mat, :name "Matt"}]
    [:put :users {:id :wot, :name "Dan"}]])

(t/deftest test-sql-roundtrip
  (let [tx (xt.d/submit-tx *node* devs)]

    (t/is (= (xt/map->TransactionInstant {:tx-id 0, :sys-time (util/->instant #inst "2020-01-01")}) tx))

    (t/is (= [{:name "James"}]
             (xt.sql/q *node* "SELECT u.name FROM users u WHERE u.name = 'James'")))))

(t/deftest test-sql-dynamic-params-103
  (xt.d/submit-tx *node* devs)

  (t/is (= [{:name "James"} {:name "Matt"}]
           (xt.sql/q *node* "SELECT u.name FROM users u WHERE u.name IN (?, ?)"
                     {:? ["James", "Matt"]}))))

(t/deftest start-and-query-empty-node-re-231-test
  (with-open [n (node/start-node {})]
    (t/is (= [] (xt.sql/q n "select a.a from a a" {})))))

(t/deftest test-basic-sql-dml
  (letfn [(all-users [tx]
            (->> (xt.sql/q *node* "SELECT u.first_name, u.last_name, u.application_time_start, u.application_time_end FROM users u"
                           {:basis {:tx tx}})
                 (into #{} (map (juxt :first_name :last_name :application_time_start :application_time_end)))))]

    (let [tx1 (xt.sql/submit-tx *node* [[:sql "INSERT INTO users (id, first_name, last_name, application_time_start) VALUES (?, ?, ?, ?)"
                                         [["dave", "Dave", "Davis", #inst "2018"]
                                          ["claire", "Claire", "Cooper", #inst "2019"]
                                          ["alan", "Alan", "Andrews", #inst "2020"]
                                          ["susan", "Susan", "Smith", #inst "2021"]]]])
          tx1-expected #{["Dave" "Davis", (util/->zdt #inst "2018"), (util/->zdt util/end-of-time)]
                         ["Claire" "Cooper", (util/->zdt #inst "2019"), (util/->zdt util/end-of-time)]
                         ["Alan" "Andrews", (util/->zdt #inst "2020"), (util/->zdt util/end-of-time)]
                         ["Susan" "Smith", (util/->zdt #inst "2021") (util/->zdt util/end-of-time)]}]

      (t/is (= (xt/map->TransactionInstant {:tx-id 0, :sys-time (util/->instant #inst "2020-01-01")}) tx1))

      (t/is (= tx1-expected (all-users tx1)))

      (let [tx2 (xt.sql/submit-tx *node* [[:sql "DELETE FROM users FOR PORTION OF APP_TIME FROM DATE '2020-05-01' TO END_OF_TIME AS u WHERE u.id = ?"
                                           [["dave"]]]])
            tx2-expected #{["Dave" "Davis", (util/->zdt #inst "2018"), (util/->zdt #inst "2020-05-01")]
                           ["Claire" "Cooper", (util/->zdt #inst "2019"), (util/->zdt util/end-of-time)]
                           ["Alan" "Andrews", (util/->zdt #inst "2020"), (util/->zdt util/end-of-time)]
                           ["Susan" "Smith", (util/->zdt #inst "2021") (util/->zdt util/end-of-time)]}]

        (t/is (= tx2-expected (all-users tx2)))
        (t/is (= tx1-expected (all-users tx1)))

        (let [tx3 (xt.sql/submit-tx *node* [[:sql "UPDATE users FOR PORTION OF APPLICATION_TIME FROM DATE '2021-07-01' TO END_OF_TIME AS u SET first_name = 'Sue' WHERE u.id = ?"
                                             [["susan"]]]])

              tx3-expected #{["Dave" "Davis", (util/->zdt #inst "2018"), (util/->zdt #inst "2020-05-01")]
                             ["Claire" "Cooper", (util/->zdt #inst "2019"), (util/->zdt util/end-of-time)]
                             ["Alan" "Andrews", (util/->zdt #inst "2020"), (util/->zdt util/end-of-time)]
                             ["Susan" "Smith", (util/->zdt #inst "2021") (util/->zdt #inst "2021-07-01")]
                             ["Sue" "Smith", (util/->zdt #inst "2021-07-01") (util/->zdt util/end-of-time)]}]

          (t/is (= tx3-expected (all-users tx3)))
          (t/is (= tx2-expected (all-users tx2)))
          (t/is (= tx1-expected (all-users tx1))))))))

(deftest test-sql-insert
  (let [tx1 (xt.sql/submit-tx *node*
                              [[:sql "INSERT INTO users (id, name, application_time_start) VALUES (?, ?, ?)"
                                [["dave", "Dave", #inst "2018"]
                                 ["claire", "Claire", #inst "2019"]]]])]

    (t/is (= (xt/map->TransactionInstant {:tx-id 0, :sys-time (util/->instant #inst "2020-01-01")}) tx1))

    (xt.sql/submit-tx *node*
                      [[:sql "INSERT INTO people (id, renamed_name, application_time_start)
                                   SELECT users.id, users.name, users.application_time_start
                                   FROM users FOR APPLICATION_TIME AS OF DATE '2019-06-01'
                                   WHERE users.name = 'Dave'"]])

    (t/is (= [{:renamed_name "Dave"}]
             (xt.sql/q *node* "SELECT people.renamed_name FROM people FOR APPLICATION_TIME AS OF DATE '2019-06-01'")))))

(deftest test-sql-insert-app-time-date-398
  (let [tx (xt.sql/submit-tx *node*
                             [[:sql "INSERT INTO foo (id, application_time_start) VALUES ('foo', DATE '2018-01-01')"]])]

    (t/is (= (xt/map->TransactionInstant {:tx-id 0, :sys-time (util/->instant #inst "2020-01-01")}) tx))

    (t/is (= [{:id "foo", :application_time_start (util/->zdt #inst "2018"), :application_time_end (util/->zdt util/end-of-time)}]
             (xt.sql/q *node* "SELECT foo.id, foo.application_time_start, foo.application_time_end FROM foo")))))

(deftest test-dml-as-of-now-flag-339
  (let [tt1 (util/->zdt #inst "2020-01-01")
        tt2 (util/->zdt #inst "2020-01-02")
        tt5 (util/->zdt #inst "2020-01-05")
        eot (util/->zdt util/end-of-time)]
    (letfn [(q []
              (set (xt.sql/q *node*
                             "SELECT foo.version, foo.application_time_start, foo.application_time_end FROM foo")))]

      (xt.sql/submit-tx *node*
                        [[:sql "INSERT INTO foo (id, version) VALUES (?, ?)"
                          [["foo", 0]]]])

      (t/is (= #{{:version 0, :application_time_start tt1, :application_time_end eot}}
               (q)))

      (t/testing "update as-of-now"
        (xt.sql/submit-tx *node*
                          [[:sql "UPDATE foo SET version = 1 WHERE foo.id = 'foo'"]]
                          {:app-time-as-of-now? true})

        (t/is (= #{{:version 0, :application_time_start tt1, :application_time_end tt2}
                   {:version 1, :application_time_start tt2, :application_time_end eot}}
                 (q))))

      (t/testing "`FOR PORTION OF` means flag is ignored"
        (xt.sql/submit-tx *node*
                          [[:sql (str "UPDATE foo "
                                      "FOR PORTION OF APP_TIME FROM ? TO ? "
                                      "SET version = 2 WHERE foo.id = 'foo'")
                            [[tt1 tt2]]]]
                          {:app-time-as-of-now? true})
        (t/is (= #{{:version 2, :application_time_start tt1, :application_time_end tt2}
                   {:version 1, :application_time_start tt2, :application_time_end eot}}
                 (q))))

      (t/testing "UPDATE for-all-time"
        (xt.sql/submit-tx *node*
                          [[:sql "UPDATE foo SET version = 3 WHERE foo.id = 'foo'"]])

        (t/is (= #{{:version 3, :application_time_start tt1, :application_time_end tt2}
                   {:version 3, :application_time_start tt2, :application_time_end eot}}
                 (q))))

      (t/testing "DELETE as-of-now"
        (xt.sql/submit-tx *node*
                          [[:sql "DELETE FROM foo WHERE foo.id = 'foo'"]]
                          {:app-time-as-of-now? true})

        (t/is (= #{{:version 3, :application_time_start tt1, :application_time_end tt2}
                   {:version 3, :application_time_start tt2, :application_time_end tt5}}
                 (q))))

      (t/testing "UPDATE FOR ALL APPLICATION_TIME"
        (xt.sql/submit-tx *node*
                          [[:sql "UPDATE foo FOR ALL APPLICATION_TIME
                                     SET version = 4 WHERE foo.id = 'foo'"]]
                          {:app-time-as-of-now? true})

        (t/is (= #{{:version 4, :application_time_start tt1, :application_time_end tt2}
                   {:version 4, :application_time_start tt2, :application_time_end tt5}}
                 (q))))

      (t/testing "DELETE FOR ALL APPLICATION_TIME"
        (xt.sql/submit-tx *node*
                          [[:sql "DELETE FROM foo FOR ALL APPLICATION_TIME
                                     WHERE foo.id = 'foo'"]]
                          {:app-time-as-of-now? true})

        (t/is (= #{} (q)))))))

(deftest test-dql-as-of-now-flag-339
  (let [tt1 (util/->zdt #inst "2020-01-01")
        tt2 (util/->zdt #inst "2020-01-02")
        eot (util/->zdt util/end-of-time)]

    (xt.sql/submit-tx *node*
                      [[:sql "INSERT INTO foo (id, version) VALUES (?, ?)"
                        [["foo", 0]]]])

    (t/is (= [{:version 0, :application_time_start tt1, :application_time_end eot}]
             (xt.sql/q *node*
                       "SELECT foo.version, foo.application_time_start, foo.application_time_end FROM foo"
                       {:app-time-as-of-now? true})))

    (t/is (= [{:version 0, :application_time_start tt1, :application_time_end eot}]
             (xt.sql/q *node*
                       "SELECT foo.version, foo.application_time_start, foo.application_time_end FROM foo")))

    (xt.sql/submit-tx *node*
                      [[:sql "UPDATE foo SET version = 1 WHERE foo.id = 'foo'"]]
                      {:app-time-as-of-now? true})

    (t/is (= [{:version 1, :application_time_start tt2, :application_time_end eot}]
             (xt.sql/q *node*
                       "SELECT foo.version, foo.application_time_start, foo.application_time_end FROM foo"
                       {:app-time-as-of-now? true})))

    (t/is (= [{:version 0, :application_time_start tt1, :application_time_end tt2}
              {:version 1, :application_time_start tt2, :application_time_end eot}]
             (xt.sql/q *node*
                       "SELECT foo.version, foo.application_time_start, foo.application_time_end FROM foo"))
          "without flag it returns all app-time")

    (t/is (= [{:version 0, :application_time_start tt1, :application_time_end tt2}]
             (xt.sql/q *node*
                       (str "SELECT foo.version, foo.application_time_start, foo.application_time_end "
                            "FROM foo FOR APPLICATION_TIME AS OF ?")
                       {:app-time-as-of-now? true, :? [tt1]}))
          "`FOR APPLICATION_TIME AS OF` overrides flag")

    (t/is (= [{:version 0, :application_time_start tt1, :application_time_end tt2}
              {:version 1, :application_time_start tt2, :application_time_end eot}]
             (xt.sql/q *node*
                       "SELECT foo.version, foo.application_time_start, foo.application_time_end
                             FROM foo FOR ALL APPLICATION_TIME"
                       {:app-time-as-of-now? true}))
          "FOR ALL APPLICATION_TIME ignores flag and returns all app-time")))

(t/deftest test-erase
  (letfn [(q [tx]
            (set (xt.sql/q *node*
                           "SELECT foo.id, foo.version, foo.application_time_start, foo.application_time_end FROM foo"
                           {:basis {:tx tx}})))]
    (let [tx1 (xt.sql/submit-tx *node*
                                [[:sql "INSERT INTO foo (id, version) VALUES (?, ?)"
                                  [["foo", 0]
                                   ["bar", 0]]]])
          tx2 (xt.sql/submit-tx *node*
                                [[:sql "UPDATE foo SET version = 1"]]
                                {:app-time-as-of-now? true})
          v0 {:version 0,
              :application_time_start (util/->zdt #inst "2020-01-01"),
              :application_time_end (util/->zdt #inst "2020-01-02")}

          v1 {:version 1,
              :application_time_start (util/->zdt #inst "2020-01-02"),
              :application_time_end (util/->zdt util/end-of-time)}]

      (t/is (= #{{:id "foo", :version 0,
                  :application_time_start (util/->zdt #inst "2020-01-01")
                  :application_time_end (util/->zdt util/end-of-time)}
                 {:id "bar", :version 0,
                  :application_time_start (util/->zdt #inst "2020-01-01")
                  :application_time_end (util/->zdt util/end-of-time)}}
               (q tx1)))

      (t/is (= #{(assoc v0 :id "foo")
                 (assoc v0 :id "bar")
                 (assoc v1 :id "foo")
                 (assoc v1 :id "bar")}
               (q tx2)))

      (let [tx3 (xt.sql/submit-tx *node*
                                  [[:sql "ERASE FROM foo WHERE foo.id = 'foo'"]])]
        (t/is (= #{(assoc v0 :id "bar") (assoc v1 :id "bar")} (q tx3)))
        (t/is (= #{(assoc v0 :id "bar") (assoc v1 :id "bar")} (q tx2)))

        (t/is (= #{{:id "bar", :version 0,
                    :application_time_start (util/->zdt #inst "2020-01-01")
                    :application_time_end (util/->zdt util/end-of-time)}}
                 (q tx1)))))))

(defmacro with-unwrapped-execution-exception [& body]
  `(try
     ~@body
     (catch ExecutionException e#
       (throw (.getCause e#)))))

(t/deftest throws-static-tx-op-errors-on-submit-346
  (t/is (thrown-with-msg?
         xtdb.IllegalArgumentException
         #"Invalid SQL query: Parse error at line 1, column 45"
         (-> (xt.sql/submit-tx tu/*node* [[:sql "INSERT INTO foo (id, dt) VALUES ('id', DATE \"2020-01-01\")"]])
             (with-unwrapped-execution-exception)))
        "parse error - date with double quotes")

  (t/testing "semantic errors"
    (t/is (thrown-with-msg?
           xtdb.IllegalArgumentException
           #"XTDB requires fully-qualified columns"
           (-> (xt.sql/submit-tx tu/*node* [[:sql "UPDATE foo SET bar = 'bar' WHERE id = 'foo'"]])
               (with-unwrapped-execution-exception))))

    (t/is (thrown-with-msg?
           xtdb.IllegalArgumentException
           #"INSERT does not contain mandatory id column"
           (-> (xt.sql/submit-tx tu/*node* [[:sql "INSERT INTO users (foo, bar) VALUES ('foo', 'bar')"]])
               (with-unwrapped-execution-exception))))

    (t/is (thrown-with-msg?
           xtdb.IllegalArgumentException
           #"Column name duplicated"
           (-> (xt.sql/submit-tx tu/*node* [[:sql "INSERT INTO users (id, foo, foo) VALUES ('foo', 'foo', 'foo')"]])
               (with-unwrapped-execution-exception)))))

  (t/testing "still an active node"
    (xt.sql/submit-tx tu/*node* [[:sql "INSERT INTO users (id, name) VALUES ('dave', 'Dave')"]])

    (t/is (= [{:name "Dave"}]
             (xt.sql/q tu/*node* "SELECT users.name FROM users")))))

(t/deftest aborts-insert-if-end-lt-start-401-425
  (letfn [(q-all []
            (->> (xt.sql/q tu/*node* "SELECT foo.id, foo.application_time_start, foo.application_time_end FROM foo")
                 (into {} (map (juxt :id (juxt :application_time_start :application_time_end))))))]

    (xt.sql/submit-tx tu/*node* [[:sql "INSERT INTO foo (id) VALUES (1)"]])

    (xt.sql/submit-tx tu/*node* [[:sql "
INSERT INTO foo (id, application_time_start, application_time_end)
VALUES (2, DATE '2022-01-01', DATE '2021-01-01')"]])

    (t/is (= {1 [(util/->zdt #inst "2020-01-01") (util/->zdt util/end-of-time)]}
             (q-all)))

    (t/testing "continues indexing after abort"
      (xt.sql/submit-tx tu/*node* [[:sql "INSERT INTO foo (id) VALUES (3)"]])

      (t/is (= {1 [(util/->zdt #inst "2020-01-01") (util/->zdt util/end-of-time)]
                3 [(util/->zdt #inst "2020-01-03") (util/->zdt util/end-of-time)]}
               (q-all))))))

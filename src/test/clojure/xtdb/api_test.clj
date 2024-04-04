(ns xtdb.api-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu :refer [*node*]]
            [xtdb.time :as time]
            [xtdb.util :as util])
  (:import (java.lang AutoCloseable)
           (java.time Duration ZoneId)
           (xtdb.api TransactionKey)))

(t/use-fixtures :each
  (tu/with-each-api-implementation
    (-> {:in-memory (t/join-fixtures [tu/with-mock-clock tu/with-node]),
         :remote (t/join-fixtures [tu/with-mock-clock tu/with-http-client-node])}
        #_(select-keys [:in-memory])
        #_(select-keys [:remote]))))

(t/deftest test-status
  (t/is (map? (xt/status *node*))))

(t/deftest test-simple-query
  (let [tx (xt/submit-tx *node* [[:put-docs :docs {:xt/id :foo, :inst #inst "2021"}]])]
    (t/is (= (TransactionKey. 0 (time/->instant #inst "2020-01-01")) tx))

    (t/is (= [{:e :foo, :inst (time/->zdt #inst "2021")}]
             (xt/q *node* '(from :docs [{:xt/id e} inst]))))))

(t/deftest test-validation-errors
  (t/is (thrown? IllegalArgumentException
                 (-> (xt/submit-tx *node* [[:pot :docs {:xt/id :foo}]])
                     (util/rethrowing-cause))))

  (t/is (thrown? IllegalArgumentException
                 (-> (xt/submit-tx *node* [[:put-docs :docs {}]])
                     (util/rethrowing-cause)))))

(t/deftest round-trips-lists
  (let [tx (xt/submit-tx *node* [[:put-docs :docs {:xt/id :foo, :list [1 2 ["foo" "bar"]]}]
                                 [:sql "INSERT INTO docs (xt$id, list) VALUES ('bar', ARRAY[?, 2, 3 + 5])"
                                  [4]]])]
    (t/is (= (TransactionKey. 0 (time/->instant #inst "2020-01-01")) tx))

    (t/is (= [{:id :foo, :list [1 2 ["foo" "bar"]]}
              {:id "bar", :list [4 2 8]}]
             (xt/q *node* '(from :docs [{:xt/id id} list])
                   {:tx-timeout (Duration/ofSeconds 1)})))

    (t/is (= [{:xt/id :foo, :list [1 2 ["foo" "bar"]]}
              {:xt/id "bar", :list [4 2 8]}]
             (xt/q *node*
                   "SELECT b.xt$id, b.list FROM docs b"
                   {:tx-timeout (Duration/ofSeconds 1)})))))

(t/deftest round-trips-sets
  (let [tx (xt/submit-tx *node* [[:put-docs :docs {:xt/id :foo, :v #{1 2 #{"foo" "bar"}}}]])]
    (t/is (= (TransactionKey. 0 (time/->instant #inst "2020-01-01")) tx))

    (t/is (= [{:id :foo, :v #{1 2 #{"foo" "bar"}}}]
             (xt/q *node* '(from :docs [{:xt/id id} v]))))

    (t/is (= [{:xt/id :foo, :v #{1 2 #{"foo" "bar"}}}]
             (xt/q *node* "SELECT b.xt$id, b.v FROM docs b")))))

(t/deftest round-trips-structs
  (let [tx (xt/submit-tx *node* [[:put-docs :docs {:xt/id :foo, :struct {:a 1, :b {:c "bar"}}}]
                                 [:put-docs :docs {:xt/id :bar, :struct {:a true, :d 42.0}}]])]
    (t/is (= (TransactionKey. 0 (time/->instant #inst "2020-01-01")) tx))

    (t/is (= #{{:id :foo, :struct {:a 1, :b {:c "bar"}}}
               {:id :bar, :struct {:a true, :d 42.0}}}
             (set (xt/q *node* '(from :docs [{:xt/id id} struct])))))))

(deftest round-trips-temporal
  (let [vs {:dt #time/date "2022-08-01"
            :ts #time/date-time "2022-08-01T14:34"
            :tstz #time/zoned-date-time "2022-08-01T14:34+01:00"
            :tm #time/time "13:21:14.932254"
            ;; :tmtz #time/offset-time "11:21:14.932254-08:00" ; TODO #323
            }]

    (xt/submit-tx *node* [[:sql "INSERT INTO foo (xt$id, dt, ts, tstz, tm) VALUES ('foo', ?, ?, ?, ?)"
                           (mapv vs [:dt :ts :tstz :tm])]])

    (t/is (= [(assoc vs :xt/id "foo")]
             (xt/q *node* "SELECT f.xt$id, f.dt, f.ts, f.tstz, f.tm FROM foo f"
                   {:default-tz (ZoneId/of "Europe/London")})))

    (let [lits [[:dt "DATE '2022-08-01'"]
                [:ts "TIMESTAMP '2022-08-01 14:34:00'"]
                [:tstz "TIMESTAMP '2022-08-01 14:34:00+01:00'"]
                [:tm "TIME '13:21:14.932254'"]

                #_ ; FIXME #323
                [:tmtz "TIME '11:21:14.932254-08:00'"]]]

      (xt/submit-tx *node* (vec (for [[t lit] lits]
                                  [:sql (format "INSERT INTO bar (xt$id, v) VALUES (?, %s)" lit)
                                   [(name t)]])))
      (t/is (= (set (for [[t _lit] lits]
                      {:xt/id (name t), :v (get vs t)}))
               (set (xt/q *node* "SELECT b.xt$id, b.v FROM bar b"
                          {:default-tz (ZoneId/of "Europe/London")})))))))

(t/deftest can-manually-specify-system-time-47
  (let [tx1 (xt/submit-tx *node* [[:put-docs :docs {:xt/id :foo}]]
                          {:system-time #inst "2012"})

        _invalid-tx (xt/submit-tx *node* [[:put-docs :docs {:xt/id :bar}]]
                                  {:system-time #inst "2011"})

        tx3 (xt/submit-tx *node* [[:put-docs :docs {:xt/id :baz}]])]

    (t/is (= (TransactionKey. 0 (time/->instant #inst "2012"))
             tx1))

    (letfn [(q-at [tx]
              (->> (xt/q *node*
                         '(from :docs [{:xt/id id}])
                         {:basis {:at-tx tx}
                          :tx-timeout (Duration/ofSeconds 1)})
                   (into #{} (map :id))))]

      (t/is (= #{:foo} (q-at tx1)))
      (t/is (= #{:foo :baz} (q-at tx3))))

    (t/is (= #{{:tx-id 0,
                :tx-time #time/zoned-date-time "2012-01-01T00:00Z[UTC]",
                :committed? true,
                :err [nil nil]}
               {:tx-id 1,
                :tx-time #time/zoned-date-time "2011-01-01T00:00Z[UTC]",
                :committed? false,
                :err ["specified system-time older than current tx"
                      {::err/error-key :invalid-system-time
                       :tx-key #xt/tx-key {:tx-id 1, :system-time #time/instant "2011-01-01T00:00:00Z"},
                       :latest-completed-tx #xt/tx-key {:tx-id 0, :system-time #time/instant "2012-01-01T00:00:00Z"}}]}
               {:tx-id 2,
                :tx-time #time/zoned-date-time "2020-01-03T00:00Z[UTC]",
                :committed? true,
                :err [nil nil]}}
             (->> (xt/q *node*
                        '(from :xt/txs [{:xt/id tx-id, :xt/tx-time tx-time, :xt/committed? committed?, :xt/error err}]))
                  (into #{} (map #(update % :err (juxt ex-message ex-data)))))))))

(def ^:private devs
  [[:put-docs :users {:xt/id :jms, :name "James"}]
   [:put-docs :users {:xt/id :hak, :name "Håkan"}]
   [:put-docs :users {:xt/id :mat, :name "Matt"}]
   [:put-docs :users {:xt/id :wot, :name "Dan"}]])

(t/deftest test-sql-roundtrip
  (let [tx (xt/submit-tx *node* devs)]

    (t/is (= (TransactionKey. 0 (time/->instant #inst "2020-01-01")) tx))

    (t/is (= [{:name "James"}]
             (xt/q *node* "SELECT u.name FROM users u WHERE u.name = 'James'")))))

(t/deftest test-sql-dynamic-params-103
  (xt/submit-tx *node* devs)

  (t/is (= #{{:name "James"} {:name "Matt"}}
           (set (xt/q *node* "SELECT u.name FROM users u WHERE u.name IN (?, ?)"
                      {:args ["James" "Matt"]})))))

(t/deftest start-and-query-empty-node-re-231-test
  (with-open [n (xtn/start-node {})]
    (t/is (= [] (xt/q n "select a.a from a a" {})))))

(t/deftest test-basic-sql-dml
  (letfn [(all-users [tx]
            (->> (xt/q *node* "SELECT u.first_name, u.last_name, u.xt$valid_from, u.xt$valid_to FROM users u"
                       {:basis {:at-tx tx}
                        :default-all-valid-time? true})
                 (into #{} (map (juxt :first-name :last-name :xt/valid-from :xt/valid-to)))))]

    (let [tx1 (xt/submit-tx *node* [[:sql "INSERT INTO users (xt$id, first_name, last_name, xt$valid_from) VALUES (?, ?, ?, ?)"
                                     ["dave", "Dave", "Davis", #inst "2018"]
                                     ["claire", "Claire", "Cooper", #inst "2019"]
                                     ["alan", "Alan", "Andrews", #inst "2020"]
                                     ["susan", "Susan", "Smith", #inst "2021"]]])
          tx1-expected #{["Dave" "Davis", (time/->zdt #inst "2018"), nil]
                         ["Claire" "Cooper", (time/->zdt #inst "2019"), nil]
                         ["Alan" "Andrews", (time/->zdt #inst "2020"), nil]
                         ["Susan" "Smith", (time/->zdt #inst "2021") nil]}]

      (t/is (= (TransactionKey. 0 (time/->instant #inst "2020-01-01")) tx1))

      (t/is (= tx1-expected (all-users tx1)))

      (let [tx2 (xt/submit-tx *node* [[:sql "DELETE FROM users FOR PORTION OF VALID_TIME FROM DATE '2020-05-01' TO NULL AS u WHERE u.xt$id = ?"
                                       ["dave"]]])
            tx2-expected #{["Dave" "Davis", (time/->zdt #inst "2018"), (time/->zdt #inst "2020-05-01")]
                           ["Claire" "Cooper", (time/->zdt #inst "2019"), nil]
                           ["Alan" "Andrews", (time/->zdt #inst "2020"), nil]
                           ["Susan" "Smith", (time/->zdt #inst "2021") nil]}]

        (t/is (= tx2-expected (all-users tx2)))
        (t/is (= tx1-expected (all-users tx1)))

        (let [tx3 (xt/submit-tx *node* [[:sql "UPDATE users FOR PORTION OF VALID_TIME FROM DATE '2021-07-01' TO NULL AS u SET first_name = 'Sue' WHERE u.xt$id = ?"
                                         ["susan"]]])

              tx3-expected #{["Dave" "Davis", (time/->zdt #inst "2018"), (time/->zdt #inst "2020-05-01")]
                             ["Claire" "Cooper", (time/->zdt #inst "2019"), nil]
                             ["Alan" "Andrews", (time/->zdt #inst "2020"), nil]
                             ["Susan" "Smith", (time/->zdt #inst "2021") (time/->zdt #inst "2021-07-01")]
                             ["Sue" "Smith", (time/->zdt #inst "2021-07-01") nil]}]

          (t/is (= tx3-expected (all-users tx3)))
          (t/is (= tx2-expected (all-users tx2)))
          (t/is (= tx1-expected (all-users tx1))))))))

(deftest test-sql-insert
  (let [tx1 (xt/submit-tx *node*
                          [[:sql "INSERT INTO users (xt$id, name, xt$valid_from) VALUES (?, ?, ?)"
                            ["dave", "Dave", #inst "2018"]
                            ["claire", "Claire", #inst "2019"]]])]

    (t/is (= (TransactionKey. 0 (time/->instant #inst "2020-01-01")) tx1))

    (xt/submit-tx *node*
                  [[:sql "INSERT INTO people (xt$id, renamed_name, xt$valid_from)
                            SELECT users.xt$id, users.name, users.xt$valid_from
                            FROM users FOR VALID_TIME AS OF DATE '2019-06-01'
                            WHERE users.name = 'Dave'"]])

    (t/is (= [{:renamed-name "Dave"}]
             (xt/q *node* "SELECT people.renamed_name FROM people FOR VALID_TIME AS OF DATE '2019-06-01'")))))

(deftest test-sql-insert-app-time-date-398
  (let [tx (xt/submit-tx *node*
                         [[:sql "INSERT INTO foo (xt$id, xt$valid_from) VALUES ('foo', DATE '2018-01-01')"]])]

    (t/is (= (TransactionKey. 0 (time/->instant #inst "2020-01-01")) tx))

    (t/is (= [{:xt/id "foo", :xt/valid-from (time/->zdt #inst "2018")}]
             (xt/q *node* "SELECT foo.xt$id, foo.xt$valid_from, foo.xt$valid_to FROM foo")))))

(deftest test-dml-default-all-valid-time-flag-339
  (let [tt1 (time/->zdt #inst "2020-01-01")
        tt2 (time/->zdt #inst "2020-01-02")
        tt5 (time/->zdt #inst "2020-01-05")]
    (letfn [(q []
              (set (xt/q *node*
                         "SELECT foo.version, foo.xt$valid_from, foo.xt$valid_to FROM foo"
                         {:default-all-valid-time? true})))]
      (xt/submit-tx *node*
                    [[:sql "INSERT INTO foo (xt$id, version) VALUES (?, ?)"
                      ["foo", 0]]])

      (t/is (= #{{:version 0, :xt/valid-from tt1}}
               (q)))

      (t/testing "update as-of-now"
        (xt/submit-tx *node*
                      [[:sql "UPDATE foo SET version = 1 WHERE foo.xt$id = 'foo'"]]
                      {:default-all-valid-time? false})

        (t/is (= #{{:version 0, :xt/valid-from tt1, :xt/valid-to tt2}
                   {:version 1, :xt/valid-from tt2}}
                 (q))))

      (t/testing "`FOR PORTION OF` means flag is ignored"
        (xt/submit-tx *node*
                      [[:sql (str "UPDATE foo "
                                        "FOR PORTION OF VALID_TIME FROM ? TO ? "
                                        "SET version = 2 WHERE foo.xt$id = 'foo'")
                        [tt1 tt2]]]
                      {:default-all-valid-time? false})
        (t/is (= #{{:version 2, :xt/valid-from tt1, :xt/valid-to tt2}
                   {:version 1, :xt/valid-from tt2}}
                 (q))))

      (t/testing "UPDATE for-all-time"
        (xt/submit-tx *node*
                      [[:sql "UPDATE foo SET version = 3 WHERE foo.xt$id = 'foo'"]]
                      {:default-all-valid-time? true})

        (t/is (= #{{:version 3, :xt/valid-from tt1, :xt/valid-to tt2}
                   {:version 3, :xt/valid-from tt2}}
                 (q))))

      (t/testing "DELETE as-of-now"
        (xt/submit-tx *node*
                      [[:sql "DELETE FROM foo WHERE foo.xt$id = 'foo'"]]
                      {:default-all-valid-time? false})

        (t/is (= #{{:version 3, :xt/valid-from tt1, :xt/valid-to tt2}
                   {:version 3, :xt/valid-from tt2, :xt/valid-to tt5}}
                 (q))))

      (t/testing "UPDATE FOR ALL VALID_TIME"
        (xt/submit-tx *node*
                      [[:sql "UPDATE foo FOR ALL VALID_TIME
                                    SET version = 4 WHERE foo.xt$id = 'foo'"]]
                      {:default-all-valid-time? false})

        (t/is (= #{{:version 4, :xt/valid-from tt1, :xt/valid-to tt2}
                   {:version 4, :xt/valid-from tt2, :xt/valid-to tt5}}
                 (q))))

      (t/testing "DELETE FOR ALL VALID_TIME"
        (xt/submit-tx *node*
                      [[:sql "DELETE FROM foo FOR ALL VALID_TIME
                                    WHERE foo.xt$id = 'foo'"]]
                      {:default-all-valid-time? false})

        (t/is (= #{} (q)))))))

(deftest test-dql-as-of-now-flag-339
  (let [tt1 (time/->zdt #inst "2020-01-01")
        tt2 (time/->zdt #inst "2020-01-02")]
    (xt/submit-tx *node*
                  [[:sql "INSERT INTO foo (xt$id, version) VALUES (?, ?)"
                    ["foo", 0]]])

    (t/is (= [{:version 0, :xt/valid-from tt1}]
             (xt/q *node*
                   "SELECT foo.version, foo.xt$valid_from, foo.xt$valid_to FROM foo"
                   {:default-all-valid-time? false})))

    (t/is (= [{:version 0, :xt/valid-from tt1}]
             (xt/q *node*
                   "SELECT foo.version, foo.xt$valid_from, foo.xt$valid_to FROM foo")))

    (xt/submit-tx *node*
                  [[:sql "UPDATE foo SET version = 1 WHERE foo.xt$id = 'foo'"]])

    (t/is (= [{:version 1, :xt/valid-from tt2}]
             (xt/q *node*
                   "SELECT foo.version, foo.xt$valid_from, foo.xt$valid_to FROM foo"))
          "without flag it returns as of now")

    (t/is (= #{{:version 0, :xt/valid-from tt1, :xt/valid-to tt2}
               {:version 1, :xt/valid-from tt2}}
             (set (xt/q *node*
                        "SELECT foo.version, foo.xt$valid_from, foo.xt$valid_to FROM foo"
                        {:default-all-valid-time? true}))))

    (t/is (= [{:version 0, :xt/valid-from tt1, :xt/valid-to tt2}]
             (xt/q *node*
                   (str "SELECT foo.version, foo.xt$valid_from, foo.xt$valid_to "
                        "FROM foo FOR VALID_TIME AS OF ?")
                   {:args [tt1]
                    :default-all-valid-time? true}))
          "`FOR VALID_TIME AS OF` overrides flag")

    (t/is (= #{{:version 0, :xt/valid-from tt1, :xt/valid-to tt2}
               {:version 1, :xt/valid-from tt2}}
             (set (xt/q *node*
                        "SELECT foo.version, foo.xt$valid_from, foo.xt$valid_to
                             FROM foo FOR ALL VALID_TIME"
                        {:default-all-valid-time? false})))
          "FOR ALL VALID_TIME ignores flag and returns all app-time")))

(t/deftest test-erase
  (letfn [(q [tx]
            (set (xt/q *node*
                       "SELECT foo.xt$id, foo.version, foo.xt$valid_from, foo.xt$valid_to FROM foo"
                       {:basis {:at-tx tx}
                        :default-all-valid-time? true})))]
    (let [tx1 (xt/submit-tx *node*
                            [[:sql "INSERT INTO foo (xt$id, version) VALUES (?, ?)"
                              ["foo", 0]
                              ["bar", 0]]])
          tx2 (xt/submit-tx *node* [[:sql "UPDATE foo SET version = 1"]])
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

      (let [tx3 (xt/submit-tx *node*
                              [[:sql "ERASE FROM foo WHERE foo.xt$id = 'foo'"]])]
        (t/is (= #{(assoc v0 :xt/id "bar") (assoc v1 :xt/id "bar")} (q tx3)))
        (t/is (= #{(assoc v0 :xt/id "bar") (assoc v1 :xt/id "bar")} (q tx2)))

        (t/is (= #{{:xt/id "bar", :version 0,
                    :xt/valid-from (time/->zdt #inst "2020-01-01")}}
                 (q tx1)))))))

(t/deftest throws-static-tx-op-errors-on-submit-346
  (t/is (thrown-with-msg?
         xtdb.IllegalArgumentException
         #"Invalid SQL query: Parse error at line 1"
         (-> (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (xt$id, dt) VALUES ('id', DATE \"2020-01-01\")"]])
             (util/rethrowing-cause)))
        "parse error - date with double quotes")

  (t/testing "still an active node"
    (xt/submit-tx tu/*node* [[:sql "INSERT INTO users (xt$id, name) VALUES ('dave', 'Dave')"]])

    (t/is (= [{:name "Dave"}]
             (xt/q tu/*node* "SELECT users.name FROM users")))))

(t/deftest aborts-insert-if-end-lt-start-401-425
  (letfn [(q-all []
            (->> (xt/q tu/*node* "SELECT foo.xt$id, foo.xt$valid_from, foo.xt$valid_to FROM foo")
                 (into {} (map (juxt :xt/id (juxt :xt/valid-from :xt/valid-to))))))]

    (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (xt$id) VALUES (1)"]])

    (xt/submit-tx tu/*node* [[:sql "
INSERT INTO foo (xt$id, xt$valid_from, xt$valid_to)
VALUES (2, DATE '2022-01-01', DATE '2021-01-01')"]])

    (t/is (= {1 [(time/->zdt #inst "2020-01-01") nil]}
             (q-all)))

    (t/testing "continues indexing after abort"
      (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (xt$id) VALUES (3)"]])

      (t/is (= {1 [(time/->zdt #inst "2020-01-01") nil]
                3 [(time/->zdt #inst "2020-01-03") nil]}
               (q-all))))))

(deftest test-insert-from-other-table-with-as-of-now
  (xt/submit-tx *node*
                [[:sql
                   "INSERT INTO posts (xt$id, user_id, text, xt$valid_from)
                    VALUES (9012, 5678, 'Happy 2050!', DATE '2050-01-01')"]])

  (t/is (= [{:text "Happy 2050!"}]
           (xt/q *node* "SELECT posts.text FROM posts FOR VALID_TIME AS OF DATE '2050-01-02'")))

  (t/is (= []
           (xt/q *node* "SELECT posts.text FROM posts")))

  (xt/submit-tx *node*
                [[:sql "INSERT INTO t1 SELECT posts.xt$id, posts.text FROM posts"]])

  (t/is (= []
           (xt/q *node* "SELECT t1.text FROM t1 FOR ALL VALID_TIME"))))

(deftest test-submit-tx-system-time-opt
  (t/is (thrown-with-msg? IllegalArgumentException
                          #"expected date-time"
                          (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1}]]
                                        {:system-time "foo"}))))

(t/deftest test-basic-xtql-dml
  (letfn [(all-users [tx]
            (->> (xt/q *node* '(from :users [first-name last-name xt/valid-from xt/valid-to])
                       {:basis {:at-tx tx}
                        ;; TODO when `from` supports for-valid-time we can shift this to the query
                        :default-all-valid-time? true})
                 (into #{} (map (juxt :first-name :last-name :xt/valid-from :xt/valid-to)))))]

    (let [tx1 (xt/submit-tx *node* [[:put-docs {:into :users, :valid-from #inst "2018"}
                                     {:xt/id "dave", :first-name "Dave", :last-name "Davis"}]
                                    [:put-docs {:into :users, :valid-from #inst "2019"}
                                     {:xt/id "claire", :first-name "Claire", :last-name "Cooper"}]
                                    [:put-docs {:into :users, :valid-from #inst "2020"}
                                     {:xt/id "alan", :first-name "Alan", :last-name "Andrews"}]
                                    [:put-docs {:into :users, :valid-from #inst "2021"}
                                     {:xt/id "susan", :first-name "Susan", :last-name "Smith"}]])
          tx1-expected #{["Dave" "Davis", (time/->zdt #inst "2018"), nil]
                         ["Claire" "Cooper", (time/->zdt #inst "2019"), nil]
                         ["Alan" "Andrews", (time/->zdt #inst "2020"), nil]
                         ["Susan" "Smith", (time/->zdt #inst "2021") nil]}]

      (t/is (= (TransactionKey. 0 (time/->instant #inst "2020-01-01")) tx1))

      (t/is (= tx1-expected (all-users tx1)))

      (t/testing "insert by query"
        (xt/submit-tx *node* [[:insert-into :users2 '(from :users {:bind [xt/id {:first-name given-name, :last-name surname} xt/valid-from xt/valid-to]
                                                                   :for-valid-time :all-time})]])

        (t/is (= #{["Dave" "Davis", (time/->zdt #inst "2018"), nil]
                   ["Claire" "Cooper", (time/->zdt #inst "2019"), nil]
                   ["Alan" "Andrews", (time/->zdt #inst "2020"), nil]
                   ["Susan" "Smith", (time/->zdt #inst "2021") nil]}
                 (->> (xt/q *node* '(from :users2 {:bind [given-name surname xt/valid-from xt/valid-to]
                                                   :for-valid-time :all-time}))
                      (into #{} (map (juxt :given-name :surname :xt/valid-from :xt/valid-to)))))))

      (let [tx2 (xt/submit-tx *node* [[:delete '{:from :users
                                                 :for-valid-time (from #inst "2020-05-01")
                                                 :bind [{:xt/id $uid}]}
                                       {:uid "dave"}]])
            tx2-expected #{["Dave" "Davis", (time/->zdt #inst "2018"), (time/->zdt #inst "2020-05-01")]
                           ["Claire" "Cooper", (time/->zdt #inst "2019"), nil]
                           ["Alan" "Andrews", (time/->zdt #inst "2020"), nil]
                           ["Susan" "Smith", (time/->zdt #inst "2021") nil]}]

        (t/is (= tx2-expected (all-users tx2)))
        (t/is (= tx1-expected (all-users tx1)))

        (let [tx3 (xt/submit-tx *node* [[:update '{:table :users
                                                   :for-valid-time (from #inst "2021-07-01")
                                                   :bind [{:xt/id $uid}]
                                                   :set {:first-name "Sue"}}

                                         {:uid "susan"}]]

                                {:default-all-valid-time? true})

              tx3-expected #{["Dave" "Davis", (time/->zdt #inst "2018"), (time/->zdt #inst "2020-05-01")]
                             ["Claire" "Cooper", (time/->zdt #inst "2019"), nil]
                             ["Alan" "Andrews", (time/->zdt #inst "2020"), nil]
                             ["Susan" "Smith", (time/->zdt #inst "2021") (time/->zdt #inst "2021-07-01")]
                             ["Sue" "Smith", (time/->zdt #inst "2021-07-01") nil]}]

          (t/is (= tx3-expected (all-users tx3)))
          (t/is (= tx2-expected (all-users tx2)))
          (t/is (= tx1-expected (all-users tx1))))))))

(t/deftest test-erase-xtql
  (letfn [(q [tx]
            (set (xt/q *node* '(from :foo {:bind [xt/id version xt/valid-from xt/valid-to]
                                           :for-valid-time :all-time})
                       {:basis {:at-tx tx}})))]
    (let [tx1 (xt/submit-tx *node*
                            [[:put-docs :foo {:xt/id "foo", :version 0}]
                             [:put-docs :foo {:xt/id "bar", :version 0}]])
          tx2 (xt/submit-tx *node* [[:update {:table :foo, :set {:version 1}}]])
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

      (let [tx3 (xt/submit-tx *node* [[:erase {:from :foo, :bind [{:xt/id "foo"}]}]])]
        (t/is (= #{(assoc v0 :xt/id "bar") (assoc v1 :xt/id "bar")} (q tx3)))
        (t/is (= #{(assoc v0 :xt/id "bar") (assoc v1 :xt/id "bar")} (q tx2)))

        (t/is (= #{{:xt/id "bar", :version 0,
                    :xt/valid-from (time/->zdt #inst "2020-01-01")}}
                 (q tx1)))))))

(t/deftest test-assert-dml
  (t/testing "assert-not-exists"
    (xt/submit-tx tu/*node* [[:assert-not-exists '(from :users [{:first-name $name}])
                              {:name "James"}]
                             [:put-docs :users {:xt/id :james, :first-name "James"}]])

    (xt/submit-tx tu/*node* [[:assert-not-exists '(from :users [{:first-name $name}])
                              {:name "Dave"}]
                             [:put-docs :users {:xt/id :dave, :first-name "Dave"}] ])

    (xt/submit-tx tu/*node* [[:assert-not-exists '(from :users [{:first-name $name}])
                              {:name "James"}]
                             [:put-docs :users {:xt/id :james2, :first-name "James"}] ])

    (t/is (= #{{:xt/id :james, :first-name "James"}
               {:xt/id :dave, :first-name "Dave"}}
             (set (xt/q tu/*node* '(from :users [xt/id first-name])))))

    (t/is (= [{:xt/id 2,
               :xt/error ["Precondition failed: assert-not-exists"
                          {::err/error-key :xtdb/assert-failed,
                           :row-count 1}]}]

             (->> (xt/q tu/*node* '(from :xt/txs [{:xt/committed? false} xt/id xt/error]))
                  (map (fn [row]
                         (update row :xt/error (juxt ex-message ex-data))))))))

  (t/testing "assert-exists"
    (xt/submit-tx tu/*node* [[:assert-exists '(from :users [{:first-name $name}])
                              {:name "Mike"}]
                             [:put-docs :users {:xt/id :mike, :first-name "Mike"}] ])

    (xt/submit-tx tu/*node* [[:assert-exists '(from :users [{:first-name $name}])
                              {:name "James"}]
                             [:delete-docs :users :james]
                             [:put-docs :users {:xt/id :james2, :first-name "James"}]])

    (t/is (= #{{:xt/id :james2, :first-name "James"}
               {:xt/id :dave, :first-name "Dave"}}
             (set (xt/q tu/*node* '(from :users [xt/id first-name])))))

    (t/is (= [{:xt/id 3,
               :xt/error ["Precondition failed: assert-exists"
                          {::err/error-key :xtdb/assert-failed,
                           :row-count 0}]}]

             (->> (xt/q tu/*node* '(-> (from :xt/txs [{:xt/committed? false} xt/id xt/error])
                                       (where (> xt/id 2))))
                  (map (fn [row]
                         (update row :xt/error (juxt ex-message ex-data)))))))))

(t/deftest test-xtql-with-param-2933
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id :petr :name "Petr"}]])

  (t/is (= [{:name "Petr"}]
           (xt/q tu/*node* '(from :docs [{:xt/id $petr} name]) {:args {:petr :petr}}))))

(t/deftest test-close-node-multiple-times
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id :petr :name "Petr"}]])
  (let [^AutoCloseable node tu/*node*]
    (.close node)
    (.close node)))

(t/deftest test-query-with-errors
  (t/is (thrown-with-msg? xtdb.IllegalArgumentException
                          #"Illegal argument: 'xtql/malformed-table'"
                          #_{:clj-kondo/ignore [:xtql/type-mismatch]}
                          (xt/q tu/*node* '(from docs [name]))))

  (t/is (thrown-with-msg? xtdb.RuntimeException
                          #"data exception — division by zero"
                          (xt/q tu/*node* '(-> (rel [{}] [])
                                               (with {:foo (/ 1 0)})))))

  ;; Might need to get updated if this kind of error gets handled differently.
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1 :name 2}]])
  (t/is (thrown-with-msg? xtdb.IllegalArgumentException
                          #"upper not applicable to types i64"
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

  (t/is (= [{"xt$id" :petr "first-name" "Petr", "last-name" "Petrov"}
            {"xt$id" :ivan "first-name" "Ivan", "last-name" "Ivanov"}]
           (xt/q tu/*node* '(from :docs [xt/id first-name last-name])
                 {:key-fn :kebab-case-string})))

  (t/is (= [{:xt/id :petr, :first_name "Petr", :last_name "Petrov"}
            {:xt/id :ivan, :first_name "Ivan", :last_name "Ivanov"}]
           (xt/q tu/*node* '(from :docs [xt/id first-name last-name])
                 {:key-fn :snake-case-keyword})))

  (t/is (= [{"xt$id" :petr, "first_name" "Petr", "last_name" "Petrov"}
            {"xt$id" :ivan, "first_name" "Ivan", "last_name" "Ivanov"}]
           (xt/q tu/*node* '(from :docs [xt/id first-name last-name])
                 {:key-fn :snake-case-string})))

  (t/is (= [{:xt/id :petr, :firstName "Petr", :lastName "Petrov"}
            {:xt/id :ivan, :firstName "Ivan", :lastName "Ivanov"}]
           (xt/q tu/*node* '(from :docs [xt/id first-name last-name])
                 {:key-fn :camel-case-keyword})))

  (t/is (= [{"xt$id" :petr, "firstName" "Petr", "lastName" "Petrov"}
            {"xt$id" :ivan, "firstName" "Ivan", "lastName" "Ivanov"}]
           (xt/q tu/*node* '(from :docs [xt/id first-name last-name])
                 {:key-fn :camel-case-string})))

  (t/is (thrown-with-msg? IllegalArgumentException
                          #"Illegal argument: "
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

(t/deftest test-xtql-dml-from-star

  (xt/submit-tx tu/*node* [[:put-docs :users {:xt/id :james, :first-name "James"}]
                           [:put-docs :users {:xt/id :dave, :first-name "Dave"}]
                           [:put-docs :users {:xt/id :rob, :first-name "Rob" :last-name "ert"}]])

  (xt/submit-tx tu/*node* [[:insert-into :users2 '(from :users [*])]])

  (t/is (= #{{:first-name "Dave", :xt/id :dave}
             {:last-name "ert", :first-name "Rob", :xt/id :rob}
             {:first-name "James", :xt/id :james}}
           (set (xt/q tu/*node* '(from :users2 [*]))))))

(t/deftest test-explain-plan-2383
  (t/is (= '[{:plan
              "[:project
 [name age]
 [:scan
  {:table people, :for-valid-time nil, :for-system-time nil}
  [{xt$id (= xt$id ?pid)} name age]]]
"}]
           (xt/q tu/*node*
                 '(from :people [{:xt/id $pid} name age])
                 {:explain? true}))))

(t/deftest test-transit-encoding-of-ast-objects-3019
  (t/is (thrown-with-msg? IllegalArgumentException
                          #"Not all variables in expression are in scope"
                          (xt/q tu/*node*
                                '(-> (from :album [xt/id])
                                     (return foo)))))
  (t/is (thrown-with-msg? IllegalArgumentException
                          #"Scalar subquery must only return a single column"
                          (xt/q tu/*node*
                                '(-> (from :album [xt/id])
                                     (with {:foo (q (from :artist [xt/id name]))})))))
  (t/is (thrown-with-msg? IllegalArgumentException
                          #"\* is not a valid in from when inside a unify context"
                          (xt/q tu/*node*
                                '(unify (from :album [*])
                                        (from :album [*]))))))

(t/deftest test-xtql-dml-without-xt-id-3183
  (t/testing "insert-into without xt/id"
    (xt/submit-tx tu/*node* [[:put-docs :start {:xt/id 1 :foo "bar"}]])
    (xt/submit-tx tu/*node* [[:insert-into :failing '(from :start [foo])]])

    (t/is (= [{:xt/committed? false
               :xt/error #xt/runtime-err [:xtdb.indexer/missing-xt-id-column "Runtime error: 'xtdb.indexer/missing-xt-id-column'"
                                          {:column-names ["foo"]}]}]
             (xt/q tu/*node* '(from :xt/txs [{:xt/id 1} xt/committed? xt/error])))))

  (t/testing "insert-into with failing query"
    (xt/submit-tx tu/*node* [[:insert-into :failing '(-> (rel [{}] [])
                                                         (with {:xt/id (+ 1 "2")}))]])

    (t/is (= [{:xt/committed? false,
               :xt/error #xt/illegal-arg [:xtdb.expression/function-type-mismatch "+ not applicable to types i64 and utf8" {}]}]
             (xt/q tu/*node* '(from :xt/txs [{:xt/id 2} xt/committed? xt/error]))))))

(deftest test-xt-id-not-allowed-in-update-3188
  (t/testing "update with xt/id in set"
    (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1}]])
    (t/is (thrown-with-msg? IllegalArgumentException
                            #"Invalid set column for update"
                            (xt/submit-tx tu/*node* [[:update {:table :docs
                                                               :binding [{:xt/id 1}]
                                                               :set {:xt/id 2}}]])))

    (t/is (thrown-with-msg? IllegalArgumentException
                            #"Invalid set column for update"
                            (xt/submit-tx tu/*node* '[[:update {:table :docs
                                                                :binding [{:xt/id 1}]
                                                                :set {:xt/valid-from #inst "2000"
                                                                      :xt/valid-to #inst "2001"}}]])))))

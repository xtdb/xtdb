(ns core2.node-test
  (:require [clojure.test :as t]
            [core2.api :as c2]
            [core2.test-util :as tu]
            [core2.util :as util]))

(defn- with-mock-clocks [f]
  (tu/with-opts {:core2.log/memory-log {:clock (tu/->mock-clock)}
                 :core2.tx-producer/tx-producer {:clock (tu/->mock-clock)}}
    f))

(t/use-fixtures :each with-mock-clocks tu/with-node)

(t/deftest test-delete-without-search-315
  (let [!tx1 (c2/submit-tx tu/*node* [[:sql "INSERT INTO foo (id) VALUES ('foo')"]])]
    (t/is (= [{:id "foo",
               :application_time_start (util/->zdt #inst "2020")
               :application_time_end (util/->zdt util/end-of-time)}]
             (c2/sql-query tu/*node* "SELECT foo.id, foo.application_time_start, foo.application_time_end FROM foo"
                           {:basis {:tx !tx1}})))

    #_ ; FIXME #45
    (let [!tx2 (c2/submit-tx tu/*node* [[:sql "DELETE FROM foo"]])]
      (t/is (= [{:id "foo",
                 :application_time_start (util/->zdt #inst "2020")
                 :application_time_end (util/->zdt #inst "2021")}]
               (c2/sql-query tu/*node* "SELECT foo.id, foo.application_time_start, foo.application_time_end FROM foo"
                             {:basis {:tx !tx2}
                              :basis-timeout (java.time.Duration/ofMillis 500)}))))))

(t/deftest test-update-set-field-from-param-328
  (c2/submit-tx tu/*node* [[:sql "INSERT INTO users (id, first_name, last_name) VALUES (?, ?, ?)"
                            [["susan", "Susan", "Smith"]]]])

  (let [!tx (c2/submit-tx tu/*node* [[:sql "UPDATE users FOR PORTION OF APP_TIME FROM ? TO ? AS u SET first_name = ? WHERE u.id = ?"
                                      [[#inst "2021", util/end-of-time, "sue", "susan"]]]])]

    (t/is (= #{["Susan" "Smith", (util/->zdt #inst "2020") (util/->zdt #inst "2021")]
               ["sue" "Smith", (util/->zdt #inst "2021") (util/->zdt util/end-of-time)]}
             (->> (c2/sql-query tu/*node* "SELECT u.first_name, u.last_name, u.application_time_start, u.application_time_end FROM users u"
                                {:basis {:tx !tx}})
                  (into #{} (map (juxt :first_name :last_name :application_time_start :application_time_end))))))))

(t/deftest test-can-submit-same-id-into-multiple-tables-338
  (let [!tx1 (c2/submit-tx tu/*node* [[:sql "INSERT INTO t1 (id, foo) VALUES ('thing', 't1-foo')"]
                                      [:sql "INSERT INTO t2 (id, foo) VALUES ('thing', 't2-foo')"]])
        !tx2 (c2/submit-tx tu/*node* [[:sql "UPDATE t2 SET foo = 't2-foo-v2' WHERE t2.id = 'thing'"]])]


    (t/is (= [{:id "thing", :foo "t1-foo"}]
             (c2/sql-query tu/*node* "SELECT t1.id, t1.foo FROM t1"
                           {:basis {:tx !tx1}})))

    (t/is (= [{:id "thing", :foo "t1-foo"}]
             (c2/sql-query tu/*node* "SELECT t1.id, t1.foo FROM t1"
                           {:basis {:tx !tx2}})))

    (t/is (= [{:id "thing", :foo "t2-foo"}]
             (c2/sql-query tu/*node* "SELECT t2.id, t2.foo FROM t2"
                           {:basis {:tx !tx1}})))

    (t/is (= [{:id "thing", :foo "t2-foo-v2"}]
             (c2/sql-query tu/*node* "SELECT t2.id, t2.foo FROM t2"
                           {:basis {:tx !tx2}})))))

(t/deftest test-put-delete-with-implicit-tables-338
  (letfn [(foos [!tx]
            (->> (for [[tk tn] {:xt "xt_docs"
                                :t1 "explicit_table1"
                                :t2 "explicit_table2"}]
                   [tk (->> (c2/sql-query tu/*node* (format "SELECT t.id, t.v FROM %s t WHERE t.application_time_end = ?" tn)
                                          {:basis {:tx !tx}
                                           :? [util/end-of-time]})
                            (into #{} (map :v)))])
                 (into {})))]
    (let [!tx1 (c2/submit-tx tu/*node*
                             [[:put {:id :foo, :v "implicit table"}]
                              [:put {:id :foo, :_table "explicit_table1", :v "explicit table 1"}]
                              [:put {:id :foo, :_table "explicit_table2", :v "explicit table 2"}]])]
      (t/is (= {:xt #{"implicit table"}, :t1 #{"explicit table 1"}, :t2 #{"explicit table 2"}}
               (foos !tx1))))

    (let [!tx2 (c2/submit-tx tu/*node* [[:delete :foo]])]
      (t/is (= {:xt #{}, :t1 #{"explicit table 1"}, :t2 #{"explicit table 2"}}
               (foos !tx2))))

    (let [!tx3 (c2/submit-tx tu/*node* [[:delete "explicit_table1" :foo]])]
      (t/is (= {:xt #{}, :t1 #{}, :t2 #{"explicit table 2"}}
               (foos !tx3))))))

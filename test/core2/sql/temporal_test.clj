(ns core2.sql.temporal-test
  (:require [clojure.test :refer [deftest is use-fixtures testing]]
            [clojure.string :as str]
            [core2.sql.plan-test :as pt]
            [core2.api :as c2]
            [core2.operator :as op]
            [core2.snapshot :as snap]
            [core2.test-util :as tu]))

(use-fixtures :each tu/with-node)


(deftest system-time-as-of
  (let [snapshot-factory (tu/component ::snap/snapshot-factory)

        !tx1 (c2/submit-tx tu/*node* [[:put {:_id :my-doc, :last_updated "tx1"}]])
        tx1-system-time (str/replace (str/replace (str (:tx-time @!tx1)) "T" " ") "Z" "")
        one-second-before-tx1-system-time (str/replace (str/replace (str (.minusSeconds (:tx-time @!tx1) 1)) "T" " ") "Z" "")]

    (is (= []
           (op/query-ra (pt/plan-sql (str "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '" one-second-before-tx1-system-time "'"))
                        (snap/snapshot snapshot-factory !tx1))))

    (is (= [{:last_updated "tx1"}]
           (op/query-ra (pt/plan-sql (str "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '" tx1-system-time "'"))
                        (snap/snapshot snapshot-factory !tx1))))))

(deftest app-time-period-predicates

  (testing "OVERLAPS"
    (let [snapshot-factory (tu/component ::snap/snapshot-factory)

          !tx1 (c2/submit-tx tu/*node* [[:put {:_id :my-doc, :last_updated "2000"}
                                         {:_valid-time-start #inst "2000"}]
                                        [:put {:_id :my-doc, :last_updated "3000"}
                                         {:_valid-time-start #inst "3000"}]
                                        [:put {:_id :some-other-doc, :last_updated "4000"}
                                         {:_valid-time-start #inst "4000"
                                          :_valid-time-end #inst "4001"}]])
          db (snap/snapshot snapshot-factory !tx1)]


      (is (= [{:last_updated "2000"}]
             (op/query-ra (pt/plan-sql "SELECT foo.last_updated FROM foo WHERE foo.APP_TIME OVERLAPS PERIOD (TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2001-01-01 00:00:00')")
                          db)))

      (is (= [{:last_updated "3000"}]
             (op/query-ra (pt/plan-sql "SELECT foo.last_updated FROM foo WHERE foo.APP_TIME OVERLAPS PERIOD (TIMESTAMP '3000-01-01 00:00:00', TIMESTAMP '3001-01-01 00:00:00')")
                          db)))

      (is (= [{:last_updated "3000"} {:last_updated "4000"}]
             (op/query-ra (pt/plan-sql "SELECT foo.last_updated FROM foo WHERE foo.APP_TIME OVERLAPS PERIOD (TIMESTAMP '4000-01-01 00:00:00', TIMESTAMP '4001-01-01 00:00:00')")
                          db)))

      (is (= [{:last_updated "3000"}]
             (op/query-ra (pt/plan-sql "SELECT foo.last_updated FROM foo WHERE foo.APP_TIME OVERLAPS PERIOD (TIMESTAMP '4002-01-01 00:00:00', TIMESTAMP '9999-01-01 00:00:00')")
                          db))))))

(deftest app-time-multiple-tables
  (let [snapshot-factory (tu/component ::snap/snapshot-factory)

        !tx1 (c2/submit-tx tu/*node* [[:put {:_id :foo-doc, :last_updated "2001"}
                                       {:_valid-time-start #inst "2000"
                                        :_valid-time-end #inst "2001"}]
                                      [:put {:_id :bar-doc, :l_updated "2003"}
                                       {:_valid-time-start #inst "2002"
                                        :_valid-time-end #inst "2003"}]])
        db (snap/snapshot snapshot-factory !tx1)]

    (is (= [{:last_updated "2001"}]
           (op/query-ra (pt/plan-sql "SELECT foo.last_updated FROM foo
                                     WHERE foo.APP_TIME OVERLAPS PERIOD (TIMESTAMP '1999-01-01 00:00:00', TIMESTAMP '2002-01-01 00:00:00')") db)))

    (is (= [{:l_updated "2003"}]
           (op/query-ra (pt/plan-sql "SELECT bar.l_updated FROM bar
                                     WHERE bar.APP_TIME OVERLAPS PERIOD (TIMESTAMP '2002-01-01 00:00:00', TIMESTAMP '2003-01-01 00:00:00')") db)))

    (is (= []
           (op/query-ra (pt/plan-sql "SELECT foo.last_updated, bar.l_updated FROM foo, bar
                                     WHERE foo.APP_TIME OVERLAPS PERIOD (TIMESTAMP '1999-01-01 00:00:00', TIMESTAMP '2001-01-01 00:00:00')
                                     AND
                                     bar.APP_TIME OVERLAPS PERIOD (TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2001-01-01 00:00:00')") db)))

    (is (= [{:last_updated "2001", :l_updated "2003"}]
           (op/query-ra (pt/plan-sql "SELECT foo.last_updated, bar.l_updated FROM foo, bar
                                     WHERE foo.APP_TIME OVERLAPS PERIOD (TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2001-01-01 00:00:00')
                                     AND
                                     bar.APP_TIME OVERLAPS PERIOD (TIMESTAMP '2002-01-01 00:00:00', TIMESTAMP '2003-01-01 00:00:00')") db)))
    (is (= []
           (op/query-ra (pt/plan-sql "SELECT foo.last_updated, bar.name FROM foo, bar
                                     WHERE foo.APP_TIME OVERLAPS bar.APP_TIME") db)))))

(deftest app-time-joins
  (let [snapshot-factory (tu/component ::snap/snapshot-factory)

        !tx1 (c2/submit-tx tu/*node* [[:put {:_id :bill, :name "Bill"}
                                       {:_valid-time-start #inst "2016"
                                        :_valid-time-end #inst "2019"}]
                                      [:put {:_id :jeff, :also_name "Jeff"}
                                       {:_valid-time-start #inst "2018"
                                        :_valid-time-end #inst "2020"}]])
        db (snap/snapshot snapshot-factory !tx1)]

    (is (= []
           (op/query-ra (pt/plan-sql "SELECT foo.name, bar.also_name
                                     FROM foo, bar
                                     WHERE foo.APP_TIME SUCCEEDS bar.APP_TIME") db)))

    (is (= [{:name "Bill" :also_name "Jeff"}]
           (op/query-ra (pt/plan-sql "SELECT foo.name, bar.also_name
                                     FROM foo, bar
                                     WHERE foo.APP_TIME OVERLAPS bar.APP_TIME") db)))))




(ns core2.operator.scan-test
  (:require [clojure.test :as t]
            [core2.core :as c2]
            [core2.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-simple-scan
  (with-open [node (c2/start-node {})]
    (let [tx (c2/submit-tx node [[:put {:_id "foo", :col1 "foo1"}]
                                 [:put {:_id "bar", :col1 "bar1", :col2 "bar2"}]
                                 [:put {:_id "foo", :col2 "baz2"}]])
          db (c2/db node {:tx tx})]
      (t/is (= [{:_id "bar", :col1 "bar1", :col2 "bar2"}]
               (into [] (c2/plan-ra '[:scan [_id col1 col2]] db)))))))

(t/deftest multiple-sources
  (with-open [node1 (c2/start-node {})
              node2 (c2/start-node {})]
    (let [db1 (c2/db node1
                     {:tx (c2/submit-tx node1 [[:put {:_id "foo", :col1 "col1"}]])})
          db2 (c2/db node2
                     {:tx (c2/submit-tx node2 [[:put {:_id "foo", :col2 "col2"}]])})]
      (t/is (= [{:_id "foo", :col1 "col1", :col2 "col2"}]
               (into [] (c2/plan-ra '[:join {_id _id}
                                      [:scan $db1 [_id col1]]
                                      [:scan $db2 [_id col2]]]
                                    {'$db1 db1, '$db2 db2})))))))

(t/deftest test-duplicates-in-scan-1
  (with-open [node (c2/start-node {})]
    (let [tx (c2/submit-tx node [[:put {:_id "foo"}]])
          db (c2/db node {:tx tx})]
      (t/is (= [{:_id "foo"}]
               (into [] (c2/plan-ra '[:scan [_id _id]] db)))))))

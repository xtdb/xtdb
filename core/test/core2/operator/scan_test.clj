(ns core2.operator.scan-test
  (:require [clojure.test :as t]
            [core2.core :as c2]
            [core2.test-util :as tu]
            [core2.operator :as op]
            [core2.snapshot :as snap]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-simple-scan
  (with-open [node (c2/start-node {})]
    (let [tx (c2/submit-tx node [[:put {:_id "foo", :col1 "foo1"}]
                                 [:put {:_id "bar", :col1 "bar1", :col2 "bar2"}]
                                 [:put {:_id "foo", :col2 "baz2"}]])
          sf (tu/component node ::snap/snapshot-factory)]
      (t/is (= [{:_id "bar", :col1 "bar1", :col2 "bar2"}]
               (into [] (op/plan-ra '[:scan [_id col1 col2]]
                                    (snap/snapshot sf tx))))))))

(t/deftest multiple-sources
  (with-open [node1 (c2/start-node {})
              node2 (c2/start-node {})]
    (let [tx1 (c2/submit-tx node1 [[:put {:_id "foo", :col1 "col1"}]])
          db1 (snap/snapshot (tu/component node1 ::snap/snapshot-factory) tx1)
          tx2 (c2/submit-tx node2 [[:put {:_id "foo", :col2 "col2"}]])
          db2 (snap/snapshot (tu/component node2 ::snap/snapshot-factory) tx2)]
      (t/is (= [{:_id "foo", :col1 "col1", :col2 "col2"}]
               (into [] (op/plan-ra '[:join {_id _id}
                                      [:scan $db1 [_id col1]]
                                      [:scan $db2 [_id col2]]]
                                    {'$db1 db1, '$db2 db2})))))))

(t/deftest test-duplicates-in-scan-1
  (with-open [node (c2/start-node {})]
    (let [sf (tu/component node ::snap/snapshot-factory)
          tx (c2/submit-tx node [[:put {:_id "foo"}]])]
      (t/is (= [{:_id "foo"}]
               (into [] (op/plan-ra '[:scan [_id _id]]
                                    (snap/snapshot sf tx))))))))

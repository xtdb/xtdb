(ns core2.operator.scan-test
  (:require [clojure.test :as t]
            [core2.core :as c2]
            [core2.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-simple-scan
  (with-open [node (c2/start-node {})]
    (let [tx @(c2/submit-tx node [{:op :put, :doc {:_id "foo", :col1 "foo1"}}
                                  {:op :put, :doc {:_id "bar", :col1 "bar1", :col2 "bar2"}}
                                  {:op :put, :doc {:_id "foo", :col2 "baz2"}}])]
      (c2/await-tx node tx)
      (tu/finish-chunk node))

    (with-open [db (c2/open-db node)
                res (c2/open-q db '[:scan [_id col1 col2]])]
      (t/is (= [{:_id "bar", :col1 "bar1", :col2 "bar2"}]
               (into [] (mapcat seq) (tu/<-cursor res)))))))

(t/deftest multiple-sources
  (with-open [node1 (c2/start-node {})
              node2 (c2/start-node {})]
    (let [tx @(c2/submit-tx node1 [{:op :put, :doc {:_id "foo", :col1 "col1"}}])]
      (c2/await-tx node1 tx)
      (tu/finish-chunk node1))

    (let [tx @(c2/submit-tx node2 [{:op :put, :doc {:_id "foo", :col2 "col2"}}])]
      (c2/await-tx node2 tx))

    (with-open [db1 (c2/open-db node1)
                db2 (c2/open-db node2)
                res (c2/open-q {:db1 db1, :db2 db2}
                               '[:join {_id _id}
                                 [:scan :db1 [_id col1]]
                                 [:scan :db2 [_id col2]]])]
      (t/is (= [{:_id "foo", :col1 "col1", :col2 "col2"}]
               (into [] (mapcat seq) (tu/<-cursor res)))))))

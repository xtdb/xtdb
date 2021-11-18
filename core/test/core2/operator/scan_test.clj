(ns core2.operator.scan-test
  (:require [clojure.test :as t]
            [core2.api :as c2]
            [core2.local-node :as node]
            [core2.operator :as op]
            [core2.snapshot :as snap]
            [core2.test-util :as tu]
            [core2.util :as util]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-simple-scan
  (with-open [node (node/start-node {})]
    (let [tx (c2/submit-tx node [[:put {:_id :foo, :col1 "foo1"}]
                                 [:put {:_id :bar, :col1 "bar1", :col2 "bar2"}]
                                 [:put {:_id :foo, :col2 "baz2"}]])
          sf (tu/component node ::snap/snapshot-factory)]
      (t/is (= [{:_id :bar, :col1 "bar1", :col2 "bar2"}]
               (op/query-ra '[:scan [_id col1 col2]]
                            (snap/snapshot sf tx)))))))

(t/deftest multiple-sources
  (with-open [node1 (node/start-node {})
              node2 (node/start-node {})]
    (let [tx1 (c2/submit-tx node1 [[:put {:_id :foo, :col1 "col1"}]])
          db1 (snap/snapshot (tu/component node1 ::snap/snapshot-factory) tx1)
          tx2 (c2/submit-tx node2 [[:put {:_id :foo, :col2 "col2"}]])
          db2 (snap/snapshot (tu/component node2 ::snap/snapshot-factory) tx2)]
      (t/is (= [{:_id :foo, :col1 "col1", :col2 "col2"}]
               (op/query-ra '[:join {_id _id}
                              [:scan $db1 [_id col1]]
                              [:scan $db2 [_id col2]]]
                            {'$db1 db1, '$db2 db2}))))))

(t/deftest test-duplicates-in-scan-1
  (with-open [node (node/start-node {})]
    (let [sf (tu/component node ::snap/snapshot-factory)
          tx (c2/submit-tx node [[:put {:_id :foo}]])]
      (t/is (= [{:_id :foo}]
               (op/query-ra '[:scan [_id _id]]
                            (snap/snapshot sf tx)))))))

(t/deftest test-scanning-temporal-cols
  (with-open [node (node/start-node {})]
    (let [snapshot-factory (tu/component node ::snap/snapshot-factory)
          tx @(c2/submit-tx node [[:put {:_id :doc}
                                   {:_valid-time-start #inst "2021"
                                    :_valid-time-end #inst "2022"}]])]

      (let [res (first (op/query-ra '[:scan [_id
                                             _valid-time-start _valid-time-end
                                             _tx-time-start _tx-time-end]]
                                    (snap/snapshot snapshot-factory tx)))]
        (t/is (= #{:_id :_valid-time-start :_valid-time-end :_tx-time-end :_tx-time-start}
                 (-> res keys set)))

        (t/is (= {:_id :doc, :_valid-time-start (util/->zdt #inst "2021"), :_valid-time-end (util/->zdt #inst "2022")}
                 (dissoc res :_tx-time-start :_tx-time-end))))

      (t/is (= {:_id :doc, :vt-start (util/->zdt #inst "2021"), :vt-end (util/->zdt #inst "2022")}
               (-> (first (op/query-ra '[:project [_id
                                                   {vt-start _valid-time-start}
                                                   {vt-end _valid-time-end}]
                                         [:scan [_id _valid-time-start _valid-time-end]]]
                                       (snap/snapshot snapshot-factory tx)))
                   (dissoc :_tx-time-start :_tx-time-end)))))))

#_ ; FIXME hangs
(t/deftest test-only-scanning-temporal-cols-45
  (with-open [node (node/start-node {})]
    (let [snapshot-factory (tu/component node ::snap/snapshot-factory)
          tx @(c2/submit-tx node [[:put {:_id :doc}]])]

      (t/is (op/query-ra '[:scan [_valid-time-start _valid-time-end
                                  _tx-time-start _tx-time-end]]
                         (snap/snapshot snapshot-factory tx))))))

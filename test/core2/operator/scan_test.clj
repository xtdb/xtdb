(ns core2.operator.scan-test
  (:require [clojure.test :as t]
            [core2.api :as c2]
            [core2.ingester :as ingest]
            [core2.local-node :as node]
            [core2.operator :as op]
            [core2.test-util :as tu]
            [core2.util :as util])
  (:import (core2 IResultCursor)))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-simple-scan
  (with-open [node (node/start-node {})]
    (let [tx (c2/submit-tx node [[:put {:id :foo, :col1 "foo1"}]
                                 [:put {:id :bar, :col1 "bar1", :col2 "bar2"}]
                                 [:put {:id :foo, :col2 "baz2"}]])
          ingester (tu/component node :core2/ingester)]
      (t/is (= [{:id :bar, :col1 "bar1", :col2 "bar2"}]
               (tu/query-ra '[:scan [id col1 col2]]
                            (ingest/snapshot ingester tx)))))))

(t/deftest multiple-sources
  (with-open [node1 (node/start-node {})
              node2 (node/start-node {})]
    (let [tx1 (c2/submit-tx node1 [[:put {:id :foo, :col1 "col1"}]])
          db1 (ingest/snapshot (tu/component node1 :core2/ingester) tx1)
          tx2 (c2/submit-tx node2 [[:put {:id :foo, :col2 "col2"}]])
          db2 (ingest/snapshot (tu/component node2 :core2/ingester) tx2)]
      (t/is (= [{:id :foo, :col1 "col1", :col2 "col2"}]
               (tu/query-ra '[:join [{id id}]
                              [:scan $db1 [id col1]]
                              [:scan $db2 [id col2]]]
                            {'$db1 db1, '$db2 db2}))))))

(t/deftest test-duplicates-in-scan-1
  (with-open [node (node/start-node {})]
    (let [ingester (tu/component node :core2/ingester)
          tx (c2/submit-tx node [[:put {:id :foo}]])]
      (t/is (= [{:id :foo}]
               (tu/query-ra '[:scan [id id]]
                            (ingest/snapshot ingester tx)))))))

(t/deftest test-scanning-temporal-cols
  (with-open [node (node/start-node {})]
    (let [ingester (tu/component node :core2/ingester)
          tx @(c2/submit-tx node [[:put {:id :doc}
                                   {:app-time-start #inst "2021"
                                    :app-time-end #inst "3000"}]])]

      (let [res (first (tu/query-ra '[:scan [id
                                             application_time_start application_time_end
                                             system_time_start system_time_end]]
                                    (ingest/snapshot ingester tx)))]
        (t/is (= #{:id :application_time_start :application_time_end :system_time_end :system_time_start}
                 (-> res keys set)))

        (t/is (= {:id :doc, :application_time_start (util/->zdt #inst "2021"), :application_time_end (util/->zdt #inst "3000")}
                 (dissoc res :system_time_start :system_time_end))))

      (t/is (= {:id :doc, :app-time-start (util/->zdt #inst "2021"), :app-time-end (util/->zdt #inst "3000")}
               (-> (first (tu/query-ra '[:project [id
                                                   {app-time-start application_time_start}
                                                   {app-time-end application_time_end}]
                                         [:scan [id application_time_start application_time_end]]]
                                       (ingest/snapshot ingester tx)))
                   (dissoc :system_time_start :system_time_end)))))))

#_ ; FIXME hangs
(t/deftest test-only-scanning-temporal-cols-45
  (with-open [node (node/start-node {})]
    (let [ingester (tu/component node :core2/ingester)
          tx @(c2/submit-tx node [[:put {:id :doc}]])]

      (t/is (tu/query-ra '[:scan [application_time_start application_time_end
                                  system_time_start system_time_end]]
                         (ingest/snapshot ingester tx))))))

(t/deftest test-scan-col-types
  (with-open [node (node/start-node {})]
    (let [ingester (tu/component node :core2/ingester)]
      (letfn [(->col-types [tx]
                (let [snap (ingest/snapshot ingester tx)]
                  (with-open [^IResultCursor rs (op/open-ra '[:scan [id]] snap {})]
                    (.columnTypes rs))))]

        (let [tx (-> (c2/submit-tx node [[:put {:id :doc}]])
                     (tu/then-await-tx node))]
          (tu/finish-chunk node)

          (t/is (= '{id [:extension-type :keyword :utf8 ""]}
                   (->col-types tx))))

        (let [tx (-> (c2/submit-tx node [[:put {:id "foo"}]])
                     (tu/then-await-tx node))]

          (t/is (= '{id [:union #{[:extension-type :keyword :utf8 ""] :utf8}]}
                   (->col-types tx))))))))

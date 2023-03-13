(ns core2.operator.scan-test
  (:require [clojure.test :as t]
            [core2.datalog :as c2]
            [core2.node :as node]
            [core2.operator :as op]
            [core2.test-util :as tu]
            [core2.util :as util])
  (:import core2.operator.IRaQuerySource))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-simple-scan
  (with-open [node (node/start-node {})]
    (c2/submit-tx node [[:put {:id :foo, :col1 "foo1"}]
                        [:put {:id :bar, :col1 "bar1", :col2 "bar2"}]
                        [:put {:id :foo, :col2 "baz2"}]])

    (t/is (= [{:id :bar, :col1 "bar1", :col2 "bar2"}]
             (tu/query-ra '[:scan {:table xt_docs} [id col1 col2]]
                          {:node node})))))

(t/deftest test-duplicates-in-scan-1
  (with-open [node (node/start-node {})]
    (c2/submit-tx node [[:put {:id :foo}]])

    (t/is (= [{:id :foo}]
             (tu/query-ra '[:scan {:table xt_docs} [id id]]
                          {:node node})))))

(t/deftest test-scanning-temporal-cols
  (with-open [node (node/start-node {})]
    (c2/submit-tx node [[:put {:id :doc}
                         {:app-time-start #inst "2021"
                          :app-time-end #inst "3000"}]])

    (let [res (first (tu/query-ra '[:scan {:table xt_docs}
                                    [id
                                     application_time_start application_time_end
                                     system_time_start system_time_end]]
                                  {:node node}))]
      (t/is (= #{:id :application_time_start :application_time_end :system_time_end :system_time_start}
               (-> res keys set)))

      (t/is (= {:id :doc, :application_time_start (util/->zdt #inst "2021"), :application_time_end (util/->zdt #inst "3000")}
               (dissoc res :system_time_start :system_time_end))))

    (t/is (= {:id :doc, :app-time-start (util/->zdt #inst "2021"), :app-time-end (util/->zdt #inst "3000")}
             (-> (first (tu/query-ra '[:project [id
                                                 {app-time-start application_time_start}
                                                 {app-time-end application_time_end}]
                                       [:scan {:table xt_docs}
                                        [id application_time_start application_time_end]]]
                                     {:node node}))
                 (dissoc :system_time_start :system_time_end))))))

(t/deftest test-only-scanning-temporal-cols-45
  (with-open [node (node/start-node {})]
    (let [{tt :sys-time} (c2/submit-tx node [[:put {:id :doc}]])]

      (t/is (= [{:application_time_start (util/->zdt tt)
                 :application_time_end (util/->zdt util/end-of-time)
                 :system_time_start (util/->zdt tt),
                 :system_time_end (util/->zdt util/end-of-time)}]
               (tu/query-ra '[:scan {:table xt_docs}
                              [application_time_start application_time_end
                               system_time_start system_time_end]]
                            {:node node}))))))

(t/deftest test-scan-col-types
  (with-open [node (node/start-node {})]
    (let [^IRaQuerySource ra-src (util/component node :core2.operator/ra-query-source)]
      (letfn [(->col-types [tx]
                (-> (.prepareRaQuery ra-src '[:scan {:table xt_docs} [id]])
                    (.bind (util/component node :core2/indexer) {:node node, :basis {:tx tx}})
                    (.columnTypes)))]

        (let [tx (-> (c2/submit-tx node [[:put {:id :doc}]])
                     (tu/then-await-tx* node))]
          (tu/finish-chunk! node)

          (t/is (= '{id [:extension-type :c2/clj-keyword :utf8 ""]}
                   (->col-types tx))))

        (let [tx (-> (c2/submit-tx node [[:put {:id "foo"}]])
                     (tu/then-await-tx* node))]

          (t/is (= '{id [:union #{[:extension-type :c2/clj-keyword :utf8 ""] :utf8}]}
                   (->col-types tx))))))))

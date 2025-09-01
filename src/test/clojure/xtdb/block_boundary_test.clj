(ns xtdb.block-boundary-test
  (:require [clojure.test :as t]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop] 
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.node :as xtn]
            [xtdb.node.impl]
            [xtdb.test-generators :as tg]
            [xtdb.test-util :as tu]))

(t/deftest ^:property block-boundary-consistency-flush
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [records1 (gen/vector (tg/generate-record {:potential-doc-ids #{1 2 3 4 5}}) 1 10)
                  records2 (gen/vector (tg/generate-record {:potential-doc-ids #{1 2 3 4 5}}) 1 10)]
                 (with-open [node (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock)}]
                                                   :compactor {:threads 0}})]
                   (xt/execute-tx node [(into [:put-docs :docs] records1)])
                   (tu/flush-block! node)

                   (xt/execute-tx node [(into [:put-docs :docs] records2)])
                   (tu/flush-block! node)

                   (and (t/testing "two transactions recorded"
                          (= 2 (count (xt/q node "FROM xt.txs"))))
                        (t/testing "all expected document IDs present"
                          (let [res (xt/q node "SELECT * FROM docs ORDER BY _id")
                                expected-ids (into #{} (map :xt/id (concat records1 records2)))]
                            (= expected-ids (into #{} (map :xt/id res))))))))))

(t/deftest ^:property block-boundary-consistency-flush+compact
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [records1 (gen/vector (tg/generate-record {:potential-doc-ids #{1 2 3 4 5}}) 1 10)
                  records2 (gen/vector (tg/generate-record {:potential-doc-ids #{1 2 3 4 5}}) 1 10)]
                 (with-open [node (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock)}]
                                                   :compactor {:threads 0}})]
                   (xt/execute-tx node [(into [:put-docs :docs] records1)])
                   (tu/flush-block! node)

                   (xt/execute-tx node [(into [:put-docs :docs] records2)])
                   (tu/flush-block! node)

                   (c/compact-all! node #xt/duration "PT1S")

                   (and (t/testing "two transactions recorded"
                          (= 2 (count (xt/q node "FROM xt.txs"))))
                        (t/testing "all expected document IDs present"
                          (let [res (xt/q node "SELECT * FROM docs ORDER BY _id")
                                expected-ids (into #{} (map :xt/id (concat records1 records2)))]
                            (= expected-ids (into #{} (map :xt/id res))))))))))

(t/deftest ^:property block-boundary-consistency-flush+live
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [records1 (gen/vector (tg/generate-record {:potential-doc-ids #{1 2 3 4 5}}) 1 10)
                  records2 (gen/vector (tg/generate-record {:potential-doc-ids #{1 2 3 4 5}}) 1 10)]
                 (with-open [node (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock)}]
                                                   :compactor {:threads 0}})]
                   (xt/execute-tx node [(into [:put-docs :docs] records1)])
                   (tu/flush-block! node)

                   (xt/execute-tx node [(into [:put-docs :docs] records2)])

                   (and (t/testing "two transactions recorded"
                          (= 2 (count (xt/q node "FROM xt.txs"))))
                        (t/testing "all expected document IDs present"
                          (let [res (xt/q node "SELECT * FROM docs ORDER BY _id")
                                expected-ids (into #{} (map :xt/id (concat records1 records2)))]
                            (= expected-ids (into #{} (map :xt/id res))))))))))

(t/deftest ^:property block-boundary-consistency-live
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [records1 (gen/vector (tg/generate-record {:potential-doc-ids #{1 2 3 4 5}}) 1 10)
                  records2 (gen/vector (tg/generate-record {:potential-doc-ids #{1 2 3 4 5}}) 1 10)]
                 (with-open [node (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock)}]
                                                   :compactor {:threads 0}})]
                   (xt/execute-tx node [(into [:put-docs :docs] records1)])
                   (xt/execute-tx node [(into [:put-docs :docs] records2)])

                   (and (t/testing "two transactions recorded"
                          (= 2 (count (xt/q node "FROM xt.txs"))))
                        (t/testing "all expected document IDs present"
                          (let [res (xt/q node "SELECT * FROM docs ORDER BY _id")
                                expected-ids (into #{} (map :xt/id (concat records1 records2)))]
                            (= expected-ids (into #{} (map :xt/id res))))))))))

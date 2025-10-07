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

(t/deftest ^:property multiple-writes-to-doc
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [records (gen/vector (tg/generate-record {:potential-doc-ids #{1}}) 10)]
                 (with-open [node (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock)}]
                                                   :compactor {:threads 0}})]
                   (doseq [record records]
                     (xt/execute-tx node [[:put-docs :docs record]]))

                   (and
                    (t/testing "multiple transaction recorded"
                      (= (count records) (count (xt/q node "FROM xt.txs"))))
                    (t/testing "all entries in history"
                      (let [res (xt/q node "SELECT * FROM docs FOR VALID_TIME ALL")]
                        (= (count records) (count res))))
                    (t/testing "only one document present at valid time"
                      (let [res (xt/q node "SELECT * FROM docs")]
                        (= 1 (count res))))
                    (t/testing "document is equal to last entry"
                      (= (tg/normalize-for-comparison (tu/remove-nils (last records)))
                         (tg/normalize-for-comparison (first (xt/q node "SELECT * FROM docs"))))))))))

(t/deftest ^:property multiple-writes-to-doc-in-same-tx
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [records (gen/vector (tg/generate-record {:potential-doc-ids #{1}}) 10)]
                 (with-open [node (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock)}]
                                                   :compactor {:threads 0}})]
                   (let [vts (take (count records) (tu/->instants :day 1 #inst "2019-01-01"))
                         docs-with-time (mapv (fn [record valid-from]
                                                [:put-docs {:into :docs :valid-from valid-from} record])
                                              records vts)]
                     (xt/execute-tx node docs-with-time)
                     (and
                      (t/testing "one transaction recorded"
                        (= 1 (count (xt/q node "FROM xt.txs"))))
                      (t/testing "all entries in history"
                        (let [res (xt/q node "SELECT * FROM docs FOR VALID_TIME ALL")]
                          (= (count records) (count res))))
                      (t/testing "only one document present at valid time"
                        (let [res (xt/q node "SELECT * FROM docs")] 
                          (= 1 (count res))))
                      (t/testing "document is equal to last entry"
                        (= (tg/normalize-for-comparison (tu/remove-nils (last records)))
                           (tg/normalize-for-comparison (first (xt/q node "SELECT * FROM docs")))))))))))

;; TODO: We've seen this namespace hang on a number of tests when increasing iterations to 1000
;; This temporarily is used to limit iterations to 100
(def tmp-max-iterations 100)

(t/deftest ^:property mixed-records-flush-boundary
  (tu/run-property-test
   {:num-tests (min tu/property-test-iterations tmp-max-iterations)}
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

(t/deftest ^:property mixed-records-flush-and-compact-boundary
  (tu/run-property-test
   {:num-tests (min tu/property-test-iterations tmp-max-iterations)}
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

(t/deftest ^:property mixed-records-flush-and-live-boundary
  (tu/run-property-test
   {:num-tests (min tu/property-test-iterations tmp-max-iterations)}
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

(t/deftest ^:property mixed-records-live-boundary
  (tu/run-property-test
   {:num-tests (min tu/property-test-iterations tmp-max-iterations)}
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

(t/deftest ^:property type-change-across-blocks
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [[vec1 vec2] tg/two-distinct-single-type-vecs-gen]
                 (with-open [node (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock)}]
                                                   :compactor {:threads 0}})]
                   (let [values1 (:vs vec1)
                         values2 (:vs vec2)
                         records1 (map-indexed (fn [i v] {:xt/id (+ 1000 i) :field v}) values1)
                         records2 (map-indexed (fn [i v] {:xt/id (+ 2000 i) :field v}) values2)]
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
                              (= expected-ids (into #{} (map :xt/id res)))))))))))

(t/deftest ^:property single-typed-value-to-nils-across-blocks
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [single-type-vec (tg/single-type-vector-vs-gen 1 100)]
                 (with-open [node (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock)}]
                                                   :compactor {:threads 0}})]
                   (let [values (:vs single-type-vec)
                         records1 (map-indexed (fn [i v] {:xt/id (+ 1000 i) :field v}) values)
                         records2 (map-indexed (fn [i _] {:xt/id (+ 2000 i)}) values)]
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
                              (= expected-ids (into #{} (map :xt/id res)))))))))))

(t/deftest ^:property mixed-ops-across-boundaries
  (tu/run-property-test
   {:num-tests (min tu/property-test-iterations tmp-max-iterations)}
   (let [id-gen (gen/one-of [(gen/return 1) (gen/return "1")])]
     (prop/for-all [ops (gen/vector (gen/one-of [(gen/fmap (fn [id] [:erase id]) id-gen)
                                                 (gen/return [:compact])
                                                 (gen/return [:flush])
                                                 (gen/fmap (fn [value] [:put value])
                                                           (tg/generate-record {:potential-doc-ids #{1 "1"}}))])
                                    1 20)]
       (with-open [node (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock)}]
                                         :compactor {:threads 0}})]
         (doseq [[op value] ops]
           (case op
             :put     (xt/execute-tx node [[:put-docs :docs value]])
             :erase   (xt/execute-tx node [[:erase-docs :docs value]])
             :compact (c/compact-all! node #xt/duration "PT1S")
             :flush   (tu/flush-block! node)))
         (xt/q node "FROM docs WHERE _id = 1"))))))

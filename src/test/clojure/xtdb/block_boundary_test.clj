(ns xtdb.block-boundary-test
  (:require [clojure.test :as t]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [honey.sql :as honey-sql]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.log :as xt-log]
            [xtdb.node :as xtn]
            [xtdb.node.impl]
            [xtdb.test-generators :as tg]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]))

(t/deftest ^:property multiple-writes-to-doc
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [records (gen/vector (tg/generate-record {:potential-doc-ids #{1}}) 10 100)]
                 (with-open [node (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock)}]
                                                   :compactor {:threads 0}})]
                   (doseq [record records]
                     (xt/submit-tx node [[:put-docs :docs record]]))
                   (xt-log/sync-node node #xt/duration "PT1M")

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

(t/deftest ^:property mixed-records-flush-boundary
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

(t/deftest ^:property mixed-records-flush-and-compact-boundary
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

(t/deftest ^:property mixed-records-flush-and-live-boundary
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

(t/deftest ^:property mixed-records-live-boundary
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

(t/deftest ^:property delete-records-across-flush-boundaries
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [records (tg/generate-unique-id-records 10 100)
                  flush-after-put? tg/bool-gen
                  flush-after-delete? tg/bool-gen]
                 (with-open [node (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock)}]
                                                   :compactor {:threads 0}})]
                   (xt/execute-tx node [(into [:put-docs :docs] records)])
                   (when flush-after-put? (tu/flush-block! node))
                   
                   (xt/execute-tx node [(into [:delete-docs :docs] (mapv :xt/id records))])
                   (when flush-after-delete? (tu/flush-block! node))

                   (and
                    (t/testing "two transactions recorded"
                      (= 2 (count (xt/q node "FROM xt.txs"))))
                    (t/testing "documents present in past"
                      (= (count records)
                         (count (xt/q node "SELECT * FROM docs FOR VALID_TIME AS OF TIMESTAMP '2020-01-01T00:00:00.000Z'"))))
                    (t/testing "no documents present"
                      (empty? (xt/q node "SELECT * FROM docs"))))))))

(t/deftest ^:property delete-and-re-add-record-across-flush-boundaries
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [record1 (tg/generate-record {:potential-doc-ids #{1}})
                  record2 (tg/generate-record {:potential-doc-ids #{1}})
                  flush-after-put? tg/bool-gen
                  flush-after-delete? tg/bool-gen
                  flush-after-readd? tg/bool-gen]
                 (with-open [node (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock)}]
                                                   :compactor {:threads 0}})]
                   (xt/execute-tx node [[:put-docs :docs record1]])
                   (when flush-after-put? (tu/flush-block! node))

                   (xt/execute-tx node [[:delete-docs :docs 1]])
                   (when flush-after-delete? (tu/flush-block! node))

                   (xt/execute-tx node [[:put-docs :docs record2]])
                   (when flush-after-readd? (tu/flush-block! node))

                   (and 
                    (t/testing "three transactions recorded"
                      (= 3 (count (xt/q node "FROM xt.txs"))))
                    (t/testing "querying period where document was deleted returns no results"
                      (empty? (xt/q node "SELECT * FROM docs FOR VALID_TIME AS OF TIMESTAMP '2020-01-02T00:00:00.000Z' WHERE _id = 1")))
                    (t/testing "document should have 2 elements in history"
                      (let [res (xt/q node "SELECT *, _valid_from FROM docs FOR VALID_TIME ALL")]
                        (= 2 (count res))))
                    (t/testing "last document is present"
                      (= (tg/normalize-for-comparison (tu/remove-nils record2))
                         (tg/normalize-for-comparison (first (xt/q node "SELECT * FROM docs"))))))))))

(t/deftest ^:property deleting-all-from-table-across-flush-boundaries
  (tu/run-property-test
   {:num-tests 10}
   (prop/for-all [records (tg/generate-unique-id-records 100)
                  flush-after-put? tg/bool-gen
                  flush-after-delete? tg/bool-gen]
                 (with-open [node (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock)}]
                                                   :compactor {:threads 0}})]
                   (xt/execute-tx node [(into [:put-docs :docs] records)])
                   (when flush-after-put? (tu/flush-block! node))

                   (xt/execute-tx node [[:sql "DELETE FROM docs"]])
                   (when flush-after-delete? (tu/flush-block! node))

                   (and
                    (t/testing "two transactions recorded"
                      (= 2 (count (xt/q node "FROM xt.txs"))))
                    (t/testing "documents present in past"
                      (= (count records)
                         (count (xt/q node "SELECT * FROM docs FOR VALID_TIME AS OF TIMESTAMP '2020-01-01T00:00:00.000Z'"))))
                    (t/testing "no documents present"
                      (empty? (xt/q node "SELECT * FROM docs"))))))))

(t/deftest ^:property erase-records-across-flush-boundaries
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [records (tg/generate-unique-id-records 10 100)
                  flush-after-put? tg/bool-gen
                  flush-after-erase? tg/bool-gen]
                 (with-open [node (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock)}]
                                                   :compactor {:threads 0}})]
                   (xt/execute-tx node [(into [:put-docs :docs] records)])
                   (when flush-after-put? (tu/flush-block! node))

                   (xt/execute-tx node [(into [:erase-docs :docs] (mapv :xt/id records))])
                   (when flush-after-erase? (tu/flush-block! node))

                   (and
                    (t/testing "two transactions recorded"
                      (= 2 (count (xt/q node "FROM xt.txs"))))
                    (t/testing "no documents present across all time"
                      (empty? (xt/q node "SELECT * FROM docs FOR VALID_TIME ALL FOR SYSTEM_TIME ALL"))))))))

(t/deftest ^:property erase-and-re-add-record-across-flush-boundaries
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
   (prop/for-all [record1 (tg/generate-record {:potential-doc-ids #{1}})
                  record2 (tg/generate-record {:potential-doc-ids #{1}})
                  flush-after-put? tg/bool-gen
                  flush-after-erase? tg/bool-gen
                  flush-after-readd? tg/bool-gen]
                 (with-open [node (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock)}]
                                                   :compactor {:threads 0}})]
                   (xt/execute-tx node [[:put-docs :docs record1]])
                   (when flush-after-put? (tu/flush-block! node))

                   (xt/execute-tx node [[:erase-docs :docs 1]])
                   (when flush-after-erase? (tu/flush-block! node))

                   (xt/execute-tx node [[:put-docs :docs record2]])
                   (when flush-after-readd? (tu/flush-block! node))

                   (and
                    (t/testing "three transactions recorded"
                      (= 3 (count (xt/q node "FROM xt.txs"))))
                    (t/testing "no document present in period before re-add (erased from all time)"
                      (empty? (xt/q node "SELECT * FROM docs FOR VALID_TIME AS OF TIMESTAMP '2020-01-02T00:00:00.000Z' WHERE _id = 1")))
                    (t/testing "document should have only 1 element in history (we've erased previous history)"
                      (let [res (xt/q node "SELECT *, _valid_from FROM docs FOR VALID_TIME ALL FOR SYSTEM_TIME ALL")]
                        (= 1 (count res))))
                    (t/testing "last document is present"
                      (= (tg/normalize-for-comparison (tu/remove-nils record2))
                         (tg/normalize-for-comparison (first (xt/q node "SELECT * FROM docs"))))))))))

(t/deftest ^:property erasing-all-from-table-across-flush-boundaries
  (tu/run-property-test
   {:num-tests 10}
   (prop/for-all [records (tg/generate-unique-id-records 100)
                  flush-after-put? tg/bool-gen
                  flush-after-erase? tg/bool-gen]
                 (with-open [node (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock)}]
                                                   :compactor {:threads 0}})]
                   (xt/execute-tx node [(into [:put-docs :docs] records)])
                   (when flush-after-put? (tu/flush-block! node))

                   (xt/execute-tx node [[:sql "ERASE FROM docs"]])
                   (when flush-after-erase? (tu/flush-block! node))

                   (and
                    (t/testing "two transactions recorded"
                      (= 2 (count (xt/q node "FROM xt.txs")))) 
                    (t/testing "no documents present across all time"
                      (empty? (xt/q node "SELECT * FROM docs FOR VALID_TIME ALL FOR SYSTEM_TIME ALL"))))))))

(t/deftest ^:property mixed-ops-across-boundaries
  (tu/run-property-test
   {:num-tests tu/property-test-iterations}
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

(t/deftest ^:property local-log-restart-block-boundary
  (tu/run-property-test
   {:num-tests 10}
   (prop/for-all [{:keys [expected-block-count total-doc-count rows-per-block partitioned-records]} (tg/blocks-counts+records)]
                 (util/with-tmp-dirs #{node-dir}
                   (let [{:keys [initial-block-count initial-doc-count initial-tx-count]}
                         (with-open [node (xtn/start-node {:log [:local {:path (.resolve node-dir "log")}]
                                                           :storage [:local {:path (.resolve node-dir "objects")}]
                                                           :indexer {:rows-per-block rows-per-block}
                                                           :compactor {:threads 0}})]
                           
                           ;; Write record batches
                           (doseq [record-batch partitioned-records]
                             (xt/submit-tx node [(into [:put-docs :docs] record-batch)]))
                           (xt-log/sync-node node #xt/duration "PT1M")
                           
                           ;; Wait for blocks to be written by the live index
                           (Thread/sleep 1000)
                           
                           {:initial-block-count (count (tu/read-files-from-bp-path node "tables/public$docs/meta/"))
                            :initial-doc-count (count (xt/q node "SELECT * FROM docs FOR VALID_TIME ALL FOR SYSTEM_TIME ALL"))
                            :initial-tx-count (count (xt/q node "SELECT * FROM xt.txs"))})]
                 
                     ;; Restart the node and verify it picks up from the correct position
                     (with-open [node (xtn/start-node {:log [:local {:path (.resolve node-dir "log")}]
                                                       :storage [:local {:path (.resolve node-dir "objects")}]
                                                       :indexer {:rows-per-block rows-per-block}
                                                       :compactor {:threads 0}})]
                       ;; Wait a bit to ensure no reindexing happens
                       (Thread/sleep 1000) 

                       (and (t/testing "should be expected amount of blocks written initially"
                              (= expected-block-count initial-block-count))

                            (t/testing "all records should have been written initially"
                              (= total-doc-count initial-doc-count))

                            (t/testing "each record batch should have produced a transaction initially"
                              (= (count partitioned-records) initial-tx-count))

                            (t/testing "block count unchanged after restart"
                              (= initial-block-count (count (tu/read-files-from-bp-path node "tables/public$docs/meta/"))))

                            (t/testing "document count preserved after restart"
                              (= initial-doc-count (count (xt/q node "SELECT * FROM docs FOR VALID_TIME ALL FOR SYSTEM_TIME ALL"))))

                            (t/testing "transaction count preserved after restart"
                              (= initial-tx-count (count (xt/q node "SELECT * FROM xt.txs")))))))))))

(t/deftest ^:property updates-adding-new-keys-cross-boundaries
  (let [exclude-gens #{tg/varbinary-gen tg/decimal-gen tg/nil-gen}]
    (tu/run-property-test
     {:num-tests tu/property-test-iterations}
     (prop/for-all [record (tg/generate-record {:potential-doc-ids #{1}
                                                :exclude-gens exclude-gens
                                                :override-field-keys [:a :b]})
                    update-statement-1 (tg/update-statement-gen 1 {:exclude-gens exclude-gens
                                                                   :override-field-keys [:c :d]})
                    update-statement-2 (tg/update-statement-gen 1 {:exclude-gens exclude-gens
                                                                   :override-field-keys [:e :f]})
                    flush-after-put? tg/bool-gen
                    flush-after-first-update? tg/bool-gen
                    flush-after-second-update? tg/bool-gen]
                   (with-open [node (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock)}]
                                                     :compactor {:threads 0}})]
                     (xt/execute-tx node [[:put-docs :docs record]])
                     (when flush-after-put? (tu/flush-block! node))

                     (xt/execute-tx node [(:sql-statement update-statement-1)])
                     (when flush-after-first-update? (tu/flush-block! node))

                     (xt/execute-tx node [(:sql-statement update-statement-2)])
                     (when flush-after-second-update? (tu/flush-block! node))

                     (and
                      (t/testing "three transactions recorded"
                        (= 3 (count (xt/q node "FROM xt.txs"))))
                      (t/testing "document should have 3 entries in history"
                        (let [res (xt/q node "SELECT * FROM docs FOR VALID_TIME ALL")]
                          (= 3 (count res))))
                      (t/testing "document has value equal to all of the updates merged"
                        (let [res (first (xt/q node "SELECT * FROM docs WHERE _id = 1"))
                              record-fields [record (:fields update-statement-1) (:fields update-statement-2)]
                              expected-merged (reduce merge {:xt/id 1} record-fields)
                              expected-merged-no-nils (tu/remove-nils expected-merged)]
                          (= (tg/normalize-for-comparison expected-merged-no-nils)
                             (tg/normalize-for-comparison res))))))))))

(defn ->insert-tx [k v]
  (let [[sql-string & params] (honey-sql/format {:insert-into [:docs]
                                                 :columns [:_id k]
                                                 :values [[1 [:lift v]]]})]
    [:sql sql-string (vec params)]))

(defn ->update-tx [k v]
  (let [[sql-string & params] (honey-sql/format {:update :docs
                                                 :set {k [:lift v]}
                                                 :where [:= :_id 1]})]
    [:sql sql-string (vec params)]))

;; TODO - Fails when :a is [[#NaN]] or when it contains a set. 
(t/deftest ^:property update-deduplication
  (let [exclude-gens #{tg/varbinary-gen tg/decimal-gen tg/nil-gen tg/set-gen}]
    (tu/run-property-test
     {:num-tests tu/property-test-iterations}
     (prop/for-all [record (tg/generate-record {:potential-doc-ids #{1}
                                                :exclude-gens exclude-gens
                                                :override-field-keys [:a]})
                    flush-after-put? tg/bool-gen
                    flush-after-first-update? tg/bool-gen
                    flush-after-second-update? tg/bool-gen]
                   (with-open [node (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock)}]
                                                     :compactor {:threads 0}})]
                     (let [value (:a record)
                           insert-sql (->insert-tx :a value)
                           update-sql (->update-tx :a value)]
                       (xt/execute-tx node [insert-sql])
                       (when flush-after-put? (tu/flush-block! node))

                       (xt/execute-tx node [update-sql])
                       (when flush-after-first-update? (tu/flush-block! node))

                       (xt/execute-tx node [update-sql])
                       (when flush-after-second-update? (tu/flush-block! node))

                       (and
                        (t/testing "three transactions recorded"
                          (= 3 (count (xt/q node "FROM xt.txs"))))
                        (t/testing "document should have only 1 entry in history (deduplicated)"
                          (let [res (xt/q node "SELECT * FROM docs FOR VALID_TIME ALL")]
                            (= 1 (count res))))
                        (t/testing "document has correct value"
                          (let [res (first (xt/q node "SELECT * FROM docs WHERE _id = 1"))
                                expected (tu/remove-nils record)]
                            (= (tg/normalize-for-comparison expected)
                               (tg/normalize-for-comparison res)))))))))))

(t/deftest ^:property update-same-keys-new-values
  (let [exclude-gens #{tg/varbinary-gen tg/decimal-gen tg/nil-gen}]
    (tu/run-property-test
     {:num-tests tu/property-test-iterations}
     (prop/for-all [[value-1 value-2 value-3] (tg/distinct-value-gen 3 {:exclude-gens exclude-gens})
                    flush-after-put? tg/bool-gen
                    flush-after-first-update? tg/bool-gen
                    flush-after-second-update? tg/bool-gen]
                   (with-open [node (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock)}]
                                                     :compactor {:threads 0}})]
                     (let [insert-sql (->insert-tx :a value-1)
                           update-statement-1 (->update-tx :a value-2)
                           update-statement-2 (->update-tx :a value-3)]
                       (xt/execute-tx node [insert-sql])
                       (when flush-after-put? (tu/flush-block! node))

                       (xt/execute-tx node [update-statement-1])
                       (when flush-after-first-update? (tu/flush-block! node))

                       (xt/execute-tx node [update-statement-2])
                       (when flush-after-second-update? (tu/flush-block! node))

                       (and
                        (t/testing "three transactions recorded"
                          (= 3 (count (xt/q node "FROM xt.txs"))))
                        (t/testing "document should have 3 entries in history"
                          (let [res (xt/q node "SELECT * FROM docs FOR VALID_TIME ALL")]
                            (= 3 (count res))))
                        (t/testing "document has final value"
                          (let [res (first (xt/q node "SELECT * FROM docs WHERE _id = 1"))
                                expected (tu/remove-nils {:xt/id 1 :a value-3})]
                            (= (tg/normalize-for-comparison expected)
                               (tg/normalize-for-comparison res)))))))))))
(ns xtdb.indexer.live-index-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.arrow-edn-test :as aet]
            [xtdb.check-pbuf :as cpb]
            [xtdb.compactor :as c]
            [xtdb.db-catalog :as db]
            xtdb.node.impl
            [xtdb.object-store :as os]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.util Random UUID]
           [org.apache.arrow.memory BufferAllocator]
           (xtdb.api TransactionResult$Committed)
           (xtdb.arrow Relation)
           (xtdb.indexer ResolvedTx)
           (xtdb.trie ArrowHashTrie ArrowHashTrie$Leaf MemoryHashTrie$Leaf)))

(t/use-fixtures :each tu/with-allocator
  (tu/with-opts {:compactor {:threads 0}})
  tu/with-node)

(t/deftest test-block
  (let [^BufferAllocator allocator (.getAllocator tu/*node*)
        db (db/primary-db tu/*node*)
        buffer-pool (.getBufferPool db)
        live-index (.getLiveIndex db)

        iids (let [rnd (Random. 0)]
               (repeatedly 12000 #(UUID. (.nextLong rnd) (.nextLong rnd))))

        iid-bytes (->> (sort-by #(.getMostSignificantBits ^UUID %) #(Long/compareUnsigned %1 %2) iids)
                       (mapv (comp vec util/uuid->bytes)))]

    (t/testing "commit"
      (xt/execute-tx tu/*node* (for [iid iids] [:put-docs :docs {:xt/id iid, :some :doc}]))

      (let [live-table (.table live-index #xt/table docs)
            live-rel (.getLiveRelation live-table)
            iid-vec (.vectorFor live-rel "_iid")

            trie (.getLiveTrie live-table)]

        (t/is (= iid-bytes
                 (->> (.getLeaves (.compactLogs trie))
                      (mapcat (fn [^MemoryHashTrie$Leaf leaf]
                                (mapv #(vec (.getObject iid-vec %)) (.getData leaf)))))))))

    (t/testing "finish block"
      (tu/flush-block! tu/*node*)

      (let [trie-ba (.getByteArray buffer-pool (util/->path "tables/public$docs/meta/l00-rc-b00.arrow"))
            leaf-ba (.getByteArray buffer-pool (util/->path "tables/public$docs/data/l00-rc-b00.arrow"))]
        (util/with-open [trie-loader (Relation/loader allocator trie-ba)
                         trie-rel (Relation. allocator (.getSchema trie-loader))
                         leaf-loader (Relation/loader allocator leaf-ba)
                         leaf-rel (Relation. allocator (.getSchema leaf-loader))]
          (let [iid-vec (.vectorFor leaf-rel "_iid")]
            (.loadPage trie-loader 0 trie-rel)
            (t/is (= iid-bytes
                     (->> (.getLeaves (ArrowHashTrie. (.get trie-rel "nodes")))
                          (mapcat (fn [^ArrowHashTrie$Leaf leaf]
                                    (.loadPage leaf-loader (.getDataPageIndex leaf) leaf-rel)

                                    (->> (range 0 (.getValueCount iid-vec))
                                         (mapv #(vec (.getObject iid-vec %)))))))))))))))

(def txs
  [[[:put-docs :hello {:xt/id #uuid "cb8815ee-85f7-4c61-a803-2ea1c949cf8d" :a 1}]
    [:put-docs :world {:xt/id #uuid "424f5622-c826-4ded-a5db-e2144d665c38" :b 2}]]
   [[:delete-docs :hello #uuid "cb8815ee-85f7-4c61-a803-2ea1c949cf8d"]
    [:put-docs {:into :world, :valid-from #inst "2023", :valid-to #inst "2024"}
     {:xt/id #uuid "424f5622-c826-4ded-a5db-e2144d665c38" :b 3}]]
   [[:erase-docs :world #uuid "424f5622-c826-4ded-a5db-e2144d665c38"]]
   ;; sql
   [[:sql "INSERT INTO foo (_id, bar, toto) VALUES (1, 1, 'toto')"]
    [:sql "UPDATE foo SET bar = 2 WHERE foo._id = 1"]
    [:sql "DELETE FROM foo WHERE foo.bar = 2"]
    [:sql "INSERT INTO foo (_id, bar) VALUES (2, 2)"]]
   ;; sql erase
   [[:sql "ERASE FROM foo WHERE foo._id = 2"]]
   ;; abort
   [[:sql "INSERT INTO foo (_id, _valid_from, _valid_to) VALUES (1, DATE '2020-01-01', DATE '2019-01-01')"]]])

(t/deftest can-build-live-index
  (binding [c/*ignore-signal-block?* true]
    (let [node-dir (util/->path "target/can-build-live-index")]
      (util/delete-dir node-dir)

      (util/with-open [node (tu/->local-node {:node-dir node-dir
                                              :compactor-threads 0})]
        (let [bp (.getBufferPool (db/primary-db node))]

          (doseq [tx-ops txs]
            (try
              (xt/execute-tx node tx-ops
                             {:default-tz #xt/zone "Europe/London"})
              (catch Exception _)))

          (tu/flush-block! node)

          (t/is (= [(os/->StoredObject "tables/public$foo/data/l00-rc-b00.arrow" 2494)]
                   (.listAllObjects bp (util/->path "tables/public$foo/data"))))

          (t/is (= [(os/->StoredObject "tables/public$foo/meta/l00-rc-b00.arrow" 3886)]
                   (.listAllObjects bp (util/->path "tables/public$foo/meta")))))

        (let [expected-dir (io/as-file (io/resource "xtdb/indexer-test/can-build-live-index"))]
          (aet/check-arrow-edn-dir (io/file expected-dir "arrow") node-dir)
          (cpb/check-pbuf (.toPath (io/file expected-dir "pbuf")) node-dir))))))

(t/deftest live-index-row-counter-reinitialization
  (binding [c/*ignore-signal-block?* true]
    (let [node-dir (util/->path "target/live-index-row-counter-reinitialization")]
      (util/delete-dir node-dir)

      (util/with-open [node (tu/->local-node {:node-dir node-dir})]
        (xt/execute-tx node [[:put-docs :docs {:xt/id 1 :foo 1}]])
        (tu/flush-block! node))

      (util/with-open [node (tu/->local-node {:node-dir node-dir})]
        (let [bp (.getBufferPool (db/primary-db node))]
          (xt/execute-tx node [[:put-docs :docs {:xt/id 1 :foo 1}]])
          (tu/flush-block! node)

          (t/is (= [(os/->StoredObject (util/->path "blocks/b00.binpb") 42)
                    (os/->StoredObject (util/->path "blocks/b01.binpb") 43)]
                   (.listAllObjects bp (util/->path "blocks")))))))))

(t/deftest staged-empty-create-table-visible-across-a-batch-5507
  ;; A tx that CREATEs a table (columns declared, 0 rows) is read behind by a later tx while still staged
  ;; in the same batch. openSnapshot drops the empty relation, so tableInfo has to carry the freshly-created
  ;; table's existence directly — else SQL base-table resolution throws "Table not found" for the later tx.
  (let [live-index (.getLiveIndex (db/primary-db tu/*node*))
        table #xt/table foo
        tx-key #xt/tx-key {:tx-id 0, :system-time #xt/instant "2020-01-01T00:00:00Z"}]
    (util/with-open [staging-alloc (util/->child-allocator (.getAllocator tu/*node*) "staging")]
      (util/with-open [staged (with-open [create-tx (tu/->open-tx tx-key)]
                                (.executeSql create-tx "CREATE TABLE foo (_id, bar)")
                                (ResolvedTx/stage staging-alloc create-tx 0 (TransactionResult$Committed. tx-key) nil))
                       observer-tx (tu/->open-tx #xt/tx-key {:tx-id 1, :system-time #xt/instant "2020-01-01T00:00:01Z"})
                       snap (.openSnapshot live-index [staged] observer-tx)]
        (t/is (.containsKey (.getTableInfo snap) table)
              "a table CREATEd in a still-staged tx is visible to a later tx resolving behind it")))))

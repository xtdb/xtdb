(ns xtdb.indexer.live-index-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.arrow-edn-test :as aet]
            [xtdb.check-pbuf :as cpb]
            [xtdb.compactor :as c]
            [xtdb.db-catalog :as db]
            [xtdb.indexer.live-index :as li]
            xtdb.node.impl
            [xtdb.object-store :as os]
            [xtdb.serde :as serde]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.nio ByteBuffer]
           [java.util HashMap Random UUID]
           [java.util.concurrent.locks StampedLock]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector FixedSizeBinaryVector]
           (xtdb.arrow Relation)
           (xtdb.indexer LiveIndex)
           (xtdb.trie ArrowHashTrie ArrowHashTrie$Leaf Bucketer MemoryHashTrie$Leaf)
           (xtdb.util RefCounter RowCounter)))

(t/use-fixtures :each tu/with-allocator
  (tu/with-opts {:compactor {:threads 0}})
  tu/with-node)

(t/deftest test-block
  (let [^BufferAllocator allocator (.getAllocator tu/*node*)
        db (db/primary-db tu/*node*)
        buffer-pool (.getBufferPool db)
        live-index (.getLiveIndex (.getSourceIndexer db))

        iids (let [rnd (Random. 0)]
               (repeatedly 12000 #(UUID. (.nextLong rnd) (.nextLong rnd))))

        iid-bytes (->> (sort-by #(.getMostSignificantBits ^UUID %) #(Long/compareUnsigned %1 %2) iids)
                       (mapv (comp vec util/uuid->bytes)))]

    (t/testing "commit"
      (with-open [live-idx-tx (.startTx live-index (serde/->TxKey 0 (.toInstant #inst "2000")))]
        (let [live-table-tx (.liveTable live-idx-tx #xt/table my-table)
              put-doc-wrt (.getDocWriter live-table-tx)]
          (doseq [^UUID iid iids]
            (.logPut live-table-tx (util/uuid->byte-buffer iid) 0 0
                     #(.writeObject put-doc-wrt {:some :doc})))

          (.commit live-idx-tx)

          (let [live-table (.liveTable live-index #xt/table my-table)
                live-rel (.getLiveRelation live-table)
                iid-vec (.vectorFor live-rel "_iid")

                trie (.getLiveTrie live-table)]

            (t/is (= iid-bytes
                     (->> (.getLeaves (.compactLogs trie))
                          (mapcat (fn [^MemoryHashTrie$Leaf leaf]
                                    (mapv #(vec (.getObject iid-vec %)) (.getData leaf)))))))))))

    (t/testing "finish block"
      (tu/flush-block! tu/*node*)

      (let [trie-ba (.getByteArray buffer-pool (util/->path "tables/public$my-table/meta/l00-rc-b00.arrow"))
            leaf-ba (.getByteArray buffer-pool (util/->path "tables/public$my-table/data/l00-rc-b00.arrow"))]
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

(deftest test-bucket-for
  (let [uuid1 #uuid "ce33e4b8-ec2f-4b80-8e9c-a4314005adbf"
        bucketer Bucketer/DEFAULT]
    (with-open [allocator (RootAllocator.)
                iid-vec (doto ^FixedSizeBinaryVector (FixedSizeBinaryVector. "iid" allocator 16)
                          (.setSafe 0 (util/uuid->bytes uuid1))
                          (.setValueCount 1))]
      (t/is (= [3 0 3 2 0 3 0 3 3 2 1 0 2 3 2 0]
               (for [^int x (range 16)]
                 (.bucketFor bucketer (.getDataPointer iid-vec 0) x))))

      (t/is (= 3 (.bucketFor bucketer (.getDataPointer iid-vec 0) 18)))
      (t/is (= 0 (.bucketFor bucketer (.getDataPointer iid-vec 0) 30)))
      (t/is (= 3 (.bucketFor bucketer (.getDataPointer iid-vec 0) 63))))))

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

(t/deftest test-new-table-discarded-on-abort-2721
  (let [live-index (.getLiveIndex (db/primary-db tu/*node*))]

    (with-open [live-tx0 (.startTx live-index #xt/tx-key {:tx-id 0, :system-time #xt/instant "2020-01-01T00:00:00Z"})]
      (let [foo-table-tx (.liveTable live-tx0 #xt/table foo)
            doc-wtr (.getDocWriter foo-table-tx)]
        (.logPut foo-table-tx (ByteBuffer/allocate 16) 0 0
                 (fn []
                   (.endStruct doc-wtr)))
        (.commit live-tx0)))

    (t/testing "aborting bar means it doesn't get added to the committed live-index")
    (with-open [live-tx1 (.startTx live-index #xt/tx-key {:tx-id 0, :system-time #xt/instant "2020-01-02T00:00:00Z"})]
      (let [bar-table-tx (.liveTable live-tx1 #xt/table bar)
            doc-wtr (.getDocWriter bar-table-tx)]
        (.logPut bar-table-tx (ByteBuffer/allocate 16) 0 0
                 (fn []
                   (.endStruct doc-wtr)))

        (t/testing "doesn't get added in the tx either"
          (t/is (nil? (.liveTable live-index #xt/table bar))))

        (.abort live-tx1)

        (t/is (some? (.liveTable live-index #xt/table foo)))
        (t/is (nil? (.liveTable live-index #xt/table bar)))))

    (t/testing "aborting foo doesn't clear it from the live-index"
      (with-open [live-tx2 (.startTx live-index #xt/tx-key {:tx-id 0, :system-time #xt/instant "2020-01-03T00:00:00Z"})]
        (let [foo-table-tx (.liveTable live-tx2 #xt/table foo)
              doc-wtr (.getDocWriter foo-table-tx)]
          (.logPut foo-table-tx (ByteBuffer/allocate 16) 0 0
                   (fn []
                     (.endStruct doc-wtr)))
          (.abort live-tx2)))

      (t/is (some? (.liveTable live-index #xt/table foo))))

    (t/testing "committing bar after an abort adds it correctly"
      (with-open [live-tx3 (.startTx live-index #xt/tx-key {:tx-id 0, :system-time #xt/instant "2020-01-04T00:00:00Z"})]
        (let [bar-table-tx (.liveTable live-tx3 #xt/table bar)
              doc-wtr (.getDocWriter bar-table-tx)]
          (.logPut bar-table-tx (ByteBuffer/allocate 16) 0 0
                   (fn []
                     (.endStruct doc-wtr)))
          (.commit live-tx3)))

      (t/is (some? (.liveTable live-index #xt/table bar))))))

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

          (t/is (= [(os/->StoredObject (util/->path "blocks/b00.binpb") 39)
                    (os/->StoredObject (util/->path "blocks/b01.binpb") 40)]
                   (.listAllObjects bp (util/->path "blocks")))))))))

;; -----------------------------------------------------------------------------
;; Tests derived from live-index.allium spec - verifying implicit behaviours
;; -----------------------------------------------------------------------------

(t/deftest external-snapshot-does-not-see-uncommitted-data
  (let [live-index (.getLiveIndex (db/primary-db tu/*node*))
        table #xt/table test-table
        iid (ByteBuffer/wrap (util/uuid->bytes (UUID/randomUUID)))]

    (with-open [live-tx (.startTx live-index #xt/tx-key {:tx-id 0, :system-time #xt/instant "2020-01-01T00:00:00Z"})]
      (let [table-tx (.liveTable live-tx table)
            doc-wtr (.getDocWriter table-tx)]
        (.logPut table-tx iid 0 0 #(.endStruct doc-wtr))

        ;; External snapshot (from live-index, not from tx) should NOT see the uncommitted row
        (with-open [external-snap (.openSnapshot live-index)]
          (let [live-idx-snap (.getLiveIndex external-snap)]
            (t/is (nil? (.liveTable live-idx-snap table))
                  "External snapshot should not see table created in uncommitted tx")))

        ;; But the transaction's own snapshot SHOULD see its data (in txRelation)
        (with-open [tx-snap (.openSnapshot live-tx)]
          (let [table-snap (.liveTable tx-snap table)]
            (t/is (some? table-snap)
                  "Transaction snapshot should see its own uncommitted table")
            (t/is (some? (.getTxRelation table-snap))
                  "Transaction snapshot should have tx relation for uncommitted data")
            (t/is (= 1 (.getRowCount (.getTxRelation table-snap)))
                  "Transaction snapshot should see its own uncommitted row in txRelation")))

        (.commit live-tx))

      ;; After commit, external snapshot should now see the data
      (with-open [external-snap (.openSnapshot live-index)]
        (let [live-idx-snap (.getLiveIndex external-snap)
              table-snap (.liveTable live-idx-snap table)]
          (t/is (some? table-snap)
                "External snapshot should see committed table")
          (t/is (= 1 (.getRowCount (.getLiveRelation table-snap)))
                "External snapshot should see committed row"))))))

(t/deftest concurrent-external-snapshot-unaffected-by-later-commit
  (let [live-index (.getLiveIndex (db/primary-db tu/*node*))
        table #xt/table test-table
        iid1 (ByteBuffer/wrap (util/uuid->bytes (UUID/randomUUID)))
        iid2 (ByteBuffer/wrap (util/uuid->bytes (UUID/randomUUID)))]

    ;; Commit first transaction
    (with-open [live-tx1 (.startTx live-index #xt/tx-key {:tx-id 0, :system-time #xt/instant "2020-01-01T00:00:00Z"})]
      (let [table-tx (.liveTable live-tx1 table)
            doc-wtr (.getDocWriter table-tx)]
        (.logPut table-tx iid1 0 0 #(.endStruct doc-wtr))
        (.commit live-tx1)))

    ;; Take a snapshot BEFORE second transaction
    (with-open [snap-before (.openSnapshot live-index)]
      (let [live-idx-snap-before (.getLiveIndex snap-before)
            table-snap-before (.liveTable live-idx-snap-before table)
            row-count-before (.getRowCount (.getLiveRelation table-snap-before))]

        ;; Commit second transaction
        (with-open [live-tx2 (.startTx live-index #xt/tx-key {:tx-id 1, :system-time #xt/instant "2020-01-02T00:00:00Z"})]
          (let [table-tx (.liveTable live-tx2 table)
                doc-wtr (.getDocWriter table-tx)]
            (.logPut table-tx iid2 0 0 #(.endStruct doc-wtr))
            (.commit live-tx2)))

        ;; The snapshot taken before should still show only 1 row (immutability)
        (t/is (= row-count-before
                 (.getRowCount (.getLiveRelation table-snap-before)))
              "Snapshot taken before commit should be unaffected by later commit")
        (t/is (= 1 row-count-before)
              "Pre-commit snapshot should have 1 row"))

      ;; New snapshot should see both rows
      (with-open [snap-after (.openSnapshot live-index)]
        (let [live-idx-snap-after (.getLiveIndex snap-after)
              table-snap-after (.liveTable live-idx-snap-after table)]
          (t/is (= 2 (.getRowCount (.getLiveRelation table-snap-after)))
                "Post-commit snapshot should have 2 rows"))))))

(t/deftest abort-updates-latest-completed-tx
  (let [live-index (.getLiveIndex (db/primary-db tu/*node*))
        table #xt/table test-table
        iid (ByteBuffer/wrap (util/uuid->bytes (UUID/randomUUID)))
        tx-key-1 #xt/tx-key {:tx-id 1, :system-time #xt/instant "2020-01-01T00:00:00Z"}
        tx-key-2 #xt/tx-key {:tx-id 2, :system-time #xt/instant "2020-01-02T00:00:00Z"}]

    (t/is (nil? (.getLatestCompletedTx live-index))
          "Initially no completed tx")

    ;; Commit tx-1
    (with-open [live-tx1 (.startTx live-index tx-key-1)]
      (let [table-tx (.liveTable live-tx1 table)
            doc-wtr (.getDocWriter table-tx)]
        (.logPut table-tx iid 0 0 #(.endStruct doc-wtr))
        (.commit live-tx1)))

    (t/is (= tx-key-1 (.getLatestCompletedTx live-index))
          "After commit, latest_completed_tx should be tx-1")

    ;; Abort tx-2 - should still update latest_completed_tx
    (with-open [live-tx2 (.startTx live-index tx-key-2)]
      (let [table-tx (.liveTable live-tx2 table)
            doc-wtr (.getDocWriter table-tx)]
        (.logPut table-tx iid 0 0 #(.endStruct doc-wtr))
        (.abort live-tx2)))

    (t/is (= tx-key-2 (.getLatestCompletedTx live-index))
          "After abort, latest_completed_tx should advance to tx-2 (processed, not committed)")))

(t/deftest get-table-tx-multiple-times-returns-same-tx
  (let [live-index (.getLiveIndex (db/primary-db tu/*node*))
        table #xt/table test-table
        iid (ByteBuffer/wrap (util/uuid->bytes (UUID/randomUUID)))]

    (with-open [live-tx (.startTx live-index #xt/tx-key {:tx-id 0, :system-time #xt/instant "2020-01-01T00:00:00Z"})]
      (let [table-tx-1 (.liveTable live-tx table)
            table-tx-2 (.liveTable live-tx table)]

        (t/is (identical? table-tx-1 table-tx-2)
              "Multiple calls to liveTable should return same tx context")

        (let [doc-wtr (.getDocWriter table-tx-1)]
          (.logPut table-tx-1 iid 0 0 #(.endStruct doc-wtr)))

        (with-open [tx-snap (.openSnapshot live-tx)]
          (let [table-snap (.liveTable tx-snap table)]
            (t/is (= 1 (.getRowCount (.getTxRelation table-snap)))
                  "Row written via first reference should be visible in txRelation")))))))

(t/deftest multiple-puts-same-iid-records-all
  (let [live-index (.getLiveIndex (db/primary-db tu/*node*))
        table #xt/table test-table
        iid (ByteBuffer/wrap (util/uuid->bytes (UUID/randomUUID)))]

    (with-open [live-tx (.startTx live-index #xt/tx-key {:tx-id 0, :system-time #xt/instant "2020-01-01T00:00:00Z"})]
      (let [table-tx (.liveTable live-tx table)
            doc-wtr (.getDocWriter table-tx)]

        (.logPut table-tx (.duplicate iid) 0 0 #(.endStruct doc-wtr))
        (.logPut table-tx (.duplicate iid) 0 0 #(.endStruct doc-wtr))
        (.logPut table-tx (.duplicate iid) 0 0 #(.endStruct doc-wtr))

        (.commit live-tx))

      (with-open [snap (.openSnapshot live-index)]
        (let [live-idx-snap (.getLiveIndex snap)
              table-snap (.liveTable live-idx-snap table)]
          (t/is (= 3 (.getRowCount (.getLiveRelation table-snap)))
                "All puts should be recorded (temporal history)"))))))

(t/deftest next-block-removes-tables
  (util/with-open [allocator (RootAllocator.)]
    (let [db (db/primary-db tu/*node*)
          bp (.getBufferPool db)
          block-cat (.getBlockCatalog db)
          table-catalog (.getTableCatalog db)
          tables (HashMap.)
          live-index-allocator (util/->child-allocator allocator "live-index")]

      (util/with-open [^LiveIndex live-index (li/->LiveIndex live-index-allocator bp
                                                             block-cat table-catalog
                                                             "xtdb"
                                                             nil tables
                                                             nil (StampedLock.)
                                                             (RefCounter.)
                                                             (RowCounter.) 102400
                                                             64 1024
                                                             [])]
        (let [table #xt/table test-table
              iid (ByteBuffer/wrap (util/uuid->bytes (UUID/randomUUID)))]

          (with-open [live-tx (.startTx live-index #xt/tx-key {:tx-id 0, :system-time #xt/instant "2020-01-01T00:00:00Z"})]
            (let [table-tx (.liveTable live-tx table)
                  doc-wtr (.getDocWriter table-tx)]
              (.logPut table-tx iid 0 0 #(.endStruct doc-wtr))
              (.commit live-tx)))

          (t/is (some? (.liveTable live-index table))
                "Table should exist after commit")

          (.finishBlock live-index 0)
          (.nextBlock live-index)

          (t/is (nil? (.liveTable live-index table))
                "Table should be removed after nextBlock (not just emptied)"))))))

(t/deftest transaction-snapshot-shows-read-your-writes
  (let [live-index (.getLiveIndex (db/primary-db tu/*node*))
        table #xt/table test-table
        iid1 (ByteBuffer/wrap (util/uuid->bytes (UUID/randomUUID)))
        iid2 (ByteBuffer/wrap (util/uuid->bytes (UUID/randomUUID)))]

    ;; First commit some data
    (with-open [live-tx1 (.startTx live-index #xt/tx-key {:tx-id 0, :system-time #xt/instant "2020-01-01T00:00:00Z"})]
      (let [table-tx (.liveTable live-tx1 table)
            doc-wtr (.getDocWriter table-tx)]
        (.logPut table-tx iid1 0 0 #(.endStruct doc-wtr))
        (.commit live-tx1)))

    ;; Start second transaction and write more data
    (with-open [live-tx2 (.startTx live-index #xt/tx-key {:tx-id 1, :system-time #xt/instant "2020-01-02T00:00:00Z"})]
      (let [table-tx (.liveTable live-tx2 table)
            doc-wtr (.getDocWriter table-tx)]
        (.logPut table-tx iid2 0 0 #(.endStruct doc-wtr))

        ;; Transaction snapshot should see BOTH committed (iid1) and own uncommitted (iid2)
        (with-open [tx-snap (.openSnapshot live-tx2)]
          (let [table-snap (.liveTable tx-snap table)
                live-rel (.getLiveRelation table-snap)
                tx-rel (.getTxRelation table-snap)]

            (t/is (= 1 (.getRowCount live-rel))
                  "Transaction snapshot should see 1 committed row")

            (t/is (= 1 (.getRowCount tx-rel))
                  "Transaction snapshot should see 1 uncommitted row")))

        (.abort live-tx2)))))

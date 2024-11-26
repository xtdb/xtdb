(ns xtdb.indexer.live-index-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.indexer.live-index :as li]
            [xtdb.metadata :as meta]
            xtdb.node.impl
            [xtdb.serde :as serde]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.nio ByteBuffer]
           java.time.Duration
           [java.util Random UUID]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector FixedSizeBinaryVector]
           [org.apache.arrow.vector.ipc ArrowFileReader]
           (xtdb.api CompactorConfig IndexerConfig)
           xtdb.api.storage.Storage$InMemoryStorageFactory
           xtdb.api.log.Logs
           (xtdb.arrow Relation VectorPosition)
           xtdb.IBufferPool
           xtdb.indexer.live_index.ILiveIndex
           (xtdb.trie ArrowHashTrie ArrowHashTrie$Leaf HashTrie MemoryHashTrie MemoryHashTrie$Leaf)))

(def with-live-index
  (partial tu/with-system {:xtdb/allocator {}
                           :xtdb.metrics/registry nil
                           :xtdb.indexer/live-index (IndexerConfig.)
                           :xtdb/log (Logs/inMemoryLog)
                           :xtdb/buffer-pool Storage$InMemoryStorageFactory/INSTANCE
                           ::meta/metadata-manager {}
                           :xtdb/compactor (doto (CompactorConfig.)
                                             (.enabled false))}))

(t/use-fixtures :each tu/with-allocator with-live-index)

(t/deftest test-chunk
  (let [{^BufferAllocator allocator :xtdb/allocator
         ^IBufferPool buffer-pool :xtdb/buffer-pool
         ^ILiveIndex live-index :xtdb.indexer/live-index} tu/*sys*

        iids (let [rnd (Random. 0)]
               (repeatedly 12000 #(UUID. (.nextLong rnd) (.nextLong rnd))))

        iid-bytes (->> (sort-by #(.getMostSignificantBits ^UUID %) #(Long/compareUnsigned %1 %2) iids)
                       (mapv (comp vec util/uuid->bytes)))]

    (t/testing "commit"
      (let [live-idx-tx (.startTx live-index (serde/->TxKey 0 (.toInstant #inst "2000")))
            live-table-tx (.liveTable live-idx-tx "my-table")
            put-doc-wrt (.docWriter live-table-tx)]
        (let [wp (VectorPosition/build)]
          (doseq [^UUID iid iids]
            (.logPut live-table-tx (ByteBuffer/wrap (util/uuid->bytes iid)) 0 0
                     #(do
                        (.getPositionAndIncrement wp)
                        (.writeObject put-doc-wrt {:some :doc})))))

        (.commit live-idx-tx)

        (let [live-table (.liveTable live-index "my-table")
              live-rel (li/live-rel live-table)
              iid-vec (.getVector (.colWriter live-rel "_iid"))

              ^MemoryHashTrie trie (li/live-trie live-table)]

          (t/is (= iid-bytes
                   (->> (.getLeaves (.compactLogs trie))
                        (mapcat (fn [^MemoryHashTrie$Leaf leaf]
                                  (mapv #(vec (.getObject iid-vec %)) (.getData leaf))))))))))

    (t/testing "finish chunk"
      (li/finish-chunk! live-index)

      (let [trie-buf (.getByteBuffer buffer-pool (util/->path "tables/my-table/meta/log-l00-fr00-nr32ee0-rs2ee0.arrow"))
            leaf-buf (.getByteBuffer buffer-pool (util/->path "tables/my-table/data/log-l00-fr00-nr32ee0-rs2ee0.arrow"))]
        (util/with-open [trie-loader (Relation/loader allocator (util/->seekable-byte-channel trie-buf))
                         trie-rel (Relation. allocator (.getSchema trie-loader))
                         leaf-rdr (ArrowFileReader. (util/->seekable-byte-channel leaf-buf) allocator)]
          (let [iid-vec (.getVector (.getVectorSchemaRoot leaf-rdr) "_iid")]
            (.loadBatch trie-loader 0 trie-rel)
            (t/is (= iid-bytes
                     (->> (.getLeaves (ArrowHashTrie. (.get trie-rel "nodes")))
                          (mapcat (fn [^ArrowHashTrie$Leaf leaf]
                                    ;; would be good if ArrowFileReader accepted a page-idx...
                                    (.loadRecordBatch leaf-rdr (.get (.getRecordBlocks leaf-rdr) (.getDataPageIndex leaf)))

                                    (->> (range 0 (.getValueCount iid-vec))
                                         (mapv #(vec (.getObject iid-vec %)))))))))))))))

(deftest test-bucket-for
  (let [uuid1 #uuid "ce33e4b8-ec2f-4b80-8e9c-a4314005adbf"]
    (with-open [allocator (RootAllocator.)
                iid-vec (doto ^FixedSizeBinaryVector (FixedSizeBinaryVector. "iid" allocator 16)
                          (.setSafe 0 (util/uuid->bytes uuid1))
                          (.setValueCount 1))]
      (t/is (= [3 0 3 2 0 3 0 3 3 2 1 0 2 3 2 0]
               (for [x (range 16)]
                 (HashTrie/bucketFor (.getDataPointer iid-vec 0) x))))

      (t/is (= 3 (HashTrie/bucketFor (.getDataPointer iid-vec 0) 18)))
      (t/is (= 0 (HashTrie/bucketFor (.getDataPointer iid-vec 0) 30)))
      (t/is (= 3 (HashTrie/bucketFor (.getDataPointer iid-vec 0) 63))))))

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

      (util/with-open [node (tu/->local-node {:node-dir node-dir})]
        (let [^IBufferPool bp (tu/component node :xtdb/buffer-pool)]

          (let [{:keys [tx-id]} (last (for [tx-ops txs] (xt/execute-tx node tx-ops)))]
            (tu/then-await-tx tx-id node (Duration/ofSeconds 2)))

          (tu/finish-chunk! node)

          (t/is (= (mapv util/->path ["tables/public$foo/data/log-l00-fr00-nr110-rs5.arrow"])
                   (.listObjects bp (util/->path "tables/public$foo/data"))))

          (t/is (= (mapv util/->path ["tables/public$foo/meta/log-l00-fr00-nr110-rs5.arrow"])
                   (.listObjects bp (util/->path "tables/public$foo/meta")))))

        (tj/check-json (.toPath (io/as-file (io/resource "xtdb/indexer-test/can-build-live-index")))
                       (.resolve node-dir "objects"))))))

(t/deftest test-new-table-discarded-on-abort-2721
  (let [{^ILiveIndex live-index :xtdb.indexer/live-index} tu/*sys*]

    (let [live-tx0 (.startTx live-index #xt/tx-key {:tx-id 0, :system-time #time/instant "2020-01-01T00:00:00Z"})
          foo-table-tx (.liveTable live-tx0 "foo")
          doc-wtr (.docWriter foo-table-tx)]
      (.logPut foo-table-tx (ByteBuffer/allocate 16) 0 0
               (fn []
                 (.startStruct doc-wtr)
                 (.endStruct doc-wtr)))
      (.commit live-tx0))

    (t/testing "aborting bar means it doesn't get added to the committed live-index")
    (let [live-tx1 (.startTx live-index #xt/tx-key {:tx-id 0, :system-time #time/instant "2020-01-02T00:00:00Z"})
          bar-table-tx (.liveTable live-tx1 "bar")
          doc-wtr (.docWriter bar-table-tx)]
      (.logPut bar-table-tx (ByteBuffer/allocate 16) 0 0
               (fn []
                 (.startStruct doc-wtr)
                 (.endStruct doc-wtr)))

      (t/testing "doesn't get added in the tx either"
        (t/is (nil? (.liveTable live-index "bar"))))

      (.abort live-tx1)

      (t/is (some? (.liveTable live-index "foo")))
      (t/is (nil? (.liveTable live-index "bar"))))

    (t/testing "aborting foo doesn't clear it from the live-index"
      (let [live-tx2 (.startTx live-index #xt/tx-key {:tx-id 0, :system-time #time/instant "2020-01-03T00:00:00Z"})
            foo-table-tx (.liveTable live-tx2 "foo")
            doc-wtr (.docWriter foo-table-tx)]
        (.logPut foo-table-tx (ByteBuffer/allocate 16) 0 0
                 (fn []
                   (.startStruct doc-wtr)
                   (.endStruct doc-wtr)))
        (.abort live-tx2))

      (t/is (some? (.liveTable live-index "foo"))))

    (t/testing "committing bar after an abort adds it correctly"
      (let [live-tx3 (.startTx live-index #xt/tx-key {:tx-id 0, :system-time #time/instant "2020-01-04T00:00:00Z"})
            bar-table-tx (.liveTable live-tx3 "bar")
            doc-wtr (.docWriter bar-table-tx)]
        (.logPut bar-table-tx (ByteBuffer/allocate 16) 0 0
                 (fn []
                   (.startStruct doc-wtr)
                   (.endStruct doc-wtr)))
        (.commit live-tx3))

      (t/is (some? (.liveTable live-index "bar"))))))

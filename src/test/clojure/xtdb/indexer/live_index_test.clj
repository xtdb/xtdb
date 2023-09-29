(ns xtdb.indexer.live-index-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.api.protocols :as xtp]
            [xtdb.indexer.live-index :as li]
            [xtdb.object-store :as os]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.nio ByteBuffer]
           java.time.Duration
           [java.util Random UUID]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector FixedSizeBinaryVector]
           [org.apache.arrow.vector.ipc ArrowFileReader]
           xtdb.indexer.live_index.ILiveIndex
           xtdb.object_store.ObjectStore
           (xtdb.trie ArrowHashTrie ArrowHashTrie$Leaf HashTrie LiveHashTrie LiveHashTrie$Leaf)
           xtdb.vector.IVectorPosition))

(def with-live-index
  (partial tu/with-system {:xtdb/allocator {}
                           :xtdb.indexer/live-index {}
                           :xtdb.object-store/memory-object-store {}}))

(t/use-fixtures :each tu/with-allocator with-live-index)

(t/deftest test-chunk
  (let [{^BufferAllocator allocator :xtdb/allocator
         ^ILiveIndex live-index :xtdb.indexer/live-index
         ^ObjectStore obj-store :xtdb.object-store/memory-object-store} tu/*sys*

        iids (let [rnd (Random. 0)]
               (repeatedly 12000 #(UUID. (.nextLong rnd) (.nextLong rnd))))

        iid-bytes (->> (sort-by #(.getMostSignificantBits ^UUID %) #(Long/compareUnsigned %1 %2) iids)
                       (mapv (comp vec util/uuid->bytes)))]

    (t/testing "commit"
      (let [live-idx-tx (.startTx live-index (xtp/->TransactionInstant 0 (.toInstant #inst "2000")))
            live-table-tx (.liveTable live-idx-tx "my-table")]
        (let [wp (IVectorPosition/build)]
          (doseq [^UUID iid iids]
            (.logPut live-table-tx (ByteBuffer/wrap (util/uuid->bytes iid)) 0 0
                     #(.getPositionAndIncrement wp))))

        (.commit live-idx-tx)

        (let [live-table (.liveTable live-index "my-table")
              live-rel (li/live-rel live-table)
              iid-vec (.getVector (.writerForName live-rel "xt$iid"))

              ^LiveHashTrie trie (li/live-trie live-table)]

          (t/is (= iid-bytes
                   (->> (.leaves (.compactLogs trie))
                        (mapcat (fn [^LiveHashTrie$Leaf leaf]
                                  (mapv #(vec (.getObject iid-vec %)) (.data leaf))))))))))

    (t/testing "finish chunk"
      (.finishChunk live-index 0 12000)

      (let [trie-buf @(.getObject obj-store "tables/my-table/log-tries/trie-l00-cf00-ct32ee0.arrow")
            leaf-buf @(.getObject obj-store "tables/my-table/log-leaves/leaf-l00-cf00-ct32ee0.arrow")]
        (with-open [trie-rdr (ArrowFileReader. (util/->seekable-byte-channel trie-buf) allocator)
                    leaf-rdr (ArrowFileReader. (util/->seekable-byte-channel leaf-buf) allocator)]
          (let [trie-root (.getVectorSchemaRoot trie-rdr)
                iid-vec (.getVector (.getVectorSchemaRoot leaf-rdr) "xt$iid")]
            (.loadNextBatch trie-rdr)
            (t/is (= iid-bytes
                     (->> (.leaves (ArrowHashTrie/from trie-root))
                          (mapcat (fn [^ArrowHashTrie$Leaf leaf]
                                    ;; would be good if ArrowFileReader accepted a page-idx...
                                    (.loadRecordBatch leaf-rdr (.get (.getRecordBlocks leaf-rdr) (.getPageIndex leaf)))

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
      (t/is (= 3 (HashTrie/bucketFor (.getDataPointer iid-vec 0) 63)))
      (t/is (= 0 (HashTrie/bucketFor (.getDataPointer iid-vec 0) 104))))))

(def txs
  [[[:put :hello {:xt/id #uuid "cb8815ee-85f7-4c61-a803-2ea1c949cf8d" :a 1}]
    [:put :world {:xt/id #uuid "424f5622-c826-4ded-a5db-e2144d665c38" :b 2}]]
   [[:delete :hello #uuid "cb8815ee-85f7-4c61-a803-2ea1c949cf8d"]
    [:put :world {:xt/id #uuid "424f5622-c826-4ded-a5db-e2144d665c38" :b 3}]]
   [[:evict :world #uuid "424f5622-c826-4ded-a5db-e2144d665c38"]]
   ;; sql
   [[:sql "INSERT INTO foo (xt$id, bar, toto) VALUES (1, 1, 'toto')"]
    [:sql "UPDATE foo SET bar = 2 WHERE foo.xt$id = 1"]
    [:sql "DELETE FROM foo WHERE foo.bar = 2"]
    [:sql "INSERT INTO foo (xt$id, bar) VALUES (2, 2)"]]
   ;; sql evict
   [[:sql "ERASE FROM foo WHERE foo.xt$id = 2"]]
   ;; abort
   [[:sql "INSERT INTO foo (xt$id, xt$valid_from, xt$valid_to) VALUES (1, DATE '2020-01-01', DATE '2019-01-01')"]]])

(t/deftest can-build-live-index
  (let [node-dir (util/->path "target/can-build-live-index")]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (let [^ObjectStore os (tu/component node ::os/file-system-object-store)]

        (let [last-tx-key (last (for [tx-ops txs] (xt/submit-tx node tx-ops)))]
          (tu/then-await-tx last-tx-key node (Duration/ofSeconds 2)))

        (tu/finish-chunk! node)

        (t/is (= ["tables/foo/log-leaves/leaf-l00-cf00-ct110.arrow"]
                 (.listObjects os "tables/foo/log-leaves")))

        (t/is (= ["tables/foo/log-tries/trie-l00-cf00-ct110.arrow"]
                 (.listObjects os "tables/foo/log-tries"))))

      (tj/check-json (.toPath (io/as-file (io/resource "xtdb/indexer-test/can-build-live-index")))
                     (.resolve node-dir "objects")))))

(t/deftest test-new-table-discarded-on-abort-2721
  (let [{^ILiveIndex live-index :xtdb.indexer/live-index} tu/*sys*]

    (let [live-tx0 (.startTx live-index #xt/tx-key {:tx-id 0, :system-time #time/instant "2020-01-01T00:00:00Z"})
          foo-table-tx (.liveTable live-tx0 "foo")]
      (.logPut foo-table-tx (ByteBuffer/allocate 16) 0 0 #())
      (.commit live-tx0))

    (t/testing "aborting bar means it doesn't get added to the committed live-index")
    (let [live-tx1 (.startTx live-index #xt/tx-key {:tx-id 0, :system-time #time/instant "2020-01-02T00:00:00Z"})
          bar-table-tx (.liveTable live-tx1 "bar")]
      (.logPut bar-table-tx (ByteBuffer/allocate 16) 0 0 #())

      (t/testing "doesn't get added in the tx either"
        (t/is (nil? (.liveTable live-index "bar"))))

      (.abort live-tx1)

      (t/is (some? (.liveTable live-index "foo")))
      (t/is (nil? (.liveTable live-index "bar"))))

    (t/testing "aborting foo doesn't clear it from the live-index"
      (let [live-tx2 (.startTx live-index #xt/tx-key {:tx-id 0, :system-time #time/instant "2020-01-03T00:00:00Z"})
            foo-table-tx (.liveTable live-tx2 "foo")]
        (.logPut foo-table-tx (ByteBuffer/allocate 16) 0 0 #())
        (.abort live-tx2))

      (t/is (some? (.liveTable live-index "foo"))))

    (t/testing "committing bar after an abort adds it correctly"
      (let [live-tx3 (.startTx live-index #xt/tx-key {:tx-id 0, :system-time #time/instant "2020-01-04T00:00:00Z"})
            bar-table-tx (.liveTable live-tx3 "bar")]
        (.logPut bar-table-tx (ByteBuffer/allocate 16) 0 0 #())
        (.commit live-tx3))

      (t/is (some? (.liveTable live-index "bar"))))))

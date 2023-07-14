(ns xtdb.indexer.live-index-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.api.protocols :as xtp]
            [xtdb.indexer.live-index :as li]
            [xtdb.object-store :as os]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import java.time.Duration
           [java.util Random UUID]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector FixedSizeBinaryVector ValueVector]
           [org.apache.arrow.vector.ipc ArrowFileReader]
           xtdb.indexer.live_index.ILiveIndex
           xtdb.object_store.ObjectStore
           (xtdb.trie TrieKeys ArrowHashTrie ArrowHashTrie$Node ArrowHashTrie$NodeVisitor LiveTrie LiveTrie$Node LiveTrie$NodeVisitor)
           xtdb.vector.IRowCopier
           xtdb.vector.IWriterPosition))

(def with-live-index
  (tu/with-system {:xtdb/allocator {}
                   :xtdb.indexer/live-index {}
                   :xtdb.object-store/memory-object-store {}}))

(t/use-fixtures :each tu/with-allocator with-live-index)

(deftype LiveTrieRenderer [^ValueVector iid-vec]
  LiveTrie$NodeVisitor
  (visitBranch [this branch]
    (into [] (mapcat #(some-> ^LiveTrie$Node % (.accept this))) (.children branch)))

  (visitLeaf [_ leaf]
    (mapv #(vec (.getObject iid-vec %)) (.data leaf))))

(deftype ArrowTrieRenderer [^ArrowFileReader leaf-rdr, ^ValueVector iid-vec,
                            ^:unsychronized-mutable ^int current-page-idx]
  ArrowHashTrie$NodeVisitor
  (visitBranch [this branch]
    (mapcat #(some-> ^ArrowHashTrie$Node % (.accept this)) (.getChildren branch)))

  (visitLeaf [_ leaf]
    ;; would be good if ArrowFileReader accepted a page-idx...
    (.loadRecordBatch leaf-rdr (.get (.getRecordBlocks leaf-rdr) (.getPageIndex leaf)))

    (->> (range 0 (.getValueCount iid-vec))
         (mapv #(vec (.getObject iid-vec %))))))

(t/deftest test-chunk
  (let [{^BufferAllocator allocator :xtdb/allocator
         ^ILiveIndex live-index :xtdb.indexer/live-index
         ^ObjectStore obj-store :xtdb.object-store/memory-object-store} tu/*sys*

        iids (let [rnd (Random. 0)]
               (repeatedly 12000 #(UUID. (.nextLong rnd) (.nextLong rnd))))

        iid-bytes (->> (sort-by #(.getMostSignificantBits ^UUID %) #(Long/compareUnsigned %1 %2) iids)
                       (mapv (comp vec util/uuid->bytes)))]

    (t/testing "commit"
      (util/with-open [live-idx-tx (.startTx live-index (xtp/->TransactionInstant 0 (.toInstant #inst "2000")))
                       live-table-tx (.liveTable live-idx-tx "my-table")]
        (let [wp (IWriterPosition/build)]
          (doseq [iid iids]
            (.logPut live-table-tx (util/uuid->bytes iid) 0 0
                     (reify IRowCopier
                       (copyRow [_ _idx]
                         (.getPositionAndIncrement wp)))
                     0)))

        (.commit live-idx-tx)

        (let [live-table (.liveTable live-index "my-table")
              live-rel (li/live-rel live-table)
              iid-vec (.getVector (.writerForName live-rel "xt$iid"))

              ^LiveTrie trie (li/live-trie live-table)]

          (t/is (= iid-bytes (-> (.compactLogs trie)
                                 (.accept (LiveTrieRenderer. iid-vec))))))))

    (t/testing "finish chunk"
      (.finishChunk live-index 0)

      (let [trie-buf @(.getObject obj-store "tables/my-table/chunks/trie-c00.arrow")
            leaf-buf @(.getObject obj-store "tables/my-table/chunks/leaf-c00.arrow")]
        (with-open [trie-rdr (ArrowFileReader. (util/->seekable-byte-channel trie-buf) allocator)
                    leaf-rdr (ArrowFileReader. (util/->seekable-byte-channel leaf-buf) allocator)]
          (.loadNextBatch trie-rdr)
          (t/is (= iid-bytes
                   (.accept (ArrowHashTrie/from (.getVectorSchemaRoot trie-rdr))
                            (ArrowTrieRenderer. leaf-rdr (.getVector (.getVectorSchemaRoot leaf-rdr) "xt$iid") -1)))))))))

(t/deftest test-bucket-for
  (let [uuid1 #uuid "7f30ffff-ffff-ffff-0000-000000000000"]
    (with-open [allocator (RootAllocator.)
                iid-vec (doto ^FixedSizeBinaryVector (FixedSizeBinaryVector. "iid" allocator 8)
                          (.setSafe 0 (util/uuid->bytes uuid1))
                          (.setValueCount 1))]
      (let [trie-keys (TrieKeys. iid-vec)]
        (t/is (= (.bucketFor trie-keys 0 0) 7))
        (t/is (= (.bucketFor trie-keys 0 1) 15))
        (t/is (= (.bucketFor trie-keys 0 2) 3))))))

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
      (let [^ObjectStore os  (tu/component node ::os/file-system-object-store)]

        (let [last-tx-key (last (for [tx-ops txs] (xt/submit-tx node tx-ops)))]
          (tu/then-await-tx last-tx-key node (Duration/ofSeconds 2)))

        (tu/finish-chunk! node)

        (t/is (= ["tables/foo/chunks/leaf-c00.arrow" "tables/foo/chunks/trie-c00.arrow"]
                 (.listObjects os "tables/foo/chunks"))))

      (tj/check-json (.toPath (io/as-file (io/resource "xtdb/indexer-test/can-build-live-index")))
                     (.resolve node-dir "objects")))))

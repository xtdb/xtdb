(ns xtdb.indexer.live-tries-test
  (:require [clojure.test :as t]
            [xtdb.api.protocols :as xtp]
            xtdb.indexer.live-tries
            xtdb.object-store
            [xtdb.test-util :as tu]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import [java.util Random UUID]
           [org.apache.arrow.memory BufferAllocator]
           [org.apache.arrow.vector ValueVector]
           [org.apache.arrow.vector.ipc ArrowFileReader]
           xtdb.indexer.live_tries.ILiveTries
           xtdb.object_store.ObjectStore
           (xtdb.trie ArrowHashTrie HashTrie HashTrie$Visitor)))

(def with-live-tries
  (tu/with-system {:xtdb/allocator {}
                   :xtdb.indexer/live-tries {}
                   :xtdb.object-store/memory-object-store {}}))

(t/use-fixtures :each with-live-tries)

(deftype TrieRenderer [^ArrowFileReader leaf-rdr, ^ValueVector iid-vec,
                       ^:unsychronized-mutable ^int current-page-idx]
  HashTrie$Visitor
  (visitBranch [this children]
    (mapcat #(.accept ^HashTrie % this) children))

  (visitLeaf [this page-idx idxs]
    (when (and leaf-rdr (not= current-page-idx page-idx))
      ;; would be good if ArrowFileReader accepted a page-idx...
      (.loadRecordBatch leaf-rdr (.get (.getRecordBlocks leaf-rdr) page-idx)))

    (set! (.current-page-idx this) page-idx)

    (->> (or idxs (range 0 (.getValueCount iid-vec)))
         (mapv #(vec (.getObject iid-vec %))))))

(t/deftest test-t1-chunk
  (let [{^BufferAllocator allocator :xtdb/allocator
         ^ILiveTries live-tries :xtdb.indexer/live-tries
         ^ObjectStore obj-store :xtdb.object-store/memory-object-store} tu/*sys*

        iids (let [rnd (Random. 0)]
               (repeatedly 12000 #(UUID. (.nextLong rnd) (.nextLong rnd))))

        iid-bytes (->> (sort-by #(.getMostSignificantBits ^UUID %) #(Long/compareUnsigned %1 %2) iids)
                       (mapv (comp vec util/uuid->bytes)))]

    (t/testing "commit"
      (util/with-open [live-tx (.startTx live-tries)]
        (let [live-trie-tx (.liveTrie live-tx "my-trie")
              wtr (.relationWriter live-trie-tx)
              iid-wtr (.writerForName wtr "xt$iid")]

          (doseq [iid iids]
            (let [pos (.getPosition (.writerPosition wtr))]
              (vw/write-value! iid iid-wtr)
              (.endRow wtr)
              (.addRow live-trie-tx pos)))

          (.commit live-trie-tx)

          (let [{:keys [static-rel !static-trie]} live-trie-tx
                iid-vec (-> (vw/rel-wtr->rdr static-rel)
                            (.vectorForName "xt$iid")
                            (.getVector))
                ^HashTrie trie @!static-trie]

            (t/is (= iid-bytes (.accept trie (TrieRenderer. nil iid-vec -1))))))))

    (t/testing "finish chunk"
      (.finishChunk live-tries 0)

      (let [trie-buf @(.getObject obj-store "my-trie/trie-c00.arrow")
            leaf-buf @(.getObject obj-store "my-trie/leaf-c00.arrow")]
        (with-open [trie-rdr (ArrowFileReader. (util/->seekable-byte-channel trie-buf) allocator)
                    leaf-rdr (ArrowFileReader. (util/->seekable-byte-channel leaf-buf) allocator)]
          (.loadNextBatch trie-rdr)
          (t/is (= iid-bytes
                   (.accept (ArrowHashTrie/from (.getVectorSchemaRoot trie-rdr))
                            (TrieRenderer. leaf-rdr (.getVector (.getVectorSchemaRoot leaf-rdr) "xt$iid") -1)))))))))

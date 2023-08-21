(ns xtdb.indexer.live-table-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.api.protocols :as xtp]
            [xtdb.indexer.live-index :as live-index]
            [xtdb.object-store-test :as obj-store-test]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.nio ByteBuffer)
           (java.util Arrays)
           (org.apache.arrow.memory RootAllocator)
           (xtdb.indexer.live_index ILiveTable TestLiveTable)
           xtdb.vector.IVectorPosition
           (xtdb.trie LiveHashTrie LiveHashTrie$Leaf)))

(defn uuid-equal-to-path? [uuid path]
  (Arrays/equals
    (util/uuid->bytes uuid)
    (byte-array
      (map (fn [[x y]]
             (bit-or (bit-shift-left x 4) y))
           (partition-all 2 (vec path))))))

(deftest test-live-trie-can-overflow-log-limit-at-max-depth
  (binding [*print-length* 100]
    (let [uuid #uuid "7fffffff-ffff-ffff-7fff-ffffffffffff"
          n 1000]
      (tu/with-tmp-dirs #{path}
        (with-open [obj-store (obj-store-test/fs path)
                    allocator (RootAllocator.)
                    live-table ^ILiveTable (live-index/->live-table
                                            allocator obj-store "foo" {:->live-trie (partial live-index/->live-trie 2 4)})
                    live-table-tx (.startTx
                                   live-table (xtp/->TransactionInstant 0 (.toInstant #inst "2000")))]

          (let [wp (IVectorPosition/build)]
            (dotimes [_n n]
              (.logPut
               live-table-tx (ByteBuffer/wrap (util/uuid->bytes uuid))
               0 0 #(.getPositionAndIncrement wp))))

          (.commit live-table-tx)

          (let [leaves (.leaves (.compactLogs ^LiveHashTrie (.live-trie ^TestLiveTable live-table)))
                leaf ^LiveHashTrie$Leaf (first leaves)]

            (t/is (= 1 (count leaves)))

            (t/is
             (uuid-equal-to-path?
              uuid
              (.path leaf)))

            (t/is (= (reverse (range n))
                     (->> leaf
                          (.data)
                          (vec)))))

          @(.finishChunk live-table 0)

          (tu/with-allocator
            #(tj/check-json
              (.toPath (io/as-file (io/resource "xtdb/live-table-test/max-depth-trie-s")))
              path)))))

    (let [uuid #uuid "7fffffff-ffff-ffff-7fff-ffffffffffff"
          n 50000]
      (tu/with-tmp-dirs #{path}
        (with-open [obj-store (obj-store-test/fs path)
                    allocator (RootAllocator.)
                    live-table ^ILiveTable (live-index/->live-table
                                            allocator obj-store "foo")
                    live-table-tx (.startTx
                                   live-table (xtp/->TransactionInstant 0 (.toInstant #inst "2000")))]

          (let [wp (IVectorPosition/build)]
            (dotimes [_n n]
              (.logPut
               live-table-tx (ByteBuffer/wrap (util/uuid->bytes uuid))
               0 0 #(.getPositionAndIncrement wp))))

          (.commit live-table-tx)

          (let [leaves (.leaves (.compactLogs ^LiveHashTrie (.live-trie ^TestLiveTable live-table)))
                leaf ^LiveHashTrie$Leaf (first leaves)]

            (t/is (= 1 (count leaves)))

            (t/is
             (uuid-equal-to-path?
              uuid
              (.path leaf)))

            (t/is (= (reverse (range n))
                     (->> leaf
                          (.data)
                          (vec)))))

          @(.finishChunk live-table 0)

          (tu/with-allocator
            #(tj/check-json
              (.toPath (io/as-file (io/resource "xtdb/live-table-test/max-depth-trie-l")))
              path)))))))

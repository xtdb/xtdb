(ns xtdb.indexer.live-table-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.api.protocols :as xtp]
            [xtdb.indexer.live-index :as live-index]
            [xtdb.object-store-test :as obj-store-test]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            xtdb.watermark)
  (:import (java.nio ByteBuffer)
           (java.util Arrays HashMap)
           (org.apache.arrow.memory RootAllocator)
           (xtdb.indexer.live_index ILiveIndex  TestLiveTable)
           (xtdb.trie LiveHashTrie LiveHashTrie$Leaf)
           (xtdb.util RefCounter)
           xtdb.vector.IVectorPosition
           xtdb.watermark.ILiveTableWatermark))

(defn uuid-equal-to-path? [uuid path]
  (Arrays/equals
    (util/uuid->bytes uuid)
    (byte-array
      (map (fn [[x y]]
             (bit-or (bit-shift-left x 4) y))
           (partition-all 2 (vec path))))))

(deftest test-live-trie-can-overflow-log-limit-at-max-depth
  (binding [*print-length* 100]
    (let [uuid #uuid "7fffffff-ffff-ffff-4fff-ffffffffffff"
          n 1000]
      (tu/with-tmp-dirs #{path}
        (with-open [obj-store (obj-store-test/fs path)
                    allocator (RootAllocator.)
                    live-table (live-index/->live-table allocator obj-store "foo" {:->live-trie (partial live-index/->live-trie 2 4)})]

          (let [live-table-tx (.startTx live-table (xtp/->TransactionInstant 0 (.toInstant #inst "2000")) false)]
            (let [wp (IVectorPosition/build)]
              (dotimes [_n n]
                (.logPut live-table-tx (ByteBuffer/wrap (util/uuid->bytes uuid))
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

            @(.finishChunk live-table "c00")

            (tu/with-allocator
              #(tj/check-json
                (.toPath (io/as-file (io/resource "xtdb/live-table-test/max-depth-trie-s")))
                path))))))

    (let [uuid #uuid "7fffffff-ffff-ffff-4fff-ffffffffffff"
          n 50000]
      (tu/with-tmp-dirs #{path}
        (with-open [obj-store (obj-store-test/fs path)
                    allocator (RootAllocator.)
                    live-table (live-index/->live-table allocator obj-store "foo")]
          (let [live-table-tx (.startTx live-table (xtp/->TransactionInstant 0 (.toInstant #inst "2000")) false)]

            (let [wp (IVectorPosition/build)]
              (dotimes [_n n]
                (.logPut live-table-tx (ByteBuffer/wrap (util/uuid->bytes uuid)) 0 0 #(.getPositionAndIncrement wp))))

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

            @(.finishChunk live-table "c00")

            (tu/with-allocator
              #(tj/check-json
                (.toPath (io/as-file (io/resource "xtdb/live-table-test/max-depth-trie-l")))
                path))))))))

(defn live-table-wm->data [^ILiveTableWatermark live-table-wm]
  (let [live-rel-data (vr/rel->rows (.liveRelation live-table-wm))
        live-trie (.compactLogs (.liveTrie live-table-wm))
        live-trie-leaf-data (->> live-trie
                                 (.leaves)
                                 (mapcat #(.data ^LiveHashTrie$Leaf %))
                                 (vec))
        live-trie-iids (map
                         #(util/byte-buffer->uuid
                            (.getBytes
                              (.iidReader live-trie) %))
                         live-trie-leaf-data)]
    {:live-rel-data live-rel-data
     :live-trie-leaf-data live-trie-leaf-data
     :live-trie-iids live-trie-iids}))

(deftest test-live-table-watermarks-are-immutable
  (let [uuids [#uuid "7fffffff-ffff-ffff-4fff-ffffffffffff"]]
    (with-open [obj-store (obj-store-test/in-memory)
                allocator (RootAllocator.)
                live-table (live-index/->live-table allocator obj-store "foo")]
      (let [live-table-tx (.startTx live-table (xtp/->TransactionInstant 0 (.toInstant #inst "2000")) false)]

        (let [wp (IVectorPosition/build)]
          (doseq [uuid uuids]
            (.logPut
             live-table-tx (ByteBuffer/wrap (util/uuid->bytes uuid))
             0 0 #(.getPositionAndIncrement wp))))

        (.commit live-table-tx)

        (with-open [live-table-wm (.openWatermark live-table true)]
          (let [live-table-before (live-table-wm->data live-table-wm)]

            @(.finishChunk live-table "c00")
            (.close live-table)

            (let [live-table-after (live-table-wm->data live-table-wm)]

              (t/is (= (:live-trie-iids live-table-before)
                       (:live-trie-iids live-table-after)
                       uuids))

              (t/is (= live-table-before live-table-after)))))))))

(deftest test-live-index-watermarks-are-immutable
  (let [uuids [#uuid "7fffffff-ffff-ffff-4fff-ffffffffffff"]
        table-name "foo"]
    (with-open [obj-store (obj-store-test/in-memory)
                allocator (RootAllocator.)]
      (let [live-index-allocator (util/->child-allocator allocator "live-index")
            ^ILiveIndex live-index (live-index/->LiveIndex live-index-allocator obj-store (HashMap.) (RefCounter.) 64 1024)
            live-index-tx (.startTx live-index (xtp/->TransactionInstant 0 (.toInstant #inst "2000")))
            live-table-tx (.liveTable live-index-tx table-name)]

        (let [wp (IVectorPosition/build)]
          (doseq [uuid uuids]
            (.logPut
             live-table-tx (ByteBuffer/wrap (util/uuid->bytes uuid))
             0 0 #(.getPositionAndIncrement wp))))

        (.commit live-index-tx)

        (with-open [live-index-wm (.openWatermark live-index)]
          (let [live-table-before (live-table-wm->data (.liveTable live-index-wm table-name))]

            (.finishChunk live-index 0 10)
            (.close live-index)

            (let [live-table-after (live-table-wm->data (.liveTable live-index-wm table-name))]

              (t/is (= (:live-trie-iids live-table-before)
                       (:live-trie-iids live-table-after)
                       uuids))

              (t/is (= live-table-before live-table-after)))))))))

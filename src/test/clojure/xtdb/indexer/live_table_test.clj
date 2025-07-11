(ns xtdb.indexer.live-table-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.block-catalog :as block-cat]
            [xtdb.indexer.live-index :as li]
            [xtdb.node :as xtn]
            [xtdb.serde :as serde]
            [xtdb.table-catalog :as table-cat]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.trie :as trie]
            [xtdb.util :as util])
  (:import (java.nio ByteBuffer)
           (java.util Arrays HashMap)
           (java.util.concurrent.locks StampedLock)
           (org.apache.arrow.memory RootAllocator)
           xtdb.BufferPool
           xtdb.compactor.Compactor
           (xtdb.indexer LiveIndex LiveTable LiveTable$Watermark)
           (xtdb.trie MemoryHashTrie$Leaf)
           (xtdb.util RefCounter RowCounter)))

(t/use-fixtures :each tu/with-node)

(defn uuid-equal-to-path? [uuid path]
  (Arrays/equals
   (util/uuid->bytes uuid)
   (byte-array
    (map (fn [[p0 p1 p2 p3]]
           (-> p3
               (bit-or (bit-shift-left p2 2))
               (bit-or (bit-shift-left p1 4))
               (bit-or (bit-shift-left p0 6))))
         (partition-all 4 (vec path))))))

(deftest test-live-trie-can-overflow-log-limit-at-max-depth
  (binding [*print-length* 100]
    (let [uuid #uuid "7fffffff-ffff-ffff-4fff-ffffffffffff"
          n 1000]
      (tu/with-tmp-dirs #{path}
        (util/with-open [node (tu/->local-node {:node-dir path, :compactor-threads 0})
                         ^BufferPool bp (tu/component node :xtdb/buffer-pool)
                         allocator (RootAllocator.)
                         live-table (LiveTable. allocator bp #xt/table foo (RowCounter.) (partial trie/->live-trie 2 4))]

          (let [live-table-tx (.startTx live-table (serde/->TxKey 0 (.toInstant #inst "2000")) false)
                doc-wtr (.getDocWriter live-table-tx)]
            (dotimes [_n n]
              (.logPut live-table-tx (ByteBuffer/wrap (util/uuid->bytes uuid))
                       0 0
                       (fn []
                         (.endStruct doc-wtr))))

            (.commit live-table-tx)

            (let [leaves (.getLeaves (.compactLogs (.getLiveTrie live-table)))
                  leaf ^MemoryHashTrie$Leaf (first leaves)]

              (t/is (= 1 (count leaves)))

              (t/is (uuid-equal-to-path? uuid (.getPath leaf)))

              (t/is (= (reverse (range n))
                       (vec (.getData leaf)))))

            (.finishBlock live-table 0)

            (tu/with-allocator
              #(tj/check-json
                (.toPath (io/as-file (io/resource "xtdb/live-table-test/max-depth-trie-s")))
                (.resolve path "objects")))))))

    (let [uuid #uuid "7fffffff-ffff-ffff-4fff-ffffffffffff"
          n 50000]
      (tu/with-tmp-dirs #{path}
        (util/with-open [node (tu/->local-node {:node-dir path, :compactor-threads 0})
                         ^BufferPool bp (tu/component node :xtdb/buffer-pool)
                         allocator (RootAllocator.)
                         live-table (LiveTable. allocator bp #xt/table foo (RowCounter.))]
          (let [live-table-tx (.startTx live-table (serde/->TxKey 0 (.toInstant #inst "2000")) false)
                doc-wtr (.getDocWriter live-table-tx)]

            (dotimes [_n n]
              (.logPut live-table-tx (ByteBuffer/wrap (util/uuid->bytes uuid)) 0 0
                       (fn []
                         (.endStruct doc-wtr))))

            (.commit live-table-tx)

            (let [leaves (.getLeaves (.compactLogs (.getLiveTrie live-table)))
                  leaf ^MemoryHashTrie$Leaf (first leaves)]

              (t/is (= 1 (count leaves)))

              (t/is (uuid-equal-to-path? uuid (.getPath leaf)))

              (t/is (= (reverse (range n))
                       (vec (.getData leaf)))))

            (.finishBlock live-table 0)

            (tu/with-allocator
              #(tj/check-json
                (.toPath (io/as-file (io/resource "xtdb/live-table-test/max-depth-trie-l")))
                (.resolve path "objects")))))))))

(defn live-table-wm->data [^LiveTable$Watermark live-table-wm]
  (let [live-rel-data (.toMaps (.getLiveRelation live-table-wm))
        live-trie (.compactLogs (.getLiveTrie live-table-wm))
        live-trie-leaf-data (->> live-trie
                                 (.getLeaves)
                                 (mapcat #(.getData ^MemoryHashTrie$Leaf %))
                                 (vec))
        live-trie-iids (map #(util/byte-buffer->uuid (.getBytes (.getIidReader live-trie) %))
                            live-trie-leaf-data)]
    {:live-rel-data live-rel-data
     :live-trie-leaf-data live-trie-leaf-data
     :live-trie-iids live-trie-iids}))

(deftest test-live-table-watermarks-are-immutable
  (let [uuids [#uuid "7fffffff-ffff-ffff-4fff-ffffffffffff"]
        rc (RowCounter.)]
    (with-open [node (xtn/start-node (merge tu/*node-opts* {:compactor {:threads 0}}))
                ^BufferPool bp (tu/component node :xtdb/buffer-pool)
                allocator (RootAllocator.)
                live-table (LiveTable. allocator bp #xt/table foo rc)]
      (let [live-table-tx (.startTx live-table (serde/->TxKey 0 (.toInstant #inst "2000")) false)
            doc-wtr (.getDocWriter live-table-tx)]

        (doseq [uuid uuids]
          (.logPut live-table-tx (ByteBuffer/wrap (util/uuid->bytes uuid)) 0 0
                   (fn []
                     (.endStruct doc-wtr))))

        (.commit live-table-tx)

        (with-open [live-table-wm (.openWatermark live-table)]
          (let [live-table-before (live-table-wm->data live-table-wm)]

            (.finishBlock live-table 0)
            (.close live-table)

            (let [live-table-after (live-table-wm->data live-table-wm)]

              (t/is (= (:live-trie-iids live-table-before)
                       (:live-trie-iids live-table-after)
                       uuids))

              (t/is (= live-table-before live-table-after)))))))))

(deftest test-live-index-watermarks-are-immutable
  (let [uuids [#uuid "7fffffff-ffff-ffff-4fff-ffffffffffff"]
        table #xt/table foo]
    (util/with-open [allocator (RootAllocator.)]
      (let [^BufferPool bp (tu/component tu/*node* :xtdb/buffer-pool)
            block-cat (block-cat/<-node tu/*node*)
            log (tu/component tu/*node* :xtdb/log)
            table-catalog (table-cat/<-node tu/*node*)
            trie-catalog (tu/component tu/*node* :xtdb/trie-catalog)
            live-index-allocator (util/->child-allocator allocator "live-index")]
        (util/with-open [^LiveIndex live-index (li/->LiveIndex live-index-allocator bp log Compactor/NOOP
                                                               block-cat table-catalog trie-catalog

                                                               nil (HashMap.)
                                                               nil (StampedLock.)
                                                               (RefCounter.)
                                                               (RowCounter.) 102400
                                                               64 1024
                                                               [])]
          (let [tx-key (serde/->TxKey 0 (.toInstant #inst "2000"))
                live-index-tx (.startTx live-index tx-key)
                live-table-tx (.liveTable live-index-tx table)
                doc-wtr (.getDocWriter live-table-tx)]

            (doseq [uuid uuids]
              (.logPut live-table-tx (ByteBuffer/wrap (util/uuid->bytes uuid)) 0 0
                       (fn []
                         (.endStruct doc-wtr))))

            (.commit live-index-tx)

            (with-open [wm (.openWatermark live-index)]
              (let [live-index-wm (.getLiveIndex wm)
                    live-table-before (live-table-wm->data (.liveTable live-index-wm table))]

                (.finishBlock live-index 0)

                (let [live-table-after (live-table-wm->data (.liveTable live-index-wm table))]

                  (t/is (= (:live-trie-iids live-table-before)
                           (:live-trie-iids live-table-after)
                           uuids))

                  (t/is (= live-table-before live-table-after)))))))))))

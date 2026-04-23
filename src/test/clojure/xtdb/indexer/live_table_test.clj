(ns xtdb.indexer.live-table-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.arrow-edn-test :as aet]
            [xtdb.db-catalog :as db]
            [xtdb.node :as xtn]
            [xtdb.serde :as serde]
            [xtdb.test-util :as tu]
            [xtdb.trie :as trie]
            [xtdb.util :as util])
  (:import (java.nio ByteBuffer)
           (java.util Arrays HashMap)
           (java.util.concurrent.locks StampedLock)
           (org.apache.arrow.memory RootAllocator)
           (xtdb.api IndexerConfig)
           (xtdb.indexer LiveIndex LiveTable OpenTx$Table TableSnapshot)
           (xtdb.trie MemoryHashTrie$Leaf)
           (xtdb.util RefCounter RowCounter)))

(t/use-fixtures :each tu/with-allocator tu/with-node)

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
                         bp (.getBufferPool (db/primary-db node))
                         allocator (RootAllocator.)
                         live-table (LiveTable. allocator #xt/table foo (RowCounter.) (partial trie/->live-trie 2 4))]

          (with-open [open-tx-table (OpenTx$Table. #xt/table foo allocator 0)]
            (let [doc-wtr (.getDocWriter open-tx-table)]
              (dotimes [_n n]
                (.logPut open-tx-table (ByteBuffer/wrap (util/uuid->bytes uuid))
                         0 0
                         (fn []
                           (.endStruct doc-wtr))))

              (.importData live-table (.getTxRelation open-tx-table))

              (let [leaves (.getLeaves (.compactLogs (.getLiveTrie live-table)))
                    leaf ^MemoryHashTrie$Leaf (first leaves)]

                (t/is (= 1 (count leaves)))

                (t/is (uuid-equal-to-path? uuid (.getPath leaf)))

                (t/is (= (reverse (range n))
                         (vec (.getData leaf)))))

              (.finishBlock live-table bp 0)

              (aet/check-arrow-edn-dir (.toPath (io/as-file (io/resource "xtdb/live-table-test/max-depth-trie-s")))
                                       (.resolve path "objects")))))))

    (let [uuid #uuid "7fffffff-ffff-ffff-4fff-ffffffffffff"
          n 50000]
      (tu/with-tmp-dirs #{path}
        (util/with-open [node (tu/->local-node {:node-dir path, :compactor-threads 0})
                         bp (.getBufferPool (db/primary-db node))
                         allocator (RootAllocator.)
                         live-table (LiveTable. allocator #xt/table foo (RowCounter.))]
          (with-open [open-tx-table (OpenTx$Table. #xt/table foo allocator 0)]
            (let [doc-wtr (.getDocWriter open-tx-table)]

              (dotimes [_n n]
                (.logPut open-tx-table (ByteBuffer/wrap (util/uuid->bytes uuid)) 0 0
                         (fn []
                           (.endStruct doc-wtr))))

              (.importData live-table (.getTxRelation open-tx-table))

              (let [leaves (.getLeaves (.compactLogs (.getLiveTrie live-table)))
                    leaf ^MemoryHashTrie$Leaf (first leaves)]

                (t/is (= 1 (count leaves)))

                (t/is (uuid-equal-to-path? uuid (.getPath leaf)))

                (t/is (= (reverse (range n))
                         (vec (.getData leaf)))))

              (.finishBlock live-table bp 0)

              (aet/check-arrow-edn-dir (.toPath (io/as-file (io/resource "xtdb/live-table-test/max-depth-trie-l")))
                                       (.resolve path "objects")))))))))

(defn live-table-snap->data [^TableSnapshot live-table-snap]
  (let [live-rel-data (.getAsMaps (.getLiveRelation live-table-snap))
        live-trie (.compactLogs (.getLiveTrie live-table-snap))
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
    (util/with-open [node (xtn/start-node (merge tu/*node-opts* {:compactor {:threads 0}}))
                     bp (.getBufferPool (db/primary-db node))
                     allocator (RootAllocator.)
                     live-table (LiveTable. allocator #xt/table foo rc)]
      (let [open-tx-table (OpenTx$Table. #xt/table foo allocator 0)
            doc-wtr (.getDocWriter open-tx-table)]

        (doseq [uuid uuids]
          (.logPut open-tx-table (ByteBuffer/wrap (util/uuid->bytes uuid)) 0 0
                   (fn []
                     (.endStruct doc-wtr))))

        (.importData live-table (.getTxRelation open-tx-table))

        (util/with-open [live-table-snap (TableSnapshot/open allocator live-table nil)]
          (let [live-table-before (live-table-snap->data live-table-snap)]

            (.finishBlock live-table bp 0)
            (.close open-tx-table)

            (let [live-table-after (live-table-snap->data live-table-snap)]

              (t/is (= (:live-trie-iids live-table-before)
                       (:live-trie-iids live-table-after)
                       uuids))

              (t/is (= (util/->clj live-table-before) (util/->clj live-table-after))))))))))

(deftest test-live-index-watermarks-are-immutable
  (let [uuids [#uuid "7fffffff-ffff-ffff-4fff-ffffffffffff"]
        table #xt/table foo]
    (util/with-open [allocator (RootAllocator.)]
      (let [db (db/primary-db tu/*node*)
            bp (.getBufferPool db)
            block-cat (.getBlockCatalog db)
            table-catalog (.getTableCatalog db)
            live-index-allocator (util/->child-allocator allocator "live-index")]
        (util/with-open [live-index (LiveIndex/open live-index-allocator
                                                                        block-cat table-catalog
                                                                        "xtdb")]
          (with-open [open-tx (tu/->open-tx allocator tu/*node* (serde/->TxKey 0 (.toInstant #inst "2000")))]
            (let [open-tx-table (.table open-tx table)
                  doc-wtr (.getDocWriter open-tx-table)]

              (doseq [uuid uuids]
                (.logPut open-tx-table (ByteBuffer/wrap (util/uuid->bytes uuid)) 0 0
                         (fn []
                           (.endStruct doc-wtr))))

              (.commitTx live-index open-tx)

              (with-open [snap (.openSnapshot live-index)]
                (let [live-table-before (live-table-snap->data (.table snap table))]

                  (.finishBlock live-index bp 0)

                  (let [live-table-after (live-table-snap->data (.table snap table))]

                    (t/is (= (:live-trie-iids live-table-before)
                             (:live-trie-iids live-table-after)
                             uuids))

                    (t/is (= (util/->clj live-table-before)
                             (util/->clj live-table-after)))))))))))))

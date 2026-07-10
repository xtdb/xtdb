(ns xtdb.indexer.live-table-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.arrow-edn-test :as aet]
            [xtdb.db-catalog :as db]
            [xtdb.serde :as serde]
            [xtdb.test-util :as tu]
            [xtdb.trie :as trie]
            [xtdb.util :as util])
  (:import (java.nio ByteBuffer)
           (java.util HashMap)
           (java.util.concurrent.locks StampedLock)
           (org.apache.arrow.memory RootAllocator)
           (xtdb.api IndexerConfig)
           (xtdb.indexer LiveIndex LiveTable TableSnapshot)
           (xtdb.trie MemoryHashTrie$Leaf)
           (xtdb.util RefCounter RowCounter)))

(t/use-fixtures :each tu/with-allocator tu/with-node)

(defn- ->max-depth-puts
  "n puts all sharing one iid (a fresh ByteBuffer per row), at system/valid-time 0."
  [uuid n]
  (repeatedly n (fn [] {:iid (ByteBuffer/wrap (util/uuid->bytes uuid)), :valid-from 0, :valid-to 0, :doc nil})))

(deftest test-live-trie-can-overflow-log-limit-at-max-depth
  ;; every row shares one iid, so the trie bottoms out in a single max-depth leaf - that
  ;; trie-structure property is asserted in xtdb.indexer.LiveTableTest; here we pin the
  ;; finished block's on-disk arrow format, at small (custom trie) and large scale.
  (let [uuid #uuid "7fffffff-ffff-ffff-4fff-ffffffffffff"
        n 1000]
    (tu/with-tmp-dirs #{path}
      (util/with-open [node (tu/->local-node {:node-dir path, :compactor-threads 0})
                       bp (.getBufferPool (db/primary-db node))
                       allocator (RootAllocator.)
                       live-table (LiveTable. allocator #xt/table foo 0 (RowCounter.) (partial trie/->live-trie 2 4))]
        (util/with-open [rel (tu/open-put-log-rel allocator 0 (->max-depth-puts uuid n))]
          (.importData live-table rel))

        (.finishBlock live-table bp 0)

        (aet/check-arrow-edn-dir (.toPath (io/as-file (io/resource "xtdb/live-table-test/max-depth-trie-s")))
                                 (.resolve path "objects")))))

  (let [uuid #uuid "7fffffff-ffff-ffff-4fff-ffffffffffff"
        n 50000]
    (tu/with-tmp-dirs #{path}
      (util/with-open [node (tu/->local-node {:node-dir path, :compactor-threads 0})
                       bp (.getBufferPool (db/primary-db node))
                       allocator (RootAllocator.)
                       live-table (LiveTable. allocator #xt/table foo 0 (RowCounter.))]
        (util/with-open [rel (tu/open-put-log-rel allocator 0 (->max-depth-puts uuid n))]
          (.importData live-table rel))

        (.finishBlock live-table bp 0)

        (aet/check-arrow-edn-dir (.toPath (io/as-file (io/resource "xtdb/live-table-test/max-depth-trie-l")))
                                 (.resolve path "objects"))))))

(defn live-table-snap->data [^TableSnapshot live-table-snap]
  (let [live-rel-data (.getAsMaps (.getRelation live-table-snap))
        live-trie (.compactLogs (.getTrie live-table-snap))
        live-trie-leaf-data (->> live-trie
                                 (.getLeaves)
                                 (mapcat #(.getData ^MemoryHashTrie$Leaf %))
                                 (vec))
        live-trie-iids (map #(util/byte-buffer->uuid (.getBytes (.getIidReader live-trie) %))
                            live-trie-leaf-data)]
    {:live-rel-data live-rel-data
     :live-trie-leaf-data live-trie-leaf-data
     :live-trie-iids live-trie-iids}))

(deftest test-live-index-watermarks-are-immutable
  (let [uuids [#uuid "7fffffff-ffff-ffff-4fff-ffffffffffff"]
        table #xt/table foo]
    (util/with-open [allocator (RootAllocator.)]
      (let [db (db/primary-db tu/*node*)
            bp (.getBufferPool db)
            block-cat (.getBlockCatalog db)
            table-catalog (.getTableCatalog db)
            trie-catalog (.getTrieCatalog db)
            live-index-allocator (util/->child-allocator allocator "live-index")]
        (util/with-open [live-index (LiveIndex/open live-index-allocator
                                                                        block-cat table-catalog trie-catalog
                                                                        "xtdb")]
          (with-open [open-tx (tu/->open-tx allocator tu/*node* (serde/->TxKey 0 (.toInstant #inst "2000")))]
            (let [open-tx-table (.table open-tx (.getTableName table))
                  doc-wtr (.getPutDocWriter open-tx-table)]

              (doseq [uuid uuids]
                (.writeId open-tx-table uuid)
                (.writeValidTimeMicros open-tx-table 0 0)
                (.endStruct doc-wtr)
                (.endPut open-tx-table))

              (tu/commit-tx! live-index open-tx)

              (with-open [snap (.openSnapshot live-index)]
                (let [live-table-before (live-table-snap->data (first (.table snap table)))]

                  (.finishBlock live-index bp 0)

                  (let [live-table-after (live-table-snap->data (first (.table snap table)))]

                    (t/is (= (:live-trie-iids live-table-before)
                             (:live-trie-iids live-table-after)
                             uuids))

                    (t/is (= (util/->clj live-table-before)
                             (util/->clj live-table-after)))))))))))))

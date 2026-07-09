(ns xtdb.indexer.live-table-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.arrow-edn-test :as aet]
            [xtdb.db-catalog :as db]
            [xtdb.test-util :as tu]
            [xtdb.trie :as trie]
            [xtdb.util :as util])
  (:import (java.nio ByteBuffer)
           (org.apache.arrow.memory RootAllocator)
           (xtdb.indexer LiveTable)
           (xtdb.util RowCounter)))

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

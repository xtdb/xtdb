(ns xtdb.indexer.live-index-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.check-pbuf :as cpb]
            [xtdb.compactor :as c]
            [xtdb.log :as xt-log]
            xtdb.node.impl
            [xtdb.object-store :as os]
            [xtdb.protocols :as xtp]
            [xtdb.serde :as serde]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.nio ByteBuffer]
           [java.util Random UUID]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector FixedSizeBinaryVector]
           (xtdb.arrow Relation)
           xtdb.BufferPool
           xtdb.indexer.LiveIndex
           (xtdb.trie ArrowHashTrie ArrowHashTrie$Leaf HashTrie MemoryHashTrie$Leaf)))

(t/use-fixtures :each tu/with-allocator
  (tu/with-opts {:compactor {:threads 0}})
  tu/with-node)

(t/deftest test-block
  (let [^BufferAllocator allocator (tu/component tu/*node* :xtdb/allocator)
        ^BufferPool buffer-pool (tu/component tu/*node* :xtdb/buffer-pool)
        ^LiveIndex live-index (tu/component tu/*node* :xtdb.indexer/live-index)

        iids (let [rnd (Random. 0)]
               (repeatedly 12000 #(UUID. (.nextLong rnd) (.nextLong rnd))))

        iid-bytes (->> (sort-by #(.getMostSignificantBits ^UUID %) #(Long/compareUnsigned %1 %2) iids)
                       (mapv (comp vec util/uuid->bytes)))]

    (t/testing "commit"
      (let [live-idx-tx (.startTx live-index (serde/->TxKey 0 (.toInstant #inst "2000")))
            live-table-tx (.liveTable live-idx-tx #xt/table my-table)
            put-doc-wrt (.getDocWriter live-table-tx)]
        (doseq [^UUID iid iids]
          (.logPut live-table-tx (ByteBuffer/wrap (util/uuid->bytes iid)) 0 0
                   #(.writeObject put-doc-wrt {:some :doc})))

        (.commit live-idx-tx)

        (let [live-table (.liveTable live-index #xt/table my-table)
              live-rel (.getLiveRelation live-table)
              iid-vec (.getVector (.vectorFor live-rel "_iid"))

              trie (.getLiveTrie live-table)]

          (t/is (= iid-bytes
                   (->> (.getLeaves (.compactLogs trie))
                        (mapcat (fn [^MemoryHashTrie$Leaf leaf]
                                  (mapv #(vec (.getObject iid-vec %)) (.getData leaf))))))))))

    (t/testing "finish block"
      (tu/finish-block! tu/*node*)

      (let [trie-ba (.getByteArray buffer-pool (util/->path "tables/public$my-table/meta/l00-rc-b00.arrow"))
            leaf-ba (.getByteArray buffer-pool (util/->path "tables/public$my-table/data/l00-rc-b00.arrow"))]
        (util/with-open [trie-loader (Relation/loader allocator (util/->seekable-byte-channel (ByteBuffer/wrap trie-ba)))
                         trie-rel (Relation/open allocator (.getSchema trie-loader))
                         leaf-loader (Relation/loader allocator (util/->seekable-byte-channel (ByteBuffer/wrap leaf-ba)))
                         leaf-rel (Relation/open allocator (.getSchema leaf-loader))]
          (let [iid-vec (.vectorFor leaf-rel "_iid")]
            (.loadPage trie-loader 0 trie-rel)
            (t/is (= iid-bytes
                     (->> (.getLeaves (ArrowHashTrie. (.get trie-rel "nodes")))
                          (mapcat (fn [^ArrowHashTrie$Leaf leaf]
                                    (.loadPage leaf-loader (.getDataPageIndex leaf) leaf-rel)

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
        (let [^BufferPool bp (tu/component node :xtdb/buffer-pool)]

          (doseq [tx-ops txs]
            (try
              (xt/execute-tx node tx-ops
                             {:default-tz #xt/zone "Europe/London"})
              (catch Exception _)))

          (tu/flush-block! node)

          (t/is (= [(os/->StoredObject "tables/public$foo/data/l00-rc-b00.arrow" 2558)]
                   (.listAllObjects bp (util/->path "tables/public$foo/data"))))

          (t/is (= [(os/->StoredObject "tables/public$foo/meta/l00-rc-b00.arrow" 3966)]
                   (.listAllObjects bp (util/->path "tables/public$foo/meta")))))

        (tj/check-json (.toPath (io/as-file (io/resource "xtdb/indexer-test/can-build-live-index")))
                       (.resolve node-dir "objects"))

        (cpb/check-pbuf (.toPath (io/as-file (io/resource "xtdb/indexer-test/can-build-live-index")))
                        (.resolve node-dir "objects"))))))

(t/deftest test-new-table-discarded-on-abort-2721
  (let [^LiveIndex live-index (tu/component tu/*node* :xtdb.indexer/live-index)]

    (let [live-tx0 (.startTx live-index #xt/tx-key {:tx-id 0, :system-time #xt/instant "2020-01-01T00:00:00Z"})
          foo-table-tx (.liveTable live-tx0 #xt/table foo)
          doc-wtr (.getDocWriter foo-table-tx)]
      (.logPut foo-table-tx (ByteBuffer/allocate 16) 0 0
               (fn []
                 (.endStruct doc-wtr)))
      (.commit live-tx0))

    (t/testing "aborting bar means it doesn't get added to the committed live-index")
    (let [live-tx1 (.startTx live-index #xt/tx-key {:tx-id 0, :system-time #xt/instant "2020-01-02T00:00:00Z"})
          bar-table-tx (.liveTable live-tx1 #xt/table bar)
          doc-wtr (.getDocWriter bar-table-tx)]
      (.logPut bar-table-tx (ByteBuffer/allocate 16) 0 0
               (fn []
                 (.endStruct doc-wtr)))

      (t/testing "doesn't get added in the tx either"
        (t/is (nil? (.liveTable live-index #xt/table bar))))

      (.abort live-tx1)

      (t/is (some? (.liveTable live-index #xt/table foo)))
      (t/is (nil? (.liveTable live-index #xt/table bar))))

    (t/testing "aborting foo doesn't clear it from the live-index"
      (let [live-tx2 (.startTx live-index #xt/tx-key {:tx-id 0, :system-time #xt/instant "2020-01-03T00:00:00Z"})
            foo-table-tx (.liveTable live-tx2 #xt/table foo)
            doc-wtr (.getDocWriter foo-table-tx)]
        (.logPut foo-table-tx (ByteBuffer/allocate 16) 0 0
                 (fn []
                   (.endStruct doc-wtr)))
        (.abort live-tx2))

      (t/is (some? (.liveTable live-index #xt/table foo))))

    (t/testing "committing bar after an abort adds it correctly"
      (let [live-tx3 (.startTx live-index #xt/tx-key {:tx-id 0, :system-time #xt/instant "2020-01-04T00:00:00Z"})
            bar-table-tx (.liveTable live-tx3 #xt/table bar)
            doc-wtr (.getDocWriter bar-table-tx)]
        (.logPut bar-table-tx (ByteBuffer/allocate 16) 0 0
                 (fn []
                   (.endStruct doc-wtr)))
        (.commit live-tx3))

      (t/is (some? (.liveTable live-index #xt/table bar))))))

(t/deftest live-index-row-counter-reinitialization
  (binding [c/*ignore-signal-block?* true]
    (let [node-dir (util/->path "target/live-index-row-counter-reinitialization")]
      (util/delete-dir node-dir)

      (util/with-open [node (tu/->local-node {:node-dir node-dir})]
        (xt/execute-tx node [[:put-docs :docs {:xt/id 1 :foo 1}]])
        (tu/finish-block! node))

      (util/with-open [node (tu/->local-node {:node-dir node-dir})]
        (let [^BufferPool bp (tu/component node :xtdb/buffer-pool)]
          (xt/execute-tx node [[:put-docs :docs {:xt/id 1 :foo 1}]])
          (tu/finish-block! node)

          (t/is (= [(os/->StoredObject (util/->path "blocks/b00.binpb") 38)
                    (os/->StoredObject (util/->path "blocks/b01.binpb") 40)]
                   (.listAllObjects bp (util/->path "blocks")))))))))

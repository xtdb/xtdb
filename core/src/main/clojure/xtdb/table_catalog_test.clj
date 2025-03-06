(ns xtdb.table-catalog-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.object-store :as os]
            [xtdb.test-util :as tu]
            [xtdb.trie :as trie]
            [xtdb.table-catalog :as table-cat]
            [xtdb.trie-catalog :as trie-cat]
            [xtdb.util :as util])
  (:import [xtdb BufferPool]
           [xtdb.block.proto TableBlock]
           [xtdb.log.proto TrieDetails]
           [xtdb.trie TrieCatalog]
           [xtdb.bloom BloomUtils]
           [xtdb.arrow VectorReader]))

(defn trie-details->edn [^TrieDetails trie]
  (cond-> {:table-name (.getTableName trie)
           :trie-key (.getTrieKey trie)
           :data-file-size (.getDataFileSize trie)}
    (.hasTrieMetadata trie) (assoc :trie-metadata (trie-cat/<-trie-metadata (.getTrieMetadata trie)))))

(defn- ->singleton-rdr [v]
  (reify VectorReader
    (hashCode [_ _ hasher]
      (let [bb (util/->iid v)
            ba (byte-array (.remaining bb))]
        (.get bb ba)
        (.hash hasher ba)))))

(deftest current-tries-on-finish-block
  (let [node-dir (util/->path "target/trie-catalog-test/current-tries-on-finish-block")]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir, :compactor-threads 0})]
      (let [^BufferPool bp (tu/component node :xtdb/buffer-pool)
            ^TrieCatalog trie-catalog (tu/component node :xtdb/trie-catalog)]
        (xt/execute-tx node [[:put-docs :foo {:xt/id 1}]])
        (tu/finish-block! node)

        (xt/execute-tx node [[:put-docs :foo {:xt/id 2}]])
        (tu/finish-block! node)

        (t/is (= [(os/->StoredObject "tables/public$foo/blocks/b00.binpb" 286)
                  (os/->StoredObject "tables/public$foo/blocks/b01.binpb" 415)]
                 (.listAllObjects bp (table-cat/->table-block-dir "public/foo"))))

        (let [current-tries (->> (.getByteArray bp (util/->path "tables/public$foo/blocks/b01.binpb"))
                                 TableBlock/parseFrom
                                 table-cat/<-table-block
                                 :current-tries
                                 (mapv trie-details->edn))
              trie-metas (map :trie-metadata current-tries)
              [trie1-bloom _trie2-bloom] (map :iid-bloom trie-metas)]
          (t/is (= [{:table-name "public/foo",
                     :trie-key "l00-rc-b00",
                     :data-file-size 1966}
                    {:table-name "public/foo",
                     :trie-key "l00-rc-b01",
                     :data-file-size 1966}]
                   (map #(dissoc % :trie-metadata) current-tries)))

          (t/is (= [{:min-valid-from #xt/instant "2020-01-01T00:00:00Z",
                     :max-valid-from #xt/instant "2020-01-01T00:00:00Z",
                     :min-valid-to #xt/instant "+294247-01-10T04:00:54.775807Z",
                     :max-valid-to #xt/instant "+294247-01-10T04:00:54.775807Z",
                     :min-system-from #xt/instant "2020-01-01T00:00:00Z",
                     :max-system-from #xt/instant "2020-01-01T00:00:00Z",
                     :row-count 1}
                    {:min-valid-from #xt/instant "2020-01-02T00:00:00Z",
                     :max-valid-from #xt/instant "2020-01-02T00:00:00Z",
                     :min-valid-to #xt/instant "+294247-01-10T04:00:54.775807Z",
                     :max-valid-to #xt/instant "+294247-01-10T04:00:54.775807Z",
                     :min-system-from #xt/instant "2020-01-02T00:00:00Z",
                     :max-system-from #xt/instant "2020-01-02T00:00:00Z",
                     :row-count 1}]
                   (map #(dissoc % :iid-bloom) trie-metas)))

          (t/is (true? (BloomUtils/contains trie1-bloom (BloomUtils/bloomHashes (->singleton-rdr 1) 0))))
          (t/is (false? (BloomUtils/contains trie1-bloom (BloomUtils/bloomHashes (->singleton-rdr 2) 0)))))

        (t/testing "artifically adding tries (simulating another node finishing and compacting these)"
          (.addTries trie-catalog
                     (->> [["l00-rc-b00" 1] ["l00-rc-b01" 1] ["l00-rc-b02" 1] ["l00-rc-b03" 1]
                           ["l01-rc-b00" 2] ["l01-rc-b01" 2] ["l01-rc-b02" 2]
                           ["l02-rc-p0-b01" 4] ["l02-rc-p1-b01" 4] ["l02-rc-p2-b01" 4] ["l02-rc-p3-b01"4]]
                          (map #(apply trie/->trie-details "public/foo" %))))

          (tu/finish-block! node)

          (t/is (= ["l00-rc-b00" "l01-rc-b00" "l00-rc-b01" "l01-rc-b01"
                    "l02-rc-p0-b01" "l02-rc-p1-b01" "l02-rc-p2-b01" "l02-rc-p3-b01"
                    "l00-rc-b02" "l01-rc-b02"
                    "l00-rc-b03"]
                   (->> (.getByteArray bp (util/->path "tables/public$foo/blocks/b02.binpb"))
                        TableBlock/parseFrom
                        table-cat/<-table-block
                        :current-tries
                        (mapv (comp :trie-key trie-details->edn))))))))))

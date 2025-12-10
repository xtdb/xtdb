(ns xtdb.pbuf
  "Utilities for working with Protocol Buffer things"
  (:require [xtdb.table-catalog :as table-cat]
            [xtdb.trie-catalog :as trie-cat]
            [xtdb.util :as util])
  (:import [java.nio.file Files]
           org.roaringbitmap.buffer.ImmutableRoaringBitmap
           (xtdb.block.proto TableBlock)
           (xtdb.log.proto TrieDetails)
           (xtdb.util HyperLogLog)))

(defn <-TableBlock
  "Parses a TableBlock protobuf and returns a readable map representation."
  [^TableBlock table-block]
  (-> (table-cat/<-table-block table-block)
      (update :hlls update-vals (juxt hash HyperLogLog/estimate))
      (update :partitions
              (partial mapv
                       (fn [partition]
                         (-> partition
                             (update :tries
                                     (partial mapv
                                              (fn [^TrieDetails trie-details]
                                                {:trie-key (.getTrieKey trie-details)
                                                 :data-file-size (.getDataFileSize trie-details)
                                                 :trie-metadata (some-> (trie-cat/<-trie-metadata (.getTrieMetadata trie-details))
                                                                        (update :iid-bloom (juxt hash #(.getCardinality ^ImmutableRoaringBitmap %))))})))))))))

(defn read-table-block-file
  "Reads a TableBlock protobuf file (.binpb) and returns a parsed map representation."
  [path-ish]
  (let [path (util/->path path-ish)
        ba (Files/readAllBytes path)
        table-block (TableBlock/parseFrom ba)]
    (<-TableBlock table-block)))

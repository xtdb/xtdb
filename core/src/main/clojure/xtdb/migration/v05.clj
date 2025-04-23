(ns xtdb.migration.v05
  (:require [clojure.tools.logging :as log]
            xtdb.node.impl
            [xtdb.object-store :as os]
            [xtdb.serde :as serde]
            [xtdb.table-catalog :as table-cat]
            [xtdb.trie :as trie]
            [xtdb.trie-catalog :as trie-cat]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr])
  (:import [java.nio ByteBuffer]
           [java.nio.channels ClosedByInterruptException]
           [java.nio.file Path]
           org.apache.arrow.memory.BufferAllocator
           (org.apache.arrow.vector.ipc ArrowFileReader)
           (org.apache.arrow.vector.types.pojo Field)
           (xtdb.arrow VectorReader Relation)
           xtdb.api.query.IKeyFn
           xtdb.BufferPool
           xtdb.catalog.BlockCatalog
           (xtdb.indexer HllCalculator TrieMetadataCalculator)
           (xtdb.trie MetadataFileWriter TrieCatalog)))

(defn- migrate-trie! [{^BufferPool src :xtdb.migration/source
                       ^BufferPool target :xtdb/buffer-pool
                       ^BufferAllocator al :xtdb/allocator}
                      table-name old-trie-key new-trie-key]
  (try
    (let [table-path (trie/table-name->table-path table-name)
          data-dir (.resolve table-path "data")
          meta-dir (.resolve table-path "meta")
          data-buf (ByteBuffer/wrap (.getByteArray src (.resolve data-dir (str old-trie-key ".arrow"))))]
      (.putObject target (.resolve data-dir (str new-trie-key ".arrow"))
                  (.duplicate data-buf))

      (let [src-meta-path (.resolve meta-dir (str old-trie-key ".arrow"))]
        (util/with-open [data-arrow-buf (util/->arrow-buf-view al (.duplicate data-buf))
                         data-loader (Relation/loader data-arrow-buf)
                         data-rel (Relation. al (.getSchema data-loader))
                         rb (.getRecordBatch src src-meta-path 0)
                         meta-rel (Relation/fromRecordBatch al (.getSchema (.getFooter src src-meta-path)) rb)
                         meta-file-wtr (MetadataFileWriter. al target table-name new-trie-key data-rel false false)]
          (let [nodes-vec (.vectorFor meta-rel "nodes")
                iid-leg (.vectorFor nodes-vec "branch-iid")
                leaf-leg (.vectorFor nodes-vec "leaf")
                data-page-idx-vec (.vectorFor leaf-leg "data-page-idx")]
            (dotimes [idx (.getRowCount meta-rel)]
              (case (.getLeg nodes-vec idx)
                "nil" (.writeNull meta-file-wtr)
                "branch-iid" (.writeIidBranch meta-file-wtr (int-array (mapv #(or % -1) (.getObject iid-leg idx))))
                "leaf" (do
                         (.loadPage data-loader (.getInt data-page-idx-vec idx) data-rel)
                         (.writeLeaf meta-file-wtr))))
            (.end meta-file-wtr))))

      data-buf)

    (catch InterruptedException e (throw e))
    (catch ClosedByInterruptException _ (throw (InterruptedException.)))
    (catch Throwable e
      (log/errorf e "error migrating trie in %s: '%s' -> '%s" table-name old-trie-key new-trie-key)
      (throw e))))

(defn trie-details [{^BufferAllocator al :xtdb/allocator}, ^ByteBuffer data-buf]
  (with-open [file-reader (ArrowFileReader. (util/->seekable-byte-channel data-buf) al)
              root (.getVectorSchemaRoot file-reader)]
    (let [rdr (vr/<-root root)
          tm-calc (TrieMetadataCalculator. (VectorReader/from (.vectorForOrNull rdr "_iid"))
                                           (.vectorForOrNull rdr "_valid_from")
                                           (.vectorForOrNull rdr "_valid_to")
                                           (.vectorForOrNull rdr "_system_from"))
          hll-calc (HllCalculator.)
          !row-count (volatile! 0)]

      (while (.loadNextBatch file-reader)
        (let [row-count (.getRowCount root)]
          (vswap! !row-count + row-count)
          (.update tm-calc 0 row-count)
          (.update hll-calc (.vectorForOrNull rdr "op") 0 row-count)))

      {:trie-metadata (.build tm-calc)
       :hlls (.build hll-calc)
       :fields (-> (.vectorFor rdr "op") (.legReader "put")
                   (.getField) (.getChildren)
                   (->> (into {} (map (juxt #(.getName ^Field %) identity)))))
       :row-count @!row-count
       :data-file-size (.capacity data-buf)})))

(defn migrate->v06! [{^BufferPool src :xtdb.migration/source
                      ^BlockCatalog block-cat :xtdb/block-catalog
                      table-cat :xtdb/table-catalog
                      ^TrieCatalog trie-cat :xtdb/trie-catalog
                      :as system}]
  (let [chunk-meta-objs (.listAllObjects src (util/->path "chunk-metadata"))
        tables-by-chunk (->> (.listAllObjects src (util/->path "tables"))
                             (sequence (comp (map (comp :key os/<-StoredObject))
                                             (filter (fn [^Path key]
                                                       (= "data" (str (.getName key 2)))))
                                             (keep (fn [^Path key]
                                                     (when-let [[_ chunk-idx] (re-find #"^log-l00-fr(\p{XDigit}*)-" (str (.getFileName key)))]
                                                       {:table-name (str (symbol (.denormalize (serde/read-key-fn :snake-case-keyword) (str (second key)))))
                                                        :trie-key (second (re-matches #"(.+)\.arrow" (str (.getFileName key))))
                                                        :chunk-idx chunk-idx})))))
                             (group-by :chunk-idx))]
    (log/infof "%d blocks to migrate..." (count (seq chunk-meta-objs)))
    (dorun
     (->> chunk-meta-objs
          (map-indexed (fn [block-idx obj]
                         (log/debugf "Migrating block %d..." block-idx)
                         (when (Thread/interrupted) (throw (InterruptedException.)))

                         (let [{obj-key :key} (os/<-StoredObject obj)
                               [_ chunk-idx-hex] (re-matches #"chunk-metadata/(\p{XDigit}+)\.transit\.json" (str obj-key))
                               {:keys [latest-completed-tx]} (-> (.getByteArray src obj-key)
                                                                 (serde/read-transit :json))
                               table-res (->> (for [{:keys [table-name], old-trie-key :trie-key} (get tables-by-chunk chunk-idx-hex)
                                                    :let [new-trie-key (trie/->l0-trie-key block-idx)]]
                                                (do
                                                  (log/debugf "Copying '%s' '%s' -> '%s'" table-name old-trie-key new-trie-key)
                                                  (let [data-buf (migrate-trie! system table-name old-trie-key new-trie-key)]
                                                    [table-name (into {:trie-key new-trie-key} (trie-details system data-buf))])))
                                              (into {}))]

                           (doseq [[table-name {:keys [trie-key data-file-size trie-metadata]}] table-res]
                             (.addTries trie-cat table-name
                                        [(trie/->trie-details table-name trie-key data-file-size trie-metadata)]))

                           (log/debugf "Writing table-block files for block %d" block-idx)
                           (let [table-block-paths (table-cat/finish-block! table-cat block-idx table-res
                                                                            (->> (for [table-name (.getTableNames trie-cat)]
                                                                                   [table-name (->> (trie-cat/trie-state trie-cat table-name)
                                                                                                    trie-cat/all-tries)])
                                                                                 (into {})))]

                             (log/debugf "Writing block file for block %d" block-idx)
                             (.finishBlock block-cat block-idx latest-completed-tx table-block-paths)))))))))

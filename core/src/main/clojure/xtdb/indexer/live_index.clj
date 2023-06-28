(ns xtdb.indexer.live-index
  (:require [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.api.protocols :as xtp]
            xtdb.buffer-pool
            xtdb.object-store
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (java.lang AutoCloseable)
           (java.util Arrays HashMap Map)
           (java.util.concurrent CompletableFuture)
           (java.util.concurrent.atomic AtomicInteger)
           (java.util.function BiConsumer Function IntConsumer)
           (java.util.stream IntStream)
           (org.apache.arrow.memory BufferAllocator)
           [org.apache.arrow.memory.util ArrowBufPointer]
           (org.apache.arrow.vector FixedSizeBinaryVector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo ArrowType$Union Schema)
           org.apache.arrow.vector.types.UnionMode
           (xtdb.api.protocols TransactionInstant)
           (xtdb.object_store ObjectStore)
           (xtdb.trie HashTrie HashTrie$Visitor MemoryHashTrie TrieKeys)
           (xtdb.vector IIndirectRelation IIndirectVector IRelationWriter IRowCopier)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveTableTx
  (^xtdb.vector.IRelationWriter writer [])
  (^void commit [])
  (^void close []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveTable
  (^xtdb.indexer.live_index.ILiveTableTx startTx [tx])
  (^java.util.concurrent.CompletableFuture #_<?> finishChunk [^long chunkIdx]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveIndexTx
  (^xtdb.indexer.live_index.ILiveTableTx liveTable [tableName])
  (^void commit []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveIndex
  (^xtdb.indexer.live_index.ILiveTable liveTable [tableName])
  (^xtdb.indexer.live_index.ILiveIndexTx startTx [^xtdb.api.protocols.TransactionInstant tx])
  (^void finishChunk [^long chunkIdx]))

(def ^:private ^org.apache.arrow.vector.types.pojo.Schema index-schema
  (Schema. [(types/col-type->field "xt$iid" [:fixed-size-binary 16])
            (types/col-type->field "xt$valid_from" types/nullable-temporal-type)
            (types/col-type->field "xt$valid_to" types/nullable-temporal-type)
            (types/col-type->field "xt$system_from" types/nullable-temporal-type)
            (types/col-type->field "xt$system_to" types/nullable-temporal-type)
            (types/col-type->field "xt$doc" [:struct {}])]))

(defn- open-index-root ^xtdb.vector.IRelationWriter [^BufferAllocator allocator]
  (vw/root->writer (VectorSchemaRoot/create index-schema allocator)))

(defn- ->t1-key-adapter ^xtdb.trie.TrieKeys [^IRelationWriter rel]
  (let [^FixedSizeBinaryVector iid-vec (.getVector (.writerForName rel "xt$iid"))
        left-abp (ArrowBufPointer.)
        right-abp (ArrowBufPointer.)]
    (reify TrieKeys
      (groupFor [_ idx lvl]
        (.getDataPointer iid-vec idx left-abp)

        (let [lvl-offset-bits (* (inc lvl) HashTrie/LEVEL_BITS)]
          (-> (.getByte (.getBuf left-abp)
                        (+ (.getOffset left-abp)
                           (quot lvl-offset-bits Byte/SIZE)))

              (bit-shift-right (mod lvl-offset-bits Byte/SIZE))
              (bit-and HashTrie/LEVEL_MASK))))

      (compare [_ left-idx right-idx]
        (.compareTo (.getDataPointer iid-vec left-idx left-abp)
                    (.getDataPointer iid-vec right-idx right-abp))))))

(defn- ->row-adder [^IRelationWriter rel]
  (let [wp (.writerPosition rel)]
    (fn add-row [{:keys [^MemoryHashTrie t1]}]
      ;; TODO figure out what tries to update
      {:t1 (-> ^MemoryHashTrie (or t1 (MemoryHashTrie/emptyTrie (->t1-key-adapter rel)))
               (.add (.getPosition wp)))})))

(defn- wrap-writer ^xtdb.vector.IRelationWriter [^IRelationWriter rel-wtr, !tries]
  (let [wp (.writerPosition rel-wtr)
        add-row (->row-adder rel-wtr)]

    (reify IRelationWriter
      (writerPosition [_] wp)
      (writerForName [_ col-name] (.writerForName rel-wtr col-name))
      (writerForName [_ col-name col-type] (.writerForName rel-wtr col-name col-type))
      (syncRowCount [_] (.syncRowCount rel-wtr))
      (rowCopier [_ src-rel]
        (let [copier (.rowCopier rel-wtr src-rel)]
          (reify IRowCopier
            (copyRow [_ src-idx]
              (swap! !tries add-row)
              (.copyRow copier src-idx)))))

      Iterable
      (iterator [_] (.iterator rel-wtr))

      (endRow [_]
        (swap! !tries add-row)
        (.endRow rel-wtr))

      (clear [_] (.clear rel-wtr))

      AutoCloseable
      (close [_] (.close rel-wtr)))))

(defrecord LiveTableTx [^String table-name, ^TransactionInstant tx
                        ^IRelationWriter static-rel, ^IRelationWriter transient-rel
                        !static-tries !transient-tries]
  ILiveTableTx
  (writer [_] (-> transient-rel (wrap-writer !transient-tries)))

  (commit [_]
    (let [copier (vw/->rel-copier static-rel (vw/rel-wtr->rdr transient-rel))
          !new-static-tries (volatile! @!static-tries)]
      (.accept ^HashTrie (:t1 @!transient-tries)
               (reify HashTrie$Visitor
                 (visitBranch [visitor children]
                   (run! #(.accept ^HashTrie % visitor) children))

                 (visitLeaf [_ _page-idx idxs]
                   (-> (Arrays/stream idxs)
                       (.forEach (reify IntConsumer
                                   (accept [_ idx]
                                     (vswap! !new-static-tries update :t1
                                             (fn [t1]
                                               (let [^MemoryHashTrie t1 (or t1 (MemoryHashTrie/emptyTrie (->t1-key-adapter static-rel)))]
                                                 (.add t1 (.copyRow copier idx))))))))))))

      (reset! !static-tries @!new-static-tries)))

  AutoCloseable
  (close [_]
    (util/close transient-rel)))

(def ^org.apache.arrow.vector.types.pojo.Schema trie-schema
  (Schema. [(types/->field "nodes" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                           (types/col-type->field "nil" :null)
                           (types/col-type->field "branch" [:list [:union #{:null :i32}]])
                           ;; TODO metadata
                           (types/col-type->field "leaf" '[:struct {page-idx :i32}]))]))

(defn- write-t1-chunk!
  ^java.util.concurrent.CompletableFuture [^BufferAllocator allocator, ^ObjectStore obj-store, ^HashTrie t1
                                           ^String chunk-idx, ^String table-name
                                           ^IIndirectRelation static-rel]

  (util/with-close-on-catch [t1-leaf-vsr (VectorSchemaRoot/create (Schema. (for [^IIndirectVector rdr static-rel]
                                                                             (.getField (.getVector rdr))))
                                                                  allocator)
                             t1-trie-vsr (VectorSchemaRoot/create trie-schema allocator)]
    (let [t1-leaf-wtr (vw/root->writer t1-leaf-vsr)
          t1-trie-wtr (vw/root->writer t1-trie-vsr)

          node-wtr (.writerForName t1-trie-wtr "nodes")
          node-wp (.writerPosition node-wtr)

          nil-wtr (.writerForTypeId node-wtr (byte 0))
          branch-wtr (.writerForTypeId node-wtr (byte 1))
          branch-el-wtr (.listElementWriter branch-wtr)

          leaf-wtr (.writerForTypeId node-wtr (byte 2))
          page-idx-wtr (.structKeyWriter leaf-wtr "page-idx")
          !page-idx (AtomicInteger. 0)
          copier (vw/->rel-copier t1-leaf-wtr static-rel)]

      (-> (.putObject obj-store
                      (format "tables/%s/t1-diff/leaf-c%s.arrow" table-name chunk-idx)
                      (util/build-arrow-ipc-byte-buffer t1-leaf-vsr :file
                        (fn [write-batch!]
                          (.accept t1
                                   (reify HashTrie$Visitor
                                     (visitBranch [visitor children]
                                       (let [!page-idxs (IntStream/builder)]
                                         (doseq [^HashTrie child children]
                                           (.add !page-idxs (if child
                                                              (do
                                                                (.accept child visitor)
                                                                (dec (.getPosition node-wp)))
                                                              -1)))
                                         (.startList branch-wtr)
                                         (.forEach (.build !page-idxs)
                                                   (reify IntConsumer
                                                     (accept [_ idx]
                                                       (if (= idx -1)
                                                         (.writeNull branch-el-wtr nil)
                                                         (.writeInt branch-el-wtr idx)))))
                                         (.endList branch-wtr)
                                         (.endRow t1-trie-wtr)))

                                     (visitLeaf [_ _page-idx idxs]
                                       (-> (Arrays/stream idxs)
                                           (.forEach (reify IntConsumer
                                                       (accept [_ idx]
                                                         (.copyRow copier idx)))))

                                       (.syncRowCount t1-leaf-wtr)
                                       (write-batch!)
                                       (.clear t1-leaf-wtr)
                                       (.clear t1-leaf-vsr)

                                       (.startStruct leaf-wtr)
                                       (.writeInt page-idx-wtr (.getAndIncrement !page-idx))
                                       (.endStruct leaf-wtr)
                                       (.endRow t1-trie-wtr)))))))
          (util/then-compose
            (fn [_]
              (.syncRowCount t1-trie-wtr)
              (.putObject obj-store
                          (format "tables/%s/t1-diff/trie-c%s.arrow" table-name chunk-idx)
                          (util/root->arrow-ipc-byte-buffer t1-trie-vsr :file))))
          (.whenComplete (reify BiConsumer
                           (accept [_ _ _]
                             (util/try-close t1-trie-vsr)
                             (util/try-close t1-leaf-vsr))))))))

(defrecord LiveTable [^BufferAllocator allocator, ^ObjectStore obj-store, ^String table-name
                      ^IRelationWriter static-rel, !static-tries]
  ILiveTable
  (startTx [_ tx]
    (LiveTableTx. table-name tx
                  static-rel (open-index-root allocator)
                  !static-tries (atom {})))

  (finishChunk [_ chunk-idx]
    (let [{:keys [t1]} @!static-tries
          chunk-idx-str (util/->lex-hex-string chunk-idx)
          static-rel-rdr (vw/rel-wtr->rdr static-rel)]
      (write-t1-chunk! allocator obj-store t1 chunk-idx-str table-name static-rel-rdr)))

  AutoCloseable
  (close [_]
    (util/close static-rel)))

(defrecord LiveIndexTx [^BufferAllocator allocator, ^ObjectStore obj-store
                        ^TransactionInstant tx
                        ^Map live-tables, ^Map live-table-txs]
  ILiveIndexTx
  (liveTable [_ table-name]
    (letfn [(->live-table [_]
              (LiveTable. allocator obj-store table-name (open-index-root allocator) (atom {})))

            (->live-table-tx [table-name]
              (-> ^ILiveTable (.computeIfAbsent live-tables table-name
                                                (util/->jfn ->live-table))
                  (.startTx tx)))]

      (.computeIfAbsent live-table-txs (util/str->normal-form-str table-name)
                        (util/->jfn ->live-table-tx))))

  (commit [_]
    (doseq [^ILiveTableTx live-table (.values live-table-txs)]
      (.commit live-table)))

  AutoCloseable
  (close [_]
    (util/close live-table-txs)))

(defrecord LiveIndex [^BufferAllocator allocator
                      ^ObjectStore object-store
                      ^Map live-tables]
  ILiveIndex
  (liveTable [_ table-name]
    (.computeIfAbsent live-tables (util/str->normal-form-str table-name)
                      (reify Function
                        (apply [_ table-name]
                          (LiveTable. allocator object-store table-name
                                      (open-index-root allocator) (atom {}))))))

  (startTx [_ tx]
    (LiveIndexTx. allocator object-store tx live-tables (HashMap.)))

  (finishChunk [_ chunk-idx]
    @(CompletableFuture/allOf (->> (for [^ILiveTable live-table (.values live-tables)]
                                     (.finishChunk live-table chunk-idx))

                                   (into-array CompletableFuture)))

    (util/close live-tables)
    (.clear live-tables))

  AutoCloseable
  (close [_]
    (util/close live-tables)))

(defmethod ig/prep-key :xtdb.indexer/live-index [_ opts]
  (merge {:allocator (ig/ref :xtdb/allocator)
          :object-store (ig/ref :xtdb/object-store)}
         opts))

(defmethod ig/init-key :xtdb.indexer/live-index [_ {:keys [allocator object-store]}]
  (LiveIndex. allocator object-store (HashMap.)))

(defmethod ig/halt-key! :xtdb.indexer/live-index [_ live-idx]
  (util/close live-idx))

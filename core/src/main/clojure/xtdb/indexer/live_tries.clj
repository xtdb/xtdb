(ns xtdb.indexer.live-tries
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
           (org.apache.arrow.vector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo ArrowType$Union Schema)
           org.apache.arrow.vector.types.UnionMode
           (xtdb.api.protocols TransactionInstant)
           (xtdb.object_store ObjectStore)
           (xtdb.trie HashTrie HashTrie$Visitor MemoryHashTrie TrieKeys)
           (xtdb.vector IIndirectRelation IIndirectVector IRelationWriter)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveTrieTx
  (^xtdb.vector.IRelationWriter relationWriter [])
  (^void addRow [^int idx])
  (^void commit [])
  (^void close []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveTrie
  (^xtdb.indexer.live_tries.ILiveTrieTx startTx [])
  (^java.util.concurrent.CompletableFuture #_<?> finishChunk [^long chunkIdx]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveTriesTx
  (^xtdb.indexer.live_tries.ILiveTrieTx liveTrie [^String triePath])
  (^void commit []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveTries
  (^xtdb.indexer.live_tries.ILiveTrie liveTrie [^String triePath])
  (^xtdb.indexer.live_tries.ILiveTriesTx startTx [])
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

(defrecord LiveTrieTx [^IRelationWriter static-rel, ^IRelationWriter transient-rel
                       !static-trie,
                       ^:unsychronized-mutable ^MemoryHashTrie transient-trie]
  ILiveTrieTx
  (relationWriter [_] transient-rel)

  (addRow [this idx]
    (set! (.transient-trie this) (-> transient-trie (.add idx))))

  (commit [_]
    (let [copier (vw/->rel-copier static-rel (vw/rel-wtr->rdr transient-rel))
          !new-static-trie (volatile! @!static-trie)]
      (.accept transient-trie
               (reify HashTrie$Visitor
                 (visitBranch [visitor children]
                   (run! #(.accept ^HashTrie % visitor) children))

                 (visitLeaf [_ _page-idx idxs]
                   (-> (Arrays/stream idxs)
                       (.forEach (reify IntConsumer
                                   (accept [_ idx]
                                     (vswap! !new-static-trie
                                             (fn [^MemoryHashTrie trie]
                                               (.add trie (.copyRow copier idx)))))))))))

      (reset! !static-trie @!new-static-trie)))

  AutoCloseable
  (close [_]
    (util/close transient-rel)))

(def ^org.apache.arrow.vector.types.pojo.Schema trie-schema
  (Schema. [(types/->field "nodes" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                           (types/col-type->field "nil" :null)
                           (types/col-type->field "branch" [:list [:union #{:null :i32}]])
                           ;; TODO metadata
                           (types/col-type->field "leaf" '[:struct {page-idx :i32}]))]))

(defn- write-trie!
  ^java.util.concurrent.CompletableFuture [^BufferAllocator allocator, ^ObjectStore obj-store,
                                           ^String trie-path, ^String chunk-idx,
                                           ^HashTrie trie, ^IIndirectRelation static-rel]

  (util/with-close-on-catch [leaf-vsr (VectorSchemaRoot/create (Schema. (for [^IIndirectVector rdr static-rel]
                                                                             (.getField (.getVector rdr))))
                                                                  allocator)
                             trie-vsr (VectorSchemaRoot/create trie-schema allocator)]
    (let [leaf-rel-wtr (vw/root->writer leaf-vsr)
          trie-rel-wtr (vw/root->writer trie-vsr)

          node-wtr (.writerForName trie-rel-wtr "nodes")
          node-wp (.writerPosition node-wtr)

          nil-wtr (.writerForTypeId node-wtr (byte 0))
          branch-wtr (.writerForTypeId node-wtr (byte 1))
          branch-el-wtr (.listElementWriter branch-wtr)

          leaf-wtr (.writerForTypeId node-wtr (byte 2))
          page-idx-wtr (.structKeyWriter leaf-wtr "page-idx")
          !page-idx (AtomicInteger. 0)
          copier (vw/->rel-copier leaf-rel-wtr static-rel)]

      (-> (.putObject obj-store
                      (format "%s/leaf-c%s.arrow" trie-path chunk-idx)
                      (util/build-arrow-ipc-byte-buffer leaf-vsr :file
                        (fn [write-batch!]
                          (.accept trie
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
                                         (.endRow trie-rel-wtr)))

                                     (visitLeaf [_ _page-idx idxs]
                                       (-> (Arrays/stream idxs)
                                           (.forEach (reify IntConsumer
                                                       (accept [_ idx]
                                                         (.copyRow copier idx)))))

                                       (.syncRowCount leaf-rel-wtr)
                                       (write-batch!)
                                       (.clear leaf-rel-wtr)
                                       (.clear leaf-vsr)

                                       (.startStruct leaf-wtr)
                                       (.writeInt page-idx-wtr (.getAndIncrement !page-idx))
                                       (.endStruct leaf-wtr)
                                       (.endRow trie-rel-wtr)))))))
          (util/then-compose
            (fn [_]
              (.syncRowCount trie-rel-wtr)
              (.putObject obj-store
                          (format "%s/trie-c%s.arrow" trie-path chunk-idx)
                          (util/root->arrow-ipc-byte-buffer trie-vsr :file))))
          (.whenComplete (reify BiConsumer
                           (accept [_ _ _]
                             (util/try-close trie-vsr)
                             (util/try-close leaf-vsr))))))))

(defrecord LiveTrie [^BufferAllocator allocator, ^ObjectStore obj-store, ^String trie-path
                     ^IRelationWriter static-rel, !static-trie]
  ILiveTrie
  (startTx [_]
    (util/with-close-on-catch [transient-rel (open-index-root allocator)]
      (LiveTrieTx. static-rel transient-rel
                   !static-trie (MemoryHashTrie/emptyTrie (TrieKeys. transient-rel)))))

  (finishChunk [_ chunk-idx]
    (let [chunk-idx-str (util/->lex-hex-string chunk-idx)
          static-rel-rdr (vw/rel-wtr->rdr static-rel)]
      (write-trie! allocator obj-store trie-path chunk-idx-str @!static-trie static-rel-rdr)))

  AutoCloseable
  (close [_]
    (util/close static-rel)))

(defrecord LiveTriesTx [^ILiveTries live-tries, ^Map live-trie-txs]
  ILiveTriesTx
  (liveTrie [_ trie-path]
    (.computeIfAbsent live-trie-txs trie-path
                      (reify Function
                        (apply [_ trie-path]
                          (-> (.liveTrie live-tries trie-path)
                              (.startTx))))))

  (commit [_]
    (doseq [^ILiveTrieTx live-trie (.values live-trie-txs)]
      (.commit live-trie)))

  AutoCloseable
  (close [_]
    (util/close live-trie-txs)))

(defrecord LiveTries [^BufferAllocator allocator, ^ObjectStore object-store, ^Map live-tries]
  ILiveTries
  (liveTrie [_ trie-path]
    (.computeIfAbsent live-tries trie-path
                      (reify Function
                        (apply [_ trie-path]
                          (util/with-close-on-catch [rel (open-index-root allocator)]
                            (LiveTrie. allocator object-store trie-path
                                       rel (atom (MemoryHashTrie/emptyTrie (TrieKeys. rel)))))))))

  (startTx [this]
    (LiveTriesTx. this (HashMap.)))

  (finishChunk [_ chunk-idx]
    @(CompletableFuture/allOf (->> (for [^ILiveTrie live-trie (.values live-tries)]
                                     (.finishChunk live-trie chunk-idx))

                                   (into-array CompletableFuture)))

    (util/close live-tries)
    (.clear live-tries))

  AutoCloseable
  (close [_]
    (util/close live-tries)))

(defmethod ig/prep-key :xtdb.indexer/live-tries [_ opts]
  (merge {:allocator (ig/ref :xtdb/allocator)
          :object-store (ig/ref :xtdb/object-store)}
         opts))

(defmethod ig/init-key :xtdb.indexer/live-tries [_ {:keys [allocator object-store]}]
  (LiveTries. allocator object-store (HashMap.)))

(defmethod ig/halt-key! :xtdb.indexer/live-tries [_ live-idx]
  (util/close live-idx))

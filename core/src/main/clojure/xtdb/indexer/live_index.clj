(ns xtdb.indexer.live-index
  (:require [juxt.clojars-mirrors.integrant.core :as ig]
            xtdb.buffer-pool
            xtdb.object-store
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (java.lang AutoCloseable)
           (java.util Arrays HashMap Map)
           (java.util.concurrent CompletableFuture)
           (java.util.concurrent.atomic AtomicInteger)
           (java.util.function BiConsumer BiFunction Function IntConsumer)
           (java.util.stream IntStream)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector BaseFixedWidthVector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo ArrowType$Union Schema)
           org.apache.arrow.vector.types.UnionMode
           (xtdb.object_store ObjectStore)
           (xtdb.trie MemoryHashTrie MemoryHashTrie$Visitor TrieKeys)
           (xtdb.vector IIndirectRelation IIndirectVector IRelationWriter)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveTableTx
  (^xtdb.vector.IRelationWriter leafWriter [])
  (^void addRow [idx])
  (^void commit [])
  (^void close []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveTable
  (^xtdb.indexer.live_index.ILiveTableTx startTx [])
  (^java.util.concurrent.CompletableFuture #_<?> finishChunk [^long chunkIdx])
  (^void close []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveIndexTx
  (^xtdb.indexer.live_index.ILiveTableTx liveTable [^String tableName])
  (^void commit [])
  (^void close []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveIndex
  (^xtdb.indexer.live_index.ILiveTable liveTable [^String tableName])
  (^xtdb.indexer.live_index.ILiveIndexTx startTx [])
  (^void finishChunk [^long chunkIdx])
  (^void close []))

(defprotocol TestLiveTable
  (^xtdb.vector.IRelationWriter leaf-writer [test-live-table])
  (^java.util.Map tries [test-live-table]))

(def ^org.apache.arrow.vector.types.pojo.Schema trie-schema
  (Schema. [(types/->field "nodes" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                           (types/col-type->field "nil" :null)
                           (types/col-type->field "branch" [:list [:union #{:null :i32}]])
                           ;; TODO metadata
                           (types/col-type->field "leaf" '[:struct {page-idx :i32}]))]))

(defn- write-trie!
  ^java.util.concurrent.CompletableFuture [^BufferAllocator allocator, ^ObjectStore obj-store,
                                           ^String table-name, ^String trie-name, ^String chunk-idx,
                                           ^MemoryHashTrie trie, ^IIndirectRelation leaf]

  (util/with-close-on-catch [leaf-vsr (VectorSchemaRoot/create (Schema. (for [^IIndirectVector rdr leaf]
                                                                          (.getField (.getVector rdr))))
                                                               allocator)
                             trie-vsr (VectorSchemaRoot/create trie-schema allocator)]
    (let [leaf-rel-wtr (vw/root->writer leaf-vsr)
          trie-rel-wtr (vw/root->writer trie-vsr)

          node-wtr (.writerForName trie-rel-wtr "nodes")
          node-wp (.writerPosition node-wtr)

          branch-wtr (.writerForTypeId node-wtr (byte 1))
          branch-el-wtr (.listElementWriter branch-wtr)

          leaf-wtr (.writerForTypeId node-wtr (byte 2))
          page-idx-wtr (.structKeyWriter leaf-wtr "page-idx")
          !page-idx (AtomicInteger. 0)
          copier (vw/->rel-copier leaf-rel-wtr leaf)]

      (-> (.putObject obj-store
                      (format "tables/%s/%s/leaf-c%s.arrow" table-name trie-name chunk-idx)
                      (util/build-arrow-ipc-byte-buffer leaf-vsr :file
                        (fn [write-batch!]
                          (.accept trie
                                   (reify MemoryHashTrie$Visitor
                                     (visitBranch [visitor branch]
                                       (let [!page-idxs (IntStream/builder)]
                                         (doseq [^MemoryHashTrie child (.children branch)]
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

                                     (visitLeaf [_ leaf]
                                       (-> (Arrays/stream (.data leaf))
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
                          (format "tables/%s/%s/trie-c%s.arrow" table-name trie-name chunk-idx)
                          (util/root->arrow-ipc-byte-buffer trie-vsr :file))))

          (.whenComplete (reify BiConsumer
                           (accept [_ _ _]
                             (util/try-close trie-vsr)
                             (util/try-close leaf-vsr))))))))

(def ^:private ^org.apache.arrow.vector.types.pojo.Schema leaf-schema
  (Schema. [(types/col-type->field "xt$iid" [:fixed-size-binary 16])
            (types/col-type->field "xt$valid_from" types/nullable-temporal-type)
            (types/col-type->field "xt$valid_to" types/nullable-temporal-type)
            (types/col-type->field "xt$system_from" types/nullable-temporal-type)
            (types/col-type->field "xt$system_to" types/nullable-temporal-type)
            (types/col-type->field "xt$doc" [:struct {}])]))

(defn- open-leaf-root ^xtdb.vector.IRelationWriter [^BufferAllocator allocator]
  (vw/root->writer (VectorSchemaRoot/create leaf-schema allocator)))

(deftype LiveTable [^BufferAllocator allocator, ^ObjectStore obj-store, ^String table-name
                    ^IRelationWriter leaf,
                    ^:unsynchronized-mutable ^Map tries]
  ILiveTable
  (startTx [this-table]
    (let [transient-tries (HashMap. tries)
          t1-trie-keys (TrieKeys. (into-array BaseFixedWidthVector [(.getVector (.writerForName leaf "xt$iid"))]))]
      (reify ILiveTableTx
        (leafWriter [_] leaf)

        (addRow [_ idx]
          (.compute transient-tries "t1-diff"
                    (reify BiFunction
                      (apply [_ _trie-name {:keys [^MemoryHashTrie trie trie-keys]}]
                        (let [^MemoryHashTrie
                              transient-trie (or trie (MemoryHashTrie/emptyTrie))
                              trie-keys (or trie-keys t1-trie-keys)]

                          {:trie (.add transient-trie trie-keys idx)
                           :trie-keys trie-keys})))))

        (commit [_]
          (set! (.-tries this-table) transient-tries))

        AutoCloseable
        (close [_]))))

  (finishChunk [_ chunk-idx]
    (let [chunk-idx-str (util/->lex-hex-string chunk-idx)]
      (CompletableFuture/allOf
       (->> (for [[trie-name {:keys [^MemoryHashTrie trie trie-keys]}] tries]
              (write-trie! allocator obj-store table-name trie-name chunk-idx-str (-> trie (.compactLogs trie-keys)) (vw/rel-wtr->rdr leaf)))
            (into-array CompletableFuture)))))

  TestLiveTable
  (leaf-writer [_] leaf)
  (tries [_] tries)

  AutoCloseable
  (close [_]
    (util/close leaf)))

(defrecord LiveIndex [^BufferAllocator allocator, ^ObjectStore object-store, ^Map tables]
  ILiveIndex
  (liveTable [_ table-name]
    (.computeIfAbsent tables table-name
                      (reify Function
                        (apply [_ table-name]
                          (util/with-close-on-catch [rel (open-leaf-root allocator)]
                            (LiveTable. allocator object-store table-name rel (HashMap.)))))))

  (startTx [live-idx]
    (let [table-txs (HashMap.)]
      (reify ILiveIndexTx
        (liveTable [_ table-name]
          (.computeIfAbsent table-txs table-name
                            (reify Function
                              (apply [_ table-name]
                                (-> (.liveTable live-idx table-name)
                                    (.startTx))))))

        (commit [_]
          (doseq [^ILiveTableTx table-tx (.values table-txs)]
            (.commit table-tx)))

        AutoCloseable
        (close [_]
          (util/close table-txs)))))

  (finishChunk [_ chunk-idx]
    @(CompletableFuture/allOf (->> (for [^ILiveTable table (.values tables)]
                                     (.finishChunk table chunk-idx))

                                   (into-array CompletableFuture)))

    (util/close tables)
    (.clear tables))

  AutoCloseable
  (close [_]
    (util/close tables)))

(defmethod ig/prep-key :xtdb.indexer/live-index [_ opts]
  (merge {:allocator (ig/ref :xtdb/allocator)
          :object-store (ig/ref :xtdb/object-store)}
         opts))

(defmethod ig/init-key :xtdb.indexer/live-index [_ {:keys [allocator object-store]}]
  (LiveIndex. allocator object-store (HashMap.)))

(defmethod ig/halt-key! :xtdb.indexer/live-index [_ live-idx]
  (util/close live-idx))

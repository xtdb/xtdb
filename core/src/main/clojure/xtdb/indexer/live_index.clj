(ns xtdb.indexer.live-index
  (:require [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.buffer-pool]
            [xtdb.object-store]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (java.lang AutoCloseable)
           (java.nio ByteBuffer)
           (java.util ArrayList Arrays HashMap Map)
           (java.util.concurrent CompletableFuture)
           (java.util.concurrent.atomic AtomicInteger)
           (java.util.function BiConsumer Function IntConsumer)
           (java.util.stream IntStream)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo ArrowType$Union Schema)
           org.apache.arrow.vector.types.UnionMode
           (xtdb.object_store ObjectStore)
           (xtdb.trie HashTrie$Node LiveHashTrie LiveHashTrie$Leaf)
           (xtdb.vector IRelationWriter IVectorReader IVectorWriter RelationReader)))

;;
#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveTableWatermark
  (^xtdb.vector.RelationReader liveRelation [])
  (^xtdb.trie.LiveHashTrie liveTrie []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveTableTx
  (^xtdb.indexer.live_index.ILiveTableWatermark openWatermark [^boolean retain])
  (^xtdb.vector.IVectorWriter docWriter [])
  (^void logPut [^bytes iid, ^long validFrom, ^long validTo, writeDocFn])
  (^void logDelete [^bytes iid, ^long validFrom, ^long validTo])
  (^void logEvict [^bytes iid])
  (^void commit [])
  (^void close []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveTable
  (^xtdb.indexer.live_index.ILiveTableTx startTx [^xtdb.api.protocols.TransactionInstant txKey])
  (^xtdb.indexer.live_index.ILiveTableWatermark openWatermark [^boolean retain])
  (^java.util.concurrent.CompletableFuture #_<?> finishChunk [^long chunkIdx])
  (^void close []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveIndexWatermark
  (^xtdb.indexer.live_index.ILiveTableWatermark liveTable [^String tableName]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveIndexTx
  (^xtdb.indexer.live_index.ILiveTableTx liveTable [^String tableName])
  (^xtdb.indexer.live_index.ILiveIndexWatermark openWatermark [])
  (^void commit [])
  (^void close []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveIndex
  (^xtdb.indexer.live_index.ILiveTable liveTable [^String tableName])
  (^xtdb.indexer.live_index.ILiveIndexTx startTx [^xtdb.api.protocols.TransactionInstant txKey])
  (^xtdb.indexer.live_index.ILiveIndexWatermark openWatermark [])
  (^void finishChunk [^long chunkIdx])
  (^void close []))

(defprotocol TestLiveTable
  (^xtdb.trie.LiveHashTrie live-trie [test-live-table])
  (^xtdb.vector.IRelationWriter live-rel [test-live-table]))

(def ^org.apache.arrow.vector.types.pojo.Schema trie-schema
  (Schema. [(types/->field "nodes" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                           (types/col-type->field "nil" :null)
                           (types/col-type->field "branch" [:list [:union #{:null :i32}]])
                           ;; TODO metadata
                           (types/col-type->field "leaf" '[:struct {page-idx :i32}]))]))

(defn- write-trie!
  ^java.util.concurrent.CompletableFuture [^BufferAllocator allocator, ^ObjectStore obj-store,
                                           ^String table-name, ^String chunk-idx,
                                           ^LiveHashTrie trie, ^RelationReader leaf-rel]

  (when (pos? (.rowCount leaf-rel))
    (util/with-close-on-catch [leaf-vsr (VectorSchemaRoot/create (Schema. (for [^IVectorReader rdr leaf-rel]
                                                                            (.getField rdr)))
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
            copier (vw/->rel-copier leaf-rel-wtr leaf-rel)

            trie-buf (util/build-arrow-ipc-byte-buffer leaf-vsr :file
                       (fn [write-batch!]
                         (letfn [(write-node! [^HashTrie$Node node]
                                   (if-let [children (.children node)]
                                     (let [!page-idxs (IntStream/builder)]
                                       (doseq [child children]
                                         (.add !page-idxs (if child
                                                            (do
                                                              (write-node! child)
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
                                       (.endRow trie-rel-wtr))

                                     (let [^LiveHashTrie$Leaf leaf node]
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
                                       (.endRow trie-rel-wtr))))]

                           (write-node! (.rootNode trie)))))]

        (-> (.putObject obj-store (format "tables/%s/chunks/leaf-c%s.arrow" table-name chunk-idx) trie-buf)
            (util/then-compose
              (fn [_]
                (.syncRowCount trie-rel-wtr)
                (.putObject obj-store
                            (format "tables/%s/chunks/trie-c%s.arrow" table-name chunk-idx)
                            (util/root->arrow-ipc-byte-buffer trie-vsr :file))))

            (.whenComplete (reify BiConsumer
                             (accept [_ _ _]
                               (util/try-close trie-vsr)
                               (util/try-close leaf-vsr)))))))))

(def ^:private put-field
  (types/col-type->field "put" [:struct {'xt$valid_from types/temporal-col-type
                                         'xt$valid_to types/temporal-col-type
                                         'xt$doc [:union #{:null [:struct {}]}]}]))

(def ^:private delete-field
  (types/col-type->field "delete" [:struct {'xt$valid_from types/temporal-col-type
                                            'xt$valid_to types/temporal-col-type}]))

(def ^:private evict-field
  (types/col-type->field "evict" :null))

(def ^:private ^org.apache.arrow.vector.types.pojo.Schema leaf-schema
  (Schema. [(types/col-type->field "xt$iid" [:fixed-size-binary 16])
            (types/col-type->field "xt$system_from" types/temporal-col-type)
            (types/->field "op" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                           put-field delete-field evict-field)]))

(defn- open-leaf-root ^xtdb.vector.IRelationWriter [^BufferAllocator allocator]
  (vw/root->writer (VectorSchemaRoot/create leaf-schema allocator)))

(defn- open-wm-live-rel ^xtdb.vector.RelationReader [^IRelationWriter rel, retain?]
  (let [out-cols (ArrayList.)]
    (try
      (doseq [^IVectorWriter w (vals rel)]
        (.syncValueCount w)
        (.add out-cols (vr/vec->reader (cond-> (.getVector w)
                                         retain? (util/slice-vec)))))

      (vr/rel-reader out-cols)

      (catch Throwable t
        (when retain? (util/close out-cols))
        (throw t)))))

(deftype LiveTable [^BufferAllocator allocator, ^ObjectStore obj-store, ^String table-name
                    ^IRelationWriter live-rel, ^:unsynchronized-mutable ^LiveHashTrie live-trie
                    ^IVectorWriter iid-wtr, ^IVectorWriter system-from-wtr
                    ^IVectorWriter put-wtr, ^IVectorWriter put-valid-from-wtr, ^IVectorWriter put-valid-to-wtr, ^IVectorWriter put-doc-wtr
                    ^IVectorWriter delete-wtr, ^IVectorWriter delete-valid-from-wtr, ^IVectorWriter delete-valid-to-wtr
                    ^IVectorWriter evict-wtr]
  ILiveTable
  (startTx [this-table tx-key]
    (let [!transient-trie (atom live-trie)
          system-from-µs (util/instant->micros (.system-time tx-key))]
      (reify ILiveTableTx
        (docWriter [_] put-doc-wtr)

        (logPut [_ iid valid-from valid-to write-doc!]
          (.startRow live-rel)

          (.writeBytes iid-wtr (ByteBuffer/wrap iid))
          (.writeLong system-from-wtr system-from-µs)

          (.startStruct put-wtr)
          (.writeLong put-valid-from-wtr valid-from)
          (.writeLong put-valid-to-wtr valid-to)
          (write-doc!)
          (.endStruct put-wtr)

          (.endRow live-rel)

          (swap! !transient-trie #(.add ^LiveHashTrie % (dec (.getPosition (.writerPosition live-rel))))))

        (logDelete [_ iid valid-from valid-to]
          (.writeBytes iid-wtr (ByteBuffer/wrap iid))
          (.writeLong system-from-wtr system-from-µs)

          (.startStruct delete-wtr)
          (.writeLong delete-valid-from-wtr valid-from)
          (.writeLong delete-valid-to-wtr valid-to)
          (.endStruct delete-wtr)

          (.endRow live-rel)

          (swap! !transient-trie #(.add ^LiveHashTrie % (dec (.getPosition (.writerPosition live-rel))))))

        (logEvict [_ iid]
          (.writeBytes iid-wtr (ByteBuffer/wrap iid))
          (.writeLong system-from-wtr system-from-µs)

          (.writeNull evict-wtr nil)

          (.endRow live-rel)

          (swap! !transient-trie #(.add ^LiveHashTrie % (dec (.getPosition (.writerPosition live-rel))))))

        (openWatermark [_ retain?]
          (locking this-table
            (let [wm-live-rel (open-wm-live-rel live-rel retain?)
                  wm-live-trie live-trie]
              (reify ILiveTableWatermark
                (liveRelation [_] wm-live-rel)
                (liveTrie [_] wm-live-trie)

                AutoCloseable
                (close [_]
                  (when retain? (util/close wm-live-rel)))))))

        (commit [_]
          (locking this-table
            (set! (.-live-trie this-table) @!transient-trie)))

        AutoCloseable
        (close [_]))))

  (finishChunk [_ chunk-idx]
    (let [chunk-idx-str (util/->lex-hex-string chunk-idx)]
      (write-trie! allocator obj-store table-name chunk-idx-str
                   (-> live-trie (.compactLogs)) (vw/rel-wtr->rdr live-rel))))

  (openWatermark [this retain?]
    (locking this
      (let [wm-live-rel (open-wm-live-rel live-rel retain?)
            wm-live-trie live-trie]
        (reify ILiveTableWatermark
          (liveRelation [_] wm-live-rel)
          (liveTrie [_] wm-live-trie)

          AutoCloseable
          (close [_]
            (when retain? (util/close wm-live-rel)))))))

  TestLiveTable
  (live-trie [_] live-trie)
  (live-rel [_] live-rel)

  AutoCloseable
  (close [this]
    (locking this
      (util/close live-rel))))

(defn- ->live-table [allocator object-store table-name]
  (util/with-close-on-catch [rel (open-leaf-root allocator)]
    (let [iid-wtr (.writerForName rel "xt$iid")
          op-wtr (.writerForName rel "op")
          put-wtr (.writerForField op-wtr put-field)
          delete-wtr (.writerForField op-wtr delete-field)]
      (LiveTable. allocator object-store table-name rel
                  (LiveHashTrie/emptyTrie (.getVector iid-wtr))
                  iid-wtr (.writerForName rel "xt$system_from")
                  put-wtr (.structKeyWriter put-wtr "xt$valid_from") (.structKeyWriter put-wtr "xt$valid_to") (.structKeyWriter put-wtr "xt$doc")
                  delete-wtr (.structKeyWriter delete-wtr "xt$valid_from") (.structKeyWriter delete-wtr "xt$valid_to")
                  (.writerForField op-wtr evict-field)))))

(defrecord LiveIndex [^BufferAllocator allocator, ^ObjectStore object-store, ^Map tables]
  ILiveIndex
  (liveTable [_ table-name]
    (.computeIfAbsent tables table-name
                      (reify Function
                        (apply [_ table-name]
                          (->live-table allocator object-store table-name)))))

  (startTx [live-idx tx-key]
    (let [table-txs (HashMap.)]
      (reify ILiveIndexTx
        (liveTable [_ table-name]
          (.computeIfAbsent table-txs table-name
                            (reify Function
                              (apply [_ table-name]
                                (-> (.liveTable live-idx table-name)
                                    (.startTx tx-key))))))

        (commit [_]
          (doseq [^ILiveTableTx table-tx (.values table-txs)]
            (.commit table-tx)))

        (openWatermark [_]
          (util/with-close-on-catch [wms (HashMap.)]
            (doseq [[table-name ^ILiveTableTx live-table] table-txs]
              (.put wms table-name (.openWatermark live-table false)))

            (doseq [[table-name ^ILiveTable live-table] tables]
              (.computeIfAbsent wms table-name
                                (util/->jfn (fn [_] (.openWatermark live-table false)))))

            (reify ILiveIndexWatermark
              (liveTable [_ table-name] (.get wms table-name))

              AutoCloseable
              (close [_] (util/close wms)))))

        AutoCloseable
        (close [_]
          (util/close table-txs)))))

  (openWatermark [_]
    (util/with-close-on-catch [wms (HashMap.)]
      (doseq [[table-name ^ILiveTable live-table] tables]
        (.put wms table-name (.openWatermark live-table true)))

      (reify ILiveIndexWatermark
        (liveTable [_ table-name] (.get wms table-name))

        AutoCloseable
        (close [_] (util/close wms)))))

  (finishChunk [_ chunk-idx]
    @(CompletableFuture/allOf (->> (for [^ILiveTable table (.values tables)]
                                     (.finishChunk table chunk-idx))

                                   (remove nil?)

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

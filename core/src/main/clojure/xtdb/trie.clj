(ns xtdb.trie
  (:require [xtdb.buffer-pool]
            [xtdb.metadata :as meta]
            [xtdb.object-store]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw]
            xtdb.watermark)
  (:import (java.lang AutoCloseable)
           (java.nio ByteBuffer)
           java.nio.channels.WritableByteChannel
           java.security.MessageDigest
           (java.util ArrayList Arrays List)
           (java.util.concurrent.atomic AtomicInteger)
           (java.util.function IntConsumer Supplier)
           (org.apache.arrow.memory ArrowBuf BufferAllocator)
           (org.apache.arrow.vector VectorLoader VectorSchemaRoot)
           [org.apache.arrow.vector.ipc ArrowFileWriter]
           (org.apache.arrow.vector.types.pojo ArrowType$Union Schema)
           org.apache.arrow.vector.types.UnionMode
           (org.roaringbitmap RoaringBitmap)
           (org.roaringbitmap.buffer ImmutableRoaringBitmap MutableRoaringBitmap)
           xtdb.buffer_pool.IBufferPool
           (xtdb.metadata IMetadataManager ITableMetadata)
           (xtdb.object_store ObjectStore)
           (xtdb.trie ArrowHashTrie ArrowHashTrie$Leaf HashTrie HashTrie$Node LeafMergeQueue LeafMergeQueue$LeafPointer LiveHashTrie LiveHashTrie$Leaf)
           (xtdb.util WritableByteBufferChannel)
           (xtdb.vector IVectorReader RelationReader)
           xtdb.watermark.ILiveTableWatermark))

(def ^:private ^java.lang.ThreadLocal !msg-digest
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (MessageDigest/getInstance "SHA-256")))))

(defn ->iid ^ByteBuffer [eid]
  (if (uuid? eid)
    (util/uuid->byte-buffer eid)
    (ByteBuffer/wrap
     (let [^bytes eid-bytes (cond
                              (string? eid) (.getBytes (str "s" eid))
                              (keyword? eid) (.getBytes (str "k" eid))
                              (integer? eid) (.getBytes (str "i" eid))
                              :else (throw (UnsupportedOperationException. (pr-str (class eid)))))]
       (-> ^MessageDigest (.get !msg-digest)
           (.digest eid-bytes)
           (Arrays/copyOfRange 0 16))))))

(def ^org.apache.arrow.vector.types.pojo.Schema trie-schema
  (Schema. [(types/->field "nodes" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                           (types/col-type->field "nil" :null)
                           (types/col-type->field "branch" [:list [:union #{:null :i32}]])
                           (types/col-type->field "leaf" [:struct {'page-idx :i32
                                                                   'columns meta/metadata-col-type}]))]))

(defn log-leaf-schema ^org.apache.arrow.vector.types.pojo.Schema [put-doc-col-type]
  (Schema. [(types/col-type->field "xt$iid" [:fixed-size-binary 16])
            (types/col-type->field "xt$system_from" types/temporal-col-type)
            (types/->field "op" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                           (types/col-type->field "put" [:struct {'xt$valid_from types/temporal-col-type
                                                                  'xt$valid_to types/temporal-col-type
                                                                  'xt$doc put-doc-col-type}])
                           (types/col-type->field "delete" [:struct {'xt$valid_from types/temporal-col-type
                                                                     'xt$valid_to types/temporal-col-type}])
                           (types/col-type->field "evict" :null))]))

(defn open-leaf-root
  (^xtdb.vector.IRelationWriter [^BufferAllocator allocator]
   (open-leaf-root allocator (log-leaf-schema [:union #{:null [:struct {}]}])))

  (^xtdb.vector.IRelationWriter [^BufferAllocator allocator log-leaf-schema]
   (util/with-close-on-catch [root (VectorSchemaRoot/create log-leaf-schema allocator)]
     (vw/root->writer root))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ITrieWriter
  (^xtdb.vector.IRelationWriter getLeafWriter [])
  (^int writeLeaf [])
  (^int writeBranch [^ints idxs])
  (^void end [])
  (^void close []))

(defn open-trie-writer ^xtdb.trie.ITrieWriter [^BufferAllocator allocator, ^Schema leaf-schema
                                               ^WritableByteChannel leaf-out-ch, ^WritableByteChannel trie-out-ch]
  (util/with-close-on-catch [leaf-vsr (VectorSchemaRoot/create leaf-schema allocator)
                             leaf-out-wtr (ArrowFileWriter. leaf-vsr nil leaf-out-ch)
                             trie-vsr (VectorSchemaRoot/create trie-schema allocator)]
    (.start leaf-out-wtr)
    (let [leaf-rel-wtr (vw/root->writer leaf-vsr)
          trie-rel-wtr (vw/root->writer trie-vsr)

          node-wtr (.writerForName trie-rel-wtr "nodes")
          node-wp (.writerPosition node-wtr)

          branch-wtr (.writerForTypeId node-wtr (byte 1))
          branch-el-wtr (.listElementWriter branch-wtr)

          leaf-wtr (.writerForTypeId node-wtr (byte 2))
          page-idx-wtr (.structKeyWriter leaf-wtr "page-idx")
          page-meta-wtr (meta/->page-meta-wtr (.structKeyWriter leaf-wtr "columns"))
          !page-idx (AtomicInteger. 0)]

      (reify ITrieWriter
        (getLeafWriter [_] leaf-rel-wtr)

        (writeLeaf [_]
          (.syncRowCount leaf-rel-wtr)

          (let [leaf-rdr (vw/rel-wtr->rdr leaf-rel-wtr)
                put-rdr (-> leaf-rdr
                            (.readerForName "op")
                            (.legReader :put)
                            (.metadataReader))

                doc-rdr (.structKeyReader put-rdr "xt$doc")]

            (.writeMetadata page-meta-wtr (into [(.readerForName leaf-rdr "xt$system_from")
                                                 (.readerForName leaf-rdr "xt$iid")
                                                 (.structKeyReader put-rdr "xt$valid_from")
                                                 (.structKeyReader put-rdr "xt$valid_to")]
                                                (map #(.structKeyReader doc-rdr %))
                                                (.structKeys doc-rdr))))

          (.writeBatch leaf-out-wtr)
          (.clear leaf-rel-wtr)
          (.clear leaf-vsr)

          (let [pos (.getPosition node-wp)]
            (.startStruct leaf-wtr)
            (.writeInt page-idx-wtr (.getAndIncrement !page-idx))
            (.endStruct leaf-wtr)
            (.endRow trie-rel-wtr)

            pos))

        (writeBranch [_ idxs]
          (let [pos (.getPosition node-wp)]
            (.startList branch-wtr)

            (dotimes [n (alength idxs)]
              (let [idx (aget idxs n)]
                (if (= idx -1)
                  (.writeNull branch-el-wtr nil)
                  (.writeInt branch-el-wtr idx))))

            (.endList branch-wtr)
            (.endRow trie-rel-wtr)

            pos))

        (end [_]
          (.end leaf-out-wtr)

          (.syncSchema trie-vsr)
          (.syncRowCount trie-rel-wtr)

          (util/with-open [trie-out-wtr (ArrowFileWriter. trie-vsr nil trie-out-ch)]
            (.start trie-out-wtr)
            (.writeBatch trie-out-wtr)
            (.end trie-out-wtr)))

        AutoCloseable
        (close [_]
          (util/close [trie-vsr leaf-out-wtr leaf-vsr]))))))

(defn write-live-trie [^ITrieWriter trie-wtr, ^LiveHashTrie trie, ^RelationReader leaf-rel]
  (let [trie (.compactLogs trie)
        copier (vw/->rel-copier (.getLeafWriter trie-wtr) leaf-rel)]
    (letfn [(write-node! [^HashTrie$Node node]
              (if-let [children (.children node)]
                (let [child-count (alength children)
                      !idxs (int-array child-count)]
                  (dotimes [n child-count]
                    (aset !idxs n
                          (unchecked-int
                           (if-let [child (aget children n)]
                             (write-node! child)
                             -1))))

                  (.writeBranch trie-wtr !idxs))

                (let [^LiveHashTrie$Leaf leaf node]
                  (-> (Arrays/stream (.data leaf))
                      (.forEach (reify IntConsumer
                                  (accept [_ idx]
                                    (.copyRow copier idx)))))

                  (.writeLeaf trie-wtr))))]

      (write-node! (.rootNode trie)))))

(defn live-trie->bufs [^BufferAllocator allocator, ^LiveHashTrie trie, ^RelationReader leaf-rel]
  (util/with-open [leaf-bb-ch (WritableByteBufferChannel/open)
                   trie-bb-ch (WritableByteBufferChannel/open)
                   trie-wtr (open-trie-writer allocator
                                              (Schema. (for [^IVectorReader rdr leaf-rel]
                                                         (.getField rdr)))
                                              (.getChannel leaf-bb-ch)
                                              (.getChannel trie-bb-ch))]

    (write-live-trie trie-wtr trie leaf-rel)

    (.end trie-wtr)

    {:leaf-buf (.getAsByteBuffer leaf-bb-ch)
     :trie-buf (.getAsByteBuffer trie-bb-ch)}))

(defn ->trie-key [^long chunk-idx]
  (format "c%s" (util/->lex-hex-string chunk-idx)))

(defn ->table-leaf-obj-key [table-name trie-key]
  (format "tables/%s/log-leaves/leaf-%s.arrow" table-name trie-key))

(defn ->table-trie-obj-key [table-name trie-key]
  (format "tables/%s/log-tries/trie-%s.arrow" table-name trie-key))

(defn write-trie-bufs! [^ObjectStore obj-store, ^String table-name, trie-key
                        {:keys [^ByteBuffer leaf-buf ^ByteBuffer trie-buf]}]
  (-> (.putObject obj-store (->table-leaf-obj-key table-name trie-key) leaf-buf)
      (util/then-compose
        (fn [_]
          (.putObject obj-store (->table-trie-obj-key table-name trie-key) trie-buf)))))

(defn list-table-tries [^ObjectStore obj-store, table-name]
  (->> (.listObjects obj-store (format "tables/%s/log-tries" table-name))
       (keep (fn [file-name]
               (when-let [[_ trie-key] (re-find #"/trie-([^/]+?)\.arrow$" file-name)]
                 {:trie-file file-name
                  :trie-key trie-key})))
       (sort-by :trie-key)
       vec))

(defn ->merge-plan
  "Returns a tree of the tasks required to merge the given tries "
  [tries, trie-page-idxs, iid-bloom-bitmap]

  (letfn [(->merge-plan* [nodes path ^long level]
            (let [trie-children (mapv #(some-> ^HashTrie$Node % (.children)) nodes)]
              (if-let [^objects first-children (some identity trie-children)]
                (let [branches (->> (range (alength first-children))
                                    (mapv (fn [bucket-idx]
                                            (->merge-plan* (mapv (fn [node ^objects node-children]
                                                                   (if node-children
                                                                     (aget node-children bucket-idx)
                                                                     node))
                                                                 nodes trie-children)
                                                           (conj path bucket-idx)
                                                           (inc level)))))]
                  (when-not (every? nil? branches)
                    {:path (byte-array path)
                     :node [:branch branches]}))

                (let [^MutableRoaringBitmap cumulative-iid-bitmap (MutableRoaringBitmap.)]
                  (loop [ordinal 0
                         [node & more-nodes] nodes
                         node-taken? false
                         leaves []]
                    (cond
                      (not (< ordinal (count nodes))) (when node-taken?
                                                        {:path (byte-array path)
                                                         :node [:leaf leaves]})
                      node (condp = (class node)
                             ArrowHashTrie$Leaf
                             (let [page-idx (.getPageIndex ^ArrowHashTrie$Leaf node)
                                   take-node? (some-> ^RoaringBitmap (nth trie-page-idxs ordinal)
                                                      (.contains page-idx))]
                               (when take-node?
                                 (.or cumulative-iid-bitmap (iid-bloom-bitmap ordinal page-idx)))
                               (recur (inc ordinal) more-nodes (or node-taken? take-node?)
                                      (conj leaves (when (or take-node?
                                                             (when node-taken?
                                                               (when-let [iid-bitmap (iid-bloom-bitmap ordinal page-idx)]
                                                                 (MutableRoaringBitmap/intersects cumulative-iid-bitmap iid-bitmap))))
                                                     {:page-idx page-idx}))))
                             LiveHashTrie$Leaf
                             (recur (inc ordinal) more-nodes true (conj leaves node)))

                      :else (recur (inc ordinal) more-nodes node-taken? (conj leaves nil))))))))]

    (->merge-plan* (map #(some-> ^HashTrie % (.rootNode)) tries) [] 0)))

(defn table-merge-plan [^IBufferPool buffer-pool, ^IMetadataManager metadata-mgr, table-tries,
                        trie-file->page-idxs, ^ILiveTableWatermark live-table-wm]

  (util/with-open [trie-roots (ArrayList. (count table-tries))]
    ;; TODO these could be kicked off asynchronously
    (let [tries (cond-> (vec (for [{:keys [trie-file]} table-tries]
                               (with-open [^ArrowBuf buf @(.getBuffer buffer-pool trie-file)]
                                 (let [{:keys [^VectorLoader loader root arrow-blocks]} (util/read-arrow-buf buf)]
                                   (with-open [record-batch (util/->arrow-record-batch-view (first arrow-blocks) buf)]
                                     (.load loader record-batch)
                                     (.add trie-roots root)
                                     {:trie (ArrowHashTrie/from root)
                                      :trie-file trie-file
                                      :page-idxs (trie-file->page-idxs trie-file)})))))
                  live-table-wm (conj {:trie (.compactLogs (.liveTrie live-table-wm))}))
          trie-files (mapv :trie-file tries)]
      (letfn [(iid-bloom-bitmap ^ImmutableRoaringBitmap [ordinal page-idx]
                @(meta/with-metadata metadata-mgr (nth trie-files ordinal)
                   (util/->jfn
                     (fn [^ITableMetadata table-meta]
                       (.iidBloomBitmap table-meta page-idx)))))]
        (->merge-plan (mapv :trie tries) (mapv :page-idxs tries) iid-bloom-bitmap)))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILeafLoader
  (^org.apache.arrow.vector.types.pojo.Schema getSchema [])
  (^xtdb.trie.LeafMergeQueue$LeafPointer getLeafPointer [])
  (^xtdb.vector.RelationReader loadLeaf [leaf]))

(deftype ArrowLeafLoader [^ArrowBuf buf
                          ^VectorSchemaRoot root
                          ^VectorLoader loader
                          ^List arrow-blocks
                          ^LeafMergeQueue$LeafPointer leaf-ptr
                          ^:unsynchronized-mutable ^int current-page-idx]
  ILeafLoader
  (getSchema [_] (.getSchema root))
  (getLeafPointer [_] leaf-ptr)

  (loadLeaf [this {:keys [page-idx]}]
    (when-not (= page-idx current-page-idx)
      (set! (.current-page-idx this) page-idx)

      (with-open [rb (util/->arrow-record-batch-view (nth arrow-blocks page-idx) buf)]
        (.load loader rb)
        (.reset leaf-ptr)))

    (vr/<-root root))

  AutoCloseable
  (close [_]
    (util/close root)
    (util/close buf)))

(deftype LiveLeafLoader [^RelationReader live-rel, ^LeafMergeQueue$LeafPointer leaf-ptr]
  ILeafLoader
  (getSchema [_]
    (Schema. (for [^IVectorReader rdr live-rel]
               (.getField rdr))))

  (getLeafPointer [_] leaf-ptr)

  (loadLeaf [_ leaf]
    (.reset leaf-ptr)
    (.select live-rel (.data ^LiveHashTrie$Leaf leaf)))

  AutoCloseable
  (close [_]))

(defn open-leaves [^IBufferPool buffer-pool, table-name, table-tries, ^ILiveTableWatermark live-table-wm]
  (util/with-close-on-catch [leaf-bufs (ArrayList.)]
    ;; TODO get hold of these a page at a time if it's a small query,
    ;; rather than assuming we'll always have/use the whole file.
    (let [arrow-leaves (->> table-tries
                            (into [] (map-indexed
                                      (fn [ordinal {:keys [trie-key]}]
                                        (let [leaf-buf @(.getBuffer buffer-pool (->table-leaf-obj-key table-name trie-key))
                                              {:keys [^VectorSchemaRoot root loader arrow-blocks]} (util/read-arrow-buf leaf-buf)]
                                          (.add leaf-bufs leaf-buf)

                                          (ArrowLeafLoader. leaf-buf root loader arrow-blocks
                                                            (LeafMergeQueue$LeafPointer. ordinal)
                                                            -1))))))]
      (cond-> arrow-leaves
        live-table-wm (conj (LiveLeafLoader. (.liveRelation live-table-wm)
                                             (LeafMergeQueue$LeafPointer. (count arrow-leaves))))))))

(defn load-leaves [leaf-loaders {:keys [leaves] :as _merge-task}]
  (->> leaves
       (into [] (map-indexed (fn [ordinal trie-leaf]
                               (when trie-leaf
                                 (let [^ILeafLoader leaf-loader (nth leaf-loaders ordinal)]
                                   {:rel-rdr (.loadLeaf leaf-loader trie-leaf)
                                    :leaf-ptr (.getLeafPointer leaf-loader)})))))))

(defn ->merge-queue ^xtdb.trie.LeafMergeQueue [loaded-leaves {:keys [path]}]
  (LeafMergeQueue. path
                   (into-array IVectorReader
                               (map (fn [{:keys [^RelationReader rel-rdr]}]
                                      (when rel-rdr
                                        (.readerForName rel-rdr "xt$iid")))
                                    loaded-leaves))
                   (map :leaf-ptr loaded-leaves)))

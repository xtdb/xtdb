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
           xtdb.IBufferPool
           (xtdb.object_store ObjectStore)
           (xtdb.trie ArrowHashTrie ArrowHashTrie$Leaf HashTrie HashTrie$Node LiveHashTrie LiveHashTrie$Leaf)
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

(defn ->log-trie-key [^long level, ^long row-from, ^long next-row]
  (format "log-l%s-rf%s-nr%s" (util/->lex-hex-string level) (util/->lex-hex-string row-from) (util/->lex-hex-string next-row)))

(defn ->table-data-file-name [table-name trie-key]
  (format "tables/%s/data/%s.arrow" table-name trie-key))

(defn ->table-meta-file-name [table-name trie-key]
  (format "tables/%s/meta/%s.arrow" table-name trie-key))

(defn list-meta-files [^IBufferPool buffer-pool, table-name]
  (vec (sort (.listObjects buffer-pool (format "tables/%s/meta/" table-name)))))

(def ^org.apache.arrow.vector.types.pojo.Schema meta-rel-schema
  (Schema. [(types/->field "nodes" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                           (types/col-type->field "nil" :null)
                           (types/col-type->field "branch" [:list [:union #{:null :i32}]])
                           (types/col-type->field "leaf" [:struct {'data-page-idx :i32
                                                                   'columns meta/metadata-col-type}]))]))

(defn data-rel-schema ^org.apache.arrow.vector.types.pojo.Schema [put-doc-col-type]
  (Schema. [(types/col-type->field "xt$iid" [:fixed-size-binary 16])
            (types/col-type->field "xt$system_from" types/temporal-col-type)
            (types/->field "op" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                           (types/col-type->field "put" [:struct {'xt$valid_from types/temporal-col-type
                                                                  'xt$valid_to types/temporal-col-type
                                                                  'xt$doc put-doc-col-type}])
                           (types/col-type->field "delete" [:struct {'xt$valid_from types/temporal-col-type
                                                                     'xt$valid_to types/temporal-col-type}])
                           (types/col-type->field "evict" :null))]))

(defn open-log-data-root
  (^xtdb.vector.IRelationWriter [^BufferAllocator allocator]
   (open-log-data-root allocator (data-rel-schema [:union #{:null [:struct {}]}])))

  (^xtdb.vector.IRelationWriter [^BufferAllocator allocator data-schema]
   (util/with-close-on-catch [root (VectorSchemaRoot/create data-schema allocator)]
     (vw/root->writer root))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ITrieWriter
  (^xtdb.vector.IRelationWriter getDataWriter [])
  (^int writeLeaf [])
  (^int writeBranch [^ints idxs])
  (^void end [])
  (^void close []))

(defn open-trie-writer ^xtdb.trie.ITrieWriter [^BufferAllocator allocator, ^Schema data-schema
                                               ^WritableByteChannel data-out-ch, ^WritableByteChannel meta-out-ch]
  (util/with-close-on-catch [data-vsr (VectorSchemaRoot/create data-schema allocator)
                             data-out-wtr (ArrowFileWriter. data-vsr nil data-out-ch)
                             meta-vsr (VectorSchemaRoot/create meta-rel-schema allocator)]
    (.start data-out-wtr)
    (let [data-rel-wtr (vw/root->writer data-vsr)
          meta-rel-wtr (vw/root->writer meta-vsr)

          node-wtr (.colWriter meta-rel-wtr "nodes")
          node-wp (.writerPosition node-wtr)

          branch-wtr (.legWriter node-wtr :branch)
          branch-el-wtr (.listElementWriter branch-wtr)

          leaf-wtr (.legWriter node-wtr :leaf)
          page-idx-wtr (.structKeyWriter leaf-wtr "data-page-idx")
          page-meta-wtr (meta/->page-meta-wtr (.structKeyWriter leaf-wtr "columns"))
          !page-idx (AtomicInteger. 0)]

      (reify ITrieWriter
        (getDataWriter [_] data-rel-wtr)

        (writeLeaf [_]
          (.syncRowCount data-rel-wtr)

          (let [leaf-rdr (vw/rel-wtr->rdr data-rel-wtr)
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

          (.writeBatch data-out-wtr)
          (.clear data-rel-wtr)
          (.clear data-vsr)

          (let [pos (.getPosition node-wp)]
            (.startStruct leaf-wtr)
            (.writeInt page-idx-wtr (.getAndIncrement !page-idx))
            (.endStruct leaf-wtr)
            (.endRow meta-rel-wtr)

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
            (.endRow meta-rel-wtr)

            pos))

        (end [_]
          (.end data-out-wtr)

          (.syncSchema meta-vsr)
          (.syncRowCount meta-rel-wtr)

          (util/with-open [meta-out-wtr (ArrowFileWriter. meta-vsr nil meta-out-ch)]
            (.start meta-out-wtr)
            (.writeBatch meta-out-wtr)
            (.end meta-out-wtr)))

        AutoCloseable
        (close [_]
          (util/close [meta-vsr data-out-wtr meta-vsr]))))))

(defn write-live-trie [^ITrieWriter trie-wtr, ^LiveHashTrie trie, ^RelationReader data-rel]
  (let [trie (.compactLogs trie)
        copier (vw/->rel-copier (.getDataWriter trie-wtr) data-rel)]
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

(defn live-trie->bufs [^BufferAllocator allocator, ^LiveHashTrie trie, ^RelationReader data-rel]
  (util/with-open [data-bb-ch (WritableByteBufferChannel/open)
                   meta-bb-ch (WritableByteBufferChannel/open)
                   trie-wtr (open-trie-writer allocator
                                              (Schema. (for [^IVectorReader rdr data-rel]
                                                         (.getField rdr)))
                                              (.getChannel data-bb-ch)
                                              (.getChannel meta-bb-ch))]

    (write-live-trie trie-wtr trie data-rel)

    (.end trie-wtr)

    {:data-buf (.getAsByteBuffer data-bb-ch)
     :meta-buf (.getAsByteBuffer meta-bb-ch)}))

(defn write-trie-bufs! [^IBufferPool buffer-pool, ^String table-name, trie-key
                        {:keys [^ByteBuffer data-buf ^ByteBuffer meta-buf]}]
  (-> (.putObject buffer-pool (->table-data-file-name table-name trie-key) data-buf)
      (util/then-compose
        (fn [_]
          (.putObject buffer-pool (->table-meta-file-name table-name trie-key) meta-buf)))))

(defn parse-trie-file-name [file-name]
  (when-let [[_ trie-key level-str row-from-str next-row-str] (re-find #"/(log-l(\p{XDigit}+)-rf(\p{XDigit}+)-nr(\p{XDigit}+)+?)\.arrow$" file-name)]
    {:file-name file-name
     :trie-key trie-key
     :level (util/<-lex-hex-string level-str)
     :row-from (util/<-lex-hex-string row-from-str)
     :next-row (util/<-lex-hex-string next-row-str)}))

(defn current-trie-files [file-names]
  (loop [next-row 0
         [level-trie-keys & more-levels] (->> file-names
                                              (keep parse-trie-file-name)
                                              (group-by :level)
                                              (sort-by key #(Long/compare %2 %1))
                                              (vals))
         res []]
    (if-not level-trie-keys
      res
      (if-let [tries (not-empty
                      (->> level-trie-keys
                           (into [] (drop-while (fn [{:keys [^long row-from]}]
                                                  (< row-from next-row))))))]
        (recur (long (:next-row (first (rseq tries))))
               more-levels
               (into res (map :file-name) tries))
        (recur next-row more-levels res)))))

(defn postwalk-merge-plan
  "Post-walks the merged tries, passing the nodes from each of the tries to the given fn.
   e.g. for a leaf: passes the trie-nodes to the fn, returns the result.
        for a branch: passes a vector the return values of the postwalk fn
                      for the inner nodes, for the fn to combine

   Returns the value returned from the postwalk fn for the root node.



   tries :: [HashTrie]

   f :: path, merge-node -> ret
     where
     merge-node :: [:branch [ret]]
                 | [:leaf [HashTrie$Node]]"
  [tries f]

  (letfn [(postwalk* [nodes path-vec]
            (let [trie-children (mapv #(some-> ^HashTrie$Node % (.children)) nodes)
                  path (byte-array path-vec)]
              (f path
                 (if-let [^objects first-children (some identity trie-children)]
                   [:branch (lazy-seq
                             (->> (range (alength first-children))
                                  (mapv (fn [bucket-idx]
                                          (postwalk* (mapv (fn [node ^objects node-children]
                                                             (if node-children
                                                               (aget node-children bucket-idx)
                                                               node))
                                                           nodes trie-children)
                                                     (conj path-vec bucket-idx))))))]

                   [:leaf nodes]))))]

    (postwalk* (mapv (fn [^HashTrie trie]
                       (some-> trie .rootNode))
                     tries)
               [])))

(defrecord MetaFile [^HashTrie trie, ^ArrowBuf buf, ^RelationReader rdr]
  AutoCloseable
  (close [_]
    (util/close rdr)
    (util/close buf)))

(defn open-meta-file [^IBufferPool buffer-pool file-name]
  (util/with-close-on-catch [^ArrowBuf buf @(.getBuffer buffer-pool file-name)]
    (let [{:keys [^VectorLoader loader root arrow-blocks]} (util/read-arrow-buf buf)]
      (with-open [record-batch (util/->arrow-record-batch-view (first arrow-blocks) buf)]
        (.load loader record-batch)
        (->MetaFile (ArrowHashTrie/from root) buf (vr/<-root root))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface IDataRel
  (^org.apache.arrow.vector.types.pojo.Schema getSchema [])
  (^xtdb.vector.RelationReader loadPage [trie-leaf]))

(deftype ArrowDataRel [^ArrowBuf buf
                       ^VectorSchemaRoot root
                       ^VectorLoader loader
                       ^List arrow-blocks
                       ^:unsynchronized-mutable ^int current-page-idx]
  IDataRel
  (getSchema [_] (.getSchema root))

  (loadPage [this trie-leaf]
    (let [page-idx (.getDataPageIndex ^ArrowHashTrie$Leaf trie-leaf)]
      (when-not (= page-idx current-page-idx)
        (set! (.current-page-idx this) page-idx)

        (with-open [rb (util/->arrow-record-batch-view (nth arrow-blocks page-idx) buf)]
          (.load loader rb))))

    (vr/<-root root))

  AutoCloseable
  (close [_]
    (util/close root)
    (util/close buf)))

(deftype LiveDataRel [^RelationReader live-rel]
  IDataRel
  (getSchema [_]
    (Schema. (for [^IVectorReader rdr live-rel]
               (.getField rdr))))

  (loadPage [_ leaf]
    (.select live-rel (.data ^LiveHashTrie$Leaf leaf)))

  AutoCloseable
  (close [_]))

(defn open-data-rels [^IBufferPool buffer-pool, table-name, trie-keys, ^ILiveTableWatermark live-table-wm]
  (util/with-close-on-catch [data-bufs (ArrayList.)]
    ;; TODO get hold of these a page at a time if it's a small query,
    ;; rather than assuming we'll always have/use the whole file.
    (let [arrow-data-rels (->> trie-keys
                               (mapv (fn [trie-key]
                                       (.add data-bufs @(.getBuffer buffer-pool (->table-data-file-name table-name trie-key)))
                                       (let [data-buf (.get data-bufs (dec (.size data-bufs)))
                                             {:keys [^VectorSchemaRoot root loader arrow-blocks]} (util/read-arrow-buf data-buf)]

                                         (ArrowDataRel. data-buf root loader arrow-blocks -1)))))]
      (cond-> arrow-data-rels
        live-table-wm (conj (->LiveDataRel (.liveRelation live-table-wm)))))))

(defn load-data-pages [data-rels trie-leaves]
  (->> trie-leaves
       (into [] (map-indexed (fn [ordinal trie-leaf]
                               (when trie-leaf
                                 (let [^IDataRel data-rel (nth data-rels ordinal)]
                                   (.loadPage data-rel trie-leaf))))))))

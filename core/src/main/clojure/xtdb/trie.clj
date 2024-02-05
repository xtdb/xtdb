(ns xtdb.trie
  (:require [xtdb.buffer-pool]
            [xtdb.metadata :as meta]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (java.lang AutoCloseable)
           (java.nio ByteBuffer)
           (java.nio.file Path)
           java.security.MessageDigest
           (java.util ArrayList Arrays List)
           (java.util.concurrent.atomic AtomicInteger)
           (java.util.function IntConsumer Supplier)
           (org.apache.arrow.memory ArrowBuf BufferAllocator)
           (org.apache.arrow.vector VectorLoader VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo ArrowType$Union Schema)
           org.apache.arrow.vector.types.UnionMode
           xtdb.IBufferPool
           (xtdb.trie ArrowHashTrie ArrowHashTrie$Leaf HashTrie HashTrie$Node LiveHashTrie LiveHashTrie$Leaf ITrieWriter)
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

(defn ->table-data-file-path [^Path table-path trie-key]
  (.resolve table-path (format "data/%s.arrow" trie-key)))

(defn ->table-meta-file-path [^Path table-path trie-key]
  (.resolve table-path (format "meta/%s.arrow" trie-key)))

(defn list-meta-files [^IBufferPool buffer-pool ^Path table-path]
  (.listObjects buffer-pool (.resolve table-path "meta")))

(def ^org.apache.arrow.vector.types.pojo.Schema meta-rel-schema
  (Schema. [(types/->field "nodes" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                           (types/col-type->field "nil" :null)
                           (types/col-type->field "branch" [:list [:union #{:null :i32}]])
                           (types/col-type->field "leaf" [:struct {'data-page-idx :i32
                                                                   'columns meta/metadata-col-type}]))]))

(defn data-rel-schema ^org.apache.arrow.vector.types.pojo.Schema [put-doc-col-type]
  (Schema. [(types/col-type->field "xt$iid" [:fixed-size-binary 16])
            (types/col-type->field "xt$system_from" types/temporal-col-type)
            (types/col-type->field "xt$valid_from" types/temporal-col-type)
            (types/col-type->field "xt$valid_to" types/temporal-col-type)
            (types/->field "op" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                           (types/col-type->field "put" put-doc-col-type)
                           (types/col-type->field "delete" :null)
                           (types/col-type->field "erase" :null))]))

(defn open-log-data-root
  (^xtdb.vector.IRelationWriter [^BufferAllocator allocator]
   (open-log-data-root allocator (data-rel-schema [:struct {}])))

  (^xtdb.vector.IRelationWriter [^BufferAllocator allocator data-schema]
   (util/with-close-on-catch [root (VectorSchemaRoot/create data-schema allocator)]
     (vw/root->writer root))))

(defn open-trie-writer ^xtdb.trie.ITrieWriter [^BufferAllocator allocator, ^IBufferPool buffer-pool,
                                               ^Schema data-schema, ^Path table-path, trie-key]
  (util/with-close-on-catch [data-vsr (VectorSchemaRoot/create data-schema allocator)
                             data-file-wtr (.openArrowWriter buffer-pool (->table-data-file-path table-path trie-key) data-vsr)
                             meta-vsr (VectorSchemaRoot/create meta-rel-schema allocator)]

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
                            (.legReader :put))

                meta-pos (.getPosition node-wp)]

            (.startStruct leaf-wtr)

            (.writeMetadata page-meta-wtr (into [(.readerForName leaf-rdr "xt$system_from")
                                                 (.readerForName leaf-rdr "xt$iid")]
                                                (map #(.structKeyReader put-rdr %))
                                                (.structKeys put-rdr)))

            (.writeInt page-idx-wtr (.getAndIncrement !page-idx))
            (.endStruct leaf-wtr)
            (.endRow meta-rel-wtr)


            (.writeBatch data-file-wtr)
            (.clear data-rel-wtr)
            (.clear data-vsr)

            meta-pos))

        (writeBranch [_ idxs]
          (let [pos (.getPosition node-wp)]
            (.startList branch-wtr)

            (dotimes [n (alength idxs)]
              (let [idx (aget idxs n)]
                (if (= idx -1)
                  (.writeNull branch-el-wtr)
                  (.writeInt branch-el-wtr idx))))

            (.endList branch-wtr)
            (.endRow meta-rel-wtr)

            pos))

        (end [_]
          (.end data-file-wtr)

          (.syncSchema meta-vsr)
          (.syncRowCount meta-rel-wtr)

          (util/with-open [meta-file-wtr (.openArrowWriter buffer-pool (->table-meta-file-path table-path trie-key) meta-vsr)]
            (.writeBatch meta-file-wtr)
            (.end meta-file-wtr)))

        AutoCloseable
        (close [_]
          (util/close [meta-vsr data-file-wtr meta-vsr]))))))

(defn write-live-trie-node [^ITrieWriter trie-wtr, ^HashTrie$Node node, ^RelationReader data-rel]
  (let [copier (vw/->rel-copier (.getDataWriter trie-wtr) data-rel)]
    (letfn [(write-node! [^HashTrie$Node node]
              (if-let [children (.getChildren node)]
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
                  (-> (Arrays/stream (.getData leaf))
                      (.forEach (reify IntConsumer
                                  (accept [_ idx]
                                    (.copyRow copier idx)))))

                  (.writeLeaf trie-wtr))))]

      (write-node! node))))

(defn write-live-trie! [^BufferAllocator allocator, ^IBufferPool buffer-pool,
                        ^Path table-path, trie-key,
                        ^LiveHashTrie trie, ^RelationReader data-rel]
  (util/with-open [trie-wtr (open-trie-writer allocator buffer-pool
                                              (Schema. (for [^IVectorReader rdr data-rel]
                                                         (.getField rdr)))
                                              table-path trie-key)]

    (let [trie (.compactLogs trie)]
      (write-live-trie-node trie-wtr (.getRootNode trie) data-rel)

      (.end trie-wtr))))

(defn parse-trie-file-path [^Path file-path]
  (let [trie-key (str (.getFileName file-path))] 
    (when-let [[_ trie-key level-str row-from-str next-row-str] (re-find #"(log-l(\p{XDigit}+)-rf(\p{XDigit}+)-nr(\p{XDigit}+)+?)\.arrow$" trie-key)]
      {:file-path file-path
       :trie-key trie-key
       :level (util/<-lex-hex-string level-str)
       :row-from (util/<-lex-hex-string row-from-str)
       :next-row (util/<-lex-hex-string next-row-str)})))

(defn current-trie-files [file-names]
  (loop [next-row 0
         [level-trie-keys & more-levels] (->> file-names
                                              (keep parse-trie-file-path)
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
               (into res (map :file-path) tries))
        (recur next-row more-levels res)))))

(defn postwalk-merge-plan
  "Post-walks the merged tries, passing the nodes from each of the tries to the given fn.
   e.g. for a leaf: passes the trie-nodes to the fn, returns the result.
        for a branch: passes a vector the return values of the postwalk fn
                      for the inner nodes, for the fn to combine

   Returns the value returned from the postwalk fn for the root node.

   segments :: [Segment]
     Segment :: {:keys [trie]} ;; and anything else you need - you'll get this back in `:leaf`
       trie :: HashTrie

   f :: path, merge-node -> ret
     merge-node :: [:branch [ret]]
                 | [:leaf [Segment] [HashTrie$Node]]"
  [segments f]

  (letfn [(postwalk* [segments nodes path-vec]
            (let [trie-children (mapv #(some-> ^HashTrie$Node % (.getChildren)) nodes)
                  path (byte-array path-vec)]
              (f path
                 (if-let [^objects first-children (some identity trie-children)]
                   [:branch (lazy-seq
                             (->> (range (alength first-children))
                                  (mapv (fn [bucket-idx]
                                          (postwalk* segments
                                                     (mapv (fn [node ^objects node-children]
                                                             (if node-children
                                                               (aget node-children bucket-idx)
                                                               node))
                                                           nodes trie-children)
                                                     (conj path-vec bucket-idx))))))]

                   [:leaf segments nodes]))))]

    (postwalk* segments
               (mapv (fn [{:keys [^HashTrie trie]}]
                       (some-> trie .getRootNode))
                     segments)
               [])))

(defrecord MetaFile [^HashTrie trie, ^ArrowBuf buf, ^RelationReader rdr]
  AutoCloseable
  (close [_]
    (util/close rdr)
    (util/close buf)))

(defn open-meta-file [^IBufferPool buffer-pool ^Path file-path]
  (util/with-close-on-catch [^ArrowBuf buf @(.getBuffer buffer-pool file-path)]
    (let [{:keys [^VectorLoader loader ^VectorSchemaRoot root arrow-blocks]} (util/read-arrow-buf buf)
          nodes-vec (.getVector root "nodes")]
      (with-open [record-batch (util/->arrow-record-batch-view (first arrow-blocks) buf)]
        (.load loader record-batch)
        (->MetaFile (ArrowHashTrie. nodes-vec) buf (vr/<-root root))))))

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
    (.select live-rel (.getData ^LiveHashTrie$Leaf leaf)))

  AutoCloseable
  (close [_]))

(defn open-data-rels [^IBufferPool buffer-pool, ^Path table-path, trie-keys, ^ILiveTableWatermark live-table-wm]
  (util/with-close-on-catch [data-bufs (ArrayList.)]
    ;; TODO get hold of these a page at a time if it's a small query,
    ;; rather than assuming we'll always have/use the whole file.
    (let [arrow-data-rels (->> trie-keys
                               (mapv (fn [trie-key]
                                       (.add data-bufs @(.getBuffer buffer-pool (->table-data-file-path table-path trie-key)))
                                       (let [data-buf (.get data-bufs (dec (.size data-bufs)))
                                             {:keys [^VectorSchemaRoot root loader arrow-blocks]} (util/read-arrow-buf data-buf)]

                                         (ArrowDataRel. data-buf root loader arrow-blocks -1)))))]
      (cond-> arrow-data-rels
        live-table-wm (conj (->LiveDataRel (.liveRelation live-table-wm)))))))

(defn load-data-pages [data-rels trie-leaves]
  (assert (= (count data-rels) (count trie-leaves)))

  (mapv (fn [^IDataRel data-rel trie-leaf]
          (when trie-leaf
            (.loadPage data-rel trie-leaf)))
        data-rels
        trie-leaves))

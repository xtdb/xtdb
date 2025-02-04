(ns xtdb.trie
  (:require [clojure.string :as str]
            [xtdb.buffer-pool]
            [xtdb.metadata :as meta]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (java.lang AutoCloseable)
           (java.nio.file Path)
           (java.util ArrayList Arrays List)
           (java.util.concurrent.atomic AtomicInteger)
           (org.apache.arrow.memory ArrowBuf BufferAllocator)
           (org.apache.arrow.vector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo ArrowType$Union Field Schema)
           org.apache.arrow.vector.types.UnionMode
           (xtdb.arrow Relation)
           xtdb.BufferPool
           (xtdb.trie ArrowHashTrie$Leaf HashTrie$Node ISegment MemoryHashTrie MemoryHashTrie$Leaf MergePlanNode TrieWriter)
           (xtdb.util TemporalBounds)))

(defn ->log-l0-l1-trie-key [^long level, ^long first-row ,^long next-row, ^long row-count]
  (assert (<= 0 level 1))

  (format "log-l%s-fr%s-nr%s-rs%s"
          (util/->lex-hex-string level)
          (util/->lex-hex-string first-row)
          (util/->lex-hex-string next-row)
          (Long/toString row-count 16)))

(defn ->log-l2+-trie-key [^long level, ^bytes part, ^long next-row]
  (assert (>= level 2))

  (format "log-l%s-p%s-nr%s"
          (util/->lex-hex-string level)
          (str/join part)
          (util/->lex-hex-string next-row)))

(def ^java.nio.file.Path tables-dir (util/->path "tables"))

(defn- table-name->table-path ^java.nio.file.Path [^String table-name]
  (.resolve tables-dir (-> table-name (str/replace #"[\.\/]" "\\$"))))

(defn ->table-data-file-path ^java.nio.file.Path [table-name trie-key]
  (-> (table-name->table-path table-name)
      (.resolve (format "data/%s.arrow" trie-key))))

(defn ->table-meta-dir ^java.nio.file.Path [table-name]
  (-> (table-name->table-path table-name)
      (.resolve "meta")))

(defn ->table-meta-file-path [table-name trie-key]
  (-> (->table-meta-dir table-name)
      (.resolve (format "%s.arrow" trie-key))))

(def ^org.apache.arrow.vector.types.pojo.Schema meta-rel-schema
  (Schema. [(types/->field "nodes" (ArrowType$Union. UnionMode/Dense (int-array (range 4))) false
                           (types/col-type->field "nil" :null)
                           (types/col-type->field "branch-iid" [:list [:union #{:null :i32}]])
                           (types/->field "branch-recency" (types/->arrow-type [:map {:sorted? true}]) false
                                          (types/->field "recency-el" (types/->arrow-type :struct) false
                                                         (types/col-type->field "recency" types/temporal-col-type)
                                                         (types/col-type->field "idx" [:union #{:null :i32}])))

                           (types/col-type->field "leaf" [:struct {'data-page-idx :i32
                                                                   'columns meta/metadata-col-type}]))]))

(defn data-rel-schema ^org.apache.arrow.vector.types.pojo.Schema [^Field put-doc-field]
  (Schema. [(types/col-type->field "_iid" [:fixed-size-binary 16])
            (types/col-type->field "_system_from" types/temporal-col-type)
            (types/col-type->field "_valid_from" types/temporal-col-type)
            (types/col-type->field "_valid_to" types/temporal-col-type)
            (types/->field "op" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                           put-doc-field
                           (types/col-type->field "delete" :null)
                           (types/col-type->field "erase" :null))]))

(defn open-log-data-wtr
  (^xtdb.vector.IRelationWriter [^BufferAllocator allocator]
   (open-log-data-wtr allocator (data-rel-schema (types/col-type->field "put" [:struct {}]))))

  (^xtdb.vector.IRelationWriter [^BufferAllocator allocator data-schema]
   (util/with-close-on-catch [root (VectorSchemaRoot/create data-schema allocator)]
     (vw/root->writer root))))

(defn open-trie-writer ^TrieWriter [^BufferAllocator allocator, ^BufferPool buffer-pool,
                                    ^Schema data-schema, table-name, trie-key
                                    write-content-metadata?]
  (util/with-close-on-catch [data-rel (Relation. allocator data-schema)
                             data-file-wtr (.openArrowWriter buffer-pool (->table-data-file-path table-name trie-key) data-rel)
                             meta-rel (Relation. allocator meta-rel-schema)]

    (let [node-wtr (.get meta-rel "nodes")

          iid-branch-wtr (.legWriter node-wtr "branch-iid")
          iid-branch-el-wtr (.elementWriter iid-branch-wtr)

          recency-branch-wtr (.legWriter node-wtr "branch-recency")
          recency-el-wtr (.elementWriter recency-branch-wtr)
          recency-wtr (.keyWriter recency-el-wtr "recency")
          recency-idx-wtr (.keyWriter recency-el-wtr "idx")

          leaf-wtr (.legWriter node-wtr "leaf")
          page-idx-wtr (.keyWriter leaf-wtr "data-page-idx")
          page-meta-wtr (meta/->page-meta-wtr (.keyWriter leaf-wtr "columns"))
          !page-idx (AtomicInteger. 0)]

      (reify TrieWriter
        (getDataRel [_] data-rel)

        (writeLeaf [_]
          (let [put-rdr (-> data-rel
                            (.get "op")
                            (.legReader "put"))

                meta-pos (.getValueCount node-wtr)]

            (.writeMetadata page-meta-wtr (into [(.get data-rel "_system_from")
                                                 (.get data-rel "_valid_from")
                                                 (.get data-rel "_valid_to")
                                                 (.get data-rel "_iid")]
                                                (map #(.keyReader put-rdr %))
                                                (when write-content-metadata?
                                                  (.getKeys put-rdr))))

            (.writeInt page-idx-wtr (.getAndIncrement !page-idx))
            (.endStruct leaf-wtr)
            (.endRow meta-rel)

            (.writePage data-file-wtr)
            (.clear data-rel)

            meta-pos))

        (writeRecencyBranch [_ buckets]
          (let [pos (.getValueCount node-wtr)]
            (doseq [[^long recency, ^long idx] buckets]
              (.writeLong recency-wtr recency)
              (.writeInt recency-idx-wtr idx)
              (.endStruct recency-el-wtr))

            (.endList recency-branch-wtr)
            (.endRow meta-rel)

            pos))

        (writeIidBranch [_ idxs]
          (let [pos (.getValueCount node-wtr)]
            (dotimes [n (alength idxs)]
              (let [idx (aget idxs n)]
                (if (= idx -1)
                  (.writeNull iid-branch-el-wtr)
                  (.writeInt iid-branch-el-wtr idx))))

            (.endList iid-branch-wtr)
            (.endRow meta-rel)

            pos))

        (end [_]
          (let [data-file-size (.end data-file-wtr)]

            (util/with-open [meta-file-wtr (.openArrowWriter buffer-pool (->table-meta-file-path table-name trie-key) meta-rel)]
              (.writePage meta-file-wtr)
              (.end meta-file-wtr))

            data-file-size))

        AutoCloseable
        (close [_]
          (util/close [data-rel data-file-wtr meta-rel]))))))

(defn write-live-trie-node [^TrieWriter trie-wtr, ^HashTrie$Node node, ^Relation data-rel]
  (let [copier (.rowCopier (.getDataRel trie-wtr) data-rel)]
    (letfn [(write-node! [^HashTrie$Node node]
              (if-let [children (.getIidChildren node)]
                (let [child-count (alength children)
                      !idxs (int-array child-count)]
                  (dotimes [n child-count]
                    (aset !idxs n
                          (unchecked-int
                           (if-let [child (aget children n)]
                             (write-node! child)
                             -1))))

                  (.writeIidBranch trie-wtr !idxs))

                (let [^MemoryHashTrie$Leaf leaf node]
                  (-> (Arrays/stream (.getData leaf))
                      (.forEach (fn [idx]
                                  (.copyRow copier idx))))

                  (.writeLeaf trie-wtr))))]

      (write-node! node))))

(defn write-live-trie! [^BufferAllocator allocator, ^BufferPool buffer-pool,
                        table-name, trie-key,
                        ^MemoryHashTrie trie, ^Relation data-rel]
  (util/with-open [trie-wtr (open-trie-writer allocator buffer-pool (.getSchema data-rel) table-name trie-key
                                              false)]
    (let [trie (.compactLogs trie)]
      (write-live-trie-node trie-wtr (.getRootNode trie) data-rel)

      (.end trie-wtr))))

(def ^:private trie-file-path-regex
  ;; e.g. `log-l01-fr0-nr12e-rs20.arrow` or `log-l04-p0010-nr12e.arrow`
  #"(log-l(\p{XDigit}+)(?:-p(\p{XDigit}+))?(?:-fr(\p{XDigit}+))?-nr(\p{XDigit}+)(?:-rs(\p{XDigit}+))?)(\.arrow)?$")

(defn parse-trie-key [trie-key]
  (when-let [[_ trie-key level-str part-str first-row next-row-str rows-str] (re-find trie-file-path-regex trie-key)]
    (cond-> {:trie-key trie-key
             :level (util/<-lex-hex-string level-str)
             :next-row (util/<-lex-hex-string next-row-str)}
      first-row (assoc :first-row (util/<-lex-hex-string first-row))
      part-str (assoc :part (byte-array (map #(Character/digit ^char % 4) part-str)))
      rows-str (assoc :rows (Long/parseLong rows-str 16)))))

(defn parse-trie-file-path [^Path file-path]
  (-> (parse-trie-key (str (.getFileName file-path)))
      (assoc :file-path file-path)))

(defrecord Segment [trie]
  ISegment
  (getTrie [_] trie))

(defprotocol MergePlanPage
  (load-page [mpg ^BufferPool buffer-pool vsr-cache])
  (test-metadata [msg])
  (temporal-bounds [msg]))

(defn ->live-trie ^MemoryHashTrie [log-limit page-limit iid-rdr]
  (-> (doto (MemoryHashTrie/builder iid-rdr)
        (.setLogLimit log-limit)
        (.setPageLimit page-limit))
      (.build)))

(defn max-valid-to ^long [^TemporalBounds tb]
  (.getUpper (.getValidTime tb)))

(defn min-valid-from ^long [^TemporalBounds tb]
  (.getLower (.getValidTime tb)))

(defn ->merge-task
  ([mp-pages] (->merge-task mp-pages (TemporalBounds.)))
  ([mp-pages ^TemporalBounds query-bounds]
   (let [leaves (ArrayList.)]
     (loop [[mp-page & more-mp-pages] mp-pages
            node-taken? false
            largest-valid-to Long/MIN_VALUE
            non-taken-pages []]
       (if mp-page
         (let [^TemporalBounds page-bounds (temporal-bounds mp-page)
               take-node? (and (.intersects page-bounds query-bounds)
                               (test-metadata mp-page))]

           (if take-node?
             (do
               (.add leaves mp-page)
               (recur more-mp-pages
                      true
                      (max largest-valid-to (max-valid-to page-bounds))
                      non-taken-pages))

             (recur more-mp-pages
                    node-taken?
                    largest-valid-to
                    (cond-> non-taken-pages
                      (let [page-valid-time (.getValidTime page-bounds)]
                        (and node-taken?
                             (.intersects (.getSystemTime page-bounds) (.getSystemTime query-bounds))
                             ;; this page can bound a page in the query set
                             (< (.getLower (.getValidTime query-bounds)) (.getUpper page-valid-time))
                             (< (.getLower page-valid-time) largest-valid-to)))
                      (conj mp-page)))))

         (when node-taken?
           (loop [[page & more-pages] non-taken-pages]
             (when page
               (let [smallest-valid-from (min-valid-from (temporal-bounds page))]
                 (when (< smallest-valid-from largest-valid-to)
                   (.add leaves page))
                 (recur more-pages))))
           (vec leaves)))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface IDataRel
  (^org.apache.arrow.vector.types.pojo.Schema getSchema [])
  (^xtdb.arrow.RelationReader loadPage [trie-leaf]))

(deftype ArrowDataRel [^BufferPool buffer-pool
                       ^Path data-file
                       ^Schema schema
                       ^List rels-to-close]
  IDataRel
  (getSchema [_] schema)

  (loadPage [_ trie-leaf]
    (util/with-open [rb (.getRecordBatch buffer-pool data-file (.getDataPageIndex ^ArrowHashTrie$Leaf trie-leaf))]
      (let [alloc (.getAllocator (.getReferenceManager ^ArrowBuf (first (.getBuffers rb))))
            rel (Relation/fromRecordBatch alloc schema rb)]
        (.add rels-to-close rel)
        rel)))

  AutoCloseable
  (close [_]
    (util/close rels-to-close)))

(defn open-data-rels [^BufferPool buffer-pool, table-name, trie-keys]
  (util/with-close-on-catch [data-rels (ArrayList.)]
    (doseq [trie-key trie-keys]
      (let [data-file (->table-data-file-path table-name trie-key)
            footer (.getFooter buffer-pool data-file)]
        (.add data-rels (ArrowDataRel. buffer-pool data-file (.getSchema footer) (ArrayList.)))))
    (vec data-rels)))

(defn load-data-page [^MergePlanNode merge-plan-node]
  (let [{:keys [^IDataRel data-rel]} (.getSegment merge-plan-node)
        trie-leaf (.getNode merge-plan-node)]
    (.loadPage data-rel trie-leaf)))

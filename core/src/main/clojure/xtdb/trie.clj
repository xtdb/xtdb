(ns xtdb.trie
  (:require [clojure.string :as str]
            [xtdb.buffer-pool]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (java.lang AutoCloseable)
           (java.nio.file Path)
           (java.util ArrayList Arrays List)
           (org.apache.arrow.memory ArrowBuf BufferAllocator)
           (org.apache.arrow.vector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo ArrowType$Union Field Schema)
           org.apache.arrow.vector.types.UnionMode
           (xtdb.arrow Relation)
           xtdb.BufferPool
           (xtdb.trie ArrowHashTrie$Leaf HashTrie$Node ISegment MemoryHashTrie MemoryHashTrie$Leaf MergePlanNode TrieWriter)
           (xtdb.util TemporalBounds TemporalDimension)))

(defn ->l0-l1-trie-key [^long level, ^long block-idx]
  (assert (<= 0 level 1))

  (format "l%s-b%s" (util/->lex-hex-string level) (util/->lex-hex-string block-idx)))

(defn ->l2+-trie-key [^long level, ^bytes part, ^long block-idx]
  (assert (>= level 2))

  (format "l%s-p%s-b%s"
          (util/->lex-hex-string level)
          (str/join part)
          (util/->lex-hex-string block-idx)))

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
  (util/with-open [trie-wtr (TrieWriter. allocator buffer-pool (.getSchema data-rel) table-name trie-key
                                         false)]
    (let [trie (.compactLogs trie)]
      (write-live-trie-node trie-wtr (.getRootNode trie) data-rel)

      (.end trie-wtr))))

(def ^:private trie-file-path-regex
  ;; e.g. `l01-b00-rs20.arrow` or `l04-p0010-b12e.arrow`
  #"(l(\p{XDigit}+)(?:-p(\p{XDigit}+))?(?:-b(\p{XDigit}+)))(\.arrow)?$")

(defn parse-trie-key [trie-key]
  (when-let [[_ trie-key level-str part-str block-idx-str] (re-find trie-file-path-regex trie-key)]
    (cond-> {:trie-key trie-key
             :level (util/<-lex-hex-string level-str)
             :block-idx  (util/<-lex-hex-string block-idx-str)}
      part-str (assoc :part (byte-array (map #(Character/digit ^char % 4) part-str))))))

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

(defn min-system-from ^long [^TemporalBounds tb]
  (.getLower (.getSystemTime tb)))

(defn max-system-from ^long [^TemporalBounds tb]
  (.getMaxSystemFrom tb))

(defn ->merge-task
  ([mp-pages] (->merge-task mp-pages (TemporalBounds.)))
  ([mp-pages ^TemporalBounds query-bounds]
   (let [leaves (ArrayList.)]
     (loop [[mp-page & more-mp-pages] mp-pages
            node-taken? false
            smallest-valid-from Long/MAX_VALUE
            largest-valid-to Long/MIN_VALUE
            smallest-system-from Long/MAX_VALUE
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
                      (min smallest-valid-from (min-valid-from page-bounds))
                      (max largest-valid-to (max-valid-to page-bounds))
                      (min smallest-system-from (min-system-from page-bounds))
                      non-taken-pages))

             (recur more-mp-pages
                    node-taken?
                    smallest-valid-from
                    largest-valid-to
                    smallest-system-from
                    (cond-> non-taken-pages
                      (.intersects (.getSystemTime page-bounds) (.getSystemTime query-bounds))
                      (conj mp-page)))))

         (when node-taken?
           (let [valid-time (TemporalDimension. smallest-valid-from largest-valid-to)]
             (loop [[page & more-pages] non-taken-pages]
               (when page
                 (let [page-valid-time (.getValidTime ^TemporalBounds (temporal-bounds page))
                       page-largest-system-from (max-system-from (temporal-bounds page))]
                   (when (and (<= smallest-system-from page-largest-system-from)
                              (.intersects valid-time page-valid-time))
                     (.add leaves page))
                   (recur more-pages)))))
           (vec leaves)))))))

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

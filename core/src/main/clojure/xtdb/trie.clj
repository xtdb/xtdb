(ns xtdb.trie
  (:require [clojure.string :as str]
            [xtdb.buffer-pool]
            [xtdb.error :as err]
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
           (org.apache.arrow.memory ArrowBuf BufferAllocator)
           (org.apache.arrow.vector VectorLoader VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo ArrowType$Union Field Schema)
           org.apache.arrow.vector.types.UnionMode
           (xtdb.arrow Relation Relation$Loader)
           xtdb.IBufferPool
           (xtdb.trie ArrowHashTrie$Leaf HashTrie$Node ISegment MemoryHashTrie MemoryHashTrie$Leaf MergePlanNode TrieWriter)
           (xtdb.util TemporalBounds TemporalDimension)))

(def ^:private ^java.lang.ThreadLocal !msg-digest
  (ThreadLocal/withInitial
   (fn []
     (MessageDigest/getInstance "SHA-256"))))

(defn ->iid ^ByteBuffer [eid]
  (if (uuid? eid)
    (util/uuid->byte-buffer eid)
    (ByteBuffer/wrap
     (let [^bytes eid-bytes (cond
                              (string? eid) (.getBytes (str "s" eid))
                              (keyword? eid) (.getBytes (str "k" eid))
                              (integer? eid) (.getBytes (str "i" eid))
                              :else (let [id-type (some-> (class eid) .getName symbol)]
                                      (throw (err/runtime-err :xtdb/invalid-id
                                                              {::err/message (format "Invalid ID type: %s" id-type)
                                                               :type id-type
                                                               :eid eid}))))]
       (-> ^MessageDigest (.get !msg-digest)
           (.digest eid-bytes)
           (Arrays/copyOfRange 0 16))))))

(def valid-iid? (some-fn uuid? string? keyword? integer?))

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

(defn ->table-data-file-path [^Path table-path trie-key]
  (.resolve table-path (format "data/%s.arrow" trie-key)))

(defn ->table-meta-file-path [^Path table-path trie-key]
  (.resolve table-path (format "meta/%s.arrow" trie-key)))

(defn list-meta-files [^IBufferPool buffer-pool ^Path table-path]
  (.listObjects buffer-pool (.resolve table-path "meta")))

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

(defn open-trie-writer ^TrieWriter [^BufferAllocator allocator, ^IBufferPool buffer-pool,
                                    ^Schema data-schema, ^Path table-path, trie-key
                                    write-content-metadata?]
  (util/with-close-on-catch [data-rel (Relation. allocator data-schema)
                             data-file-wtr (.openArrowWriter buffer-pool (->table-data-file-path table-path trie-key) data-rel)
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

            (.writeBatch data-file-wtr)
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
          (.end data-file-wtr)

          (util/with-open [meta-file-wtr (.openArrowWriter buffer-pool (->table-meta-file-path table-path trie-key) meta-rel)]
            (.writeBatch meta-file-wtr)
            (.end meta-file-wtr)))

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

(defn write-live-trie! [^BufferAllocator allocator, ^IBufferPool buffer-pool,
                        ^Path table-path, trie-key,
                        ^MemoryHashTrie trie, ^Relation data-rel]
  (util/with-open [trie-wtr (open-trie-writer allocator buffer-pool (.getSchema data-rel) table-path trie-key
                                              false)]
    (let [trie (.compactLogs trie)]
      (write-live-trie-node trie-wtr (.getRootNode trie) data-rel)

      (.end trie-wtr))))

(def ^:private trie-file-path-regex
  ;; e.g. `log-l01-fr0-nr12e-rs20.arrow` or `log-l04-p0010-nr12e.arrow`
  #"(log-l(\p{XDigit}+)(?:-p(\p{XDigit}+))?(?:-fr(\p{XDigit}+))?-nr(\p{XDigit}+)(?:-rs(\p{XDigit}+))?)\.arrow$")

(defn parse-trie-file-path [^Path file-path]
  (let [trie-key (str (.getFileName file-path))]
    (when-let [[_ trie-key level-str part-str first-row next-row-str rows-str] (re-find trie-file-path-regex trie-key)]
      (cond-> {:file-path file-path
               :trie-key trie-key
               :level (util/<-lex-hex-string level-str)
               :next-row (util/<-lex-hex-string next-row-str)}
        first-row (assoc :first-row (util/<-lex-hex-string first-row))
        part-str (assoc :part (byte-array (map #(Character/digit ^char % 4) part-str)))
        rows-str (assoc :rows (Long/parseLong rows-str 16))))))

(defn path-array
  "path-arrays are a flattened array containing one element for every possible path at the given level.
   e.g. for L3, the path array has 16 elements; path [1 3] can be found at element 13r4 = 7.

   returns :: path-array for the given level with all elements initialised to -1"
  ^longs [^long level]
  {:pre [(>= level 1)]}

  (doto (long-array (bit-shift-left 1 (* 2 (dec level))))
    (Arrays/fill -1)))

(defn path-array-idx
  "returns the idx for the given path in the flattened path array.
   in effect, returns the path as a base-4 number"
  (^long [^bytes path] (path-array-idx path (alength path)))

  (^long [^bytes path, ^long len]
   (loop [idx 0
          res 0]
     (if (= idx len)
       res
       (recur (inc idx)
              (+ (* res 4) (aget path idx)))))))

(defn rows-covered-below
  "This function returns the row for each path that L<n> has completely covered.
   e.g. if L<n> has paths 0130, 0131, 0132 and 0133 covered to a minimum of row 384,
        then everything in L<n-1> for path 013 is covered for row <= 384.

   covered-rows :: a path-array for the maximum written row for each path at L<n>
   returns :: a path-array of the covered row for every path at L<n-1>"

  ^longs [^longs covered-rows]

  (let [out-len (/ (alength covered-rows) 4)
        res (doto (long-array out-len)
              (Arrays/fill -1))]
    (dotimes [n out-len]
      (let [start-idx (* n 4)]
        (aset res n (-> (aget covered-rows start-idx)
                        (min (aget covered-rows (+ start-idx 1)))
                        (min (aget covered-rows (+ start-idx 2)))
                        (min (aget covered-rows (+ start-idx 3)))))))
    res))

(defn- file-names->level-groups [file-names]
  (->> file-names
       (keep parse-trie-file-path)
       (group-by :level)))

(defn current-trie-files [file-names]
  (when (seq file-names)
    (let [!current-trie-keys (ArrayList.)

          {l0-trie-keys 0, l1-trie-keys 1, :as level-grouped-file-names} (file-names->level-groups file-names)

          ;; filtering superseded L1 files
          l1-trie-keys (into [] (comp (partition-by :first-row) (map last)) l1-trie-keys)

          max-level (long (last (sort (keys level-grouped-file-names))))

          ^long l2+-covered-row (if (<= max-level 1)
                                  -1

                                  (loop [level max-level
                                         ^longs !covered-rows (path-array level)]
                                    (let [lvl-trie-keys (get level-grouped-file-names level)
                                          _ (assert (= lvl-trie-keys (seq (sort-by :file-path lvl-trie-keys)))
                                                    "lvl-trie-keys not sorted")

                                          uncovered-lvl-trie-keys (->> lvl-trie-keys
                                                                       ;; eager because we're mutating `!covered-rows`
                                                                       (filterv (fn not-covered-above [{:keys [part ^long next-row]}]
                                                                                  (let [idx (path-array-idx part)]
                                                                                    (when (> next-row (aget !covered-rows idx))
                                                                                      (aset !covered-rows idx next-row)
                                                                                      true)))))

                                          !covered-below (rows-covered-below !covered-rows)]

                                      ;; when the whole path-set is covered below, this is a current file
                                      (doseq [{:keys [^bytes part, ^long next-row] :as trie-key} uncovered-lvl-trie-keys
                                              :when (>= (aget !covered-below (path-array-idx part (dec (alength part)))) next-row)]
                                        (.add !current-trie-keys trie-key))

                                      (if (= level 2)
                                        (aget !covered-below 0)
                                        (recur (dec level) !covered-below)))))

          l1-covered-row (long (or (last (for [{:keys [^long next-row] :as trie-key} l1-trie-keys
                                               :when (< l2+-covered-row next-row)]
                                           (do
                                             (.add !current-trie-keys trie-key)
                                             next-row)))
                                   l2+-covered-row))]

      (doseq [{:keys [^long next-row] :as trie-key} l0-trie-keys
              :when (< l1-covered-row next-row)]
        (.add !current-trie-keys trie-key))

      (mapv :file-path !current-trie-keys))))

(defrecord Segment [trie]
  ISegment
  (getTrie [_] trie))

(defprotocol MergePlanPage
  (load-page [mpg buffer-pool vsr-cache])
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

(deftype ArrowDataRel [^ArrowBuf buf
                       ^Relation$Loader loader
                       ^List rels-to-close]
  IDataRel
  (getSchema [_] (.getSchema loader))

  (loadPage [_ trie-leaf]
    (let [rel (Relation. (.getAllocator (.getReferenceManager buf)) (.getSchema loader))]
      (.add rels-to-close rel)
      (.loadBatch loader (.getDataPageIndex ^ArrowHashTrie$Leaf trie-leaf) rel)
      rel))

  AutoCloseable
  (close [_]
    (util/close rels-to-close)
    (util/close loader)
    (util/close buf)))

(defn open-data-rels [^IBufferPool buffer-pool, ^Path table-path, trie-keys]
  (util/with-close-on-catch [data-rels (ArrayList.)]
    (doseq [trie-key trie-keys]
      (util/with-close-on-catch [data-buf (.getBuffer buffer-pool (->table-data-file-path table-path trie-key))]
        (.add data-rels (ArrowDataRel. data-buf (Relation/loader data-buf) (ArrayList.)))))

    (vec data-rels)))

(defn load-data-page [^MergePlanNode merge-plan-node]
  (let [{:keys [^IDataRel data-rel]} (.getSegment merge-plan-node)
        trie-leaf (.getNode merge-plan-node)]
    (.loadPage data-rel trie-leaf)))

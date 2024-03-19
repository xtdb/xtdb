(ns xtdb.trie
  (:require [clojure.string :as str]
            [xtdb.buffer-pool]
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
           (xtdb.trie MergePlanNode ArrowHashTrie$Leaf HashTrie$Node ITrieWriter LiveHashTrie LiveHashTrie$Leaf ISegment)
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

(defn ->log-l0-l1-trie-key [^long level, ^long next-row, ^long row-count]
  (assert (<= 0 level 1))

  (format "log-l%s-nr%s-rs%s"
          (util/->lex-hex-string level)
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
                           (types/->field "branch-recency" #xt.arrow/type [:map {:sorted? true}] false
                                          (types/->field "recency-el" #xt.arrow/type :struct false
                                                         (types/col-type->field "recency" types/temporal-col-type)
                                                         (types/col-type->field "idx" [:union #{:null :i32}])))

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

(defn open-log-data-wtr
  (^xtdb.vector.IRelationWriter [^BufferAllocator allocator]
   (open-log-data-wtr allocator (data-rel-schema [:struct {}])))

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

          iid-branch-wtr (.legWriter node-wtr :branch-iid)
          iid-branch-el-wtr (.listElementWriter iid-branch-wtr)

          recency-branch-wtr (.legWriter node-wtr :branch-recency)
          recency-el-wtr (.listElementWriter recency-branch-wtr)
          recency-wtr (.structKeyWriter recency-el-wtr "recency")
          recency-idx-wtr (.structKeyWriter recency-el-wtr "idx")

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

        (writeRecencyBranch [_ buckets]
          (let [pos (.getPosition node-wp)]
            (.startList recency-branch-wtr)

            (doseq [[^long recency, ^long idx] buckets]
              (.startStruct recency-el-wtr)
              (.writeLong recency-wtr recency)
              (.writeInt recency-idx-wtr idx)
              (.endStruct recency-el-wtr))

            (.endList recency-branch-wtr)
            (.endRow meta-rel-wtr)

            pos))

        (writeIidBranch [_ idxs]
          (let [pos (.getPosition node-wp)]
            (.startList iid-branch-wtr)

            (dotimes [n (alength idxs)]
              (let [idx (aget idxs n)]
                (if (= idx -1)
                  (.writeNull iid-branch-el-wtr)
                  (.writeInt iid-branch-el-wtr idx))))

            (.endList iid-branch-wtr)
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
          (util/close [data-vsr data-file-wtr meta-vsr]))))))

(defn write-live-trie-node [^ITrieWriter trie-wtr, ^HashTrie$Node node, ^RelationReader data-rel]
  (let [copier (.rowCopier (.getDataWriter trie-wtr) data-rel)]
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

(def ^:private trie-file-path-regex
  ;; e.g. `log-l01-nr12e-rs20.arrow` or `log-l04-p0010-nr12e.arrow`
  #"(log-l(\p{XDigit}+)(?:-p(\p{XDigit}+))?-nr(\p{XDigit}+)(?:-rs(\p{XDigit}+))?)\.arrow$")

(defn parse-trie-file-path [^Path file-path]
  (let [trie-key (str (.getFileName file-path))]
    (when-let [[_ trie-key level-str part-str next-row-str rows-str] (re-find trie-file-path-regex trie-key)]
      (cond-> {:file-path file-path
               :trie-key trie-key
               :level (util/<-lex-hex-string level-str)
               :next-row (util/<-lex-hex-string next-row-str)}
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

(defn load-data-page [^MergePlanNode merge-plan-node]
  (let [{:keys [^IDataRel data-rel]} (.getSegment merge-plan-node)
        trie-leaf (.getNode merge-plan-node)]
    (.loadPage data-rel trie-leaf)))

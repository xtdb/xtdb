(ns xtdb.indexer.rrrr
  (:require [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import [java.util Comparator PriorityQueue]
           [java.util.function IntConsumer Supplier]
           [java.util.stream IntStream]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.memory.util ArrowBufPointer]
           [org.apache.arrow.vector.ipc ArrowFileWriter]
           [org.apache.arrow.vector.types UnionMode]
           [org.apache.arrow.vector.types.pojo ArrowType$Union Schema]
           org.apache.arrow.vector.VectorSchemaRoot
           [xtdb.trie ArrowHashTrie HashTrie HashTrie$Node LiveHashTrie TrieKeys]
           [xtdb.vector IRelationWriter IVectorPosition IVectorReader]))

;; TODO shift these to some kind of test util

(def ^org.apache.arrow.vector.types.pojo.Schema trie-schema
  (Schema. [(types/->field "nodes" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                           (types/col-type->field "nil" :null)
                           (types/col-type->field "branch" [:list [:union #{:null :i32}]])
                           ;; TODO metadata
                           (types/col-type->field "leaf" '[:struct {page-idx :i32}]))]))

(defn open-arrow-hash-trie-root ^org.apache.arrow.vector.VectorSchemaRoot [^BufferAllocator al, paths]
  (util/with-close-on-catch [trie-root (VectorSchemaRoot/create trie-schema al)]
    (let [trie-wtr (vw/root->writer trie-root)
          trie-wp (.writerPosition trie-wtr)
          nodes-wtr (.writerForName trie-wtr "nodes")
          nil-wtr (.writerForTypeId nodes-wtr (byte 0))
          branch-wtr (.writerForTypeId nodes-wtr (byte 1))
          branch-el-wtr (.listElementWriter branch-wtr)
          leaf-wtr (.writerForTypeId nodes-wtr (byte 2))
          page-idx-wtr (.structKeyWriter leaf-wtr "page-idx")]
      (letfn [(write-paths [paths]
                (cond
                  (nil? paths) (.writeNull nil-wtr nil)

                  (number? paths) (do
                                    (.startStruct leaf-wtr)
                                    (.writeInt page-idx-wtr paths)
                                    (.endStruct leaf-wtr))

                  (vector? paths) (let [!page-idxs (IntStream/builder)]
                                    (doseq [child paths]
                                      (.add !page-idxs (if child
                                                         (do
                                                           (write-paths child)
                                                           (dec (.getPosition trie-wp)))
                                                         -1)))
                                    (.startList branch-wtr)
                                    (.forEach (.build !page-idxs)
                                              (reify IntConsumer
                                                (accept [_ idx]
                                                  (if (= idx -1)
                                                    (.writeNull branch-el-wtr nil)
                                                    (.writeInt branch-el-wtr idx)))))
                                    (.endList branch-wtr)))
                (.endRow trie-wtr))]
        (write-paths paths))

      (.syncRowCount trie-wtr))

    trie-root))

;; T1 trie + log -> T1 trie, T2/T3 log
;; T2 trie + T2 log -> T2 trie, T4 log
;; T3 trie + T3 log -> T3 trie
;; T4 trie + T4 log -> T4 trie

(def ^:private fields
  {:iid (types/col-type->field "xt$iid" [:fixed-size-binary 16])
   :sys-from (types/col-type->field "xt$system_from" types/temporal-col-type)
   :sys-to (types/col-type->field "xt$system_to" types/temporal-col-type)
   :valid-from (types/col-type->field "xt$valid_from" types/temporal-col-type)
   :valid-to (types/col-type->field "xt$valid_to" types/temporal-col-type)
   :doc (types/col-type->field 'xt$doc [:struct {}])})

(def ^:private ^org.apache.arrow.vector.types.pojo.Schema t1-schema
  (Schema. (mapv fields [:iid :valid-from :sys-from :doc])))

(def ^:private ^org.apache.arrow.vector.types.pojo.Schema t2-schema
  (Schema. (mapv fields [:iid :valid-from :valid-to :sys-from :doc])))

(def ^:private ^org.apache.arrow.vector.types.pojo.Schema t3-schema
  (Schema. (mapv fields [:iid :valid-from :sys-from :sys-to :doc])))

(def ^:private ^org.apache.arrow.vector.types.pojo.Schema t4-schema
  (Schema. (mapv fields [:iid :valid-from :valid-to :sys-from :sys-to :doc])))

(def ^:private abp-supplier
  (reify Supplier
    (get [_] (ArrowBufPointer.))))

(def ^:private ^java.lang.ThreadLocal !left-abp (ThreadLocal/withInitial abp-supplier))
(def ^:private ^java.lang.ThreadLocal !right-abp (ThreadLocal/withInitial abp-supplier))
(def ^:private ^java.lang.ThreadLocal !reuse-ptr (ThreadLocal/withInitial abp-supplier))

(defn- ->log-ptr-cmp ^java.util.Comparator []
  (let [left-abp (.get !left-abp)
        right-abp (.get !right-abp)]
    (-> (reify Comparator
          (compare [_
                    {^IVectorReader l-iid :iid-rdr, ^IVectorPosition l-rp :rp}
                    {^IVectorReader r-iid :iid-rdr, ^IVectorPosition r-rp :rp}]
            (.compareTo (.getPointer l-iid (.getPosition l-rp) left-abp)
                        (.getPointer r-iid (.getPosition r-rp) right-abp))))

        (.thenComparing
         (reify Comparator
           (compare [_ {^long left-trie-idx :trie-idx} {^long right-trie-idx :trie-idx}]
             (Long/compare right-trie-idx left-trie-idx)))))))

(defn ->iid-partitions [^bytes path pages]
  (let [reuse-ptr (.get !reuse-ptr)
        pq (PriorityQueue. (->log-ptr-cmp))]
    (letfn [(get-ptr [{:keys [^IVectorPosition rp ^IVectorReader iid-rdr]} reuse-ptr]
              (.getPointer iid-rdr (.getPosition rp) reuse-ptr))

            (enqueue-log-ptr! [{:keys [^IVectorReader iid-rdr, ^IVectorPosition rp] :as log-ptr}]
              (when log-ptr
                (when (and log-ptr
                           (> (.valueCount iid-rdr) (.getPosition rp))
                           (zero? (TrieKeys/compareToPath (get-ptr log-ptr reuse-ptr) path)))
                  (.add pq log-ptr))))

            (->iid-parts* []
              (lazy-seq
               (when-let [log-ptr (.peek pq)]
                 (cons (let [!trie-idxs (IntStream/builder)
                             !rel-idxs (IntStream/builder)]
                         (letfn [(add! [{:keys [trie-idx ^IVectorPosition rp] :as log-ptr}]
                                   (.add !trie-idxs trie-idx)
                                   (.add !rel-idxs (.getPositionAndIncrement rp))

                                   (enqueue-log-ptr! log-ptr))]

                           (let [cmp-abp (get-ptr log-ptr (ArrowBufPointer.))]
                             (add! (.remove pq))
                             (while (when-let [log-ptr (.peek pq)]
                                      (when (= cmp-abp (get-ptr log-ptr reuse-ptr))
                                        (add! (.remove pq))
                                        true)))))

                         [(.toArray (.build !trie-idxs))
                          (.toArray (.build !rel-idxs))])

                       (->iid-parts*)))))]

      (run! enqueue-log-ptr! pages)

      (->iid-parts*))))

(comment
  (defn apply-t1-logs [^bytes path pages
                       ^LiveHashTrie t1-trie, ^IRelationWriter t1-rel]
    ;; TODO static page
    (let [#_#_
          t1-doc-writer (.writerForName t1-rel "xt$doc")
          [_static-page & log-pages] pages
          #_#_
          t1-doc-copiers (->> log-pages
                              (mapv (fn [{:keys [^RelationReader leaf-rel]}]
                                      (.rowCopier (-> (.readerForName leaf-rel "op")
                                                      (.legReader :put)
                                                      (.structKeyReader "xt$doc"))
                                                  t1-doc-writer))))
          ]


      (vec (->iid-partitions path pages))))

  (with-open [al (RootAllocator.)
              iid1 (-> (types/col-type->field 'iid1 [:fixed-size-binary 16])
                       (.createVector al)
                       (vw/->writer))
              iid2 (-> (types/col-type->field 'iid2 [:fixed-size-binary 16])
                       (.createVector al)
                       (vw/->writer))]
    (.writeObject iid1 (util/uuid->bytes #uuid "7b2c1b5f-f6e4-4219-a3c7-06d10007aff0"))
    (.writeObject iid1 (util/uuid->bytes #uuid "7c2c1b5f-f6e4-4219-a3c7-06d10007aff0"))

    (.writeObject iid2 (util/uuid->bytes #uuid "7b2c1b5f-f6e4-4219-a3c7-06d10007aff0"))
    (.writeObject iid2 (util/uuid->bytes #uuid "84a4568f-f2bf-4720-94f3-348d6037817e"))

    (let [pages (into-array [nil
                             {:trie-idx 1, :iid-rdr (vw/vec-wtr->rdr iid1), :rp (IVectorPosition/build)}
                             {:trie-idx 2, :iid-rdr (vw/vec-wtr->rdr iid2), :rp (IVectorPosition/build)}])]
      [(apply-t1-logs (byte-array [7]) pages nil nil)
       (apply-t1-logs (byte-array [8]) pages nil nil)])))

(defn trie-merge-tasks [tries]
  (letfn [(trie-merge-tasks* [nodes path]
            (let [trie-children (mapv #(some-> ^HashTrie$Node % (.children)) nodes)]
              (if-let [^objects first-children (some identity trie-children)]
                (->> (range (alength first-children))
                     (mapcat (fn [bucket-idx]
                               (trie-merge-tasks* (mapv (fn [node ^objects node-children]
                                                          (if node-children
                                                            (aget node-children bucket-idx)
                                                            node))
                                                        nodes trie-children)
                                                  (conj path bucket-idx)))))
                [{:path (byte-array path)
                  :leaves (->> nodes
                               (into [] (keep-indexed
                                         (fn [trie-idx ^HashTrie$Node node]
                                           (when node
                                             {:trie-idx trie-idx, :leaf node})))))}])))]

    (vec (trie-merge-tasks* (map #(some-> ^HashTrie % (.rootNode)) tries) []))))

(comment
  (with-open [al (RootAllocator.)
              t1-root (open-arrow-hash-trie-root al [[nil 1 nil 3] 1 nil 3])
              log-root (open-arrow-hash-trie-root al 1)
              log2-root (open-arrow-hash-trie-root al [nil nil 3 4])]
    (trie-merge-tasks [nil (ArrowHashTrie/from t1-root) (ArrowHashTrie/from log-root) (ArrowHashTrie/from log2-root)])))

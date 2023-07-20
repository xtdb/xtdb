(ns xtdb.indexer.rrrr
  (:require [xtdb.trie :as trie]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import [java.util Comparator PriorityQueue]
           [java.util.function IntConsumer Supplier]
           [java.util.stream IntStream]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.memory.util ArrowBufPointer]
           [org.apache.arrow.vector.types UnionMode]
           [org.apache.arrow.vector.types.pojo ArrowType$Union Schema]
           org.apache.arrow.vector.VectorSchemaRoot
           [xtdb.trie ArrowHashTrie HashTrie HashTrie$Node LiveHashTrie TrieKeys]
           [xtdb.vector IRelationWriter IRowCopier IVectorPosition IVectorReader RelationReader]))

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
   :doc-diff (types/->field "xt$doc" (ArrowType$Union. UnionMode/Dense (int-array (range 2))) false
                            (types/col-type->field :put [:struct {}])
                            (types/col-type->field :delete :null))})

(def ^:private ^org.apache.arrow.vector.types.pojo.Schema t1-diff-schema
  (Schema. (mapv fields [:iid :valid-from :sys-from :doc-diff])))

(def ^:private ^org.apache.arrow.vector.types.pojo.Schema t2-diff-schema
  (Schema. (mapv fields [:iid :valid-from :valid-to :sys-from :doc-diff])))

(def ^:private ^org.apache.arrow.vector.types.pojo.Schema t3-diff-schema
  (Schema. (mapv fields [:iid :valid-from :sys-from :sys-to :doc-diff])))

(def ^:private ^org.apache.arrow.vector.types.pojo.Schema t4-diff-schema
  (Schema. (mapv fields [:iid :valid-from :valid-to :sys-from :sys-to :doc-diff])))

(def ^:private abp-supplier
  (reify Supplier
    (get [_] (ArrowBufPointer.))))

(def ^:private ^java.lang.ThreadLocal !left-abp (ThreadLocal/withInitial abp-supplier))
(def ^:private ^java.lang.ThreadLocal !right-abp (ThreadLocal/withInitial abp-supplier))
(def ^:private ^java.lang.ThreadLocal !reuse-ptr (ThreadLocal/withInitial abp-supplier))

(defn- ->trie-ptr-cmp ^java.util.Comparator []
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

(defn ->iid-partitions [tries, ^bytes path]
  (let [reuse-ptr (.get !reuse-ptr)
        pq (PriorityQueue. (->trie-ptr-cmp))]
    (letfn [(get-ptr [{:keys [^IVectorPosition rp ^IVectorReader iid-rdr]} reuse-ptr]
              (.getPointer iid-rdr (.getPosition rp) reuse-ptr))

            (enqueue-trie-ptr! [{:keys [^IVectorReader iid-rdr, ^IVectorPosition rp] :as trie-ptr}]
              (when trie-ptr
                (when (and trie-ptr
                           (> (.valueCount iid-rdr) (.getPosition rp))
                           (zero? (TrieKeys/compareToPath (get-ptr trie-ptr reuse-ptr) path)))
                  (.add pq trie-ptr))))

            (->iid-parts* []
              (lazy-seq
               (when-let [{:keys [^long trie-idx, ^IVectorPosition rp] :as trie-ptr} (.poll pq)]
                 (if (zero? trie-idx)
                   (do
                     (.getPositionAndIncrement rp)
                     (enqueue-trie-ptr! trie-ptr)
                     (->iid-parts*))

                   (cons (let [!trie-idxs (IntStream/builder)
                               !trie-row-idxs (IntStream/builder)]
                           (letfn [(add! [{:keys [trie-idx ^IVectorPosition rp] :as trie-ptr}]
                                     (.add !trie-idxs trie-idx)
                                     (.add !trie-row-idxs (.getPositionAndIncrement rp))

                                     (enqueue-trie-ptr! trie-ptr))]

                             (let [cmp-abp (get-ptr trie-ptr (ArrowBufPointer.))]
                               (add! trie-ptr)
                               (while (when-let [trie-ptr (.peek pq)]
                                        (when (= cmp-abp (get-ptr trie-ptr reuse-ptr))
                                          (add! (.remove pq))
                                          true)))))

                           {:trie-idxs (.toArray (.build !trie-idxs))
                            :trie-row-idxs (.toArray (.build !trie-row-idxs))})

                         (->iid-parts*))))))]

      (run! enqueue-trie-ptr! tries)

      (->iid-parts*))))

(comment
  (defn apply-t1-logs
    "assumes first page is the static trie, or nil if no static trie yet"
    [in-tries, ^bytes path, ^LiveHashTrie t1-trie, ^IRelationWriter t1-rel]

    (loop [[iid-part & more-parts] (->iid-partitions in-tries path)
           ^LiveHashTrie t1-trie t1-trie]
      (if-not iid-part
        t1-trie

        (let [{:keys [^ints trie-idxs ^ints trie-row-idxs]} iid-part
              {:keys [^IRowCopier t1-row-copier]} (nth in-tries (aget trie-idxs 0))
              pos (.copyRow t1-row-copier (aget trie-row-idxs 0))]
          (recur more-parts (.add t1-trie pos))))))

  (with-open [al (RootAllocator.)
              t1-root (VectorSchemaRoot/create t1-diff-schema al)
              log1 (trie/open-leaf-root al)
              log2 (trie/open-leaf-root al)]

    (letfn [(write-puts! [^IRelationWriter wtr, puts]
              (let [iid-wtr (.writerForName wtr "xt$iid")
                    sys-time-wtr (.writerForName wtr "xt$system_from")
                    put-wtr (-> (.writerForName wtr "op")
                                (.writerForField trie/put-field))
                    doc-wtr (.structKeyWriter put-wtr "xt$doc")
                    valid-from-wtr (.structKeyWriter put-wtr "xt$valid_from")]
                (doseq [{:keys [system-time doc]} puts]
                  (.writeObject iid-wtr (util/uuid->bytes (:xt/id doc)))
                  (vw/write-value! system-time sys-time-wtr)

                  (.startStruct put-wtr)
                  (vw/write-value! doc doc-wtr)
                  (vw/write-value! system-time valid-from-wtr)
                  (.endStruct put-wtr)

                  (.endRow wtr))))]

      (write-puts! log1
                   [{:system-time #inst "2020"
                     :doc {:xt/id #uuid "7b2c1b5f-f6e4-4219-a3c7-06d10007aff0", :version 0}}
                    {:system-time #inst "2021"
                     :doc {:xt/id #uuid "7c2c1b5f-f6e4-4219-a3c7-06d10007aff0", :version 0}}])

      (write-puts! log2
                   [{:system-time #inst "2022"
                     :doc {:xt/id #uuid "7b2c1b5f-f6e4-4219-a3c7-06d10007aff0", :version 1}}
                    {:system-time #inst "2023"
                     :doc {:xt/id #uuid "84a4568f-f2bf-4720-94f3-348d6037817e", :version 0}}]))

    (let [t1-wtr (vw/root->writer t1-root)
          t1-iid-writer (.writerForName t1-wtr "xt$iid")
          t1-vf-writer (.writerForName t1-wtr "xt$valid_from")
          t1-sf-writer (.writerForName t1-wtr "xt$system_from")
          t1-doc-writer (.writerForName t1-wtr "xt$doc")]

      (letfn [(leaf->page [trie-idx ^RelationReader leaf-rdr]
                (when leaf-rdr
                  (let [iid-rdr (.readerForName leaf-rdr "xt$iid")
                        sys-from-rdr (.readerForName leaf-rdr "xt$system_from")
                        put-rdr (-> (.readerForName leaf-rdr "op")
                                    (.legReader :put))
                        doc-rdr (.structKeyReader put-rdr "xt$doc")
                        valid-from-rdr (.structKeyReader put-rdr "xt$valid_from")
                        valid-to-rdr (.structKeyReader put-rdr "xt$valid_to")]
                    {:trie-idx trie-idx,
                     :iid-rdr (.readerForName leaf-rdr "xt$iid"),
                     :system-from-rdr sys-from-rdr
                     :valid-from-rdr valid-from-rdr
                     :valid-to-rdr valid-to-rdr
                     :t1-row-copier (let [copiers [(.rowCopier iid-rdr t1-iid-writer)
                                                   (.rowCopier sys-from-rdr t1-sf-writer)
                                                   (.rowCopier valid-from-rdr t1-vf-writer)
                                                   (.rowCopier doc-rdr t1-doc-writer)]]
                                      (reify IRowCopier
                                        (copyRow [_ idx]
                                          (let [pos (.getPosition (.writerPosition t1-wtr))]
                                            (.startRow t1-wtr)
                                            (doseq [^IRowCopier copier copiers]
                                              (.copyRow copier idx))
                                            (.endRow t1-wtr)
                                            pos))))
                     :rp (IVectorPosition/build)})))]

        (let [pages (into-array (map-indexed leaf->page [nil (vw/rel-wtr->rdr log1) (vw/rel-wtr->rdr log2)]))

              ^LiveHashTrie t1-trie (reduce (fn [t1-trie key-path]
                                              (apply-t1-logs pages key-path t1-trie t1-wtr))
                                            (LiveHashTrie/emptyTrie (TrieKeys. (.getVector t1-root "xt$iid")))
                                            [(byte-array [7]) (byte-array [8])])]

          (.syncRowCount t1-wtr)
          (println (.contentToTSVString t1-root))
          t1-trie)))))

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
                                             {:trie-idx trie-idx, :leaf (format "%x" (hash node))})))))}])))]

    (vec (trie-merge-tasks* (map #(some-> ^HashTrie % (.rootNode)) tries) []))))

(comment
  (with-open [al (RootAllocator.)
              t1-root (open-arrow-hash-trie-root al [[nil -1 nil -1] -1 nil -1])
              log-root (open-arrow-hash-trie-root al -1)
              log2-root (open-arrow-hash-trie-root al [nil nil -1 -1])]
    (trie-merge-tasks [(ArrowHashTrie/from t1-root) (ArrowHashTrie/from log-root) (ArrowHashTrie/from log2-root)])))

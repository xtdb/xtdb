(ns xtdb.indexer.rrrr
  (:require [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import [java.util.function IntConsumer]
           [java.util.stream IntStream]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector.types UnionMode]
           [org.apache.arrow.vector.types.pojo ArrowType$Union Schema]
           org.apache.arrow.vector.VectorSchemaRoot
           [xtdb.trie ArrowHashTrie HashTrie HashTrie$Node]))

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

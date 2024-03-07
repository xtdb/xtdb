(ns xtdb.vector.reader
  (:require [clojure.set :as set]
            [xtdb.types :as types])
  (:import clojure.lang.MapEntry
           com.carrotsearch.hppc.IntArrayList
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector ValueVector VectorSchemaRoot)
           xtdb.api.query.IKeyFn
           (xtdb.vector IVectorReader RelationReader ValueVectorReadersKt IMultiVectorRelationFactory
                        IVectorIndirection$Selection IndirectMultiVectorReader)))

(defn vec->reader ^IVectorReader [^ValueVector v]
  (ValueVectorReadersKt/from v))

(defn rel-reader
  (^xtdb.vector.RelationReader [cols] (RelationReader/from cols))
  (^xtdb.vector.RelationReader [cols ^long row-count] (RelationReader/from cols row-count)))

(defn <-root ^xtdb.vector.RelationReader [^VectorSchemaRoot root]
  (rel-reader (map vec->reader (.getFieldVectors root))
              (.getRowCount root)))

(defn ->absent-col [col-name allocator row-count]
  (vec->reader (doto (-> (types/col-type->field col-name :null)
                         (.createVector allocator))
                 (.setValueCount row-count))))

(defn- available-col-names [^RelationReader rel]
  (into #{} (map #(.getName ^IVectorReader %)) rel))

;; we don't allocate anything here, but we need it because BaseValueVector
;; (a distant supertype of NullVector) thinks it needs one.
(defn with-absent-cols ^xtdb.vector.RelationReader [^RelationReader rel, ^BufferAllocator allocator, col-names]
  (let [row-count (.rowCount rel)
        available-col-names (available-col-names rel)]
    (rel-reader (concat rel
                        (->> (set/difference col-names available-col-names)
                             (map #(->absent-col % allocator row-count))))
                (.rowCount rel))))

(defn rel->rows
  (^java.util.List [^RelationReader rel] (rel->rows rel #xt/key-fn :kebab-case-keyword))
  (^java.util.List [^RelationReader rel ^IKeyFn key-fn]
   (let [col-ks (for [^IVectorReader col rel]
                  [col (.denormalize key-fn (.getName col))])]
     (mapv (fn [idx]
             (->> col-ks
                  (into {} (keep (fn [[^IVectorReader col k]]
                                   (let [v (.getObject col idx key-fn)]
                                     (when (some? v)
                                       (MapEntry/create k v))))))))
           (range (.rowCount rel))))))

(defn rels->multi-vector-rel-factory ^xtdb.vector.IMultiVectorRelationFactory [rels, ^BufferAllocator allocator, col-names]
  (let [rels (map #(with-absent-cols % allocator (set col-names)) rels)
        reader-indirection (IntArrayList.)
        vector-indirection (IntArrayList.)]
    (reify IMultiVectorRelationFactory
      (accept [_ rdrIdx vecIdx]
        (.add reader-indirection rdrIdx)
        (.add vector-indirection vecIdx))
      (realize [_]
        (let [reader-selection (IVectorIndirection$Selection. (.toArray reader-indirection))
              vector-selection (IVectorIndirection$Selection. (.toArray vector-indirection))]
          (letfn [(->indirect-multi-vec [col-name]
                    (IndirectMultiVectorReader.
                     (map #(.readerForName ^RelationReader % col-name) rels)
                     reader-selection
                     vector-selection))]
            (RelationReader/from (map ->indirect-multi-vec col-names))))))))

(defn concat-rels [^RelationReader rel1 ^RelationReader rel2]
  (cond (empty? (seq rel1)) rel2
        (empty? (seq rel2)) rel1
        :else (do
                (assert (= (.rowCount rel1) (.rowCount rel2)))
                (assert (empty? (set/intersection (available-col-names rel1) (available-col-names rel2))))
                (RelationReader/from (into (seq rel1) (seq rel2))))))

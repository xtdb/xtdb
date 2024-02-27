(ns xtdb.vector.reader
  (:require [clojure.set :as set]
            [xtdb.types :as types])
  (:import clojure.lang.MapEntry
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector ValueVector VectorSchemaRoot)
           xtdb.api.query.IKeyFn
           (xtdb.vector IVectorReader RelationReader ValueVectorReadersKt)))

(defn vec->reader ^IVectorReader [^ValueVector v]
  (ValueVectorReadersKt/from v))

(defn rel-reader
  (^xtdb.vector.RelationReader [cols] (RelationReader/from cols))
  (^xtdb.vector.RelationReader [cols ^long row-count] (RelationReader/from cols row-count)))

(defn <-root ^xtdb.vector.RelationReader [^VectorSchemaRoot root]
  (rel-reader (map vec->reader (.getFieldVectors root))
              (.getRowCount root)))

(defn ->absent-col [col-name allocator row-count]
  (vec->reader (doto (-> (types/col-type->field col-name :absent)
                         (.createVector allocator))
                 (.setValueCount row-count))))

;; we don't allocate anything here, but we need it because BaseValueVector
;; (a distant supertype of AbsentVector) thinks it needs one.
(defn with-absent-cols ^xtdb.vector.RelationReader [^RelationReader rel, ^BufferAllocator allocator, col-names]
  (let [row-count (.rowCount rel)
        available-col-names (into #{} (map #(.getName ^IVectorReader %)) rel)]
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
                                   (when-not (.isAbsent col idx)
                                     (MapEntry/create k (.getObject col idx key-fn))))))))
           (range (.rowCount rel))))))

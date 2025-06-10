(ns xtdb.vector.reader
  (:require [clojure.set :as set]
            [xtdb.types :as types])
  (:import java.util.List
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector ValueVector VectorSchemaRoot)
           xtdb.arrow.VectorReader
           (xtdb.vector IVectorReader RelationReader ValueVectorReadersKt)))

(defn vec->reader ^IVectorReader [^ValueVector v]
  (ValueVectorReadersKt/from v))

(defn rel-reader
  (^xtdb.vector.RelationReader [^List cols] (RelationReader/from cols))
  (^xtdb.vector.RelationReader [cols ^long row-count] (RelationReader/from cols row-count)))

(defn <-root ^xtdb.vector.RelationReader [^VectorSchemaRoot root]
  (rel-reader (map vec->reader (.getFieldVectors root))
              (.getRowCount root)))

(defn- ->absent-col [col-name allocator row-count]
  (vec->reader (doto (-> (types/col-type->field col-name :null)
                         (.createVector allocator))
                 (.setValueCount row-count))))

(defn- available-col-names [^RelationReader rel]
  (into #{} (map #(.getName ^VectorReader %)) (.getVectors rel)))

;; we don't allocate anything here, but we need it because BaseValueVector
;; (a distant supertype of NullVector) thinks it needs one.
(defn with-absent-cols ^xtdb.vector.RelationReader [^RelationReader rel, ^BufferAllocator allocator, col-names]
  (let [row-count (.getRowCount rel)
        available-col-names (available-col-names rel)]
    (rel-reader (concat rel
                        (->> (set/difference col-names available-col-names)
                             (map #(->absent-col % allocator row-count))))
                (.getRowCount rel))))

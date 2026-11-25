(ns xtdb.vector.reader
  (:require [clojure.set :as set]
            [xtdb.types :as types])
  (:import (java.util LinkedHashMap List)
           (org.apache.arrow.memory BufferAllocator)
           (xtdb.arrow NullVector RelationReader VectorReader)))

(defn keyed-rel-reader
  "A relation of `cols`, a map of column name to column."
  ^xtdb.arrow.RelationReader [^java.util.Map cols row-count]
  (RelationReader/from cols (int row-count)))

(defn assoc-cols
  "`rel` with the `[col-name col]` pairs of `kvs` added after its own columns, in order.
  A pair whose name `rel` already has replaces that column in its place."
  ^xtdb.arrow.RelationReader [^RelationReader rel kvs row-count]
  (let [cols (LinkedHashMap. (.getCols rel))]
    (doseq [[col-name col] kvs]
      (.put cols (str col-name) col))
    (keyed-rel-reader cols row-count)))

(defn select-cols
  "`rel` restricted to the columns whose names are in the set `col-names`, in `rel`'s order."
  ^xtdb.arrow.RelationReader [^RelationReader rel col-names]
  (let [cols (LinkedHashMap.)]
    (doseq [[col-name col] (.getCols rel)
            :when (contains? col-names col-name)]
      (.put cols col-name col))
    (keyed-rel-reader cols (.getRowCount rel))))

(defn- ->absent-col [row-count]
  (NullVector. true row-count))

(defn- available-col-names [^xtdb.arrow.RelationReader rel]
  (into #{} (keys (.getCols rel))))

;; we don't allocate anything here, but we need it because BaseValueVector
;; (a distant supertype of NullVector) thinks it needs one.
(defn with-absent-cols ^xtdb.arrow.RelationReader [^RelationReader rel, ^BufferAllocator _allocator, col-names]
  (let [row-count (.getRowCount rel)
        available-col-names (available-col-names rel)]
    (assoc-cols rel
                (->> (set/difference col-names available-col-names)
                     (map (fn [col-name] [col-name (->absent-col row-count)])))
                row-count)))

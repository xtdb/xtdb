(ns core2.test-util
  (:require [core2.util :as sut]
            [clojure.test :as t])
  (:import [org.apache.arrow.vector FieldVector VectorSchemaRoot]))

(defn root->rows [^VectorSchemaRoot root]
  (let [field-vecs (.getFieldVectors root)]
    (mapv (fn [idx]
            (vec (for [^FieldVector field-vec field-vecs]
                   (.getObject field-vec idx))))
          (range (.getRowCount root)))))

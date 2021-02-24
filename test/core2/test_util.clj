(ns core2.test-util
  (:require [core2.util :as sut]
            [clojure.test :as t])
  (:import [org.apache.arrow.vector FieldVector ValueVector VectorSchemaRoot]
           java.util.ArrayList))

(defn root->rows [^VectorSchemaRoot root]
  (let [field-vecs (.getFieldVectors root)]
    (mapv (fn [idx]
            (vec (for [^FieldVector field-vec field-vecs]
                   (.getObject field-vec idx))))
          (range (.getRowCount root)))))

(defn ->list ^java.util.List [^ValueVector v]
  (let [acc (ArrayList.)]
    (dotimes [n (.getValueCount v)]
      (.add acc (.getObject v n)))
    acc))

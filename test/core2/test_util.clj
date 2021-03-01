(ns core2.test-util
  (:require core2.core
            [clojure.test :as t]
            [core2.util :as util])
  (:import core2.core.IngestLoop
           java.time.Duration
           java.util.ArrayList
           java.util.concurrent.CompletableFuture
           org.apache.arrow.memory.RootAllocator
           [org.apache.arrow.vector FieldVector ValueVector VectorSchemaRoot]))

(def ^:dynamic ^org.apache.arrow.memory.BufferAllocator *allocator*)

(defn with-allocator [f]
  (with-open [allocator (RootAllocator.)]
    (binding [*allocator* allocator]
      (f))))

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

(defn then-await-tx
  ([^CompletableFuture submit-tx-fut, ^IngestLoop il]
   (-> submit-tx-fut
       (then-await-tx il (Duration/ofSeconds 2))))

  ([^CompletableFuture submit-tx-fut, ^IngestLoop il, timeout]
   (-> submit-tx-fut
       (util/then-apply
        (fn [tx]
          (.awaitTx il tx timeout))))))

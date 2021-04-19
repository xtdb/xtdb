(ns core2.bloom
  (:require [core2.types :as types])
  (:import org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector BitVector FieldVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.FixedSizeListVector
           org.apache.arrow.vector.types.Types))

(defn bloom-contains? [^FixedSizeListVector bloom-vec
                       ^BitVector bit-vec
                       ^long idx
                       hashes]
  (let [bit-idx (.getElementStartIndex bloom-vec idx)]
    (every? (fn [^long hash]
              (pos? (.get bit-vec (+ bit-idx hash))))
            hashes)))

(defn literal-hashes [^BufferAllocator allocator literal]
  (let [arrow-type (types/->arrow-type (class literal))
        minor-type (Types/getMinorTypeForArrowType arrow-type)]
    (with-open [^FieldVector vec (.getNewVector minor-type (types/->field "_" arrow-type false) allocator nil)]
      (types/set-safe! vec 0 literal)
      (let [el-hash (.hashCode vec 0)]
        [(-> el-hash (bit-and 0x3ff))
         (-> el-hash (bit-shift-right 10) (bit-and 0x3ff))
         (-> el-hash (bit-shift-right 20) (bit-and 0x3ff))] ))))

(defn write-bloom [^FieldVector field-vec, ^VectorSchemaRoot metadata-root, idx]
  (let [^FixedSizeListVector bloom-vec (.getVector metadata-root "bloom")
        ^BitVector bit-vec (.getDataVector bloom-vec)
        bit-idx (.startNewValue bloom-vec idx)]
    (dotimes [in-idx (.getValueCount field-vec)]
      (let [el-hash (.hashCode field-vec in-idx)]
        (.setSafeToOne bit-vec (+ bit-idx (-> el-hash (bit-and 0x3ff))))
        (.setSafeToOne bit-vec (+ bit-idx (-> el-hash (bit-shift-right 10) (bit-and 0x3ff))))
        (.setSafeToOne bit-vec (+ bit-idx (-> el-hash (bit-shift-right 20) (bit-and 0x3ff))))))))

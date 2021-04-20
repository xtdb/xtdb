(ns core2.bloom
  (:require [core2.types :as types])
  (:import org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector BitVector FieldVector ValueVector]
           [org.apache.arrow.vector.complex FixedSizeListVector StructVector]
           org.apache.arrow.vector.types.Types))

(def bloom-bits (bit-shift-left 1 10))
(def bit-mask (dec bloom-bits))

(defn bloom-contains? [^FixedSizeListVector bloom-vec
                       ^long idx
                       hashes]
  (let [bit-idx (.getElementStartIndex bloom-vec idx)]
    (every? (fn [^long hash]
              (pos? (.get ^BitVector (.getDataVector bloom-vec) (+ bit-idx hash))))
            hashes)))

(defn literal-hashes [^BufferAllocator allocator literal]
  (let [arrow-type (types/->arrow-type (class literal))
        minor-type (Types/getMinorTypeForArrowType arrow-type)]
    (with-open [^FieldVector vec (.getNewVector minor-type (types/->field "_" arrow-type false) allocator nil)]
      (types/set-safe! vec 0 literal)
      (let [el-hash (.hashCode vec 0)]
        [(-> el-hash (bit-and bit-mask))
         (-> el-hash (bit-shift-right 10) (bit-and bit-mask))
         (-> el-hash (bit-shift-right 20) (bit-and bit-mask))]))))

(defn write-bloom [^FixedSizeListVector bloom-vec, meta-idx, ^ValueVector field-vec]
  (let [^BitVector bit-vec (.getDataVector bloom-vec)
        bit-idx (.startNewValue bloom-vec meta-idx)]
    (dotimes [in-idx (.getValueCount field-vec)]
      (let [el-hash (.hashCode field-vec in-idx)]
        (.setSafeToOne bit-vec (+ bit-idx (-> el-hash (bit-and bit-mask))))
        (.setSafeToOne bit-vec (+ bit-idx (-> el-hash (bit-shift-right 10) (bit-and bit-mask))))
        (.setSafeToOne bit-vec (+ bit-idx (-> el-hash (bit-shift-right 20) (bit-and bit-mask))))))))

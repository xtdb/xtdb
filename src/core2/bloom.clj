(ns core2.bloom
  (:require [core2.types :as types])
  (:import org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.memory.util.hash.MurmurHasher
           [org.apache.arrow.vector BitVector FieldVector ValueVector]
           [org.apache.arrow.vector.complex FixedSizeListVector StructVector]
           org.apache.arrow.vector.types.Types))

(set! *unchecked-math* :warn-on-boxed)

;; TODO: this is way too low, needs to be 16-17 Assumes n = 10000
;; (max-rows-per-chunk) false-positive is then: 0.008561966 (for 17)

(def ^:const bloom-bits (bit-shift-left 1 10))
(def ^:const bit-mask (dec bloom-bits))
(def ^:const bloom-k 3)

(defn bloom-false-positive-probability? ^double [^long n ^long k ^long m]
  (Math/pow (- 1 (Math/exp (/ (- k) (double (/ m n))))) k))


(defn bloom-contains? [^FixedSizeListVector bloom-vec
                       ^long idx
                       ^ints hashes]
  (let [^BitVector bit-vec (.getDataVector bloom-vec)
        bit-idx (.getElementStartIndex bloom-vec idx)]
    (loop [n 0]
      (if (= n (alength hashes))
        true
        (if (pos? (.get bit-vec (+ bit-idx (aget hashes n))))
          (recur (inc n))
          false)))))

(def ^:private initial-murmur-hasher (MurmurHasher. 0))

;; Cassandra-style hashes:
;; https://www.researchgate.net/publication/220770131_Less_Hashing_Same_Performance_Building_a_Better_Bloom_Filter
(defn bloom-hashes ^ints [^ValueVector vec ^long idx ^long k ^long mask]
  (let [hash-1 (.hashCode vec idx initial-murmur-hasher)
        hash-2 (.hashCode vec idx (MurmurHasher. hash-1))
        acc (int-array k)]
    (dotimes [n k]
      (aset acc n (unchecked-int (bit-and mask (+ hash-1 (* hash-2 n))))))
    acc))

(defn literal-hashes [^BufferAllocator allocator literal]
  (let [arrow-type (types/->arrow-type (class literal))
        minor-type (Types/getMinorTypeForArrowType arrow-type)]
    (with-open [^ValueVector vec (.getNewVector minor-type (types/->field "_" arrow-type false) allocator nil)]
      (types/set-safe! vec 0 literal)
      (bloom-hashes vec 0 bloom-k bit-mask))))

(defn write-bloom [^FixedSizeListVector bloom-vec, meta-idx, ^ValueVector field-vec]
  (let [^BitVector bit-vec (.getDataVector bloom-vec)
        bit-idx (.startNewValue bloom-vec meta-idx)]
    (dotimes [in-idx (.getValueCount field-vec)]
      (let [^ints el-hashes (bloom-hashes field-vec in-idx bloom-k bit-mask)]
        (dotimes [m (alength el-hashes)]
          (.setSafeToOne bit-vec (+ bit-idx (aget el-hashes m))))))))

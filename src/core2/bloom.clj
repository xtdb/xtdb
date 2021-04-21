(ns core2.bloom
  (:require [core2.types :as types])
  (:import org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.memory.util.hash.MurmurHasher
           [org.apache.arrow.vector BitVector FieldVector ValueVector VarBinaryVector]
           org.apache.arrow.vector.types.Types
           java.nio.ByteBuffer
           org.roaringbitmap.RoaringBitmap
           org.roaringbitmap.buffer.ImmutableRoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

;; TODO: this is way too low, needs to be 16-17 Assumes n = 10000
;; (max-rows-per-chunk) false-positive is then: 0.008561966 (for 17)

(def ^:const bloom-bits (bit-shift-left 1 10))
(def ^:const bit-mask (dec bloom-bits))
(def ^:const bloom-k 3)

(defn bloom-false-positive-probability? ^double [^long n ^long k ^long m]
  (Math/pow (- 1 (Math/exp (/ (- k) (double (/ m n))))) k))

(defn bloom->bitmap ^org.roaringbitmap.buffer.ImmutableRoaringBitmap [^VarBinaryVector bloom-vec ^long idx]
  (let [pointer (.getDataPointer bloom-vec idx)
        nio-buffer (.nioBuffer (.getBuf pointer) (.getOffset pointer) (.getLength pointer))]
    (ImmutableRoaringBitmap. nio-buffer)))

(defn bloom-contains? [^VarBinaryVector bloom-vec
                       ^long idx
                       ^ints hashes]
  (let [^ImmutableRoaringBitmap bloom (bloom->bitmap bloom-vec idx)]
    (loop [n 0]
      (if (= n (alength hashes))
        true
        (if (.contains bloom (aget hashes n))
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

(defn write-bloom [^VarBinaryVector bloom-vec, ^long meta-idx, ^ValueVector field-vec]
  (let [bloom (RoaringBitmap.)]
    (dotimes [in-idx (.getValueCount field-vec)]
      (let [^ints el-hashes (bloom-hashes field-vec in-idx bloom-k bit-mask)]
        (.add bloom el-hashes)))
    (let [ba (byte-array (.serializedSizeInBytes bloom))]
      (.serialize bloom (ByteBuffer/wrap ba))
      (.setSafe bloom-vec meta-idx ba))))

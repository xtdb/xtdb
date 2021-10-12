(ns core2.bloom
  (:require [core2.types :as types]
            [core2.relation :as rel])
  (:import java.nio.ByteBuffer
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.memory.util.hash.MurmurHasher
           [org.apache.arrow.vector ValueVector VarBinaryVector]
           org.roaringbitmap.buffer.ImmutableRoaringBitmap
           org.roaringbitmap.RoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

;; max-rows  bloom-bits  false-positive
;; 1000      13           0.0288
;; 10000     16           0.0495
;; 10000     17           0.0085
;; 100000    20           0.0154

(def ^:const ^:private default-bloom-bits (bit-shift-left 1 20))

(defn- init-bloom-bits ^long []
  (let [bloom-bits (Long/getLong "core2.bloom.bits" default-bloom-bits)]
    (assert (= 1 (Long/bitCount bloom-bits)))
    bloom-bits))

(def ^:const bloom-bits (init-bloom-bits))
(def ^:const bloom-bit-mask (dec bloom-bits))
(def ^:const bloom-k 3)

(defn bloom-false-positive-probability?
  (^double [^long n]
   (bloom-false-positive-probability? n bloom-k bloom-bits))
  (^double [^long n ^long k ^long m]
   (Math/pow (- 1 (Math/exp (/ (- k) (double (/ m n))))) k)))

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
(defn bloom-hashes
  (^ints [^ValueVector vec ^long idx]
   (bloom-hashes vec idx bloom-k bloom-bit-mask))
  (^ints [^ValueVector vec ^long idx ^long k ^long mask]
   (let [hash-1 (.hashCode vec idx initial-murmur-hasher)
         hash-2 (.hashCode vec idx (MurmurHasher. hash-1))
         acc (int-array k)]
     (dotimes [n k]
       (aset acc n (unchecked-int (bit-and mask (+ hash-1 (* hash-2 n))))))
     acc)))

(defn literal-hashes ^ints [^BufferAllocator allocator literal]
  (let [arrow-type (types/value->arrow-type literal)]
    (with-open [^ValueVector vec (.createVector (types/->field "_" arrow-type false) allocator)]
      (let [writer (rel/vec->writer vec)]
        (.startValue writer)
        (types/write-value! literal writer)
        (.endValue writer))
      (bloom-hashes vec 0))))

(defn write-bloom [^VarBinaryVector bloom-vec, ^long meta-idx, ^ValueVector field-vec]
  (let [bloom (RoaringBitmap.)]
    (dotimes [in-idx (.getValueCount field-vec)]
      (let [^ints el-hashes (bloom-hashes field-vec in-idx)]
        (.add bloom el-hashes)))
    (let [ba (byte-array (.serializedSizeInBytes bloom))]
      (.serialize bloom (ByteBuffer/wrap ba))
      (.setSafe bloom-vec meta-idx ba))))

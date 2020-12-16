(ns ^:no-doc crux.memory
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [crux.error :as err]
            [crux.io :as cio]
            [taoensso.nippy :as nippy])
  (:import [java.io Closeable DataInputStream DataOutputStream]
           java.nio.ByteBuffer
           java.util.Comparator
           java.util.function.Supplier
           [org.agrona BufferUtil DirectBuffer ExpandableDirectByteBuffer MutableDirectBuffer]
           org.agrona.concurrent.UnsafeBuffer
           [org.agrona.io DirectBufferInputStream ExpandableDirectBufferOutputStream]
           crux.ByteUtils))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol Memory
  (->on-heap ^bytes [this])

  (->off-heap
    ^org.agrona.MutableDirectBuffer [this]
    ^org.agrona.MutableDirectBuffer [this ^MutableDirectBuffer to])

  (off-heap? [this])

  (as-buffer ^org.agrona.MutableDirectBuffer [this])

  (^long capacity [this]))

(definterface Allocator
  (^org.agrona.MutableDirectBuffer malloc [^long size])

  (^void free [^org.agrona.DirectBuffer buffer])

  (^long allocatedSize []))

(def ^:private ^:const page-size (ByteUtils/pageSize))
(def ^:private ^:const default-alignment 16)

(deftype DirectRootAllocator []
  Allocator
  (malloc [this size]
    (UnsafeBuffer. (ByteBuffer/allocateDirect size)))

  (free [this buffer]
    (BufferUtil/free ^DirectBuffer buffer))

  (allocatedSize [this]
    (cio/buffer-pool-memory-used "direct"))

  Closeable
  (close [_]))

(defn ->direct-root-allocator ^crux.memory.DirectRootAllocator []
  (->DirectRootAllocator))

(deftype BumpAllocator [^Allocator allocator ^long chunk-size ^long large-buffer-size
                        ^:unsynchronized-mutable ^DirectBuffer chunk ^:unsynchronized-mutable ^long position]
  Allocator
  (malloc [this size]
    (if-not chunk
      (do (set! chunk (.malloc allocator chunk-size))
          (set! position 0)
          (recur size))
      (let [size (long size)
            offset position
            alignment-mask (dec default-alignment)
            new-aligned-offset (bit-and-not (+ offset size alignment-mask)
                                            alignment-mask)]
        (cond
          (> size large-buffer-size)
          (.malloc allocator size)

          (> new-aligned-offset chunk-size)
          (do (set! chunk nil)
              (recur size))

          :else
          (do (set! position new-aligned-offset)
              (UnsafeBuffer. chunk offset size))))))

  (free [this buffer]
    (if (= (+ (.addressOffset ^DirectBuffer buffer)
              (.capacity ^DirectBuffer buffer))
           (+ (.addressOffset chunk) position))
      (set! position (- position (.capacity ^DirectBuffer buffer)))
      (log/warn "can only free/undo latest allocation with bump allocator")))

  (allocatedSize [this]
    (- (.allocatedSize allocator)
       (if chunk
         (- chunk-size position)
         0)))

  Closeable
  (close [this]
    (set! chunk nil)
    (cio/try-close allocator)))

(defn ->bump-allocator ^crux.memory.Allocator [allocator ^long chunk-size]
  (->BumpAllocator allocator chunk-size (quot chunk-size 4) nil 0))

(def ^:private ^:const default-chunk-size (* 32 page-size))

(def ^crux.memory.Allocator root-allocator (->direct-root-allocator))
(def ^ThreadLocal allocator-tl (ThreadLocal/withInitial
                                (reify Supplier
                                  (get [_]
                                    (->bump-allocator root-allocator default-chunk-size)))))

(defn allocate-buffer ^org.agrona.MutableDirectBuffer [^long size]
  (.malloc ^Allocator (.get allocator-tl) size))

(defn allocate-root-buffer ^org.agrona.MutableDirectBuffer [^long size]
  (.malloc root-allocator size))

(defonce empty-buffer (allocate-root-buffer 0))

(defn copy-buffer
  (^org.agrona.MutableDirectBuffer [^DirectBuffer from]
   (copy-buffer from (.capacity from)))
  (^org.agrona.MutableDirectBuffer [^DirectBuffer from ^long limit]
   (copy-buffer from limit (allocate-buffer limit)))
  (^org.agrona.MutableDirectBuffer [^DirectBuffer from ^long limit ^MutableDirectBuffer to]
   (doto to
     (.putBytes 0 from 0 limit))))

(defn copy-buffer-to-allocator ^org.agrona.MutableDirectBuffer [^DirectBuffer from ^Allocator allocator]
  (copy-buffer from (.capacity from) (.malloc allocator (.capacity from))))

(defn copy-buffer-to-root-allocator ^org.agrona.MutableDirectBuffer [^DirectBuffer from]
  (copy-buffer-to-allocator from root-allocator))

(defn slice-buffer ^org.agrona.MutableDirectBuffer [^DirectBuffer buffer ^long offset ^long limit]
  (UnsafeBuffer. buffer offset limit))

(defn limit-buffer ^org.agrona.MutableDirectBuffer [^DirectBuffer buffer ^long limit]
  (slice-buffer buffer 0 limit))

(extend-protocol Memory
  (class (byte-array 0))
  (->on-heap [this]
    this)

  (->off-heap
    ([this]
     (let [b (allocate-buffer (alength ^bytes this))]
       (->off-heap this b)))

    ([this ^MutableDirectBuffer to]
     (doto to
       (.putBytes 0 ^bytes this))))

  (off-heap? [this]
    false)

  (as-buffer [this]
    (UnsafeBuffer. ^bytes this))

  (capacity [this]
    (alength ^bytes this))

  DirectBuffer
  (->on-heap [this]
    (if (and (.byteArray this)
             (= (.capacity this)
                (alength (.byteArray this))))
      (.byteArray this)
      (let [bytes (byte-array (.capacity this))]
        (.getBytes this 0 bytes)
        bytes)))

  (->off-heap
    ([this]
     (if (off-heap? this)
       this
       (->off-heap this (allocate-buffer (.capacity this)))))

    ([this ^MutableDirectBuffer to]
     (doto to
       (.putBytes 0 this 0 (.capacity this)))))

  (off-heap? [this]
    (or (some-> (.byteBuffer this) (.isDirect))
        (and (nil? (.byteArray this))
             (nil? (.byteBuffer this)))))

  (as-buffer [this]
    this)

  (capacity [this]
    (.capacity this))

  ByteBuffer
  (->on-heap [this]
    (if (and (.hasArray this)
             (= (.remaining this)
                (alength (.array this))))
      (.array this)
      (doto (byte-array (.remaining this))
        (->> (.get (.duplicate this))))))

  (->off-heap
    ([this]
     (if (.isDirect this)
       (as-buffer this)
       (->off-heap this (allocate-buffer (.remaining this)))))

    ([this ^MutableDirectBuffer to]
     (doto to
       (.putBytes 0 this (.position this) (.remaining this)))))

  (off-heap? [this]
    (.isDirect this))

  (as-buffer [this]
    (UnsafeBuffer. this (.position this) (.remaining this)))

  (capacity [this]
    (.remaining this)))

(defn ensure-off-heap ^org.agrona.DirectBuffer [b ^MutableDirectBuffer tmp]
  (if (off-heap? b)
    b
    (UnsafeBuffer. (->off-heap b tmp) 0 (capacity b))))

(defn direct-byte-buffer ^java.nio.ByteBuffer [b]
  (let [b (->off-heap b)
        offset (.wrapAdjustment b)]
    (-> (.byteBuffer b)
        (.duplicate)
        (.clear)
        (.position offset)
        ^ByteBuffer (.limit (+ offset (.capacity b)))
        (.slice))))

(defn buffer->hex ^String [^DirectBuffer b]
  (some-> b (ByteUtils/bufferToHex)))

(defn hex->buffer
  (^org.agrona.DirectBuffer [^String b]
   (hex->buffer b (ExpandableDirectByteBuffer.)))
  (^org.agrona.DirectBuffer [^String b ^MutableDirectBuffer to]
   (some-> b (ByteUtils/hexToBuffer to))))

(defn compare-buffers
  {:inline (fn [a b & [max-length]]
             (if max-length
               `(ByteUtils/compareBuffers ~a ~b ~max-length)
               `(ByteUtils/compareBuffers ~a ~b)))
   :inline-arities #{2 3}}
  (^long [^DirectBuffer a ^DirectBuffer b]
   (ByteUtils/compareBuffers a b))
  (^long [^DirectBuffer a ^DirectBuffer b ^long max-length]
   (ByteUtils/compareBuffers a b max-length)))

(def ^java.util.Comparator buffer-comparator
  ByteUtils/UNSIGNED_BUFFER_COMPARATOR)

(defn buffers=?
  {:inline (fn [a b & [max-length]]
             (if max-length
               `(ByteUtils/equalBuffers ~a ~b ~max-length)
               `(ByteUtils/equalBuffers ~a ~b)))
   :inline-arities #{2 3}}
  ([^DirectBuffer a ^DirectBuffer b]
   (ByteUtils/equalBuffers a b))
  ([^DirectBuffer a ^DirectBuffer b ^long max-length]
   (ByteUtils/equalBuffers a b max-length)))

(defn inc-unsigned-buffer!
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer buffer]
   (inc-unsigned-buffer! buffer (.capacity buffer)))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer buffer ^long prefix-length]
   (loop [idx (dec (int prefix-length))]
     (when-not (neg? idx)
       (let [b (Byte/toUnsignedInt (.getByte buffer idx))]
         (if (= 0xff b)
           (do (.putByte buffer idx (byte 0))
               (recur (dec idx)))
           (doto buffer
             (.putByte idx (unchecked-byte (inc b))))))))))

(defn <-nippy-buffer [buf]
  (nippy/thaw-from-in! (-> (DirectBufferInputStream. buf)
                           (DataInputStream.))))

(defn ->nippy-buffer [v]
  (let [to (ExpandableDirectByteBuffer. 64)
        dos (-> (ExpandableDirectBufferOutputStream. to)
                (DataOutputStream.))]
    (nippy/-freeze-without-meta! v dos)
    (-> to
        (limit-buffer (.size dos)))))

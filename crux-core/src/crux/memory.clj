(ns ^:no-doc crux.memory
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [crux.error :as err]
            [crux.io :as cio]
            [taoensso.nippy :as nippy])
  (:import [java.io Closeable DataInputStream DataOutputStream]
           java.lang.reflect.Constructor
           [java.lang.ref PhantomReference Reference ReferenceQueue SoftReference WeakReference]
           [java.nio ByteBuffer DirectByteBuffer]
           [java.util Comparator HashMap Map Set]
           java.util.function.Supplier
           [java.util.concurrent ConcurrentHashMap ConcurrentSkipListSet]
           java.util.concurrent.atomic.AtomicLong
           [org.agrona BufferUtil DirectBuffer ExpandableDirectByteBuffer MutableDirectBuffer]
           org.agrona.concurrent.UnsafeBuffer
           [org.agrona.io DirectBufferInputStream ExpandableDirectBufferOutputStream]
           crux.ByteUtils))

(defprotocol Memory
  (->on-heap ^bytes [this])

  (->off-heap
    ^org.agrona.MutableDirectBuffer [this]
    ^org.agrona.MutableDirectBuffer [this ^MutableDirectBuffer to])

  (off-heap? [this])

  (as-buffer ^org.agrona.MutableDirectBuffer [this])

  (^long capacity [this]))

(defprotocol Allocator
  (malloc ^org.agrona.MutableDirectBuffer [this size])

  (free [this ^DirectBuffer buffer])

  (^long allocated-size [this]))

(def ^:private ^:const page-size (ByteUtils/pageSize))
(def ^:private ^:const default-alignment 16)

(deftype DirectAllocator [^AtomicLong allocated-bytes ^Map ref->address ^Map address->cleaner ^ReferenceQueue reference-queue]
  Allocator
  (malloc [this size]
    (let [buffer (ByteBuffer/allocateDirect size)
          address (BufferUtil/address buffer)]
      (.addAndGet allocated-bytes size)
      (.put address->cleaner address #(.addAndGet allocated-bytes (- size)))
      (.put ref->address (PhantomReference. buffer reference-queue) address)
      (UnsafeBuffer. buffer)))

  (free [this buffer]
    (let [address (.addressOffset ^DirectBuffer buffer)]
      (if-let [cleaner (.remove address->cleaner address)]
        (do (BufferUtil/free ^DirectBuffer buffer)
            (cleaner))
        (log/warn "trying to free unknown buffer: " buffer))))

  (allocated-size [this]
    (loop [ref (.poll reference-queue)]
      (when ref
        (when-let [address (.remove ref->address ref)]
          (when-let [cleaner (.remove address->cleaner address)]
            (cleaner)))
        (recur (.poll reference-queue))))
    (.get allocated-bytes))

  Closeable
  (close [this]
    (allocated-size this)
    (.clear ref->address)
    (.clear address->cleaner)))

(defn ->direct-allocator ^crux.memory.DirectAllocator []
  (->DirectAllocator (AtomicLong.) (ConcurrentHashMap.) (ConcurrentHashMap.) (ReferenceQueue.)))

(def ^:private ^Constructor direct-byte-buffer-constructor
  (doto (.getDeclaredConstructor DirectByteBuffer (into-array [Long/TYPE Integer/TYPE]))
    (.setAccessible true)))

(defn- ->byte-buffer ^java.nio.DirectByteBuffer [^long address ^long size]
  (.newInstance direct-byte-buffer-constructor (object-array [address (int size)])))

(deftype UnsafeAllocator [^Map address->size]
  Allocator
  (malloc [this size]
    (let [address (ByteUtils/malloc size)
          buffer (UnsafeBuffer. address size)]
      (.put address->size address size)
      (cio/register-cleaner buffer #(when (.remove address->size address)
                                      (ByteUtils/free address)))
      buffer))

  (free [this buffer]
    (let [address (.addressOffset ^DirectBuffer buffer)]
      (if (.remove address->size address)
        (do (ByteUtils/free address)
            nil)
        (log/warn "trying to free unknown buffer: " buffer))))

  (allocated-size [this]
    (reduce + (vals address->size)))

  Closeable
  (close [this]))

(defn ->unsafe-allocator ^crux.memory.UnsafeAllocator []
  (->UnsafeAllocator (ConcurrentHashMap.)))

(deftype RegionAllocator [parent-allocator ^Map address->reference]
  Allocator
  (malloc [this size]
    (let [buffer (malloc parent-allocator size)
          address (.addressOffset buffer)]
      (.put address->reference address (SoftReference. (.byteBuffer buffer)))
      buffer))

  (free [this buffer]
    (if (.remove address->reference (.addressOffset ^DirectBuffer buffer))
      (free parent-allocator buffer)
      (log/warn "trying to free unknown buffer: " buffer)))

  (allocated-size [this]
    (allocated-size parent-allocator))

  Closeable
  (close [this]
    (doseq [ref (vals address->reference)
            :let [b (.get ^Reference ref)]
            :when b]
      (free this (UnsafeBuffer. ^ByteBuffer b)))
    (let [used (allocated-size this)]
      (when-not (zero? used)
        (log/warn "memory still used after close:" used)))
    (.clear address->reference)))

(defn ->region-allocator
  (^crux.memory.RegionAllocator []
   (->region-allocator (->direct-allocator)))
  (^crux.memory.RegionAllocator [parent-allocator]
   (->RegionAllocator parent-allocator (ConcurrentHashMap.))))

(deftype BumpAllocator [owned-allocator ^long chunk-size ^long large-buffer-size ^:unsynchronized-mutable ^ByteBuffer chunk]
  Allocator
  (malloc [this size]
    (if-not chunk
      (do (set! chunk (let [buffer (malloc owned-allocator chunk-size)]
                        (or (.byteBuffer buffer)
                            (->byte-buffer (.addressOffset buffer) (.capacity buffer)))))
          (recur size))
      (let [size (long size)
            offset (.position chunk)
            alignment-mask (dec default-alignment)
            new-aligned-offset (bit-and-not (+ offset size alignment-mask)
                                            alignment-mask)]
        (cond
          (> size large-buffer-size)
          (malloc owned-allocator size)

          (> new-aligned-offset (.capacity chunk))
          (do (set! chunk nil)
              (recur size))

          :else
          (let [buffer (.slice (.limit (.duplicate chunk) (+ offset size)))]
            (.position chunk new-aligned-offset)
            (UnsafeBuffer. buffer 0 size))))))

  (free [this buffer]
    (if (= (+ (.addressOffset ^DirectBuffer buffer)
              (.capacity ^DirectBuffer buffer))
           (+ (BufferUtil/address chunk)
              (.position chunk)))
      (.position chunk (- (.position chunk) (.capacity ^DirectBuffer buffer)))
      (log/warn "can only free/undo latest allocation with bump allocator")))

  (allocated-size [this]
    (- (allocated-size owned-allocator)
       (if chunk
         (.remaining chunk)
         0)))

  Closeable
  (close [this]
    (set! chunk nil)
    (cio/try-close owned-allocator)))

(def ^:private ^:const default-chunk-size (* 4 1024))

(defn ->bump-allocator
  (^crux.memory.BumpAllocator []
   (->bump-allocator (->region-allocator)))
  (^crux.memory.BumpAllocator [owned-allocator]
   (->bump-allocator owned-allocator default-chunk-size))
  (^crux.memory.BumpAllocator [owned-allocator chunk-size]
   (->BumpAllocator owned-allocator chunk-size (quot default-chunk-size 4) nil)))

(deftype QuotaAllocator [owned-allocator ^long quota]
  Allocator
  (malloc [_ size]
    (when (> (+ size (allocated-size owned-allocator)) quota)
      (throw (err/illegal-arg :qouta-exceeded
                              {::err/message (str "Exceeded allocator quota")
                               :quota quota
                               :allocated-size (allocated-size owned-allocator)
                               :requested-size size})))
    (malloc owned-allocator size))

  (free [_ buffer]
    (free owned-allocator buffer))

  (allocated-size [_]
    (allocated-size owned-allocator))

  Closeable
  (close [_]
    (cio/try-close owned-allocator)))

(defn ->quota-allocator ^crux.memory.QuotaAllocator [owned-allocator ^long quota]
  (->QuotaAllocator owned-allocator quota))

(def ^crux.memory.Allocator default-allocator (->direct-allocator))
(def ^:dynamic ^crux.memory.Allocator *allocator* default-allocator)

(defn allocate-buffer ^org.agrona.MutableDirectBuffer [^long size]
  (malloc *allocator* size))

(defonce empty-buffer (allocate-buffer 0))

(defn copy-buffer
  (^org.agrona.MutableDirectBuffer [^DirectBuffer from]
   (copy-buffer from (.capacity from)))
  (^org.agrona.MutableDirectBuffer [^DirectBuffer from ^long limit]
   (copy-buffer from limit (allocate-buffer limit)))
  (^org.agrona.MutableDirectBuffer [^DirectBuffer from ^long limit ^MutableDirectBuffer to]
   (doto to
     (.putBytes 0 from 0 limit))))

(defn copy-buffer-to-allocator ^org.agrona.MutableDirectBuffer [^DirectBuffer from allocator]
  (copy-buffer from (.capacity from) (malloc allocator (.capacity from))))

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
        offset (- (.addressOffset b)
                  (BufferUtil/address (.byteBuffer b)))]
    (-> (.byteBuffer b)
        (.duplicate)
        (.clear)
        (.position offset)
        (.limit (+ offset (.capacity b)))
        (.slice))))

(defn on-heap-buffer ^org.agrona.DirectBuffer [^bytes b]
  (UnsafeBuffer. b))

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

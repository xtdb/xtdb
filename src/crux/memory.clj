(ns crux.memory
  (:import [java.nio ByteOrder ByteBuffer]
           [org.agrona DirectBuffer MutableDirectBuffer]
           org.agrona.concurrent.UnsafeBuffer))

(defprotocol MemoryRegion
  (->on-heap ^bytes [this])

  (->off-heap ^DirectBuffer [this])

  (off-heap? [this]))

(extend-protocol MemoryRegion
  (class (byte-array 0))
  (->on-heap [this]
    this)

  (->off-heap [this]
    (let [b (UnsafeBuffer. (ByteBuffer/allocateDirect (alength ^bytes this)))]
      (.putBytes b 0 this)
      b))

  (off-heap? [this]
    false)

  DirectBuffer
  (->on-heap [this]
    (if (and (.byteArray this)
             (= (.capacity this)
                (alength (.byteArray this))))
      (.byteArray this)
      (let [bytes (byte-array (.capacity this))]
        (.getBytes this 0 bytes)
        bytes)))

  (->off-heap [this]
    (if (off-heap? this)
      this
      (->off-heap (->on-heap this))))

  (off-heap? [this]
    (or (some-> (.byteBuffer this) (.isDirect))
        (and (nil? (.byteArray this))
             (nil? (.byteBuffer this)))))

  ByteBuffer
  (->on-heap [this]
    (if (and (.hasArray this)
             (= (.remaining this)
                (alength (.array this))))
      (.array this)
      (doto (byte-array (.remaining this))
        (->> (.get this)))))

  (->off-heap [this]
    (if (.isDirect this)
      (UnsafeBuffer. this (.position this) (.remaining this))
      (->off-heap (->on-heap this))))

  (off-heap? [this]
    (.isDirect this)))

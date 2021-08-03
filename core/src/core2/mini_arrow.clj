(ns core2.mini-arrow
  "Minimal implementation of Arrow based on the C data interface
  specification using plain java.nio.ByteBuffer for memory
  management."
  (:import [java.nio Buffer BufferOverflowException ByteBuffer ByteOrder CharBuffer]
           [java.nio.charset StandardCharsets]
           [java.util Arrays ArrayList List RandomAccess]
           [java.time Duration Instant LocalDate LocalTime Period ZoneId ZonedDateTime]
           [java.time.temporal ChronoField]
           [java.lang AutoCloseable]
           [java.math MathContext]))

(set! *unchecked-math* :warn-on-boxed)

;; https://arrow.apache.org/docs/format/Columnar.html
;; https://arrow.apache.org/docs/format/CDataInterface.html

(definterface IPrimitiveList
  (^boolean add [^boolean e])
  (^boolean add [^byte e])
  (^boolean add [^short e])
  (^boolean add [^int e])
  (^boolean add [^long e])
  (^boolean add [^float e])
  (^boolean add [^double e])

  (^boolean getBoolean [^int index])
  (^byte getByte [^int index])
  (^short getShort [^int index])
  (^int getInt [^int index])
  (^long getLong [^int index])
  (^float getFloat [^int index])
  (^double getDouble [^int index]))

(declare realloc realloc-array)

(defn get-bit ^long [^ByteBuffer bb ^long idx]
  (let [byte-idx (bit-shift-right (int idx) (int 3))
        bit-idx (bit-and (byte idx) (byte 7))]
    (bit-and 1 (bit-shift-right (.get bb byte-idx) bit-idx))))

(defn set-bit ^java.nio.ByteBuffer [^ByteBuffer bb ^long idx]
  (let [byte-idx (bit-shift-right (int idx) (int 3))
        bit-idx (bit-and (byte idx) (byte 7))]
    (try
      (.put bb byte-idx (byte (bit-or (.get bb byte-idx) (bit-shift-left (byte 1) bit-idx))))
      (catch BufferOverflowException _
        (set-bit (realloc bb) idx)))))

(defn- ^String base-format [^String format]
  (aget (.split ^String format ":") 0))

(deftype ArrowSchema [^String format ^String name ^String metadata ^long flags children dictionary])

(defn ->schema
  ([^String format]
   (->schema format ""))
  ([^String format ^String name]
   (->schema format name []))
  ([^String format ^String name children]
   (->ArrowSchema format name nil 0 children nil)))

(defn compare-buffers-unsigned ^long [^ByteBuffer x ^ByteBuffer y]
  (let [rem-x (.remaining x)
        rem-y (.remaining y)
        limit (min rem-x rem-y)
        char-limit (bit-shift-right limit 1)
        diff (.compareTo (.limit (.asCharBuffer x) char-limit)
                         (.limit (.asCharBuffer y) char-limit))]
    (if (zero? diff)
      (loop [n (bit-and-not limit 1)]
        (if (= n limit)
          (- rem-x rem-y)
          (let [x-byte (.get x n)
                y-byte (.get y n)]
            (if (= x-byte y-byte)
              (recur (inc n))
              (Byte/compareUnsigned x-byte y-byte)))))
      diff)))

(defn- ts-format->zone-id ^java.time.ZoneId [^String format]
  (let [maybe-tz (.split format ":")]
    (when (= 2 (alength maybe-tz))
      (ZoneId/of (aget maybe-tz 1)))))

(defn- ->instant ^java.time.Instant [ts]
  (if (instance? Instant ts)
    ts
    (.toInstant ^ZonedDateTime ts)))

(definterface IArrowArray
  (^boolean isNull [^int index])
  (^core2.mini_arrow.IArrowArray finish []))

;; TODO: wants to extend java.util.AbstractList
(deftype ArrowArray [^:unsynchronized-mutable ^long length
                     ^:unsynchronized-mutable ^long null-count
                     ^long offset
                     ^objects buffers
                     ^objects children
                     dictionary
                     ^String format
                     ^String base-format
                     ^ArrowSchema schema]
  IArrowArray
  (^boolean isNull [this ^int index]
   (let [validity-buffer (aget buffers 0)]
     (and (< index length)
          (some? validity-buffer)
          (zero? (get-bit validity-buffer index)))))

  (^core2.mini_arrow.IArrowArray finish [this]
   (dotimes [n (alength buffers)]
     (let [^ByteBuffer buffer (aget buffers n)]
       (aset buffers n (.asReadOnlyBuffer (.flip buffer)))))
   (dotimes [n (alength children)]
     (let [^ArrowArray child (aget children n)]
       (aset children n (.finish child))))
   this)

  RandomAccess
  List
  (^boolean add [this ^Object e]
   (let [add-null-primitive (fn add-null-primitive [^long width]
                              (let [^ByteBuffer data-buffer (aget buffers 1)
                                    new-length (+ (.position data-buffer) width)
                                    ^ByteBuffer data-buffer (if (< (.capacity data-buffer) new-length)
                                                              (aset buffers 0 (realloc data-buffer))
                                                              data-buffer)]
                                (.position data-buffer new-length)
                                (set! (.length this) (inc length))
                                (set! (.null-count this) (inc null-count))
                                true))]
     (case base-format
       "n" (do (assert (nil? e))
               (set! length (inc length))
               (set! null-count (inc null-count))
               true)
       "b" (if (nil? e)
             (do (set! length (inc length))
                 (set! null-count (inc null-count))
                 true)
             (.add this ^boolean e))
       ("c" "C") (if (nil? e)
                   (add-null-primitive Byte/BYTES)
                   (.add this ^byte e))
       ("s" "S") (if (nil? e)
                   (add-null-primitive Short/BYTES)
                   (.add this ^short e))
       ("i" "I") (if (nil? e)
                   (add-null-primitive Integer/BYTES)
                   (.add this ^int e true))
       "tdD" (if (nil? e)
               (add-null-primitive Integer/BYTES)
               (.add this (.get ^LocalDate e ChronoField/EPOCH_DAY)))
       "tts" (if (nil? e)
               (add-null-primitive Integer/BYTES)
               (.add this (.get ^LocalTime e ChronoField/SECOND_OF_DAY)))
       "ttm" (if (nil? e)
               (add-null-primitive Integer/BYTES)
               (.add this (.get ^LocalTime e ChronoField/MILLI_OF_DAY)))
       "tiM" (if (nil? e)
               (add-null-primitive Integer/BYTES)
               (.add this (.getMonths ^Period e)))
       ("l" "L") (if (nil? e)
                   (add-null-primitive Long/BYTES)
                   (.add this ^long e))
       "tdm" (if (nil? e)
               (add-null-primitive Long/BYTES)
               (.add this (* (.getLong ^LocalDate e ChronoField/EPOCH_DAY) 86400000)))
       "ttu" (if (nil? e)
               (add-null-primitive Long/BYTES)
               (.add this (quot (.toNanoOfDay ^LocalTime e) 1000)))
       "ttn" (if (nil? e)
               (add-null-primitive Long/BYTES)
               (.add this (.toNanoOfDay ^LocalTime e)))
       "tss" (if (nil? e)
               (add-null-primitive Long/BYTES)
               (.add this (quot (.toEpochMilli (->instant e)) 1000)))
       "tsm" (if (nil? e)
               (add-null-primitive Long/BYTES)
               (.add this (.toEpochMilli (->instant e))))
       "tsu" (if (nil? e)
               (add-null-primitive Long/BYTES)
               (let [instant (->instant e)]
                 (.add this (+ (* (.getEpochSecond instant) 1000000)
                               (quot (.getNano instant) 1000)))))
       "tsn" (if (nil? e)
               (add-null-primitive Long/BYTES)
               (let [instant (->instant e)]
                 (.add this (+ (* (.getEpochSecond instant) 1000000000)
                               (.getNano instant)))))
       "tDs" (if (nil? e)
               (add-null-primitive Long/BYTES)
               (.add this (.toSeconds ^Duration e)))
       "tDm" (if (nil? e)
               (add-null-primitive Long/BYTES)
               (.add this (.toMillis ^Duration e)))
       "tDu" (if (nil? e)
               (add-null-primitive Long/BYTES)
               (.add this (quot (.toNanos ^Duration e) 1000)))
       "tDn" (if (nil? e)
               (add-null-primitive Long/BYTES)
               (.add this (.toNanos ^Duration e)))
       "tiD" (if (nil? e)
               (add-null-primitive Long/BYTES)
               (.add this ^long (bit-or (bit-shift-left (.toDaysPart ^Duration e) Integer/SIZE) (.toMillisPart ^Duration e))))
       "f" (if (nil? e)
             (add-null-primitive Float/BYTES)
             (.add this ^float e))
       "g" (if (nil? e)
             (add-null-primitive Double/BYTES)
             (.add this ^double e))
       "z" (let [^ByteBuffer offset-buffer (aget buffers 1)
                 ^ByteBuffer offset-buffer (if (< (+ (* 2 Integer/BYTES) (.position offset-buffer))
                                                  (.capacity offset-buffer))
                                             (aset buffers 1 (realloc offset-buffer))
                                             offset-buffer)
                 ^ByteBuffer data-buffer (aget buffers 2)
                 position (.position data-buffer)]
             (try
               (if e
                 (do (.put data-buffer (.duplicate ^ByteBuffer e))
                     (aset buffers 0 (set-bit (aget buffers 0) length)))
                 (set! null-count (inc null-count)))
               (set! length (inc length))
               (when (zero? (.position offset-buffer))
                 (.putInt offset-buffer 0))
               (.putInt offset-buffer (.position data-buffer))
               true
               (catch BufferOverflowException _
                 (aset buffers 2 (realloc (.position data-buffer position)))
                 (.add ^ArrowArray this e))))
       "u" (let [^ByteBuffer offset-buffer (aget buffers 1)
                 ^ByteBuffer offset-buffer (if (< (+ (* 2 Integer/BYTES) (.position offset-buffer))
                                                  (.capacity offset-buffer))
                                             (aset buffers 1 (realloc offset-buffer))
                                             offset-buffer)
                 ^ByteBuffer data-buffer (aget buffers 2)
                 position (.position data-buffer)
                 encoder (.newEncoder StandardCharsets/UTF_8)]
             (try
               (if e
                 (let [coder-result (.encode encoder (CharBuffer/wrap ^CharSequence e) data-buffer true)]
                   (when-not (.isUnderflow coder-result)
                     (.throwException coder-result))
                   (aset buffers 0 (set-bit (aget buffers 0) length)))
                 (set! null-count (inc null-count)))
               (when (zero? (.position offset-buffer))
                 (.putInt offset-buffer 0))
               (.putInt offset-buffer (.position data-buffer))
               (set! length (inc length))
               true
               (catch BufferOverflowException _
                 (aset buffers 2 (realloc (.position data-buffer position)))
                 (.add ^ArrowArray this e))))
       "d" (let [^ByteBuffer data-buffer (aget buffers 1)
                 precision+scale+bit-width (.split ^String (aget (.split format ":") 1) ",")
                 precision (Integer/parseInt (aget precision+scale+bit-width 0))
                 scale (Integer/parseInt (aget precision+scale+bit-width 1))
                 width (quot (if (= 3 (alength precision+scale+bit-width))
                               (Integer/parseInt (aget precision+scale+bit-width 2))
                               (int 128)) Byte/SIZE)
                 pad (byte (if (and e (neg? (.signum ^BigDecimal e)))
                             -1
                             0))
                 ba (when e
                      (.toByteArray (.unscaledValue (doto ^BigDecimal e
                                                      (.setScale scale)
                                                      (.round (MathContext. precision))))))
                 position (.position data-buffer)]
             (try
               (dotimes [n (max 0 (- width (count ba)))]
                 (.put data-buffer pad))
               (if e
                 (do (.put data-buffer ba 0 (min width (alength ba)))
                     (aset buffers 0 (set-bit (aget buffers 0) length)))
                 (set! null-count (inc null-count)))
               (set! length (inc length))
               true
               (catch BufferOverflowException _
                 (aset buffers 1 (realloc (.position data-buffer position)))
                 (.add ^ArrowArray this e))))
       "w" (let [^ByteBuffer data-buffer (aget buffers 1)
                 width (Integer/parseInt (aget (.split format ":") 1))
                 position (.position data-buffer)]
             (when e
               (assert (= width (.capacity ^ByteBuffer e))))
             (try
               (.put data-buffer (if e
                                   (.duplicate ^ByteBuffer e)
                                   (ByteBuffer/allocate width)))
               (if e
                 (aset buffers 0 (set-bit (aget buffers 0) length))
                 (set! null-count (inc null-count)))
               (set! length (inc length))
               true
               (catch BufferOverflowException _
                 (aset buffers 1 (realloc (.position data-buffer position)))
                 (.add ^ArrowArray this e))))
       (throw (UnsupportedOperationException.)))))

  (^Object get [this ^int index]
   (when-not (< index length)
     (throw (IndexOutOfBoundsException.)))
   (when-not (.isNull this index)
     (case base-format
       "n" nil
       "b" (.getBoolean this index)
       "c" (.getByte this index)
       "C" (Byte/toUnsignedInt (.getByte this index))
       "s" (.getShort this index)
       "S" (Short/toUnsignedInt (.getShort this index))
       "i" (.getInt this index)
       "I" (Integer/toUnsignedLong (.getInt this index))
       "tdD" (LocalDate/ofEpochDay (.getInt this index))
       "tts" (LocalTime/ofSecondOfDay (.getInt this index))
       "ttm" (LocalTime/ofNanoOfDay (* 1000000 (.getInt this index)))
       "tiM" (Period/ofMonths (.getInt this index))
       "l" (.getLong this index)
       "L" (BigInteger. (Long/toUnsignedString (.getLong this index)))
       "tdm" (LocalDate/ofEpochDay (quot (.getLong this index) 86400000))
       "ttu" (LocalTime/ofNanoOfDay (* 1000 (.getLong this index)))
       "ttn" (LocalTime/ofNanoOfDay (.getLong this index))
       "tss" (let [tz (ts-format->zone-id format)]
               (cond-> (Instant/ofEpochSecond (.getLong this index))
                 tz (.atZone tz)))
       "tsm" (let [tz (ts-format->zone-id format)]
               (cond-> (Instant/ofEpochMilli (.getLong this index))
                 tz (.atZone tz)))
       "tsu" (let [tz (ts-format->zone-id format)]
               (cond-> (Instant/ofEpochSecond 0 (* 1000 (.getLong this index)))
                 tz (.atZone tz)))
       "tsn" (let [tz (ts-format->zone-id format)]
               (cond-> (Instant/ofEpochSecond 0 (.getLong this index))
                 tz (.atZone tz)))
       "tDs" (Duration/ofSeconds (.getLong this index))
       "tDm" (Duration/ofMillis (.getLong this index))
       "tDu" (Duration/ofNanos (* 1000 (.getLong this index)))
       "tDn" (Duration/ofNanos (.getLong this index))
       "tiD" (let [e (.getLong this index)]
               (.plusMillis (Duration/ofDays (bit-shift-right e Integer/SIZE))
                            (unchecked-int e)))
       "f" (.getFloat this index)
       "g" (.getDouble this index)
       "z" (let [^ByteBuffer offset-buffer (aget buffers 1)
                 ^ByteBuffer data-buffer (aget buffers 2)
                 offset (.getInt offset-buffer (* Integer/BYTES index))
                 end-offset (.getInt offset-buffer (* Integer/BYTES (inc index)))]
             (.slice data-buffer offset (- end-offset offset)))
       "u" (let [^ByteBuffer offset-buffer (aget buffers 1)
                 ^ByteBuffer data-buffer (aget buffers 2)
                 offset (.getInt offset-buffer (* Integer/BYTES index))
                 end-offset (.getInt offset-buffer (* Integer/BYTES (inc index)))]
             (.decode StandardCharsets/UTF_8 (.slice data-buffer offset (- end-offset offset))))
       "d" (let [^ByteBuffer data-buffer (aget buffers 1)
                 precision+scale+bit-width (.split ^String (aget (.split format ":") 1) ",")
                 precision (Integer/parseInt (aget precision+scale+bit-width 0))
                 scale (Integer/parseInt (aget precision+scale+bit-width 1))
                 width (quot (if (= 3 (alength precision+scale+bit-width))
                               (Integer/parseInt (aget precision+scale+bit-width 2))
                               (int 128)) Byte/SIZE)
                 offset (* width index)
                 ba (byte-array width)]
             (.get data-buffer offset ba)
             (BigDecimal. (BigInteger. ba) scale (MathContext. precision)))
       "w" (let [^ByteBuffer data-buffer (aget buffers 1)
                 width (Integer/parseInt (aget (.split format ":") 1))
                 offset (* index width)]
             (.slice data-buffer offset width))
       (throw (UnsupportedOperationException. format)))))

  (^int size [_] length)

  IPrimitiveList
  (^boolean add [this ^boolean e]
   (when e
     (aset buffers 1 (set-bit (aget buffers 1) length)))
   (aset buffers 0 (set-bit (aget buffers 0) length))
   (set! length (inc length))
   true)
  (^boolean add [this ^byte e]
   (let [^ByteBuffer data-buffer (aget buffers 1)]
     (try
       (.put data-buffer e)
       (aset buffers 0 (set-bit (aget buffers 0) length))
       (set! length (inc length))
       true
       (catch BufferOverflowException _
         (aset buffers 1 (realloc data-buffer))
         (.add ^ArrowArray this e)))))
  (^boolean add [this ^short e]
   (let [^ByteBuffer data-buffer (aget buffers 1)]
     (try
       (.putShort data-buffer e)
       (aset buffers 0 (set-bit (aget buffers 0) length))
       (set! length (inc length))
       true
       (catch BufferOverflowException _
         (aset buffers 1 (realloc data-buffer))
         (.add ^ArrowArray this e)))))
  (^boolean add [this ^int e]
   (let [^ByteBuffer data-buffer (aget buffers 1)]
     (try
       (.putInt data-buffer e)
       (aset buffers 0 (set-bit (aget buffers 0) length))
       (set! length (inc length))
       true
       (catch BufferOverflowException _
         (aset buffers 1 (realloc data-buffer))
         (.add ^ArrowArray this e)))))
  (^boolean add [this ^long e]
   (let [^ByteBuffer data-buffer (aget buffers 1)]
     (try
       (.putLong data-buffer e)
       (aset buffers 0 (set-bit (aget buffers 0) length))
       (set! length (inc length))
       true
       (catch BufferOverflowException _
         (aset buffers 1 (realloc data-buffer))
         (.add ^ArrowArray this e)))))
  (^boolean add [this ^float e]
   (let [^ByteBuffer data-buffer (aget buffers 1)]
     (try
       (.putFloat data-buffer e)
       (aset buffers 0 (set-bit (aget buffers 0) length))
       (set! length (inc length))
       true
       (catch BufferOverflowException _
         (aset buffers 1 (realloc data-buffer))
         (.add ^ArrowArray this e)))))
  (^boolean add [this ^double e]
   (let [^ByteBuffer data-buffer (aget buffers 1)]
     (try
       (.putDouble data-buffer e)
       (aset buffers 0 (set-bit (aget buffers 0) length))
       (set! length (inc length))
       true
       (catch BufferOverflowException _
         (aset buffers 1 (realloc data-buffer))
         (.add ^ArrowArray this e)))))

  (^boolean getBoolean [this ^int index]
   (when-not (< index length)
     (throw (IndexOutOfBoundsException.)))
   (when (.isNull this index)
     (throw (NullPointerException.)))
   (= 1 (get-bit (aget buffers 1) index)))
  (^byte getByte [this ^int index]
   (when (.isNull this index)
     (throw (NullPointerException.)))
   (.get ^ByteBuffer (aget buffers 1) index))
  (^short getShort [this ^int index]
   (when (.isNull this index)
     (throw (NullPointerException.)))
   (.getShort ^ByteBuffer (aget buffers 1) (* Short/BYTES index)))
  (^int getInt [this ^int index]
   (when (.isNull this index)
     (throw (NullPointerException.)))
   (.getInt ^ByteBuffer (aget buffers 1) (* Integer/BYTES index)))
  (^long getLong [this ^int index]
   (when (.isNull this index)
     (throw (NullPointerException.)))
   (.getLong ^ByteBuffer (aget buffers 1) (* Long/BYTES index)))
  (^float getFloat [this ^int index]
   (when (.isNull this index)
     (throw (NullPointerException.)))
   (.getFloat ^ByteBuffer (aget buffers 1) (* Float/BYTES index)))
  (^double getDouble [this ^int index]
   (when (.isNull this index)
     (throw (NullPointerException.)))
   (.getDouble ^ByteBuffer (aget buffers 1) (* Double/BYTES index)))

  AutoCloseable
  (close [_]
    (Arrays/fill buffers nil)
    (doseq [child children]
      (.close ^AutoCloseable child))
    (Arrays/fill children nil)))

(defn map->ArrowArray [{:keys [length null-count offset buffers children dictionary format schema]
                        :or {length 0 null-count 0 offset 0}}]
  (->ArrowArray length null-count offset (object-array buffers) (object-array children) dictionary format (base-format format) schema))

(def ^:private ^{:tag 'long} default-capacity 64)
(def ^:private ^{:tag 'long} alignment 64)

(defn- round-up-to-nearest-power-of-2 ^long [^long x ^long power-of-two]
  (assert (= 1 (Long/bitCount power-of-two)))
  (bit-and-not (+ x (dec power-of-two)) (dec power-of-two)))

(def ^:dynamic *allocate-buffer*
  (fn [^long capacity]
    (ByteBuffer/allocateDirect (round-up-to-nearest-power-of-2 capacity alignment))))

(defn- allocate-buffer
  (^java.nio.ByteBuffer []
   (allocate-buffer default-capacity))
  (^java.nio.ByteBuffer [^long capacity]
   (.order ^ByteBuffer (*allocate-buffer* capacity) ByteOrder/LITTLE_ENDIAN)))

(defn- allocate-validity-buffer
  (^java.nio.ByteBuffer []
   (allocate-validity-buffer default-capacity))
  (^java.nio.ByteBuffer [^long capacity]
   (allocate-buffer (quot capacity Byte/SIZE))))

(def allocate-offset-buffer allocate-buffer)
(def allocate-type-id-buffer allocate-buffer)

(defn realloc
  (^java.nio.ByteBuffer [^java.nio.ByteBuffer b]
   (realloc b (* (.capacity b) 2)))
  (^java.nio.ByteBuffer [^java.nio.ByteBuffer b ^long new-capacity]
   (.put ^ByteBuffer (allocate-buffer new-capacity) (.flip ^ByteBuffer b))))

(defn realloc-array ^core2.mini_arrow.ArrowArray [^ArrowArray a]
  (let [^objects buffers (.buffers a)]
    (dotimes [n (alength buffers)]
      (aset buffers n (realloc (aget buffers n))))
    a))

(def type->format
  {nil "n"
   Boolean "b"
   Byte "c"
   Short "s"
   Integer "i"
   Long "l"
   Float "f"
   Double "g"
   ByteBuffer "z"
   CharSequence "u"})

(defmulti ->array (fn [format-or-schema] (if (string? format-or-schema)
                                           (aget (.split ^String format-or-schema ":") 0)
                                           (type format-or-schema))))

(defmethod ->array ArrowSchema [^ArrowSchema schema]
  (let [^ArrowArray array (->array (.format schema))]
    (map->ArrowArray {:length 0
                      :null-count 0
                      :offset 0
                      :buffers (.buffers array)
                      :children (mapv ->array (.children schema))
                      :format (.format schema)
                      :schema schema})))

(defmethod ->array "n" [format]
  (map->ArrowArray {:length 0
                    :null-count 0
                    :offset 0
                    :buffers []
                    :format format}))

(defmethod ->array "b" [format]
  (map->ArrowArray {:length 0
                    :null-count 0
                    :offset 0
                    :buffers [(allocate-validity-buffer)
                              (allocate-buffer)]
                    :format format}))

(doseq [f ["c" "C"]]
  (defmethod ->array f [format]
    (map->ArrowArray {:length 0
                      :null-count 0
                      :offset 0
                      :buffers [(allocate-validity-buffer)
                                (allocate-buffer)]
                      :format format})))

(doseq [f ["s" "S"]]
  (defmethod ->array f [format]
    (map->ArrowArray {:length 0
                      :null-count 0
                      :offset 0
                      :buffers [(allocate-validity-buffer)
                                (allocate-buffer)]
                      :format format})))

(doseq [f ["i" "I" "tdD" "tts" "ttm" "tiM"]]
  (defmethod ->array f [format]
    (map->ArrowArray {:length 0
                      :null-count 0
                      :offset 0
                      :buffers [(allocate-validity-buffer)
                                (allocate-buffer)]
                      :format format})))

(doseq [f ["l" "L" "tdm" "ttu" "ttn" "tss" "tsm" "tsu" "tsn" "tDs" "tDm" "tDu" "tDn" "tiD"]]
  (defmethod ->array f [format]
    (map->ArrowArray {:length 0
                      :null-count 0
                      :offset 0
                      :buffers [(allocate-validity-buffer)
                                (allocate-buffer)]
                      :format format})))

(defmethod ->array "e" [format]
  (throw (UnsupportedOperationException. "e")))

(defmethod ->array "f" [format]
  (map->ArrowArray {:length 0
                    :null-count 0
                    :offset 0
                    :buffers [(allocate-validity-buffer)
                              (allocate-buffer)]
                    :format format}))

(defmethod ->array "g" [format]
  (map->ArrowArray {:length 0
                    :null-count 0
                    :offset 0
                    :buffers [(allocate-validity-buffer)
                              (allocate-buffer)]
                    :format format}))

(defmethod ->array "z" [format]
  (map->ArrowArray {:length 0
                    :null-count 0
                    :offset 0
                    :buffers [(allocate-validity-buffer)
                              (allocate-offset-buffer)
                              (allocate-buffer)]
                    :format format}))

(defmethod ->array "Z" [format]
  (throw (UnsupportedOperationException. "Z")))

(defmethod ->array "u" [format]
  (map->ArrowArray {:length 0
                    :null-count 0
                    :offset 0
                    :buffers [(allocate-validity-buffer)
                              (allocate-offset-buffer)
                              (allocate-buffer)]
                    :format format}))

(defmethod ->array "U" [format]
  (throw (UnsupportedOperationException. "U")))

(defmethod ->array "d" [format]
  (map->ArrowArray {:length 0
                    :null-count 0
                    :offset 0
                    :buffers [(allocate-validity-buffer)
                              (allocate-buffer)]
                    :format format}))

(defmethod ->array "w" [format]
  (map->ArrowArray {:length 0
                    :null-count 0
                    :offset 0
                    :buffers [(allocate-validity-buffer)
                              (allocate-buffer)]
                    :format format}))

(defmethod ->array "+l" [format]
  (map->ArrowArray {:length 0
                    :null-count 0
                    :offset 0
                    :buffers [(allocate-validity-buffer)
                              (allocate-offset-buffer)]
                    :format format}))

(defmethod ->array "+L" [format]
  (throw (UnsupportedOperationException. "+L")))

(defmethod ->array "+w" [format]
  (map->ArrowArray {:length 0
                    :null-count 0
                    :offset 0
                    :buffers [(allocate-validity-buffer)]
                    :format format}))

(defmethod ->array "+s" [format]
  (map->ArrowArray {:length 0
                    :null-count 0
                    :offset 0
                    :buffers [(allocate-validity-buffer)]
                    :format format}))

(defmethod ->array "+m" [format]
  (map->ArrowArray {:length 0
                    :null-count 0
                    :offset 0
                    :buffers [(allocate-validity-buffer)
                              (allocate-buffer)]
                    :format format}))

(defmethod ->array "+us" [format]
  (map->ArrowArray {:length 0
                    :null-count 0
                    :offset 0
                    :buffers [(allocate-type-id-buffer)]
                    :format format}))

(defmethod ->array "+ud" [format]
  (map->ArrowArray {:length 0
                    :null-count 0
                    :offset 0
                    :buffers [(allocate-type-id-buffer)
                              (allocate-offset-buffer)]
                    :format format}))

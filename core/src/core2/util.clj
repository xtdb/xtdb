(ns core2.util
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log])
  (:import clojure.lang.MapEntry
           [core2 DenseUnionUtil ICursor]
           [java.io ByteArrayOutputStream File]
           java.lang.AutoCloseable
           [java.lang.invoke LambdaMetafactory MethodHandles MethodType]
           [java.lang.reflect Field Method]
           java.net.URI
           java.nio.ByteBuffer
           [java.nio.channels Channels FileChannel FileChannel$MapMode SeekableByteChannel]
           java.nio.charset.StandardCharsets
           [java.nio.file CopyOption Files FileVisitResult LinkOption OpenOption Path Paths SimpleFileVisitor StandardCopyOption StandardOpenOption]
           java.nio.file.attribute.FileAttribute
           [java.time Duration Instant LocalDateTime ZonedDateTime ZoneId]
           java.time.temporal.ChronoUnit
           [java.util ArrayList Collections Date IdentityHashMap LinkedHashMap LinkedList List Map$Entry Queue UUID]
           [java.util.concurrent CompletableFuture Executors ExecutorService ThreadFactory TimeUnit]
           java.util.concurrent.atomic.AtomicInteger
           [java.util.function BiFunction Consumer Function IntUnaryOperator Supplier]
           [org.apache.arrow.compression CommonsCompressionFactory ZstdCompressionCodec]
           [org.apache.arrow.flatbuf Footer Message RecordBatch]
           [org.apache.arrow.memory AllocationManager ArrowBuf BufferAllocator RootAllocator]
           [org.apache.arrow.memory.util ArrowBufPointer ByteFunctionHelpers MemoryUtil]
           [org.apache.arrow.vector BaseVariableWidthVector BitVector ElementAddressableVector ValueVector VectorLoader VectorSchemaRoot VectorUnloader]
           [org.apache.arrow.vector.complex DenseUnionVector NonNullableStructVector]
           [org.apache.arrow.vector.ipc ArrowFileReader ArrowFileWriter ArrowStreamWriter ArrowWriter]
           [org.apache.arrow.vector.ipc.message ArrowBlock ArrowFooter ArrowRecordBatch MessageSerializer]
           org.apache.arrow.vector.util.VectorSchemaRootAppender
           org.roaringbitmap.RoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

(defn maybe-update [m k f & args]
  (if (contains? m k)
    (apply update m k f args)
    m))

;;; Common specs

(defn ->path ^Path [path-ish]
  (cond
    (instance? Path path-ish) path-ish
    (instance? File path-ish) (.toPath ^File path-ish)
    (uri? path-ish) (Paths/get ^URI path-ish)
    (string? path-ish) (let [uri (URI. path-ish)]
                         (if (.getScheme uri)
                           (Paths/get uri)
                           (Paths/get path-ish (make-array String 0))))
    :else ::s/invalid))

(s/def ::path
  (s/and (s/conformer ->path) #(instance? Path %)))

(s/def ::string-map (s/map-of string? string?))
(s/def ::string-list (s/coll-of string?))

(defn ->duration [d]
  (cond
    (instance? Duration d) d
    (nat-int? d) (Duration/ofMillis d)
    (string? d) (Duration/parse d)
    :else ::s/invalid))

(s/def ::duration
  (s/and (s/conformer ->duration) #(instance? Duration %)))

(defprotocol TimeConversions
  (^java.time.Instant ->instant [v])
  (^java.time.ZonedDateTime ->zdt [v]))

(def utc (ZoneId/of "UTC"))

(extend-protocol TimeConversions
  nil
  (->instant [i] nil)
  (->zdt [i] nil)

  Instant
  (->instant [i] i)
  (->zdt [i] (-> i (.atZone utc)))

  Date
  (->instant [d] (.toInstant d))
  (->zdt [d] (->zdt (->instant d)))

  ZonedDateTime
  (->instant [zdt] (.toInstant zdt))
  (->zdt [zdt] zdt))

(defn instant->micros ^long [^Instant inst]
  (-> (Math/multiplyExact (.getEpochSecond inst) 1000000)
      (Math/addExact (quot (.getNano inst) 1000))))

(defn micros->instant ^java.time.Instant [^long μs]
  (.plus Instant/EPOCH μs ChronoUnit/MICROS))

(defn component
  ([node k] (get @(:!system node) k)))

;;; IO

(defn ->seekable-byte-channel ^java.nio.channels.SeekableByteChannel [^ByteBuffer buffer]
  (let [buffer (.slice buffer)]
    (proxy [SeekableByteChannel] []
      (isOpen []
        true)

      (close [])

      (read [^ByteBuffer dst]
        (let [^ByteBuffer src (-> buffer (.slice) (.limit (.remaining dst)))]
          (.put dst src)
          (let [bytes-read (.position src)]
            (.position buffer (+ (.position buffer) bytes-read))
            bytes-read)))

      (position
        ([]
         (.position buffer))
        ([^long new-position]
         (.position buffer new-position)
         this))

      (size []
        (.capacity buffer))

      (write [src]
        (throw (UnsupportedOperationException.)))

      (truncate [size]
        (throw (UnsupportedOperationException.))))))

(def write-new-file-opts ^"[Ljava.nio.file.OpenOption;"
  (into-array OpenOption #{StandardOpenOption/CREATE StandardOpenOption/WRITE StandardOpenOption/TRUNCATE_EXISTING}))

(defn ->file-channel
  (^java.nio.channels.FileChannel [^Path path]
   (->file-channel path #{StandardOpenOption/READ}))
  (^java.nio.channels.FileChannel [^Path path options]
   (FileChannel/open path (into-array OpenOption options))))

(defn ->mmap-path
  (^java.nio.MappedByteBuffer [^Path path]
   (->mmap-path path FileChannel$MapMode/READ_ONLY))
  (^java.nio.MappedByteBuffer [^Path path ^FileChannel$MapMode map-mode]
   (with-open [in (->file-channel path (if (= FileChannel$MapMode/READ_ONLY map-mode)
                                         #{StandardOpenOption/READ}
                                         #{StandardOpenOption/READ StandardOpenOption/WRITE}))]
     (.map in map-mode 0 (.size in)))))

(defn write-buffer-to-path [^ByteBuffer from-buffer ^Path to-path]
  (with-open [file-ch (->file-channel to-path write-new-file-opts)
              buf-ch (->seekable-byte-channel from-buffer)]
    (.transferFrom file-ch buf-ch 0 (.size buf-ch))))

(defn atomic-move [^Path from-path ^Path to-path]
  (Files/move from-path to-path (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE]))
  to-path)

(defn write-buffer-to-path-atomically [^ByteBuffer from-buffer ^Path to-path]
  (let [to-path-temp (.resolveSibling to-path (str "." (UUID/randomUUID)))]
    (try
      (write-buffer-to-path from-buffer to-path-temp)
      (atomic-move to-path-temp to-path)
      (finally
        (Files/deleteIfExists to-path-temp)))))

(def ^:private file-deletion-visitor
  (proxy [SimpleFileVisitor] []
    (visitFile [file _]
      (Files/delete file)
      FileVisitResult/CONTINUE)

    (postVisitDirectory [dir _]
      (Files/delete dir)
      FileVisitResult/CONTINUE)))

(defn path-exists [^Path path]
  (Files/exists path (make-array LinkOption 0)))

(defn path-size ^long [^Path path]
  (Files/size path))

(defn delete-dir [^Path dir]
  (when (path-exists dir)
    (Files/walkFileTree dir file-deletion-visitor)))

(defn delete-file [^Path file]
  (Files/deleteIfExists file))

(defn mkdirs [^Path path]
  (Files/createDirectories path (make-array FileAttribute 0)))

(defn ->temp-file ^Path [^String prefix ^String suffix]
  (doto (Files/createTempFile prefix suffix (make-array FileAttribute 0))
    (delete-file)))

(def ^:private ^ZoneId utc (ZoneId/of "UTC"))

(defn local-date-time->date ^java.util.Date [^LocalDateTime ldt]
  (Date/from (.toInstant (.atZone ldt utc))))

(defn date->local-date-time ^java.time.LocalDateTime [^Date d]
  (LocalDateTime/ofInstant (.toInstant d) utc))

(defn ->supplier {:style/indent :defn} ^java.util.function.Supplier [f]
  (reify Supplier
    (get [_]
      (f))))

(defn ->jfn {:style/indent :defn} ^java.util.function.Function [f]
  (reify Function
    (apply [_ v]
      (f v))))

(defn ->jbifn {:style/indent :defn} ^java.util.function.BiFunction [f]
  (reify BiFunction
    (apply [_ a b]
      (f a b))))

(defn ->sam [f ^Class target-interface]
  (let [caller (MethodHandles/lookup)

        implementing-class (class f)
        [implementing-method] (.getDeclaredMethods implementing-class)
        implementing-method-handle (.unreflect caller implementing-method)

        [^Method sam-method] (for [^Method m (.getMethods target-interface)
                                   :when (not (.isDefault m))]
                               m)
        sam-method-type (.type (.unreflect caller sam-method))
        sam-method-type-without-this (.dropParameterTypes sam-method-type 0 1)

        factory-method-type (MethodType/methodType target-interface implementing-class)
        call-site (LambdaMetafactory/metafactory caller
                                                 (.getName sam-method)
                                                 factory-method-type
                                                 sam-method-type-without-this
                                                 implementing-method-handle
                                                 sam-method-type-without-this)
        ^List args [f]]
    (.invokeWithArguments (.getTarget call-site) args)))

(defn then-apply {:style/indent :defn}
  ^java.util.concurrent.CompletableFuture
  [^CompletableFuture fut f]
  (.thenApply fut (->jfn f)))

(defn then-compose {:style/indent :defn}
  ^java.util.concurrent.CompletableFuture
  [^CompletableFuture fut f]
  (.thenCompose fut (->jfn f)))

(defmacro completable-future {:style/indent 1} [pool & body]
  `(CompletableFuture/supplyAsync (->supplier (fn [] ~@body)) ~pool))

(defn ->prefix-thread-factory ^java.util.concurrent.ThreadFactory [^String prefix]
  (let [default-thread-factory (Executors/defaultThreadFactory)]
    (reify ThreadFactory
      (newThread [_ r]
        (let [t (.newThread default-thread-factory r)]
          (.setName t (str prefix (.getName t)))
          t)))))

(defn try-close [c]
  (try
    (when (instance? AutoCloseable c)
      (.close ^AutoCloseable c))
    (catch Exception e
      (log/warn e "could not close"))))

(def uncaught-exception-handler
  (reify Thread$UncaughtExceptionHandler
    (uncaughtException [_ thread throwable]
      (log/error throwable "Uncaught exception:"))))

(defn install-uncaught-exception-handler! []
  (when-not (Thread/getDefaultUncaughtExceptionHandler)
    (Thread/setDefaultUncaughtExceptionHandler uncaught-exception-handler)))

(defn shutdown-pool
  ([^ExecutorService pool]
   (shutdown-pool pool 60))
  ([^ExecutorService pool ^long timeout-seconds]
   (.shutdownNow pool)
   (when-not (.awaitTermination pool timeout-seconds TimeUnit/SECONDS)
     (log/warn "pool did not terminate" pool))))

;;; Arrow

(defn root-field-count ^long [^VectorSchemaRoot root]
  (.size (.getFields (.getSchema root))))

;; TODO: can maybe tweak in DenseUnionVector, but that doesn't
;; solve the VSR calling this.
(defn set-value-count [^ValueVector v ^long value-count]
  (let [value-count (int value-count)]
    (cond
      (instance? DenseUnionVector v)
      (DenseUnionUtil/setValueCount v value-count)

      (and (instance? NonNullableStructVector v)
           (zero? (.getNullCount v)))
      (let [^NonNullableStructVector v v]
        (doseq [v (.getChildrenFromFields v)]
          (set-value-count v value-count))
        (set! (.valueCount v) value-count))

      :else
      (.setValueCount v value-count))))

(def ^:private ^Field vector-schema-root-row-count-field
  (doto (.getDeclaredField VectorSchemaRoot "rowCount")
    (.setAccessible true)))

(defn set-vector-schema-root-row-count [^VectorSchemaRoot root ^long row-count]
  (let [row-count (int row-count)]
    (.set vector-schema-root-row-count-field root row-count)
    (dotimes [n (root-field-count root)]
      (set-value-count (.getVector root n) row-count))))

(defn slice-root
  (^org.apache.arrow.vector.VectorSchemaRoot [^VectorSchemaRoot root]
   (slice-root root 0))

  (^org.apache.arrow.vector.VectorSchemaRoot [^VectorSchemaRoot root ^long start-idx]
   (slice-root root start-idx (- (.getRowCount root) start-idx)))

  (^org.apache.arrow.vector.VectorSchemaRoot [^VectorSchemaRoot root ^long start-idx ^long len]
   (let [num-fields (root-field-count root)
         acc (ArrayList. num-fields)]
     (dotimes [n num-fields]
       (let [field-vec (.getVector root n)]
         (.add acc (.getTo (doto (.getTransferPair field-vec (.getAllocator field-vec))
                             (.splitAndTransfer start-idx len))))))

     (VectorSchemaRoot. acc))))

(defn build-arrow-ipc-byte-buffer ^java.nio.ByteBuffer {:style/indent 2}
  [^VectorSchemaRoot root ipc-type f]

  (with-open [baos (ByteArrayOutputStream.)
              ch (Channels/newChannel baos)
              ^ArrowWriter sw (case ipc-type
                                :file (ArrowFileWriter. root nil ch)
                                :stream (ArrowStreamWriter. root nil ch))]

    (.start sw)

    (f (fn write-batch! []
         (.writeBatch sw)))

    (.end sw)

    (ByteBuffer/wrap (.toByteArray baos))))

(defn root->arrow-ipc-byte-buffer ^java.nio.ByteBuffer [^VectorSchemaRoot root ipc-type]
  (build-arrow-ipc-byte-buffer root ipc-type
    (fn [write-batch!]
      (write-batch!))))

(def ^{:tag 'bytes} arrow-magic (.getBytes "ARROW1" StandardCharsets/UTF_8))

(defn- validate-arrow-magic [^ArrowBuf ipc-file-format-buffer]
  (when-not (zero? (ByteFunctionHelpers/compare ipc-file-format-buffer
                                                (- (.capacity ipc-file-format-buffer) (alength arrow-magic))
                                                (.capacity ipc-file-format-buffer)
                                                arrow-magic
                                                0
                                                (alength arrow-magic)))
    (throw (IllegalArgumentException. "invalid Arrow IPC file format"))))

(defn read-arrow-footer ^org.apache.arrow.vector.ipc.message.ArrowFooter [^ArrowBuf ipc-file-format-buffer]
  (validate-arrow-magic ipc-file-format-buffer)
  (let [footer-size-offset (- (.capacity ipc-file-format-buffer) (+ Integer/BYTES (alength arrow-magic)))
        footer-size (.getInt ipc-file-format-buffer footer-size-offset)
        footer-position (- footer-size-offset footer-size)
        footer-bb (.nioBuffer ipc-file-format-buffer footer-position footer-size)]
    (ArrowFooter. (Footer/getRootAsFooter footer-bb))))

(defn- try-open-reflective-access [^Class from ^Class to]
  (try
    (let [this-module (.getModule from)]
      (when-not (.isNamed this-module)
        (.addOpens (.getModule to) (.getName (.getPackage to)) this-module)))
    (catch Exception e
      (log/warn e "could not open reflective access from" from "to" to))))

(defonce ^:private direct-byte-buffer-access
  (try-open-reflective-access ArrowBuf (class (ByteBuffer/allocateDirect 0))))

(def ^:private try-free-direct-buffer
  (try
    (Class/forName "io.netty.util.internal.PlatformDependent")
    (eval
     '(fn free-direct-buffer [nio-buffer]
        (try
          (io.netty.util.internal.PlatformDependent/freeDirectBuffer nio-buffer)
          (catch Exception e
            ;; NOTE: this happens when we give a sliced buffer from
            ;; the in-memory object store to the buffer pool.
            (let [cause (.getCause e)]
              (when-not (and (instance? IllegalArgumentException cause)
                             (= "duplicate or slice" (.getMessage cause)))
                (throw e)))))))
    (catch ClassNotFoundException e
      (fn free-direct-buffer-nop [_]))))

(defn inc-ref-count
  (^long [^AtomicInteger ref-count]
   (inc-ref-count ref-count 1))
  (^long [^AtomicInteger ref-count ^long increment]
   (.updateAndGet ^AtomicInteger ref-count
                  (reify IntUnaryOperator
                    (applyAsInt [_ x]
                      (if (pos? x)
                        (+ x increment)
                        x))))))

(defn dec-ref-count ^long [^AtomicInteger ref-count]
  (let [new-ref-count (.updateAndGet ^AtomicInteger ref-count
                                     (reify IntUnaryOperator
                                       (applyAsInt [_ x]
                                         (if (pos? x)
                                           (dec x)
                                           -1))))]
    (when (neg? new-ref-count)
      (.set ref-count 0)
      (throw (IllegalStateException. "ref count has gone negative")))
    new-ref-count))

(def ^:private ^Method allocation-manager-associate-method
  (doto (.getDeclaredMethod AllocationManager "associate" (into-array Class [BufferAllocator]))
    (.setAccessible true)))

(defn ->arrow-buf-view
  (^org.apache.arrow.memory.ArrowBuf [^BufferAllocator allocator ^ByteBuffer nio-buffer]
   (->arrow-buf-view allocator nio-buffer nil))
  (^org.apache.arrow.memory.ArrowBuf [^BufferAllocator allocator ^ByteBuffer nio-buffer on-close-fn]
   (let [nio-buffer (if (.isDirect nio-buffer)
                      nio-buffer
                      (-> (ByteBuffer/allocateDirect (.capacity nio-buffer))
                          (.put (.duplicate nio-buffer))
                          (.clear)))
         address (MemoryUtil/getByteBufferAddress nio-buffer)
         size (.capacity nio-buffer)
         allocation-manager (proxy [AllocationManager] [allocator]
                              (getSize [] size)
                              (memoryAddress [] address)
                              (release0 []
                                (try-free-direct-buffer nio-buffer)
                                (when on-close-fn
                                  (on-close-fn))))
         listener (.getListener allocator)
         _ (with-open [reservation (.newReservation allocator)]
             (.onPreAllocation listener size)
             (.reserve reservation size)
             (.onAllocation listener size))
         buffer-ledger (.invoke allocation-manager-associate-method allocation-manager (object-array [allocator]))]
     (ArrowBuf. buffer-ledger nil size address))))

(defn ->arrow-record-batch-view ^org.apache.arrow.vector.ipc.message.ArrowRecordBatch [^ArrowBlock block ^ArrowBuf buffer]
  (let [prefix-size (if (= (.getInt buffer (.getOffset block)) MessageSerializer/IPC_CONTINUATION_TOKEN)
                      8
                      4)
        ^RecordBatch batch (.header (Message/getRootAsMessage
                                     (.nioBuffer buffer
                                                 (+ (.getOffset block) prefix-size)
                                                 (- (.getMetadataLength block) prefix-size)))
                                    (RecordBatch.))
        body-buffer (doto (.slice buffer
                                  (+ (.getOffset block)
                                     (.getMetadataLength block))
                                  (.getBodyLength block))
                      (-> (.getReferenceManager) (.retain)))]
    (MessageSerializer/deserializeRecordBatch batch body-buffer)))

(deftype ChunkCursor [^ArrowBuf buf,
                      ^Queue blocks
                      ^VectorSchemaRoot root
                      ^VectorLoader loader
                      ^:unsynchronized-mutable ^ArrowRecordBatch current-batch
                      ^boolean close-buffer?]
  ICursor
  (tryAdvance [this c]
    (when current-batch
      (try-close current-batch)
      (set! (.current-batch this) nil))

    (if-let [block (.poll blocks)]
      (let [record-batch (->arrow-record-batch-view block buf)]
        (set! (.current-batch this) record-batch)
        (.load loader record-batch)
        (.accept c root)
        true)
      false))

  (close [_]
    (when current-batch
      (try-close current-batch))
    (try-close root)
    (when close-buffer?
      (try-close buf))))

(defn ^core2.ICursor ->chunks
  ([^ArrowBuf ipc-file-format-buffer]
   (->chunks ipc-file-format-buffer {}))
  ([^ArrowBuf ipc-file-format-buffer {:keys [^RoaringBitmap block-idxs close-buffer?]}]
   (let [footer (read-arrow-footer ipc-file-format-buffer)
         root (VectorSchemaRoot/create (.getSchema footer) (.getAllocator (.getReferenceManager ipc-file-format-buffer)))]
     (ChunkCursor. ipc-file-format-buffer
                   (LinkedList. (cond->> (.getRecordBatches footer)
                                  block-idxs (keep-indexed (fn [idx record-batch]
                                                             (when (.contains block-idxs ^int idx)
                                                               record-batch)))))
                   root
                   (VectorLoader. root CommonsCompressionFactory/INSTANCE)
                   nil
                   (boolean close-buffer?)))))

(defn with-last-block [^ArrowBuf buf, f]
  (let [res (promise)
        last-block-idx (-> (.getRecordBatches (read-arrow-footer buf))
                           (count) (dec))]
    (with-open [chunk (->chunks buf {:block-idxs (doto (RoaringBitmap.)
                                                   (.add last-block-idx))
                                     :close-buffer? true})]
      (when (.tryAdvance chunk
                         (reify Consumer
                           (accept [_ root]
                             (deliver res (f root)))))
        @res))))

(defn ^core2.ICursor and-also-close [^ICursor cursor, ^AutoCloseable closeable]
  (reify ICursor
    (characteristics [_] (.characteristics cursor))
    (estimateSize [_] (.estimateSize cursor))
    (getComparator [_] (.getComparator cursor))
    (getExactSizeIfKnown [_] (.getExactSizeIfKnown cursor))
    (hasCharacteristics [_ c] (.hasCharacteristics cursor c))
    (tryAdvance [_ c] (.tryAdvance cursor c))
    (trySplit [_] (.trySplit cursor))

    (close [_]
      (try-close cursor)
      (try-close closeable))))

(defn element-addressable-vector? [^ValueVector v]
  (and (instance? ElementAddressableVector v)
       (not (instance? BitVector v))))

(defn pointer-or-object
  ([^ValueVector v ^long idx]
   (pointer-or-object v idx nil))
  ([^ValueVector v ^long idx ^ArrowBufPointer pointer]
   (cond
     (element-addressable-vector? v)
     (.getDataPointer ^ElementAddressableVector v idx (or pointer (ArrowBufPointer.)))

     (instance? DenseUnionVector v)
     (let [v ^DenseUnionVector v]
       (recur (.getVectorByType v (.getTypeId v idx)) (.getOffset v idx) pointer))

     :else
     (.getObject v idx))))

(defn maybe-copy-pointer [^BufferAllocator allocator x]
  (if (instance? ArrowBufPointer x)
    (let [^ArrowBufPointer x x
          length (.getLength x)
          buffer-copy (.buffer allocator length)]
      (.setBytes buffer-copy 0 (.getBuf x) (.getOffset x) length)
      (ArrowBufPointer. buffer-copy 0 length))
    x))

(defn compare-nio-buffers-unsigned ^long [^ByteBuffer x ^ByteBuffer y]
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

(defn element->nio-buffer ^java.nio.ByteBuffer [^BaseVariableWidthVector vec ^long idx]
  (let [value-buffer (.getDataBuffer vec)
        offset-buffer (.getOffsetBuffer vec)
        offset-idx (* idx BaseVariableWidthVector/OFFSET_WIDTH)
        offset (.getInt offset-buffer offset-idx)
        end-offset (.getInt offset-buffer (+ offset-idx BaseVariableWidthVector/OFFSET_WIDTH))]
    (.nioBuffer value-buffer offset (- end-offset offset))))

(defn ->region-allocator
  (^org.apache.arrow.memory.BufferAllocator []
   (let [buffers (ArrayList.)]
     (proxy [RootAllocator] []
       (buffer [size]
         (let [^RootAllocator this this
               ^ArrowBuf buf (proxy-super buffer size)]
           (.add buffers buf)
           buf))

       (close []
         (let [^RootAllocator this this]
           (doseq [^ArrowBuf buf buffers
                   :let [ref-count (.refCnt buf)]
                   :when (pos? ref-count)]
             (.release (.getReferenceManager buf) ref-count))
           (.clear buffers)
           (proxy-super close)))))))

(def ^:private ^Field arrow-writer-unloader-field
  (doto (.getDeclaredField ArrowWriter "unloader")
    (.setAccessible true)))

(defn compress-arrow-ipc-file-blocks
  (^java.nio.file.Path [^Path path]
   (let [compressed-path (.resolve (.getParent path) (str (.getFileName path) ".compressed"))]
     (compress-arrow-ipc-file-blocks path compressed-path)
     (atomic-move compressed-path path)))
  (^java.nio.file.Path [^Path from ^Path to]
   (with-open [allocator (RootAllocator.)
               from-ch (->file-channel from)
               in (ArrowFileReader. from-ch allocator)
               to-ch (->file-channel to write-new-file-opts)
               out (ArrowFileWriter. (.getVectorSchemaRoot in) nil to-ch)]
     (while (.loadNextBatch in)
       (let [root (.getVectorSchemaRoot in)]
         (with-open [inner-allocator (->region-allocator)]
           (let [out-root (VectorSchemaRoot/create (.getSchema root) inner-allocator)]
             (VectorSchemaRootAppender/append out-root (into-array [root]))
             (.set arrow-writer-unloader-field out (VectorUnloader. out-root true (ZstdCompressionCodec.) true))
             (.writeBatch out)))))
     to)))

(defn into-linked-map
  ([coll]
   (let [res (LinkedHashMap.)]
     (doseq [^Map$Entry entry coll]
       (.put res (.getKey entry) (.getValue entry)))))

  ([xform coll]
   (transduce xform
              (completing (fn [^LinkedHashMap lhm ^Map$Entry el]
                            (doto lhm (.put (.getKey el) (.getValue el)))))
              (LinkedHashMap.)
              coll)))

(defn map-entries [f]
  (map (fn [^Map$Entry entry]
         (f (.getKey entry) (.getValue entry)))))

(defn map-values [f]
  (map (fn [^Map$Entry entry]
         (let [k (.getKey entry)]
           (MapEntry/create k (f k (.getValue entry)))))))

(defn ->identity-set []
  (Collections/newSetFromMap (IdentityHashMap.)))

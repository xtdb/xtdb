(ns xtdb.util
  (:require [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            xtdb.api.impl
            [xtdb.error :as err]
            [clojure.string :as str])
  (:import clojure.lang.MapEntry
           xtdb.ICursor
           [java.io ByteArrayOutputStream File]
           java.lang.AutoCloseable
           java.lang.reflect.Method
           [java.net MalformedURLException URI URL]
           java.nio.ByteBuffer
           [java.nio.channels Channels FileChannel FileChannel$MapMode SeekableByteChannel]
           java.nio.charset.StandardCharsets
           [java.nio.file CopyOption FileVisitResult Files LinkOption OpenOption Path Paths SimpleFileVisitor StandardCopyOption StandardOpenOption]
           java.nio.file.attribute.FileAttribute
           [java.time Duration Instant LocalDate LocalDateTime LocalTime OffsetDateTime ZoneId ZonedDateTime]
           java.time.temporal.ChronoUnit
           [java.util ArrayList Collections Date HashMap Iterator LinkedHashMap LinkedList Map Queue UUID WeakHashMap]
           [java.util.concurrent CompletableFuture ExecutorService Executors ThreadFactory TimeUnit]
           [java.util.function BiFunction Consumer Function Supplier]
           [org.apache.arrow.compression CommonsCompressionFactory]
           [org.apache.arrow.flatbuf Footer Message RecordBatch]
           [org.apache.arrow.memory AllocationManager ArrowBuf BufferAllocator]
           [org.apache.arrow.memory.util ByteFunctionHelpers MemoryUtil]
           [org.apache.arrow.vector VectorLoader VectorSchemaRoot]
           [org.apache.arrow.vector.ipc ArrowFileWriter ArrowStreamWriter ArrowWriter]
           [org.apache.arrow.vector.ipc.message ArrowBlock ArrowFooter ArrowRecordBatch MessageSerializer]
           org.roaringbitmap.RoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

(defn maybe-update [m k f & args]
  (if (contains? m k)
    (apply update m k f args)
    m))

(defn try-close [c]
  (try
    (when (instance? AutoCloseable c)
      (.close ^AutoCloseable c))
    (catch Exception e
      (log/warn e "could not close"))))

(defn uuid->bytes ^bytes [^UUID uuid]
  (let [bb (doto (ByteBuffer/allocate 16)
             (.putLong (.getMostSignificantBits uuid))
             (.putLong (.getLeastSignificantBits uuid)))]
    (.array bb)))

(defn ->lex-hex-string
  "Turn a long into a lexicographically-sortable hex string by prepending the length"
  [^long l]

  (let [s (format "%x" l)]
    (format "%x%s" (dec (count s)) s)))

(defn <-lex-hex-string [^String s]
  (Long/parseLong (subs s 1) 16))

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

(defn ->url [url-ish]
  (try
    (io/as-url url-ish)
    (catch MalformedURLException _
      (.toURL (.toUri (->path url-ish))))))

(s/def ::url
  (s/and (s/conformer ->url) #(instance? URL %)))

(s/def ::string-map (s/map-of string? string?))

(defn ->duration [d]
  (cond
    (instance? Duration d) d
    (nat-int? d) (Duration/ofMillis d)
    (string? d) (Duration/parse d)
    :else ::s/invalid))

(s/def ::duration
  (s/and (s/conformer ->duration) #(instance? Duration %)))

(s/def ::datetime-value
  (some-fn (partial instance? Date)
           (partial instance? Instant)
           (partial instance? ZonedDateTime)))

(defprotocol TimeConversions
  (^java.time.Instant ->instant [v])
  (^java.time.ZonedDateTime ->zdt [v]))

(def utc (ZoneId/of "UTC"))

(extend-protocol TimeConversions
  nil
  (->instant [_] nil)
  (->zdt [_] nil)

  Instant
  (->instant [i] i)
  (->zdt [i] (-> i (.atZone utc)))

  Date
  (->instant [d] (.toInstant d))
  (->zdt [d] (->zdt (->instant d)))

  ZonedDateTime
  (->instant [zdt] (.toInstant zdt))
  (->zdt [zdt] zdt)

  OffsetDateTime
  (->instant [odt] (.toInstant odt)))

(defn instant->micros ^long [^Instant inst]
  (-> (Math/multiplyExact (.getEpochSecond inst) #=(long 1e6))
      (Math/addExact (quot (.getNano inst) 1000))))

(defn sql-temporal->micros
  "Given some temporal value (such as a Date, LocalDateTime, OffsetDateTime and so on) will return the corresponding Instant.

   To do this for LocalDate and LocalDateTime, the provided SQL session time zone is assumed to be the implied time zone of the date/time."
  ^long [temporal ^ZoneId session-zone]
  (condp instance? temporal
    LocalDate (sql-temporal->micros (.atTime ^LocalDate temporal LocalTime/MIDNIGHT) session-zone)
    LocalDateTime (instant->micros (.toInstant (.atZone ^LocalDateTime temporal session-zone)))
    (instant->micros (->instant temporal))))

(defn instant->nanos ^long [^Instant inst]
  (-> (Math/multiplyExact (.getEpochSecond inst) #=(long 1e9))
      (Math/addExact (long (.getNano inst)))))

(defn micros->instant ^java.time.Instant [^long μs]
  (.plus Instant/EPOCH μs ChronoUnit/MICROS))

(defn nanos->instant ^java.time.Instant [^long ns]
  (.plus Instant/EPOCH ns ChronoUnit/NANOS))

(def ^java.time.Instant end-of-time
  (Instant/parse "9999-12-31T23:59:59.999999Z"))

(def ^{:tag 'long} end-of-time-μs
  (instant->micros end-of-time))

(defn component
  ([node k] (get (:system node) k)))

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

(defn write-buffer-to-path [^ByteBuffer from-buffer ^Path to-path]
  (with-open [file-ch (->file-channel to-path write-new-file-opts)
              buf-ch (->seekable-byte-channel from-buffer)]
    (.transferFrom file-ch buf-ch 0 (.size buf-ch))))

(defn atomic-move [^Path from-path ^Path to-path]
  (Files/move from-path to-path (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE]))
  to-path)

(defn write-buffer-to-path-atomically [^ByteBuffer from-buffer, ^Path root-path, ^Path to-path]
  (let [to-path-temp (doto (.resolve root-path (str ".tmp/" (UUID/randomUUID)))
                       (-> (.getParent) mkdirs))]
    (try
      (write-buffer-to-path from-buffer to-path-temp)
      (atomic-move to-path-temp to-path)
      (finally
        (Files/deleteIfExists to-path-temp)))))

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

(def uncaught-exception-handler
  (reify Thread$UncaughtExceptionHandler
    (uncaughtException [_ _thread throwable]
      (log/error throwable "Uncaught exception:"))))

(defn install-uncaught-exception-handler! []
  (when-not (Thread/getDefaultUncaughtExceptionHandler)
    (Thread/setDefaultUncaughtExceptionHandler uncaught-exception-handler)))

(defn ->prefix-thread-factory ^java.util.concurrent.ThreadFactory [^String prefix]
  (let [default-thread-factory (Executors/defaultThreadFactory)]
    (reify ThreadFactory
      (newThread [_ r]
        (let [t (.newThread default-thread-factory r)]
          (doto t
            (.setName (str prefix "-" (.getName t)))
            (.setUncaughtExceptionHandler uncaught-exception-handler)))))))

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
    (throw (err/illegal-arg ::invalid-arrow-ipc-file
                            {::err/message "invalid Arrow IPC file format"}))))

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

#_{:clj-kondo/ignore [:unused-private-var]} ; side-effect
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
    (catch ClassNotFoundException _
      (fn free-direct-buffer-nop [_]))))

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

(defn ->chunks
  (^xtdb.ICursor [^ArrowBuf ipc-file-format-buffer]
   (->chunks ipc-file-format-buffer {}))
  (^xtdb.ICursor [^ArrowBuf ipc-file-format-buffer {:keys [^RoaringBitmap block-idxs close-buffer?]}]
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

(defn combine-col-cursors ^ICursor #_<VSR> [^Map #_#_<String, ICursor<VSR>> col-cursors]
  (reify ICursor
    (tryAdvance [_ c]
      (let [out-vecs (ArrayList. (count col-cursors))]
        ;; normally rels aren't supposed to escape the scope of the tryAdvance call
        ;; but we effectively still own it, so we assume its state doesn't change
        ;; immediately after the call
        (if (every? (fn [[^String col-name ^ICursor col-cursor]]
                      (.tryAdvance col-cursor
                                   (reify Consumer
                                     (accept [_ root]
                                       (.add out-vecs (.getVector ^VectorSchemaRoot root col-name))))))
                    col-cursors)
          (do
            (.accept c (VectorSchemaRoot. out-vecs))
            true)
          false)))

    (close [_] (run! try-close (vals col-cursors)))))

(deftype ConcatCursor [^Iterator #_<ICursor<E>> cursors
                       ^:volatile-mutable ^ICursor #_<E> current-cursor]
  ICursor #_<E>
  (tryAdvance [this c]
    (loop []
      (cond
        current-cursor
        (or (.tryAdvance current-cursor c)
            (do
              (.close current-cursor)
              (set! (.current-cursor this) nil)
              (recur)))

        (.hasNext cursors)
        (do
          (set! (.current-cursor this) (.next cursors))
          (recur))

        :else false)))

  (close [_]
    (try-close current-cursor)
    (run! try-close (iterator-seq cursors))))

(defn ->concat-cursor [& cursors]
  (ConcatCursor. (.iterator ^Iterable cursors) nil))

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

(defmacro case-enum
  "Like `case`, but explicitly dispatch on Java enum ordinals.

  See: https://stackoverflow.com/questions/16777814/is-it-possible-to-use-clojures-case-form-with-a-java-enum"
  {:style/indent 1}
  [e & clauses]
  (letfn [(enum-ordinal [e] `(let [^Enum e# ~e] (.ordinal e#)))]
    `(case ~(enum-ordinal e)
       ~@(concat
           (mapcat (fn [[test result]]
                     [(eval (enum-ordinal test)) result])
                   (partition 2 clauses))
           (when (odd? (count clauses))
             (list (last clauses)))))))

(def ^:const default-lru-memoize-size (* 4 1024))

(def ^:private ^Map known-memo-tables (WeakHashMap.))

(defn lru-memoize
  ([f]
   (lru-memoize default-lru-memoize-size f))
  ([^long cache-size f]
   (let [cache (Collections/synchronizedMap (proxy [LinkedHashMap] [cache-size 0.75 true]
                                              (removeEldestEntry [_]
                                                (> (.size ^Map this) cache-size))))]
     (.put known-memo-tables f cache)
     (fn
       ([]
        (let [k f
              v (.getOrDefault cache k ::not-found)]
          (if (identical? ::not-found v)
            (doto (f)
              (->> (.put cache k)))
            v)))
       ([x]
        (let [k x
              v (.getOrDefault cache k ::not-found)]
          (if (identical? ::not-found v)
            (doto (f x)
              (->> (.put cache k)))
            v)))
       ([x y]
        (let [k (MapEntry/create x y)
              v (.getOrDefault cache k ::not-found)]
          (if (identical? ::not-found v)
            (doto (f x y)
              (->> (.put cache k)))
            v)))
       ([x y & args]
        (let [k (cons x (cons y args))
              v (.getOrDefault cache k ::not-found)]
          (if (identical? ::not-found v)
            (doto (apply f k)
              (->> (.put cache k)))
            v)))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn clear-known-lru-memo-tables! []
  (doseq [[_ ^Map table] known-memo-tables]
    (.clear table)))

(defn ->kebab-case-kw [s]
  (-> s name str/lower-case (str/replace "_" "-") keyword))

(defn- ->normal-form [s]
  (-> s str/lower-case (str/replace "." "$") (str/replace "-" "_")))

(defn- <-normal-form [s]
  (-> s (str/replace "$" ".") (str/replace "_" "-")))

(defn kw->normal-form-kw [kw]
  (if-let [ns (namespace kw)]
    (keyword (str (->normal-form ns) "$" (->normal-form (name kw))))
    (keyword (->normal-form (name kw)))))

(defn normal-form-kw->datalog-form-kw [kw]
  (let [kw (name kw)]
    (if-let [^int i (str/last-index-of kw "$")]
      (let [ns (subs kw 0 i)
            kw (subs kw (inc i))]
        (keyword (<-normal-form ns) (<-normal-form kw)))
      (keyword (<-normal-form kw)))))

(defn symbol->normal-form-symbol [s]
  (if-let [ns (namespace s)]
    (symbol (str (->normal-form ns) "$" (->normal-form (name s))))
    (symbol (->normal-form (name s)))))

(defn str->normal-form-str [s]
  (if-let [^int i (str/last-index-of s "/")]
    (str (->normal-form (subs s 0 i)) "$" (->normal-form (subs s (inc i))))
    (->normal-form s)))

(defn normal-form-str->datalog-form-str [s]
  (if-let [^int i (str/last-index-of s "$")]
    (let [ns (subs s 0 i)
          s (subs s (inc i))]
      (str (<-normal-form ns)  "/" (<-normal-form s)))
    (<-normal-form s)))

(ns core2.log
  (:require [clojure.tools.logging :as log]
            [core2.util :as util])
  (:import clojure.lang.MapEntry
           [java.io Closeable]
           [java.nio.channels ClosedByInterruptException FileChannel]
           java.nio.ByteBuffer
           [java.nio.file Files Path StandardOpenOption]
           [java.time Clock]
           [java.util ArrayList Date List]
           [java.util.concurrent ArrayBlockingQueue BlockingQueue CompletableFuture Executors ExecutorService Future TimeUnit]))

(set! *unchecked-math* :warn-on-boxed)

(definterface LogWriter
  (^java.util.concurrent.CompletableFuture appendRecord [^java.nio.ByteBuffer record]))

(definterface LogReader
  (^java.util.List readRecords [^Long after-offset ^int limit]))

(defrecord LogRecord [^long offset ^Date time ^ByteBuffer record])

(def ^:private ^{:tag 'long} header-size (+ Integer/BYTES Integer/BYTES Long/BYTES))

(deftype LocalDirectoryLogReader [^Path root-path ^:volatile-mutable ^FileChannel log-channel]
  LogReader
  (readRecords [this after-offset limit]
    (if (nil? log-channel)
      (let [log-path (.resolve root-path "LOG")]
        (if (util/path-exists log-path)
          (do (set! log-channel (util/->file-channel log-path #{StandardOpenOption/READ}))
              (recur after-offset limit))
          []))
      (let [header (ByteBuffer/allocate header-size)]
        (.position log-channel (long (or after-offset 0)))
        (loop [limit (int (if after-offset
                            (inc limit)
                            limit))
               acc []
               offset (.position log-channel)]
          (if (or (zero? limit) (= offset (.size log-channel)))
            (if after-offset
              (subvec acc 1)
              acc)
            (if-let [record (do (.clear header)
                                (while (pos? (.read log-channel header)))
                                (when-not (.hasRemaining header)
                                  (.flip header)
                                  (let [check (.getInt header)
                                        size (.getInt header)
                                        _ (when-not (= check (bit-xor (unchecked-int offset) size))
                                            (throw (IllegalStateException. "invalid record")))
                                        time-ms (.getLong header)
                                        record (ByteBuffer/allocate size)]
                                    (while (pos? (.read log-channel record)))
                                    (when-not (.hasRemaining record)
                                      (->LogRecord offset (Date. time-ms) (.flip record))))))]
              (recur (dec limit)
                     (conj acc record)
                     (+ offset header-size (.capacity ^ByteBuffer (.record ^LogRecord record))))
              (if after-offset
                (subvec acc 1)
                acc)))))))

  Closeable
  (close [_]
    (when log-channel
      (.close log-channel))))

(deftype LocalDirectoryLogWriter [^Path root-path ^ExecutorService pool ^BlockingQueue queue ^Future append-loop-future]
  LogWriter
  (appendRecord [this record]
    (if (.isShutdown pool)
      (throw (IllegalStateException. "writer is closed"))
      (let [f (CompletableFuture.)]
        (.put queue (MapEntry/create f record))
        f)))

  Closeable
  (close [_]
    (try
      (future-cancel append-loop-future)
      (util/shutdown-pool pool)
      (finally
        (loop []
          (when-let [[^CompletableFuture f] (.poll queue)]
            (when-not (.isDone f)
              (.cancel f true))
            (recur)))))))

(defn- writer-append-loop [^Path root-path ^BlockingQueue queue ^Clock clock]
  (with-open [log-channel (util/->file-channel (.resolve root-path "LOG")
                                               #{StandardOpenOption/CREATE
                                                 StandardOpenOption/WRITE})]
    (let [header (ByteBuffer/allocate header-size)
          ^"[Ljava.nio.ByteBuffer;" buffers (into-array ByteBuffer [header nil])
          elements (ArrayList.)]
      (.position log-channel (.size log-channel))
      (while (not (Thread/interrupted))
        (try
          (when-let [element (.take queue)]
            (.add elements element)
            (.drainTo queue elements)
            (let [previous-offset (.position log-channel)]
              (try
                (loop [n (int 0)
                       offset previous-offset]
                  (when-not (= n (.size elements))
                    (let [[f ^ByteBuffer record] (.get elements n)
                          time-ms (.millis clock)
                          written-record (.duplicate record)
                          size (.remaining written-record)
                          check (bit-xor (unchecked-int offset) size)]
                      (-> (.clear header)
                          (.putInt check)
                          (.putInt size)
                          (.putLong time-ms)
                          (.flip))
                      (aset buffers 1 written-record)
                      (while (pos? (.write log-channel buffers)))
                      (.set elements n (MapEntry/create f (->LogRecord offset (Date. time-ms) record)))
                      (recur (inc n) (+ offset header-size size)))))
                (catch Throwable t
                  (.truncate log-channel previous-offset)
                  (throw t)))
              (.force log-channel true)
              (doseq [[^CompletableFuture f log-record] elements]
                (.complete f log-record))))
          (catch ClosedByInterruptException e
            (log/warn e "channel interrupted while closing")
            (doseq [[^CompletableFuture f] elements
                    :when (not (.isDone f))]
              (.cancel f true)))
          (catch InterruptedException e
            (doseq [[^CompletableFuture f] elements
                    :when (not (.isDone f))]
              (.cancel f true))
            (.interrupt (Thread/currentThread)))
          (catch Throwable t
            (log/error t "failed appending to log")
            (doseq [[^CompletableFuture f] elements
                    :when (not (.isDone f))]
              (.completeExceptionally f t)))
          (finally
            (.clear elements)))))))

(defn ->local-directory-log-reader ^core2.log.LocalDirectoryLogReader [^Path root-path]
  (->LocalDirectoryLogReader root-path nil))

(defn ->local-directory-log-writer ^core2.log.LocalDirectoryLogWriter
  [^Path root-path {:keys [buffer-size clock]
                    :or {buffer-size 4096, clock (Clock/systemUTC)}}]
  (util/mkdirs root-path)
  (let [pool (Executors/newSingleThreadExecutor (util/->prefix-thread-factory "local-directory-log-writer-"))
        queue (ArrayBlockingQueue. buffer-size)
        append-loop-future (.submit pool ^Runnable #(writer-append-loop root-path queue clock))]
    (->LocalDirectoryLogWriter root-path pool queue append-loop-future)))

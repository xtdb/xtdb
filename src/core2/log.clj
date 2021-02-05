(ns core2.log
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [core2.util :as util])
  (:import clojure.lang.MapEntry
           [java.io Closeable EOFException File RandomAccessFile]
           java.nio.channels.FileChannel
           java.nio.ByteBuffer
           [java.nio.file OpenOption StandardOpenOption]
           [java.time Clock]
           [java.util ArrayList Date List]
           [java.util.concurrent ArrayBlockingQueue BlockingQueue CompletableFuture Executors ExecutorService TimeUnit]))

(set! *unchecked-math* :warn-on-boxed)

(definterface LogWriter
  (^java.util.concurrent.CompletableFuture appendRecord [^java.nio.ByteBuffer record]))

(definterface LogReader
  (^java.util.List readRecords [^Long after-offset ^int limit]))

(defrecord LogRecord [^long offset ^Date time ^ByteBuffer record])

(deftype LocalDirectoryLogReader [^File dir ^:volatile-mutable ^FileChannel log-channel]
  LogReader
  (readRecords [this after-offset limit]
    (if (nil? log-channel)
      (let [log-file (io/file dir "LOG")]
        (if (.exists log-file)
          (do (set! log-channel (FileChannel/open (.toPath log-file)
                                                  (into-array OpenOption #{StandardOpenOption/READ})))
              (recur after-offset limit))
          []))
      (let [header (ByteBuffer/allocate (+ Integer/BYTES Integer/BYTES Long/BYTES))]
        (.position log-channel (long (or after-offset 0)))
        (loop [limit (int (if after-offset
                            (inc limit)
                            limit))
               acc []]
          (let [offset (.position log-channel)]
            (if (or (zero? limit) (= offset (.size log-channel)))
              (if after-offset
                (subvec acc 1)
                acc)
              (if-let [record (try
                                (.clear header)
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
                                      (->LogRecord offset (Date. time-ms) (.flip record)))))
                                (catch EOFException _))]
                (recur (dec limit) (conj acc record))
                (if after-offset
                  (subvec acc 1)
                  acc))))))))

  Closeable
  (close [_]
    (when log-channel
      (.close log-channel))))

(deftype LocalDirectoryLogWriter [^File dir ^ExecutorService pool ^BlockingQueue queue]
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
      (doto pool
        (.shutdownNow)
        (.awaitTermination 5 TimeUnit/SECONDS))
      (finally
        (loop []
          (when-let [[^CompletableFuture f] (.poll queue)]
            (when-not (.isDone f)
              (.cancel f true))
            (recur)))))))

(defn- writer-append-loop [^File dir ^BlockingQueue queue ^Clock clock]
  (with-open [log-channel (FileChannel/open (.toPath (io/file dir "LOG"))
                                            (into-array OpenOption #{StandardOpenOption/CREATE
                                                                     StandardOpenOption/WRITE}))]
    (.position log-channel (.size log-channel))
    (let [header (ByteBuffer/allocate (+ Integer/BYTES Integer/BYTES Long/BYTES))
          ^"[Ljava.nio.ByteBuffer;" buffers (into-array ByteBuffer [header nil])]
      (while (not (Thread/interrupted))
        (when-let [element (.take queue)]
          (let [elements (doto (ArrayList.)
                           (.add element))]
            (try
              (.drainTo queue elements)
              (let [previous-offset (.position log-channel)]
                (try
                  (dotimes [n (.size elements)]
                    (let [[f ^ByteBuffer record] (.get elements n)
                          offset (.position log-channel)
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
                      (.set elements n (MapEntry/create f (->LogRecord offset (Date. time-ms) record)))))
                  (catch Throwable t
                    (log/error t "failed appending record to log")
                    (.truncate log-channel previous-offset)
                    (throw t)))
                (.force log-channel true)
                (doseq [[^CompletableFuture f log-record] elements]
                  (.complete f log-record)))
              (catch InterruptedException e
                (doseq [[^CompletableFuture f] elements]
                  (when-not (.isDone f)
                    (.cancel f true)))
                (.interrupt (Thread/currentThread)))
              (catch Throwable t
                (doseq [[^CompletableFuture f] elements]
                  (when-not (.isDone f)
                    (.completeExceptionally f t)))))))))))

(defn ->local-directory-log-reader ^core2.log.LocalDirectoryLogReader [^File dir]
  (->LocalDirectoryLogReader dir nil))

(defn ->local-directory-log-writer ^core2.log.LocalDirectoryLogWriter
  [^File dir {:keys [buffer-size clock]
              :or {buffer-size 4096, clock (Clock/systemUTC)}}]
  (.mkdirs dir)
  (let [pool (Executors/newSingleThreadExecutor (util/->prefix-thread-factory "local-directory-log-writer-"))
        queue (ArrayBlockingQueue. buffer-size)]
    (.submit pool ^Runnable #(writer-append-loop dir queue clock))
    (->LocalDirectoryLogWriter dir pool queue)))

(ns core2.log
  (:require [clojure.java.io :as io])
  (:import clojure.lang.MapEntry
           [java.io Closeable EOFException File RandomAccessFile]
           java.nio.ByteBuffer
           [java.time Clock]
           [java.util ArrayList Date List]
           [java.util.concurrent ArrayBlockingQueue BlockingQueue CompletableFuture Executors ExecutorService TimeUnit]))

(set! *unchecked-math* :warn-on-boxed)

(definterface LogWriter
  (^java.util.concurrent.CompletableFuture appendRecord [^java.nio.ByteBuffer record]))

(definterface LogReader
  (^java.util.List readRecords [^Long after-offset ^int limit]))

(defrecord LogRecord [^long offset ^Date time ^ByteBuffer record])

(deftype LocalDirectoryLogReader [^File dir ^:volatile-mutable ^RandomAccessFile log-file]
  LogReader
  (readRecords [this after-offset limit]
    (when (nil? log-file)
      (set! log-file (RandomAccessFile. (io/file dir "LOG") "r")))
    (.seek log-file (or after-offset 0))
    (loop [limit (int (if after-offset
                        (inc limit)
                        limit))
           acc []]
      (let [offset (.getFilePointer log-file)]
        (if (or (zero? limit) (= offset (.length log-file)))
          (if after-offset
            (subvec acc 1)
            acc)
          (if-let [record (try
                            (let [check (.readInt log-file)
                                  size (.readInt log-file)
                                  _ (when-not (= check (bit-xor (unchecked-int offset) size))
                                      (throw (IllegalStateException. "invalid record")))
                                  time (Date. (.readLong log-file))
                                  record (doto (byte-array size)
                                           (->> (.readFully log-file)))]
                              (->LogRecord offset time (ByteBuffer/wrap record)))
                            (catch EOFException _))]
            (recur (dec limit) (conj acc record))
            (if after-offset
              (subvec acc 1)
              acc))))))

  Closeable
  (close [_]
    (when log-file
      (.close log-file))))

(deftype LocalDirectoryLogWriter [^File dir ^RandomAccessFile log-file ^ExecutorService pool ^BlockingQueue queue]
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
        (.close log-file)
        (loop []
          (when-let [[^CompletableFuture f] (.poll queue)]
            (when-not (.isDone f)
              (.cancel f true))
            (recur)))))))

(defn- writer-append-loop [^RandomAccessFile log-file ^BlockingQueue queue ^Clock clock]
  (with-open [log-channel (.getChannel log-file)]
    (while (not (Thread/interrupted))
      (when-let [element (.take queue)]
        (let [elements (doto (ArrayList.)
                         (.add element))]
          (try
            (.drainTo queue elements)
            (let [previous-offset (.getFilePointer log-file)]
              (try
                (dotimes [n (.size elements)]
                  (let [[f ^ByteBuffer record] (.get elements n)
                        offset (.getFilePointer log-file)
                        time (Date. (.millis clock))
                        written-record (.duplicate record)
                        size (.remaining written-record)
                        check (bit-xor (unchecked-int offset) size)]
                    (.writeInt log-file check)
                    (.writeInt log-file size)
                    (.writeLong log-file (.getTime time))
                    (while (pos? (.write log-channel written-record)))
                    (.set elements n (MapEntry/create f (->LogRecord offset time record)))))
                (catch Throwable t
                  (.setLength log-file previous-offset)
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
                  (.completeExceptionally f t))))))))))

(defn ->local-directory-log-reader ^core2.log.LocalDirectoryLogReader [^File dir]
  (->LocalDirectoryLogReader dir nil))

(defn ->local-directory-log-writer ^core2.log.LocalDirectoryLogWriter
  [^File dir {:keys [buffer-size clock]
              :or {buffer-size 1024, clock (Clock/systemUTC)}}]
  (.mkdirs dir)
  (let [pool (Executors/newSingleThreadExecutor)
        queue (ArrayBlockingQueue. buffer-size)
        log-file (RandomAccessFile. (io/file dir "LOG") "rw")]
    (.seek log-file (.length log-file))
    (.submit pool ^Runnable #(writer-append-loop log-file queue clock))
    (->LocalDirectoryLogWriter dir log-file pool queue)))

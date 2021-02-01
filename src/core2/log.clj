(ns core2.log
  (:require [clojure.java.io :as io])
  (:import [java.io Closeable File RandomAccessFile]
           [java.util ArrayList Date List]
           [java.util.concurrent ArrayBlockingQueue BlockingQueue Executors ExecutorService CompletableFuture TimeUnit]))

(definterface LogWriter
  (^java.util.concurrent.CompletableFuture appendRecord [^bytes record]))

(definterface LogReader
  (^java.util.List readRecords [^long from-offset ^int limit]))

(defrecord LogRecord [^long offset ^Date time ^bytes record])

(deftype LocalDirectoryLog [^File dir ^RandomAccessFile read-raf ^ExecutorService pool ^BlockingQueue queue]
  LogWriter
  (appendRecord [this record]
    (let [f (CompletableFuture.)]
      (.put queue [f record])
      f))

  LogReader
  (readRecords [this from-offset limit]
    (.seek read-raf from-offset)
    (loop [limit limit
           acc []]
      (let [offset (.getFilePointer read-raf)]
        (if (or (zero? limit) (= offset (.length read-raf)))
          acc
          (recur (dec limit)
                 (conj acc (let [size (.readInt read-raf)
                                 time (Date. (.readLong read-raf))
                                 record (doto (byte-array size)
                                          (->> (.readFully read-raf)))]
                             (->LogRecord offset time record))))))))

  Closeable
  (close [_]
    (try
      (doto pool
        (.shutdownNow)
        (.awaitTermination 5 TimeUnit/SECONDS))
      (finally
        (.close read-raf)))))

(defn- append-loop [^RandomAccessFile log-file ^BlockingQueue queue]
  (while true
    (when-let [element (.take queue)]
      (let [elements (doto (ArrayList.)
                       (.add element))]
        (try
          (.drainTo queue elements)
          (let [jobs (reduce
                      (fn [acc [f ^bytes record]]
                        (let [offset (.getFilePointer log-file)
                              time (Date.)]
                          (.writeInt log-file (alength record))
                          (.writeLong log-file (.getTime time))
                          (.write log-file record)
                          (conj acc [f (->LogRecord offset time record)])))
                      []
                      elements)]
            (.sync (.getFD log-file))
            (doseq [[^CompletableFuture f log-record] jobs]
              (.complete f log-record)))
          (catch Throwable t
            (doseq [[^CompletableFuture f] elements]
              (when-not (.isDone f)
                (.completeExceptionally f t)))))))))

(defn ->local-directory-log ^core2.log.LocalDirectoryLog [dir]
  (let [pool (Executors/newSingleThreadExecutor)
        queue (ArrayBlockingQueue. 1024)
        log-file (doto (io/file dir "LOG")
                   (io/make-parents))
        log-raf (RandomAccessFile. log-file "rw")]
    (.seek log-raf (.length log-file))
    (.submit pool ^Runnable #(append-loop log-raf queue))
    (->LocalDirectoryLog (io/file dir) (RandomAccessFile. log-file "r") pool queue)))

(ns xtdb.log.local-directory-log
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [xtdb.log :as xt.log]
            [xtdb.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.api :as xt])
  (:import clojure.lang.MapEntry
           xtdb.InstantSource
           [xtdb.log Log LogRecord]
           [java.io BufferedInputStream BufferedOutputStream Closeable DataInputStream DataOutputStream EOFException]
           java.nio.ByteBuffer
           [java.nio.channels Channels ClosedByInterruptException FileChannel]
           [java.nio.file Path StandardOpenOption]
           [java.time Duration]
           java.time.temporal.ChronoUnit
           java.util.ArrayList
           [java.util.concurrent ArrayBlockingQueue BlockingQueue CompletableFuture Executors ExecutorService Future]))

(def ^:private ^{:tag 'byte} record-separator 0x1E)
(def ^:private ^{:tag 'long} header-size (+ Byte/BYTES Integer/BYTES Long/BYTES))
(def ^:private ^{:tag 'long} footer-size Long/BYTES)

(deftype LocalDirectoryLog [^Path root-path, ^Duration poll-sleep-duration
                            ^ExecutorService pool, ^BlockingQueue queue, ^Future append-loop-future
                            ^:volatile-mutable ^FileChannel log-channel]
  Log
  (readRecords [_ after-offset limit]
    (when-not log-channel
      (let [log-path (.resolve root-path "LOG")]
        (when (util/path-exists log-path)
          (set! log-channel (util/->file-channel log-path)))))

    (when log-channel
      (let [log-in (DataInputStream. (BufferedInputStream. (Channels/newInputStream log-channel)))]
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
            (if-let [record (try
                              (when-not (= record-separator (.read log-in))
                                (throw (IllegalStateException. "invalid record")))
                              (let [size (.readInt log-in)
                                    system-time (util/micros->instant (.readLong log-in))
                                    record (byte-array size)
                                    read-bytes (.read log-in record)
                                    offset-check (.readLong log-in)]
                                (when (and (= size read-bytes)
                                           (= offset-check offset))
                                  (xt.log/->LogRecord (xt/->TransactionInstant offset system-time) (ByteBuffer/wrap record))))
                              (catch EOFException _))]
              (recur (dec limit)
                     (conj acc record)
                     (+ offset header-size (.capacity ^ByteBuffer (.record ^LogRecord record)) footer-size))
              (if after-offset
                (subvec acc 1)
                acc)))))))

  (appendRecord [_ record]
    (if (.isShutdown pool)
      (throw (IllegalStateException. "writer is closed"))
      (let [f (CompletableFuture.)]
        (.put queue (MapEntry/create f record))
        f)))

  (subscribe [this after-tx-id subscriber]
    (xt.log/handle-polling-subscription this after-tx-id {:poll-sleep-duration poll-sleep-duration} subscriber))

  Closeable
  (close [_]
    (when log-channel
      (.close log-channel))

    (try
      (future-cancel append-loop-future)
      (util/shutdown-pool pool)
      (finally
        (loop []
          (when-let [[^CompletableFuture f] (.poll queue)]
            (when-not (.isDone f)
              (.cancel f true))
            (recur)))))))

(defn- writer-append-loop [^Path root-path, ^BlockingQueue queue, ^InstantSource instant-src, ^long buffer-size]
  (with-open [log-channel (util/->file-channel (.resolve root-path "LOG")
                                               #{StandardOpenOption/CREATE
                                                 StandardOpenOption/WRITE})]
    (let [elements (ArrayList. buffer-size)]
      (.position log-channel (.size log-channel))
      (while (not (Thread/interrupted))
        (try
          (when-let [element (.take queue)]
            (.add elements element)
            (.drainTo queue elements (.size queue))
            (let [previous-offset (.position log-channel)
                  log-out (DataOutputStream. (BufferedOutputStream. (Channels/newOutputStream log-channel)))]
              (try
                (loop [n (int 0)
                       offset previous-offset]
                  (when-not (= n (.size elements))
                    (let [[f ^ByteBuffer record] (.get elements n)
                          system-time (-> (.instant instant-src) (.truncatedTo ChronoUnit/MICROS))
                          size (.remaining record)
                          written-record (.duplicate record)]
                      (.write log-out ^byte record-separator)
                      (.writeInt log-out size)
                      (.writeLong log-out (util/instant->micros system-time))
                      (while (>= (.remaining written-record) Long/BYTES)
                        (.writeLong log-out (.getLong written-record)))
                      (while (.hasRemaining written-record)
                        (.write log-out (.get written-record)))
                      (.writeLong log-out offset)
                      (.set elements n (MapEntry/create f (xt.log/->LogRecord (xt/->TransactionInstant offset system-time) record)))
                      (recur (inc n) (+ offset header-size size footer-size)))))
                (catch Throwable t
                  (.truncate log-channel previous-offset)
                  (throw t)))
              (.flush log-out)
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

(derive :xtdb.log/local-directory-log :xtdb/log)

(defmethod ig/prep-key :xtdb.log/local-directory-log [_ opts]
  (-> (merge {:buffer-size 4096
              :instant-src InstantSource/SYSTEM
              :poll-sleep-duration "PT0.1S"}
             opts)
      (util/maybe-update :root-path util/->path)
      (util/maybe-update :poll-sleep-duration util/->duration)))

(defmethod ig/init-key :xtdb.log/local-directory-log [_ {:keys [root-path poll-sleep-duration buffer-size instant-src]}]
  (util/mkdirs root-path)

  (let [pool (Executors/newSingleThreadExecutor (util/->prefix-thread-factory "local-directory-log-writer-"))
        queue (ArrayBlockingQueue. buffer-size)
        append-loop-future (.submit pool ^Runnable #(writer-append-loop root-path queue instant-src buffer-size))]
    (->LocalDirectoryLog root-path poll-sleep-duration pool queue append-loop-future nil)))

(defmethod ig/halt-key! :xtdb.log/local-directory-log [_ log]
  (util/try-close log))

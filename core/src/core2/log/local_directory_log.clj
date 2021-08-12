(ns core2.log.local-directory-log
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [core2.api :as c2]
            [core2.log :as c2.log]
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import clojure.lang.MapEntry
           [core2.log INotifyingSubscriberHandler Log LogRecord]
           [java.io BufferedInputStream BufferedOutputStream Closeable DataInputStream DataOutputStream EOFException]
           java.nio.ByteBuffer
           [java.nio.channels Channels ClosedByInterruptException FileChannel]
           [java.nio.file Path StandardOpenOption]
           java.time.Clock
           [java.util ArrayList Date]
           [java.util.concurrent ArrayBlockingQueue BlockingQueue CompletableFuture Executors ExecutorService Future]
           java.util.function.Consumer))

(def ^:private ^{:tag 'byte} record-separator 0x1E)
(def ^:private ^{:tag 'long} header-size (+ Byte/BYTES Integer/BYTES Long/BYTES))
(def ^:private ^{:tag 'long} footer-size Long/BYTES)

(defn- ->log-ch ^java.nio.channels.FileChannel [^Path root-path]
  (let [log-path (.resolve root-path "LOG")]
    (when (util/path-exists log-path)
      (util/->file-channel log-path))))

(defn- open-in ^java.io.DataInputStream [^java.nio.channels.FileChannel ch]
  (DataInputStream. (BufferedInputStream. (Channels/newInputStream ch))))

(defn- latest-submitted-tx-id [^FileChannel log-ch]
  (let [size (.size log-ch)]
    (when (pos? size)
      (.position log-ch (- (.size log-ch) Long/BYTES))
      (.readLong (open-in log-ch)))))

(deftype LocalDirectoryLog [^Path root-path, ^ExecutorService pool, ^BlockingQueue queue, ^Future append-loop-future,
                            ^INotifyingSubscriberHandler subscriber-handler
                            ^:volatile-mutable ^FileChannel log-channel]
  Log
  (readRecords [_ after-offset limit]
    (when-not log-channel
      (set! log-channel (->log-ch root-path)))

    (when log-channel
      (let [log-in (open-in log-channel)]
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
                                    time-ms (.readLong log-in)
                                    record (byte-array size)
                                    read-bytes (.read log-in record)
                                    offset-check (.readLong log-in)]
                                (when (and (= size read-bytes)
                                           (= offset-check offset))
                                  (c2.log/->LogRecord (c2/->TransactionInstant offset (Date. time-ms)) (ByteBuffer/wrap record))))
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
        (.thenAccept f (reify Consumer
                         (accept [_ record]
                           (.notifyTx subscriber-handler (.tx ^LogRecord record)))))
        f)))

  (subscribe [this after-tx-id subscriber]
    (.subscribe subscriber-handler this after-tx-id subscriber))

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

(defn- writer-append-loop [^Path root-path ^BlockingQueue queue ^Clock clock ^long buffer-size]
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
                          time-ms (.millis clock)
                          size (.remaining record)
                          written-record (.duplicate record)]
                      (.write log-out ^byte record-separator)
                      (.writeInt log-out size)
                      (.writeLong log-out time-ms)
                      (while (>= (.remaining written-record) Long/BYTES)
                        (.writeLong log-out (.getLong written-record)))
                      (while (.hasRemaining written-record)
                        (.write log-out (.get written-record)))
                      (.writeLong log-out offset)
                      (.set elements n (MapEntry/create f (c2.log/->LogRecord (c2/->TransactionInstant offset (Date. time-ms)) record)))
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

(s/def ::root-path ::util/path)

(derive :core2.log/local-directory-log :core2/log)

(defmethod ig/prep-key :core2.log/local-directory-log [_ opts]
  (-> (merge {:buffer-size 4096
              :clock (Clock/systemUTC)}
             opts)
      (util/maybe-update :root-path util/->path)))

(defmethod ig/init-key :core2.log/local-directory-log [_ {:keys [root-path buffer-size clock]}]
  (util/mkdirs root-path)

  (let [pool (Executors/newSingleThreadExecutor (util/->prefix-thread-factory "local-directory-log-writer-"))
        queue (ArrayBlockingQueue. buffer-size)
        append-loop-future (.submit pool ^Runnable #(writer-append-loop root-path queue clock buffer-size))
        log-ch (->log-ch root-path)]

    (try
      (->LocalDirectoryLog root-path pool queue append-loop-future
                           (c2.log/->notifying-subscriber-handler (some-> log-ch latest-submitted-tx-id))
                           log-ch)
      (catch Throwable e
        (.close log-ch)
        (throw e)))))

(defmethod ig/halt-key! :core2.log/local-directory-log [_ log]
  (util/try-close log))

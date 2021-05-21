(ns core2.tx
  (:require [core2.util :as util])
  (:import [java.util Date Map SortedMap]
           [java.util.concurrent.atomic AtomicInteger]
           [java.io Closeable]))

(defrecord TransactionInstant [^long tx-id, ^Date tx-time])

(defrecord Watermark [^long chunk-idx ^long row-count ^SortedMap column->root ^TransactionInstant tx-instant
                      ^Object temporal-watermark ^AtomicInteger ref-count ^int max-rows-per-block ^Map thread->count]
  Closeable
  (close [_]
    (let [thread (Thread/currentThread)]
      (when-let [^AtomicInteger thread-ref-count (.get thread->count thread)]
        (when (zero? (util/dec-ref-count thread-ref-count))
          (.remove thread->count thread)))
      (when (zero? (util/dec-ref-count ref-count))
        (util/try-close temporal-watermark)
        (doseq [root (vals column->root)]
          (util/try-close root))))))

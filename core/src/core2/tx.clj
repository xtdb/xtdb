(ns core2.tx
  (:require [clojure.tools.logging :as log]
            core2.api
            [core2.util :as util])
  (:import core2.api.TransactionInstant
           java.io.Closeable
           [java.util Map SortedMap]
           java.util.concurrent.atomic.AtomicInteger
           java.util.function.IntUnaryOperator))

(defrecord Watermark [^long chunk-idx ^long row-count ^SortedMap column->root ^TransactionInstant tx-instant
                      ^Object temporal-watermark ^AtomicInteger ref-count ^int max-rows-per-block ^Map thread->count]
  Closeable
  (close [_]
    (let [thread (Thread/currentThread)]
      (when-let [^AtomicInteger thread-ref-count (.get thread->count thread)]
        (when (zero? (.decrementAndGet thread-ref-count))
          (.remove thread->count thread)))

      (let [new-ref-count (.updateAndGet ^AtomicInteger ref-count
                                         (reify IntUnaryOperator
                                           (applyAsInt [_ x]
                                             (if (pos? x)
                                               (dec x)
                                               -1))))]
        (cond
          (neg? new-ref-count)
          (do (.set ref-count 0)
              (log/warn "watermark ref count has gone negative:" new-ref-count))

          (zero? new-ref-count)
          (do (util/try-close temporal-watermark)
              (doseq [root (vals column->root)]
                (util/try-close root))))))))

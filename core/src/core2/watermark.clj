(ns core2.watermark
  (:require [clojure.tools.logging :as log]
            core2.api
            [core2.blocks :as blocks]
            [core2.types :as types]
            [core2.util :as util])
  (:import clojure.lang.MapEntry
           core2.api.TransactionInstant
           java.io.Closeable
           [java.util Collections Map]
           java.util.concurrent.ConcurrentHashMap
           java.util.concurrent.atomic.AtomicInteger
           java.util.function.IntUnaryOperator
           org.apache.arrow.vector.VectorSchemaRoot))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface IWatermark
  (columnType [^String columnName])
  (liveSlices [^Iterable columnNames]))

(defrecord Watermark [^long chunk-idx ^long row-count ^Map column->root ^TransactionInstant tx-key
                      ^Object temporal-watermark ^AtomicInteger ref-count ^int max-rows-per-block ^Map thread->count]
  IWatermark
  (columnType [_ col-name]
    (when-let [^VectorSchemaRoot root (.get column->root col-name)]
      (-> (.getVector root col-name)
          (.getField)
          (types/field->col-type))))

  (liveSlices [_ col-names]
    (into {}
          (keep (fn [col-name]
                  (when-let [root (.get column->root col-name)]
                    (let [row-counts (blocks/row-id-aligned-blocks root chunk-idx max-rows-per-block)]
                      (MapEntry/create col-name (blocks/->slices root row-counts))))))
          col-names))

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

(defn ->empty-watermark ^core2.watermark.Watermark [^long chunk-idx ^TransactionInstant tx-key temporal-watermark ^long max-rows-per-block]
  (->Watermark chunk-idx 0 (Collections/emptySortedMap) tx-key temporal-watermark (AtomicInteger. 1) max-rows-per-block (ConcurrentHashMap.)))

(ns core2.watermark
  (:require [clojure.tools.logging :as log]
            core2.api
            [core2.blocks :as blocks]
            core2.temporal
            [core2.types :as types]
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import clojure.lang.MapEntry
           core2.api.TransactionInstant
           core2.temporal.ITemporalRelationSource
           java.lang.AutoCloseable
           [java.util Collections Map Set]
           java.util.function.Function
           java.util.concurrent.atomic.AtomicInteger
           java.util.concurrent.ConcurrentHashMap
           java.util.concurrent.locks.StampedLock
           java.util.function.IntUnaryOperator
           org.apache.arrow.vector.VectorSchemaRoot))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IWatermark
  (columnType [^String columnName])
  (^Iterable liveSlices [^Iterable columnNames])

  ;; this is a lot of duplication - I guess we'd extend interfaces here if we were in Java
  (^core2.vector.IIndirectRelation createTemporalRelation [^org.apache.arrow.memory.BufferAllocator allocator
                                                           ^java.util.List columns
                                                           ^longs temporalMinRange
                                                           ^longs temporalMaxRange
                                                           ^org.roaringbitmap.longlong.Roaring64Bitmap rowIdBitmap]))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IWatermarkManager
  (^core2.watermark.IWatermark getWatermark [])
  (^void setWatermark [^long chunkIdx,
                       ^core2.api.TransactionInstant txKey,
                       ^java.util.Map liveRoots,
                       ^java.util.Map txLiveRoots,
                       ^core2.temporal.ITemporalRelationSource temporalWatermark]))

(defrecord Watermark [^TransactionInstant tx-key, ^Map live-roots, ^Map tx-live-roots,
                      ^ITemporalRelationSource temporal-roots-src
                      ^long chunk-idx, ^int max-rows-per-block]
  IWatermark
  (columnType [_ col-name]
    (when-let [^VectorSchemaRoot root (.get live-roots col-name)]
      (-> (.getVector root col-name)
          (.getField)
          (types/field->col-type))))

  (liveSlices [_ col-names]
    (for [^Map roots [live-roots tx-live-roots]
          :when roots
          :let [slices (into {}
                             (keep (fn [col-name]
                                     (when-let [root (.get roots col-name)]
                                       (let [row-counts (blocks/row-id-aligned-blocks root chunk-idx max-rows-per-block)]
                                         (MapEntry/create col-name (blocks/->slices root row-counts))))))
                             col-names)]
          :when (= (count col-names) (count slices))]
      slices))

  (createTemporalRelation [_ allocator columns temporal-min-range temporal-max-range row-id-bitmap]
    (.createTemporalRelation temporal-roots-src allocator columns
                             temporal-min-range temporal-max-range
                             row-id-bitmap)))

(defrecord ReferenceCountingWatermark [^Watermark watermark, ^AtomicInteger ref-count, ^Map thread->count]
  IWatermark
  (columnType [_ col-name] (.columnType watermark col-name))
  (liveSlices [_ col-names] (.liveSlices watermark col-names))

  (createTemporalRelation [_ allocator columns temporal-min-range temporal-max-range row-id-bitmap]
    (.createTemporalRelation watermark allocator columns temporal-min-range temporal-max-range row-id-bitmap))

  AutoCloseable
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
          (do (util/try-close (.temporal-roots-src watermark))
              (doseq [root (vals (.live-roots watermark))]
                (util/try-close root))))))))

(defn- remove-closed-watermarks [^StampedLock open-watermarks-lock, ^Set open-watermarks]
  (let [stamp (.writeLock open-watermarks-lock)]
    (try
      (let [i (.iterator open-watermarks)]
        (while (.hasNext i)
          (let [^ReferenceCountingWatermark open-watermark (.next i)]
            (when (empty? (.thread->count open-watermark))
              (.remove i)))))
      (finally
        (.unlock open-watermarks-lock stamp)))))

(deftype WatermarkManager [^long max-rows-per-block
                           ^Set open-watermarks
                           ^StampedLock open-watermarks-lock
                           ^:volatile-mutable ^ReferenceCountingWatermark watermark]
  IWatermarkManager
  (getWatermark [_]
    (remove-closed-watermarks open-watermarks-lock open-watermarks)

    (loop []
      (when-let [current-watermark watermark]
        (if (pos? (util/inc-ref-count (.ref-count current-watermark)))
          (let [stamp (.writeLock open-watermarks-lock)]
            (try
              (let [^Map thread->count (.thread->count current-watermark)
                    ^AtomicInteger thread-ref-count (.computeIfAbsent thread->count
                                                                      (Thread/currentThread)
                                                                      (reify Function
                                                                        (apply [_ _k]
                                                                          (AtomicInteger. 0))))]
                (.incrementAndGet thread-ref-count)
                (.add open-watermarks current-watermark)
                current-watermark)
              (finally
                (.unlock open-watermarks-lock stamp))))
          (recur)))))

  (setWatermark [this chunk-idx tx-key live-roots tx-live-roots temporal-watermark]
    (let [old-wm watermark]
      (try
        (let [wm (-> (->Watermark tx-key
                                  (or live-roots (Collections/emptySortedMap))
                                  (or tx-live-roots (Collections/emptySortedMap))
                                  temporal-watermark
                                  chunk-idx max-rows-per-block)
                     (->ReferenceCountingWatermark (AtomicInteger. 1) (ConcurrentHashMap.)))]
          (remove-closed-watermarks open-watermarks-lock open-watermarks)
          (set! (.watermark this) wm)
          wm)
        (finally
          (util/try-close old-wm)))))

  AutoCloseable
  (close [this]
    (util/try-close watermark)

    (let [stamp (.writeLock open-watermarks-lock)]
      (try
        (doseq [^ReferenceCountingWatermark open-watermark open-watermarks]
          (let [^AtomicInteger watermark-ref-cnt (.ref-count open-watermark)]
            (doseq [[^Thread thread ^AtomicInteger thread-ref-count] (.thread->count open-watermark)
                    :let [rc (.get thread-ref-count)]
                    :when (pos? rc)]
              (log/warn "interrupting:" thread "on close, has outstanding watermarks:" rc)
              (.interrupt thread))
            (loop [rc (.get watermark-ref-cnt)]
              (when (pos? rc)
                (util/try-close open-watermark)
                (recur (.get watermark-ref-cnt))))))
        (finally
          (.unlock open-watermarks-lock stamp))))

    (.clear open-watermarks)

    (set! (.watermark this) nil)))

(defmethod ig/prep-key ::watermark-manager [_ opts]
  (merge {:row-counts (ig/ref :core2/row-counts)}
         opts))

(defmethod ig/init-key ::watermark-manager [_ {{:keys [max-rows-per-block]} :row-counts}]
  (WatermarkManager. max-rows-per-block (util/->identity-set) (StampedLock.) nil))

(defmethod ig/halt-key! ::watermark-manager [_ wm-mgr]
  (util/try-close wm-mgr))

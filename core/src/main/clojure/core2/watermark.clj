(ns core2.watermark
  (:require [clojure.tools.logging :as log]
            core2.api.impl
            core2.live-chunk
            core2.temporal
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import core2.api.TransactionInstant
           core2.live_chunk.ILiveChunkWatermark
           core2.temporal.ITemporalRelationSource
           java.lang.AutoCloseable
           [java.util Map]
           (java.util.concurrent ConcurrentHashMap Semaphore TimeUnit)
           java.util.concurrent.atomic.AtomicInteger))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IWatermark
  (^core2.api.TransactionInstant txBasis [])
  (^core2.live_chunk.ILiveChunkWatermark liveChunk [])
  (^core2.temporal.ITemporalRelationSource temporalRootsSource [])

  (^void close []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ISharedWatermark
  (^core2.api.TransactionInstant txBasis [])
  (^core2.watermark.IWatermark retain [])
  (^void release []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IWatermarkManager
  (^core2.watermark.ISharedWatermark wrapWatermark [^core2.watermark.IWatermark wm]))

(deftype Watermark [^TransactionInstant tx-key, ^ILiveChunkWatermark live-chunk
                    ^ITemporalRelationSource temporal-roots-src
                    ^boolean close-temporal-roots?]
  IWatermark
  (txBasis [_] tx-key)
  (liveChunk [_] live-chunk)
  (temporalRootsSource [_] temporal-roots-src)

  (close [_]
    (util/try-close live-chunk)

    (when close-temporal-roots?
      (util/try-close temporal-roots-src))))

(deftype WatermarkManager [^Map thread-ref-counts
                           ^:volatile-mutable ^Semaphore closing-semaphore]
  IWatermarkManager
  (wrapWatermark [wm-mgr wm]
    (let [ref-cnt (AtomicInteger. 1)]
      (reify ISharedWatermark
        (txBasis [_] (.txBasis wm))

        (retain [shared-wm]
          (log/trace "retain wm" (.txBasis wm))

          (.incrementAndGet ref-cnt)

          (locking wm-mgr
            (when closing-semaphore
              (throw (InterruptedException.)))

            (.compute thread-ref-counts (Thread/currentThread)
                      (util/->jbifn (fn [_t cnt]
                                      (inc (or cnt 0))))))

          (reify IWatermark
            (txBasis [_] (.txBasis wm))
            (liveChunk [_] (.liveChunk wm))
            (temporalRootsSource [_] (.temporalRootsSource wm))

            AutoCloseable
            (close [_]
              (log/trace "closing wm")
              (locking wm-mgr
                (.compute thread-ref-counts (Thread/currentThread)
                          (util/->jbifn (fn [_t cnt]
                                          (let [new-cnt (dec cnt)]
                                            (when (pos? new-cnt)
                                              new-cnt)))))

                (.release shared-wm)

                (when-some [^Semaphore closing-semaphore (.closing-semaphore wm-mgr)]
                  (.release closing-semaphore))))))

        (release [_]
          (when (zero? (.decrementAndGet ref-cnt))
            (.close wm))))))

  AutoCloseable
  (close [wm-mgr]
    (when-some [open-thread-count (locking wm-mgr
                                    (when-not closing-semaphore
                                      (set! (.closing-semaphore wm-mgr) (Semaphore. 0))
                                      (let [open-threads (set (keys thread-ref-counts))
                                            open-wm-count (apply + 0 (vals thread-ref-counts))]
                                        (when (pos? open-wm-count)
                                          (log/infof "%d watermarks open - interrupting %s" open-wm-count (pr-str open-threads)))
                                        (doseq [^Thread thread (set open-threads)]
                                          (.interrupt thread))
                                        open-wm-count)))]

      (when-not (.tryAcquire closing-semaphore open-thread-count 60 TimeUnit/SECONDS)
        (log/warn "Failed to shut down after 60s due to outstanding watermarks"
                  (pr-str thread-ref-counts))))))

(defmethod ig/init-key ::watermark-manager [_ _]
  (WatermarkManager. (ConcurrentHashMap.) nil))

(defmethod ig/halt-key! ::watermark-manager [_ wm-mgr]
  (util/try-close wm-mgr))

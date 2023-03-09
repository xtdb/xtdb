(ns core2.watermark
  (:require [clojure.tools.logging :as log]
            core2.api.impl
            core2.live-chunk
            core2.temporal
            [core2.util :as util])
  (:import core2.api.TransactionInstant
           core2.live_chunk.ILiveChunkWatermark
           core2.temporal.ITemporalRelationSource
           java.lang.AutoCloseable
           java.util.concurrent.atomic.AtomicInteger))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IWatermark
  (^core2.api.TransactionInstant txBasis [])
  (^core2.live_chunk.ILiveChunkWatermark liveChunk [])
  (^core2.temporal.ITemporalRelationSource temporalRootsSource [])

  (^void retain [])
  (^void close []
   "releases a reference to the Watermark.
    if this was the last reference, close it."))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IWatermarkSource
  (^core2.watermark.IWatermark openWatermark [^core2.api.TransactionInstant txKey]))

(deftype Watermark [^TransactionInstant tx-key, ^ILiveChunkWatermark live-chunk
                    ^ITemporalRelationSource temporal-roots-src
                    ^boolean close-temporal-roots?
                    ^AtomicInteger ref-cnt]
  IWatermark
  (txBasis [_] tx-key)
  (liveChunk [_] live-chunk)
  (temporalRootsSource [_] temporal-roots-src)

  (retain [this]
    (log/trace "retain wm" (hash this))
    (when (zero? (.getAndIncrement ref-cnt))
      (throw (IllegalStateException. "watermark closed"))))

  AutoCloseable
  (close [this]
    (log/trace "release wm" (hash this))

    (when (zero? (.decrementAndGet ref-cnt))
      (log/trace "close wm" (hash this))
      (util/try-close live-chunk)

      (when close-temporal-roots?
        (util/try-close temporal-roots-src)))))

(defn ->wm ^core2.watermark.IWatermark [tx-key live-chunk temporal-roots-src close-temporal-roots?]
  (Watermark. tx-key live-chunk temporal-roots-src close-temporal-roots? (AtomicInteger. 1)))

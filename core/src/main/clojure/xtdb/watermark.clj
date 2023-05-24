(ns xtdb.watermark
  (:require [clojure.tools.logging :as log]
            xtdb.api.protocols
            xtdb.live-chunk
            xtdb.temporal
            [xtdb.util :as util])
  (:import xtdb.api.protocols.TransactionInstant
           xtdb.live_chunk.ILiveChunkWatermark
           xtdb.temporal.ITemporalRelationSource
           java.lang.AutoCloseable
           java.util.concurrent.atomic.AtomicInteger))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IWatermark
  (^xtdb.api.protocols.TransactionInstant txBasis [])
  (^xtdb.live_chunk.ILiveChunkWatermark liveChunk [])
  (^xtdb.temporal.ITemporalRelationSource temporalRootsSource [])

  (^void retain [])
  (^void close []
   "releases a reference to the Watermark.
    if this was the last reference, close it."))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IWatermarkSource
  (^xtdb.watermark.IWatermark openWatermark [^xtdb.api.protocols.TransactionInstant txKey]))

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

(defn ->wm ^xtdb.watermark.IWatermark [tx-key live-chunk temporal-roots-src close-temporal-roots?]
  (Watermark. tx-key live-chunk temporal-roots-src close-temporal-roots? (AtomicInteger. 1)))

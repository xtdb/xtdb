(ns xtdb.watermark
  (:require [clojure.tools.logging :as log]
            xtdb.api.protocols
            xtdb.indexer.live-index
            [xtdb.util :as util])
  (:import java.lang.AutoCloseable
           java.util.concurrent.atomic.AtomicInteger
           xtdb.api.protocols.TransactionInstant
           xtdb.indexer.live_index.ILiveIndexWatermark))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IWatermark
  (^xtdb.api.protocols.TransactionInstant txBasis [])
  (^xtdb.indexer.live_index.ILiveIndexWatermark liveIndex [])

  (^void retain [])
  (^void close []
   "releases a reference to the Watermark.
    if this was the last reference, close it."))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IWatermarkSource
  (^xtdb.watermark.IWatermark openWatermark [^xtdb.api.protocols.TransactionInstant txKey]))

(deftype Watermark [^TransactionInstant tx-key, ^ILiveIndexWatermark live-idx-wm, ^AtomicInteger ref-cnt]
  IWatermark
  (txBasis [_] tx-key)
  (liveIndex [_] live-idx-wm)

  (retain [this]
    (log/trace "retain wm" (hash this))
    (when (zero? (.getAndIncrement ref-cnt))
      (throw (IllegalStateException. "watermark closed"))))

  AutoCloseable
  (close [this]
    (log/trace "release wm" (hash this))

    (when (zero? (.decrementAndGet ref-cnt))
      (log/trace "close wm" (hash this))
      (util/try-close live-idx-wm))))

(defn ->wm ^xtdb.watermark.IWatermark [tx-key live-idx-wm]
  (Watermark. tx-key live-idx-wm (AtomicInteger. 1)))

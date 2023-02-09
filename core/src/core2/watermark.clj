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
           [java.util HashMap Map]
           java.util.concurrent.atomic.AtomicInteger
           (java.util.concurrent Semaphore TimeUnit)
           org.apache.arrow.vector.VectorSchemaRoot))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IWatermark
  (columnType [^String tableName, ^String columnName])
  (^Iterable liveSlices [^String tableName, ^Iterable columnNames])
  (^core2.api.TransactionInstant txBasis [])
  (^long chunkIdx [])

  ;; this is a lot of duplication - I guess we'd extend interfaces here if we were in Java
  (^core2.vector.IIndirectRelation createTemporalRelation [^org.apache.arrow.memory.BufferAllocator allocator
                                                           ^java.util.List columns
                                                           ^longs temporalMinRange
                                                           ^longs temporalMaxRange
                                                           ^org.roaringbitmap.longlong.Roaring64Bitmap rowIdBitmap])

  (^void close []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ISharedWatermark
  (^core2.api.TransactionInstant txBasis [])
  (^core2.watermark.IWatermark retain [])
  (^void release []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IWatermarkManager
  (^core2.watermark.ISharedWatermark wrapWatermark [^core2.watermark.IWatermark wm]))

(deftype Watermark [^TransactionInstant tx-key, ^Map live-roots, ^Map tx-live-roots,
                    ^ITemporalRelationSource temporal-roots-src
                    ^long chunk-idx, ^int max-rows-per-block
                    ^boolean close-roots?]
  IWatermark
  (columnType [_ table-name col-name]
    (when-let [^VectorSchemaRoot root (some-> live-roots ^Map (.get table-name) (.get col-name))]
      (-> (.getVector root col-name)
          (.getField)
          (types/field->col-type))))

  (liveSlices [_ table-name col-names]
    (for [^Map roots [(.get live-roots table-name)
                      (.get tx-live-roots table-name)]
          :when roots
          :let [slices (into {}
                             (keep (fn [col-name]
                                     (when-let [root (.get roots col-name)]
                                       (let [row-counts (blocks/row-id-aligned-blocks root chunk-idx max-rows-per-block)]
                                         (MapEntry/create col-name (blocks/->slices root row-counts))))))
                             col-names)]
          :when (= (count col-names) (count slices))]
      slices))

  (txBasis [_] tx-key)
  (chunkIdx [_] chunk-idx)

  (createTemporalRelation [_ allocator columns temporal-min-range temporal-max-range row-id-bitmap]
    (.createTemporalRelation temporal-roots-src allocator columns
                             temporal-min-range temporal-max-range
                             row-id-bitmap))

  (close [_]
    (when close-roots?
      (util/try-close temporal-roots-src)
      (doseq [root (->> (vals live-roots) (mapcat vals))]
        (util/try-close root)))))

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
            (columnType [_ table-name col-name] (.columnType wm table-name col-name))
            (liveSlices [_ table-name col-names] (.liveSlices wm table-name col-names))
            (txBasis [_] (.txBasis wm))
            (chunkIdx [_] (.chunkIdx wm))

            (createTemporalRelation [_ allocator columns temporal-min-range temporal-max-range row-id-bitmap]
              (.createTemporalRelation wm allocator columns temporal-min-range temporal-max-range row-id-bitmap))

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
  (WatermarkManager. (HashMap.) nil))

(defmethod ig/halt-key! ::watermark-manager [_ wm-mgr]
  (util/try-close wm-mgr))

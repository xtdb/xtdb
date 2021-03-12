(ns core2.operator.scan
  (:require [core2.align :as align]
            [core2.indexer :as idx]
            [core2.metadata :as meta]
            [core2.temporal :as temporal]
            [core2.select :as sel]
            core2.tx
            [core2.types :as t]
            [core2.util :as util])
  (:import clojure.lang.MapEntry
           core2.buffer_pool.BufferPool
           core2.ICursor
           core2.metadata.IMetadataManager
           core2.temporal.ITemporalManager
           [core2.tx TransactionInstant Watermark]
           core2.util.IChunkCursor
           [java.util HashMap LinkedList List Map Queue]
           java.util.function.Consumer
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.VectorSchemaRoot
           org.roaringbitmap.longlong.Roaring64Bitmap))

(defn- next-roots [col-names chunks]
  (when (= (count col-names) (count chunks))
    (let [in-roots (HashMap.)]
      (when (every? true? (for [col-name col-names
                                :let [^ICursor chunk (get chunks col-name)]
                                :when chunk]
                            (.tryAdvance chunk
                                         (reify Consumer
                                           (accept [_ root]
                                             (.put in-roots col-name root))))))
        in-roots))))

(defn- ->row-id-bitmap ^org.roaringbitmap.longlong.Roaring64Bitmap [^VectorSchemaRoot root vec-pred]
  (-> (when vec-pred
        (sel/select (.getVector root 1) vec-pred))
      (align/->row-id-bitmap (.getVector root t/row-id-field))))

(defn- tx-time-end-col? [^String col-name]
  (= (.getName temporal/tx-time-end-field) col-name))

(defn- align-roots [^ITemporalManager temporal-manager ^TransactionInstant tx-instant ^List col-names ^Map col-preds ^Map in-roots ^VectorSchemaRoot out-root]
  (let [row-id-bitmaps (for [col-name col-names
                             :when (not (tx-time-end-col? col-name))]
                         (->row-id-bitmap (.get in-roots col-name) (.get col-preds col-name)))
        row-id-bitmap (reduce #(doto ^Roaring64Bitmap %1
                                 (.and %2))
                              (first row-id-bitmaps)
                              (rest row-id-bitmaps))
        row-id-bitmap (.removeTombstonesFrom temporal-manager row-id-bitmap)
        tx-time-end-root (when (some tx-time-end-col? col-names)
                           (first (.createTemporalRoots temporal-manager tx-instant [(.getName temporal/tx-time-end-field)] row-id-bitmap)))]
    (try
      (let [roots (for [col-name col-names]
                    (if (tx-time-end-col? col-name)
                      tx-time-end-root
                      (.get in-roots col-name)))
            row-id-bitmap (if-let [tx-time-end-pred (.get col-preds (.getName temporal/tx-time-end-field))]
                            (doto (->row-id-bitmap tx-time-end-root tx-time-end-pred)
                              (.and row-id-bitmap))
                            row-id-bitmap)]
        ;; We can augment this (and project-vec) to take a row-id/idx
        ;; -> repetition count map, to make this aligned with the
        ;; temporal roots with potentially duplicated row-ids.
        (align/align-vectors roots row-id-bitmap out-root))
      (finally
        (util/try-close tx-time-end-root)))
    out-root))

(deftype ScanCursor [^BufferAllocator allocator
                     ^BufferPool buffer-pool
                     ^ITemporalManager temporal-manager
                     ^Watermark watermark
                     ^Queue #_<Long> chunk-idxs
                     ^List col-names
                     ^Map col-preds
                     ^:unsynchronized-mutable ^VectorSchemaRoot out-root
                     ^:unsynchronized-mutable ^Map #_#_<String, IChunkCursor> chunks
                     ^:unsynchronized-mutable ^boolean live-chunk-done?]

  ICursor
  (tryAdvance [this c]
    (let [real-col-names (remove tx-time-end-col? col-names)]
      (letfn [(create-out-root [^Map chunks]
                (when (= (count chunks) (count real-col-names))
                  (VectorSchemaRoot/create (align/align-schemas (for [col-name col-names]
                                                                  (if (tx-time-end-col? col-name)
                                                                    temporal/tx-time-end-schema
                                                                    (.getSchema ^IChunkCursor (.get chunks col-name)))))
                                           allocator)))

              (next-block [chunks ^VectorSchemaRoot out-root]
                (loop []
                  (if-let [in-roots (next-roots real-col-names chunks)]
                    (do
                      (align-roots temporal-manager (.tx-instant watermark) col-names col-preds in-roots out-root)
                      (if (pos? (.getRowCount out-root))
                        (do
                          (.accept c out-root)
                          true)
                        (recur)))

                    (do
                      (doseq [^ICursor chunk (vals chunks)]
                        (.close chunk))
                      (set! (.chunks this) nil)

                      (util/try-close out-root)
                      (set! (.out-root this) nil)

                      false))))

              (live-chunk []
                (let [chunks (idx/->live-slices watermark real-col-names)
                      out-root (create-out-root chunks)]
                  (set! (.chunks this) chunks)
                  (set! (.out-root this) out-root)

                  (next-block chunks out-root)))

              (next-chunk []
                (loop []
                  (when-let [chunk-idx (.poll chunk-idxs)]
                    (let [chunks (->> (for [col-name real-col-names]
                                        (-> (.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx col-name))
                                            (util/then-apply
                                              (fn [buf]
                                                (MapEntry/create col-name (util/->chunks buf allocator))))))
                                      vec
                                      (into {} (map deref)))
                          out-root (create-out-root chunks)]
                      (set! (.chunks this) chunks)
                      (set! (.out-root this) out-root)

                      (or (next-block chunks out-root)
                          (recur))))))]

        (or (when chunks
              (next-block chunks out-root))

            (next-chunk)

            (when-not live-chunk-done?
              (set! (.live-chunk-done? this) true)
              (live-chunk))

            false))))

  (close [_]
    (doseq [^ICursor chunk (vals chunks)]
      (.close chunk))
    (when out-root
      (.close out-root))))

(defn ->scan-cursor [^BufferAllocator allocator
                     ^IMetadataManager metadata-mgr
                     ^ITemporalManager temporal-manager
                     ^BufferPool buffer-pool
                     ^Watermark watermark
                     ^List col-names
                     metadata-pred ;; TODO derive this from col-preds
                     ^Map col-preds]
  (let [chunk-idxs (LinkedList. (meta/matching-chunks metadata-mgr watermark metadata-pred))]
    (ScanCursor. allocator buffer-pool temporal-manager watermark
                 chunk-idxs col-names col-preds
                 nil nil false)))

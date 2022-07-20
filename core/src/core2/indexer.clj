(ns core2.indexer
  (:require [clojure.tools.logging :as log]
            [core2.api :as c2]
            [core2.blocks :as blocks]
            [core2.bloom :as bloom]
            [core2.buffer-pool :as bp]
            [core2.metadata :as meta]
            core2.object-store
            [core2.temporal :as temporal]
            [core2.types :as t]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw]
            [core2.watermark :as wm]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import core2.api.TransactionInstant
           core2.buffer_pool.BufferPool
           core2.ICursor
           core2.metadata.IMetadataManager
           core2.object_store.ObjectStore
           core2.temporal.ITemporalManager
           (core2.watermark IWatermarkManager)
           core2.vector.IVectorWriter
           java.io.Closeable
           java.lang.AutoCloseable
           [java.util Collections Map Map$Entry TreeMap]
           [java.util.concurrent CompletableFuture ConcurrentHashMap ConcurrentSkipListMap]
           java.util.concurrent.atomic.AtomicInteger
           [java.util.function Consumer]
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           [org.apache.arrow.vector BigIntVector TimeStampMicroTZVector TimeStampVector VectorLoader VectorSchemaRoot VectorUnloader]
           [org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector]
           [org.apache.arrow.vector.types.pojo ArrowType$Union Field Schema]
           org.apache.arrow.vector.types.UnionMode))

(set! *unchecked-math* :warn-on-boxed)

(defmethod ig/prep-key :core2/row-counts [_ opts]
  (merge {:max-rows-per-block 1000
          :max-rows-per-chunk 100000}
         opts))

(defmethod ig/init-key :core2/row-counts [_ {:keys [max-rows-per-chunk] :as opts}]
  (let [bloom-false-positive-probability (bloom/bloom-false-positive-probability? max-rows-per-chunk)]
    (when (> bloom-false-positive-probability 0.05)
      (log/warn "Bloom should be sized for large chunks:" max-rows-per-chunk
                "false positive probability:" bloom-false-positive-probability
                "bits:" bloom/bloom-bits
                "can be set via system property core2.bloom.bits")))

  opts)

#_{:clj-kondo/ignore [:unused-binding]}
(definterface TransactionIndexer
  (^org.apache.arrow.vector.VectorSchemaRoot getLiveRoot [^String fieldName])
  (^core2.watermark.Watermark getWatermark [])
  (^core2.api.TransactionInstant indexTx [^core2.api.TransactionInstant tx
                                          ^org.apache.arrow.vector.VectorSchemaRoot txRoot])
  (^core2.api.TransactionInstant latestCompletedTx []))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface IndexerPrivate
  (^java.nio.ByteBuffer writeColumn [^org.apache.arrow.vector.VectorSchemaRoot live-root])
  (^void closeCols [])
  (^void finishChunk []))

(defn- ->live-root [field-name allocator]
  (VectorSchemaRoot/create (Schema. [t/row-id-field (t/->field field-name t/dense-union-type false)]) allocator))

(defn- snapshot-roots [^Map live-roots]
  (Collections/unmodifiableSortedMap
   (reduce
    (fn [^Map acc ^Map$Entry kv]
      (let [k (.getKey kv)
            v (util/slice-root ^VectorSchemaRoot (.getValue kv))]
        (doto acc
          (.put k v))))
    (TreeMap.)
    live-roots)))

(def ^:private log-schema
  (Schema. [(t/col-type->field "_tx-id" :i64)
            (t/col-type->field "_tx-time" [:timestamp-tz :micro "UTC"])
            (t/->field "ops" t/list-type true
                       (t/->field "ops" t/struct-type false
                                  (t/col-type->field "_row-id" :i64)
                                  (t/->field "op" (ArrowType$Union. UnionMode/Dense (int-array [0 1 2])) false
                                             (t/col-type->field "put"
                                                                '[:struct {_valid-time-start [:timestamp-tz :micro "UTC"]
                                                                           _valid-time-end [:timestamp-tz :micro "UTC"]}])
                                             (t/->field "delete" t/struct-type false
                                                        (t/->field "_id" t/dense-union-type false)
                                                        (t/col-type->field "_valid-time-start" [:timestamp-tz :micro "UTC"])
                                                        (t/col-type->field "_valid-time-end" [:timestamp-tz :micro "UTC"]))
                                             (t/->field "evict" t/struct-type false))))]))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface ILogOpIndexer
  (^void logPut [^long rowId, ^long txOpIdx])
  (^void logDelete [^long rowId, ^long txOpIdx])
  (^void logEvict [^long rowId, ^long txOpIdx])
  (^void endTx []))

(definterface ILogIndexer
  (^core2.indexer.ILogOpIndexer startTx [^core2.api.TransactionInstant txKey,
                                         ^org.apache.arrow.vector.VectorSchemaRoot txRoot])
  (^java.nio.ByteBuffer writeLog [])
  (^void clear [])
  (^void close []))

(defn- ->log-indexer [^BufferAllocator allocator, ^long max-rows-per-block]
  (let [log-root (VectorSchemaRoot/create log-schema allocator)
        ^BigIntVector tx-id-vec (.getVector log-root "_tx-id")
        ^TimeStampMicroTZVector tx-time-vec (.getVector log-root "_tx-time")

        ops-vec (.getVector log-root "ops")
        ops-writer (.asList (vw/vec->writer ops-vec))
        ops-data-writer (.asStruct (.getDataWriter ops-writer))
        row-id-writer (.writerForName ops-data-writer "_row-id")
        ^BigIntVector row-id-vec (.getVector row-id-writer)
        op-writer (.asDenseUnion (.writerForName ops-data-writer "op"))

        put-writer (.asStruct (.writerForTypeId op-writer 0))
        put-vt-start-writer (.writerForName put-writer "_valid-time-start")
        ^TimeStampMicroTZVector put-vt-start-vec (.getVector put-vt-start-writer)
        put-vt-end-writer (.writerForName put-writer "_valid-time-end")
        ^TimeStampMicroTZVector put-vt-end-vec (.getVector put-vt-end-writer)

        delete-writer (.asStruct (.writerForTypeId op-writer 1))
        delete-id-writer (.writerForName delete-writer "_id")
        delete-vt-start-writer (.writerForName delete-writer "_valid-time-start")
        ^TimeStampMicroTZVector delete-vt-start-vec (.getVector delete-vt-start-writer)
        delete-vt-end-writer (.writerForName delete-writer "_valid-time-end")
        ^TimeStampMicroTZVector delete-vt-end-vec (.getVector delete-vt-end-writer)

        evict-writer (.asStruct (.writerForTypeId op-writer 2))]

    (reify ILogIndexer
      (startTx [_ tx-key tx-root]
        (let [tx-idx (.getRowCount log-root)]
          (.startValue ops-writer)
          (.setSafe tx-id-vec tx-idx (.tx-id tx-key))
          (.setSafe tx-time-vec tx-idx (util/instant->micros (.tx-time tx-key)))

          (let [^DenseUnionVector tx-ops-vec (-> ^ListVector (.getVector tx-root "tx-ops")
                                                 (.getDataVector))
                tx-put-vec (.getStruct tx-ops-vec 0)
                tx-put-vt-start-vec (.getChild tx-put-vec "_valid-time-start")
                tx-put-vt-end-vec (.getChild tx-put-vec "_valid-time-end")

                tx-delete-vec (.getStruct tx-ops-vec 1)
                tx-delete-id-vec (.getChild tx-delete-vec "_id")
                tx-delete-vt-start-vec (.getChild tx-delete-vec "_valid-time-start")
                tx-delete-vt-end-vec (.getChild tx-delete-vec "_valid-time-end")
                delete-id-row-copier (.rowCopier delete-id-writer tx-delete-id-vec)]

            (reify ILogOpIndexer
              (logPut [_ row-id tx-op-idx]
                (let [op-idx (.startValue ops-data-writer)]
                  (.setSafe row-id-vec op-idx row-id))

                (let [src-offset (.getOffset tx-ops-vec tx-op-idx)
                      dest-offset (.startValue put-writer)]
                  (.copyFromSafe put-vt-start-vec src-offset dest-offset tx-put-vt-start-vec)
                  (.copyFromSafe put-vt-end-vec src-offset dest-offset tx-put-vt-end-vec)
                  (.endValue put-writer))

                (.endValue ops-data-writer))

              (logDelete [_ row-id tx-op-idx]
                (let [op-idx (.startValue ops-data-writer)]
                  (.setSafe row-id-vec op-idx row-id))

                (let [src-offset (.getOffset tx-ops-vec tx-op-idx)
                      dest-offset (.startValue delete-writer)]
                  (.copyFromSafe delete-vt-start-vec src-offset dest-offset tx-delete-vt-start-vec)
                  (.copyFromSafe delete-vt-end-vec src-offset dest-offset tx-delete-vt-end-vec)
                  (.copyRow delete-id-row-copier src-offset)
                  (.endValue delete-writer))

                (.endValue ops-data-writer))

              (logEvict [_ row-id _tx-op-idx]
                (let [op-idx (.startValue ops-data-writer)]
                  (.setSafe row-id-vec op-idx row-id))

                (doto evict-writer (.startValue) (.endValue)))

              (endTx [_]
                (.endValue ops-writer)
                (.setRowCount log-root (inc tx-idx)))))))

      (writeLog [_]
        (.syncSchema log-root)
        (with-open [write-root (VectorSchemaRoot/create (.getSchema log-root) allocator)]
          (let [loader (VectorLoader. write-root)
                row-counts (blocks/list-count-blocks ops-vec max-rows-per-block)]
            (with-open [^ICursor slices (blocks/->slices log-root row-counts)]
              (util/build-arrow-ipc-byte-buffer write-root :file
                (fn [write-batch!]
                  (.forEachRemaining slices
                                     (reify Consumer
                                       (accept [_ sliced-root]
                                         (with-open [arb (.getRecordBatch (VectorUnloader. sliced-root))]
                                           (.load loader arb)
                                           (write-batch!)))))))))))

      (clear [_]
        (.clear ops-writer)
        (.clear log-root))

      Closeable
      (close [_]
        (.close log-root)))))

(defn- with-latest-log-chunk [{:keys [^ObjectStore object-store ^BufferPool buffer-pool]} f]
  (when-let [latest-log-k (last (.listObjects object-store "log-"))]
    @(-> (.getBuffer buffer-pool latest-log-k)
         (util/then-apply
           (fn [^ArrowBuf log-buffer]
             (assert log-buffer)

             (when log-buffer
               (f log-buffer)))))))

(defn latest-tx [deps]
  (with-latest-log-chunk deps
    (fn [log-buf]
      (util/with-last-block log-buf
        (fn [^VectorSchemaRoot log-root]
          (let [tx-count (.getRowCount log-root)
                ^BigIntVector tx-id-vec (.getVector log-root "_tx-id")
                ^TimeStampMicroTZVector tx-time-vec (.getVector log-root "_tx-time")
                ^BigIntVector row-id-vec (-> ^ListVector (.getVector log-root "ops")
                                             ^StructVector (.getDataVector)
                                             (.getChild "_row-id"))]
            {:latest-tx (c2/->TransactionInstant (.get tx-id-vec (dec tx-count))
                                                 (util/micros->instant (.get tx-time-vec (dec tx-count))))
             :latest-row-id (.get row-id-vec (dec (.getValueCount row-id-vec)))}))))))

(definterface DocRowCopier
  (^void copyDocRow [^long rowId, ^int srcIdx]))

(defn- copy-docs [^TransactionIndexer chunk-manager, ^DenseUnionVector tx-ops-vec, ^long base-row-id]
  (let [doc-rdr (-> (.getStruct tx-ops-vec 0)
                    (.getChild "document")
                    (iv/->direct-vec)
                    (.structReader))

        doc-copiers (vec
                     (for [^String col-name (.structKeys doc-rdr)
                           :let [col-rdr (.readerForKey doc-rdr col-name)
                                 ^VectorSchemaRoot live-root (.getLiveRoot chunk-manager col-name)
                                 ^BigIntVector row-id-vec (.getVector live-root "_row-id")
                                 ^IVectorWriter vec-writer (-> (.getVector live-root col-name)
                                                               (vw/vec->writer))
                                 row-copier (.rowCopier col-rdr vec-writer)]]
                       (reify DocRowCopier
                         (copyDocRow [_ row-id src-idx]
                           (when (.isPresent col-rdr src-idx)
                             (let [dest-idx (.getValueCount row-id-vec)]
                               (.setValueCount row-id-vec (inc dest-idx))
                               (.set row-id-vec dest-idx row-id))
                             (.startValue vec-writer)
                             (.copyRow row-copier src-idx)
                             (.endValue vec-writer)

                             (.setRowCount live-root (inc (.getRowCount live-root)))
                             (.syncSchema live-root))))))]

    (dotimes [op-idx (.getValueCount tx-ops-vec)]
      (when (zero? (.getTypeId tx-ops-vec op-idx))
        (let [row-id (+ base-row-id op-idx)
              doc-idx (.getOffset tx-ops-vec op-idx)]
          (doseq [^DocRowCopier doc-row-copier doc-copiers]
            (.copyDocRow doc-row-copier row-id doc-idx)))))))

(deftype Indexer [^BufferAllocator allocator
                  ^ObjectStore object-store
                  ^IMetadataManager metadata-mgr
                  ^ITemporalManager temporal-mgr
                  ^IWatermarkManager watermark-mgr
                  ^long max-rows-per-chunk
                  ^long max-rows-per-block
                  ^Map live-roots
                  ^ILogIndexer log-indexer
                  ^:volatile-mutable ^long chunk-idx
                  ^:volatile-mutable ^TransactionInstant latest-completed-tx
                  ^:volatile-mutable ^long chunk-row-count]

  TransactionIndexer
  (getLiveRoot [_ field-name]
    (.computeIfAbsent live-roots field-name
                      (util/->jfn
                        (fn [field-name]
                          (->live-root field-name allocator)))))

  (indexTx [this tx-key tx-root]
    (let [^DenseUnionVector tx-ops-vec (-> ^ListVector (.getVector tx-root "tx-ops")
                                           (.getDataVector))
          next-row-id (+ chunk-idx chunk-row-count)

          op-type-ids (object-array (mapv (fn [^Field field]
                                            (keyword (.getName field)))
                                          (.getChildren (.getField tx-ops-vec))))
          log-op-idxer (.startTx log-indexer tx-key tx-root)
          temporal-idxer (.startTx temporal-mgr tx-key)]

      (dotimes [tx-op-idx (.getValueCount tx-ops-vec)]
        (let [op-type-id (.getTypeId tx-ops-vec tx-op-idx)
              per-op-offset (.getOffset tx-ops-vec tx-op-idx)
              op-vec (.getStruct tx-ops-vec op-type-id)

              ^TimeStampVector valid-time-start-vec (.getChild op-vec "_valid-time-start")
              ^TimeStampVector valid-time-end-vec (.getChild op-vec "_valid-time-end")
              row-id (+ next-row-id tx-op-idx)
              op (aget op-type-ids op-type-id)]
          (case op
            :put (let [^DenseUnionVector doc-duv (.getChild op-vec "document" DenseUnionVector)
                       leg-type-id (.getTypeId doc-duv per-op-offset)
                       leg-offset (.getOffset doc-duv per-op-offset)
                       id-vec (-> ^StructVector (.getVectorByType doc-duv leg-type-id)
                                  (.getChild "_id"))]
                   (.logPut log-op-idxer row-id tx-op-idx)
                   (.indexPut temporal-idxer (t/get-object id-vec leg-offset) row-id
                              valid-time-start-vec valid-time-end-vec per-op-offset))

            :delete (let [^DenseUnionVector id-vec (.getChild op-vec "_id" DenseUnionVector)]
                      (.logDelete log-op-idxer row-id tx-op-idx)
                      (.indexDelete temporal-idxer (t/get-object id-vec per-op-offset) row-id
                                    valid-time-start-vec valid-time-end-vec per-op-offset))

            :evict (let [^DenseUnionVector id-vec (.getChild op-vec "_id" DenseUnionVector)]
                     (.logEvict log-op-idxer row-id tx-op-idx)
                     (.indexEvict temporal-idxer (t/get-object id-vec per-op-offset) row-id)))))

      (copy-docs this tx-ops-vec next-row-id)

      (.endTx log-op-idxer)

      (let [evicted-row-ids (.endTx temporal-idxer)]

        #_{:clj-kondo/ignore [:missing-body-in-when]}
        (when-not (.isEmpty evicted-row-ids)
          ;; TODO create work item
          ))

      (let [new-chunk-row-count (+ chunk-row-count (.getValueCount tx-ops-vec))]
        (set! (.-chunk-row-count this) new-chunk-row-count)
        (set! (.-latest-completed-tx this) tx-key)
        (.setWatermark watermark-mgr chunk-idx tx-key (snapshot-roots live-roots) (.getTemporalWatermark temporal-mgr))

        (when (>= new-chunk-row-count max-rows-per-chunk)
          (.finishChunk this))

        tx-key)))

  (latestCompletedTx [_] latest-completed-tx)

  IndexerPrivate
  (writeColumn [_this live-root]
    (with-open [write-root (VectorSchemaRoot/create (.getSchema live-root) allocator)]
      (let [loader (VectorLoader. write-root)
            row-counts (blocks/row-id-aligned-blocks live-root chunk-idx max-rows-per-block)]
        (with-open [^ICursor slices (blocks/->slices live-root row-counts)]
          (util/build-arrow-ipc-byte-buffer write-root :file
            (fn [write-batch!]
              (.forEachRemaining slices
                                 (reify Consumer
                                   (accept [_ sliced-root]
                                     (with-open [arb (.getRecordBatch (VectorUnloader. sliced-root))]
                                       (.load loader arb)
                                       (write-batch!)))))))))))

  (closeCols [_this]
    (doseq [^VectorSchemaRoot live-root (vals live-roots)]
      (util/try-close live-root))

    (.clear live-roots)
    (.clear log-indexer))

  (finishChunk [this]
    (when-not (.isEmpty live-roots)
      (log/debugf "finishing chunk '%x', tx '%s'" chunk-idx (pr-str latest-completed-tx))

      (try
        @(CompletableFuture/allOf (->> (cons
                                        (.putObject object-store (format "log-%016x.arrow" chunk-idx) (.writeLog log-indexer))
                                        (for [[^String col-name, ^VectorSchemaRoot live-root] live-roots]
                                          (.putObject object-store (meta/->chunk-obj-key chunk-idx col-name) (.writeColumn this live-root))))
                                       (into-array CompletableFuture)))
        (.registerNewChunk temporal-mgr chunk-idx)
        (.registerNewChunk metadata-mgr live-roots chunk-idx)

        (set! (.-chunk-idx this) (+ chunk-idx chunk-row-count))
        (set! (.-chunk-row-count this) 0)

        (.setWatermark watermark-mgr chunk-idx latest-completed-tx nil (.getTemporalWatermark temporal-mgr))
        (log/debug "finished chunk.")
        (finally
          (.closeCols this)))))

  Closeable
  (close [this]
    (.closeCols this)
    (.close log-indexer)))

(defmethod ig/prep-key ::indexer [_ opts]
  (merge {:allocator (ig/ref :core2/allocator)
          :object-store (ig/ref :core2/object-store)
          :metadata-mgr (ig/ref ::meta/metadata-manager)
          :temporal-mgr (ig/ref ::temporal/temporal-manager)
          :watermark-mgr (ig/ref :core2.watermark/watermark-manager)
          :buffer-pool (ig/ref ::bp/buffer-pool)
          :row-counts (ig/ref :core2/row-counts)}
         opts))

(defmethod ig/init-key ::indexer
  [_ {:keys [allocator object-store metadata-mgr ^ITemporalManager temporal-mgr, ^IWatermarkManager watermark-mgr]
      {:keys [max-rows-per-chunk max-rows-per-block]} :row-counts
      :as deps}]

  (let [{:keys [latest-row-id latest-tx]} (latest-tx deps)
        chunk-idx (if latest-row-id
                    (inc (long latest-row-id))
                    0)]
    (.setWatermark watermark-mgr chunk-idx latest-tx nil (.getTemporalWatermark temporal-mgr))

    (Indexer. allocator object-store metadata-mgr temporal-mgr watermark-mgr
              max-rows-per-chunk max-rows-per-block
              (ConcurrentSkipListMap.)
              (->log-indexer allocator max-rows-per-block)
              chunk-idx latest-tx 0)))

(defmethod ig/halt-key! ::indexer [_ ^AutoCloseable indexer]
  (.close indexer))

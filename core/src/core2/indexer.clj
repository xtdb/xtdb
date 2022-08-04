(ns core2.indexer
  (:require [clojure.tools.logging :as log]
            [core2.api :as c2]
            [core2.blocks :as blocks]
            [core2.bloom :as bloom]
            [core2.buffer-pool :as bp]
            [core2.metadata :as meta]
            core2.object-store
            [core2.operator :as op]
            core2.operator.scan
            [core2.rewrite :refer [zmatch]]
            [core2.temporal :as temporal]
            [core2.types :as t]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw]
            [core2.watermark :as wm]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import core2.api.TransactionInstant
           core2.buffer_pool.IBufferPool
           core2.ICursor
           core2.metadata.IMetadataManager
           core2.object_store.ObjectStore
           core2.operator.scan.ScanSource
           (core2.temporal ITemporalManager ITemporalTxIndexer)
           (core2.vector IIndirectRelation IIndirectVector IVectorWriter)
           (core2.watermark IWatermarkManager)
           java.io.Closeable
           java.lang.AutoCloseable
           java.nio.ByteBuffer
           java.time.Instant
           [java.util Collections Map TreeMap]
           [java.util.concurrent CompletableFuture ConcurrentHashMap ConcurrentSkipListMap]
           [java.util.function Consumer Function]
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           [org.apache.arrow.vector BigIntVector BitVector TimeStampMicroTZVector TimeStampVector VarCharVector VectorLoader VectorSchemaRoot VectorUnloader]
           [org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector]
           [org.apache.arrow.vector.types.pojo Schema]))

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
(definterface IInternalIdManager
  (^long getOrCreateInternalId [^Object id ^long row-id])
  (^boolean isKnownId [^Object id]))

(defn- normalize-id [id]
  (cond-> id
    (bytes? id) (ByteBuffer/wrap)))

(defmethod ig/prep-key ::internal-id-manager [_ opts]
  (merge {:metadata-mgr (ig/ref ::meta/metadata-manager)
          :buffer-pool (ig/ref :core2.buffer-pool/buffer-pool)}
         opts))

(deftype InternalIdManager [^Map id->internal-id]
  IInternalIdManager
  (getOrCreateInternalId [_ id row-id]
    (.computeIfAbsent id->internal-id
                      (normalize-id id)
                      (reify Function
                        (apply [_ _]
                          ;; big endian for index distribution
                          (Long/reverseBytes row-id)))))

  (isKnownId [_ id]
    (.containsKey id->internal-id (normalize-id id)))

  Closeable
  (close [_]
    (.clear id->internal-id)))

(defmethod ig/init-key ::internal-id-manager [_ {:keys [^IBufferPool buffer-pool, ^IMetadataManager metadata-mgr]}]
  (let [iid-mgr (InternalIdManager. (ConcurrentHashMap.))
        known-chunks (.knownChunks metadata-mgr)
        futs (for [chunk-idx known-chunks]
               (-> (.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx "id"))
                   (util/then-apply util/try-close)))]
    @(CompletableFuture/allOf (into-array CompletableFuture futs))

    (doseq [chunk-idx known-chunks]
      (with-open [^ArrowBuf id-buffer @(.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx "id"))
                  id-chunks (util/->chunks id-buffer)]
        (.forEachRemaining id-chunks
                           (reify Consumer
                             (accept [_ id-root]
                               (let [^VectorSchemaRoot id-root id-root
                                     ^BigIntVector row-id-vec (.getVector id-root 0)
                                     id-vec (.getVector id-root 1)]
                                 (dotimes [n (.getRowCount id-root)]
                                   (.getOrCreateInternalId iid-mgr (t/get-object id-vec n) (.get row-id-vec n)))))))))
    iid-mgr))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface TransactionIndexer
  (^org.apache.arrow.vector.VectorSchemaRoot getLiveRoot [^String fieldName])
  (^long nextRowId [])
  (^core2.watermark.Watermark getWatermark [])
  (^core2.api.TransactionInstant indexTx [^core2.api.TransactionInstant tx
                                          ^org.apache.arrow.vector.VectorSchemaRoot txRoot])
  (^core2.api.TransactionInstant latestCompletedTx []))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface IndexerPrivate
  (^core2.operator.scan.ScanSource scanSource [^core2.api.TransactionInstant tx])
  (^java.nio.ByteBuffer writeColumn [^org.apache.arrow.vector.VectorSchemaRoot live-root])
  (^void closeCols [])
  (^void finishChunk []))

(defn- ->live-root [field-name allocator]
  (VectorSchemaRoot/create (Schema. [t/row-id-field (t/->field field-name t/dense-union-type false)]) allocator))

(defn- snapshot-roots [^Map live-roots]
  (Collections/unmodifiableSortedMap
   (reduce-kv (fn [^Map acc k ^VectorSchemaRoot v]
                (doto acc (.put k (util/slice-root v))))
              (TreeMap.)
              live-roots)))

(def ^:private log-schema
  (Schema. [(t/col-type->field "tx-id" :i64)
            (t/col-type->field "system-time" [:timestamp-tz :micro "UTC"])
            (t/col-type->field "ops" '[:list [:struct {iid :i64
                                                       row-id [:union #{:null :i64}]
                                                       application-time-start [:union #{:null [:timestamp-tz :micro "UTC"]}]
                                                       application-time-end [:union #{:null [:timestamp-tz :micro "UTC"]}]
                                                       evict? :bool}]])]))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface ILogOpIndexer
  (^void logPut [^long iid, ^long rowId, ^long app-timeStart, ^long app-timeEnd])
  (^void logDelete [^long iid, ^long app-timeStart, ^long app-timeEnd])
  (^void logEvict [^long iid])
  (^void endTx []))

(definterface ILogIndexer
  (^core2.indexer.ILogOpIndexer startTx [^core2.api.TransactionInstant txKey])
  (^java.nio.ByteBuffer writeLog [])
  (^void clear [])
  (^void close []))

(defn- ->log-indexer [^BufferAllocator allocator, ^long max-rows-per-block]
  (let [log-root (VectorSchemaRoot/create log-schema allocator)
        ^BigIntVector tx-id-vec (.getVector log-root "tx-id")
        ^TimeStampMicroTZVector sys-time-vec (.getVector log-root "system-time")

        ops-vec (.getVector log-root "ops")
        ops-writer (.asList (vw/vec->writer ops-vec))
        ops-data-writer (.asStruct (.getDataWriter ops-writer))

        row-id-writer (.writerForName ops-data-writer "row-id")
        ^BigIntVector row-id-vec (.getVector row-id-writer)
        iid-writer (.writerForName ops-data-writer "iid")
        ^BigIntVector iid-vec (.getVector iid-writer)

        app-time-start-writer (.writerForName ops-data-writer "application-time-start")
        ^TimeStampMicroTZVector app-time-start-vec (.getVector app-time-start-writer)
        app-time-end-writer (.writerForName ops-data-writer "application-time-end")
        ^TimeStampMicroTZVector app-time-end-vec (.getVector app-time-end-writer)

        evict-writer (.writerForName ops-data-writer "evict?")
        ^BitVector evict-vec (.getVector evict-writer)]

    (reify ILogIndexer
      (startTx [_ tx-key]
        (let [tx-idx (.getRowCount log-root)]
          (.startValue ops-writer)
          (.setSafe tx-id-vec tx-idx (.tx-id tx-key))
          (.setSafe sys-time-vec tx-idx (util/instant->micros (.sys-time tx-key)))

          (reify ILogOpIndexer
            (logPut [_ iid row-id app-time-start app-time-end]
              (let [op-idx (.startValue ops-data-writer)]
                (.setSafe row-id-vec op-idx row-id)
                (.setSafe iid-vec op-idx iid)
                (.setSafe app-time-start-vec op-idx app-time-start)
                (.setSafe app-time-end-vec op-idx app-time-end)
                (.setSafe evict-vec op-idx 0)

                (.endValue ops-data-writer)))

            (logDelete [_ iid app-time-start app-time-end]
              (let [op-idx (.startValue ops-data-writer)]
                (.setSafe iid-vec op-idx iid)
                (.setNull row-id-vec op-idx)
                (.setSafe app-time-start-vec op-idx app-time-start)
                (.setSafe app-time-end-vec op-idx app-time-end)
                (.setSafe evict-vec op-idx 0)

                (.endValue ops-data-writer)))

            (logEvict [_ iid]
              (let [op-idx (.startValue ops-data-writer)]
                (.setSafe iid-vec op-idx iid)
                (.setNull row-id-vec op-idx)
                (.setNull app-time-start-vec op-idx)
                (.setNull app-time-end-vec op-idx)
                (.setSafe evict-vec op-idx 1))

              (.endValue ops-data-writer))

            (endTx [_]
              (.endValue ops-writer)
              (.setRowCount log-root (inc tx-idx))))))

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

(defn- with-latest-log-chunk [{:keys [^ObjectStore object-store ^IBufferPool buffer-pool]} f]
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
                ^BigIntVector tx-id-vec (.getVector log-root "tx-id")
                ^TimeStampMicroTZVector sys-time-vec (.getVector log-root "system-time")
                ^BigIntVector row-id-vec (-> ^ListVector (.getVector log-root "ops")
                                             ^StructVector (.getDataVector)
                                             (.getChild "row-id"))]
            {:latest-tx (c2/->TransactionInstant (.get tx-id-vec (dec tx-count))
                                                 (util/micros->instant (.get sys-time-vec (dec tx-count))))
             :latest-row-id (.get row-id-vec (dec (.getValueCount row-id-vec)))}))))))

(definterface OpIndexer
  (^void indexOp [^long tx-op-idx]))

(definterface DocRowCopier
  (^void copyDocRow [^long rowId, ^int srcIdx]))

(defn- doc-row-copier [^TransactionIndexer indexer, ^IIndirectVector col-rdr]
  (let [col-name (.getName col-rdr)
        ^VectorSchemaRoot live-root (.getLiveRoot indexer col-name)
        ^BigIntVector row-id-vec (.getVector live-root "_row-id")
        ^IVectorWriter vec-writer (-> (.getVector live-root col-name)
                                      (vw/vec->writer))
        row-copier (.rowCopier col-rdr vec-writer)]
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
          (.syncSchema live-root))))))

(defn- put-doc-copier ^core2.indexer.DocRowCopier [^TransactionIndexer indexer, ^DenseUnionVector tx-ops-vec]
  (let [doc-rdr (-> (.getStruct tx-ops-vec 0)
                    (.getChild "document")
                    (iv/->direct-vec)
                    (.structReader))
        doc-copiers (vec
                     (for [^String col-name (.structKeys doc-rdr)]
                       (doc-row-copier indexer (.readerForKey doc-rdr col-name))))]
    (reify DocRowCopier
      (copyDocRow [_ row-id src-idx]
        (doseq [^DocRowCopier doc-copier doc-copiers]
          (.copyDocRow doc-copier row-id src-idx))))))

(defn- ->put-indexer ^core2.indexer.OpIndexer [^TransactionIndexer indexer, ^IInternalIdManager iid-mgr
                                               ^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer
                                               ^DenseUnionVector tx-ops-vec]
  (let [put-vec (.getStruct tx-ops-vec 0)
        ^DenseUnionVector doc-duv (.getChild put-vec "document" DenseUnionVector)
        ^TimeStampVector app-time-start-vec (.getChild put-vec "application_time_start")
        ^TimeStampVector app-time-end-vec (.getChild put-vec "application_time_end")
        doc-copier (put-doc-copier indexer tx-ops-vec)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [row-id (.nextRowId indexer)
              put-offset (.getOffset tx-ops-vec tx-op-idx)
              leg-type-id (.getTypeId doc-duv put-offset)
              leg-offset (.getOffset doc-duv put-offset)
              id-vec (-> ^StructVector (.getVectorByType doc-duv leg-type-id)
                         (.getChild "id"))
              eid (t/get-object id-vec leg-offset)
              new-entity? (not (.isKnownId iid-mgr eid))
              iid (.getOrCreateInternalId iid-mgr eid row-id)
              start-app-time (.get app-time-start-vec put-offset)
              end-app-time (.get app-time-end-vec put-offset)]
          (.copyDocRow doc-copier row-id put-offset)
          (.logPut log-op-idxer iid row-id start-app-time end-app-time)
          (.indexPut temporal-idxer iid row-id start-app-time end-app-time new-entity?))))))

(defn- ->delete-indexer ^core2.indexer.OpIndexer [^TransactionIndexer indexer, ^IInternalIdManager iid-mgr
                                                  ^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer,
                                                  ^DenseUnionVector tx-ops-vec]
  (let [delete-vec (.getStruct tx-ops-vec 1)
        ^DenseUnionVector id-vec (.getChild delete-vec "id" DenseUnionVector)
        ^TimeStampVector app-time-start-vec (.getChild delete-vec "application_time_start")
        ^TimeStampVector app-time-end-vec (.getChild delete-vec "application_time_end")]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [row-id (.nextRowId indexer)
              delete-offset (.getOffset tx-ops-vec tx-op-idx)
              eid (t/get-object id-vec delete-offset)
              new-entity? (not (.isKnownId iid-mgr eid))
              iid (.getOrCreateInternalId iid-mgr eid row-id)
              start-app-time (.get app-time-start-vec delete-offset)
              end-app-time (.get app-time-end-vec delete-offset)]
          (.logDelete log-op-idxer iid start-app-time end-app-time)
          (.indexDelete temporal-idxer iid row-id start-app-time end-app-time new-entity?))))))

(defn- ->evict-indexer ^core2.indexer.OpIndexer [^TransactionIndexer indexer, ^IInternalIdManager iid-mgr
                                                 ^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer
                                                 ^DenseUnionVector tx-ops-vec]
  (let [evict-vec (.getStruct tx-ops-vec 2)
        ^DenseUnionVector id-vec (.getChild evict-vec "id" DenseUnionVector)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [row-id (.nextRowId indexer)
              evict-offset (.getOffset tx-ops-vec tx-op-idx)
              iid (.getOrCreateInternalId iid-mgr (t/get-object id-vec evict-offset) row-id)]
          (.logEvict log-op-idxer iid)
          (.indexEvict temporal-idxer iid))))))

(defn- table-row-writer [^TransactionIndexer indexer, ^String table-name]
  (let [^VectorSchemaRoot live-root (.getLiveRoot indexer "_table")
        ^BigIntVector row-id-vec (.getVector live-root "_row-id")
        ^IVectorWriter vec-writer (-> (.getVector live-root "_table")
                                      (vw/vec->writer))
        ^IVectorWriter vec-str-writer (.writerForType (.asDenseUnion vec-writer) :utf8)]
    (reify DocRowCopier
      (copyDocRow [_ row-id _src-idx]
        (let [dest-idx (.getValueCount row-id-vec)]
          (.setValueCount row-id-vec (inc dest-idx))
          (.set row-id-vec dest-idx row-id))
        (.startValue vec-writer)
        (.startValue vec-str-writer)
        (t/write-value! table-name vec-str-writer)
        (.endValue vec-writer)

        (.setRowCount live-root (inc (.getRowCount live-root)))
        (.syncSchema live-root)))))

(definterface SqlOpIndexer
  (^void indexOp [innerQuery paramRows opts]))

(defn- ->sql-insert-indexer ^core2.indexer.SqlOpIndexer [^TransactionIndexer indexer, ^IInternalIdManager iid-mgr
                                                         ^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer
                                                         ^Instant current-time, ^ScanSource scan-src]
  (let [current-time-µs (util/instant->micros current-time)]
    (reify SqlOpIndexer
      (indexOp [_ inner-query param-rows {:keys [table]}]
        (with-open [pq (op/open-prepared-ra inner-query)]
          (doseq [param-row param-rows
                  :let [param-row (->> param-row
                                       (into {} (map (juxt (comp symbol key) val))))]]
            (with-open [res (.openCursor pq (into {'$ scan-src} param-row) {:current-time current-time})]
              (.forEachRemaining res
                                 (reify Consumer
                                   (accept [_ in-rel]
                                     (let [^IIndirectRelation in-rel in-rel
                                           row-count (.rowCount in-rel)
                                           doc-row-copiers (vec
                                                            (cons (table-row-writer indexer table)
                                                                  (for [^IIndirectVector in-col in-rel
                                                                        :when (not (temporal/temporal-column? (.getName in-col)))]
                                                                    (doc-row-copier indexer in-col))))
                                           id-col (.vectorForName in-rel "id")
                                           app-time-start-rdr (some-> (.vectorForName in-rel "application_time_start") (.monoReader))
                                           app-time-end-rdr (some-> (.vectorForName in-rel "application_time_end") (.monoReader))]
                                       (dotimes [idx row-count]
                                         (let [row-id (.nextRowId indexer)]
                                           (doseq [^DocRowCopier doc-row-copier doc-row-copiers]
                                             (.copyDocRow doc-row-copier row-id idx))

                                           (let [eid (t/get-object (.getVector id-col) (.getIndex id-col idx))
                                                 new-entity? (.isKnownId iid-mgr eid)
                                                 iid (.getOrCreateInternalId iid-mgr eid row-id)
                                                 start-app-time (if app-time-start-rdr
                                                                  (.readLong app-time-start-rdr idx)
                                                                  current-time-µs)
                                                 end-app-time (if app-time-end-rdr
                                                                (.readLong app-time-end-rdr idx)
                                                                util/end-of-time-μs)]

                                             (.logPut log-op-idxer iid row-id start-app-time end-app-time)
                                             (.indexPut temporal-idxer iid row-id start-app-time end-app-time new-entity?)))))))))))))))

(defn- ->sql-update-indexer ^core2.indexer.SqlOpIndexer [^TransactionIndexer indexer, ^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer
                                                         ^Instant current-time, ^ScanSource scan-src]
  (reify SqlOpIndexer
    (indexOp [_ inner-query param-rows {:keys [app-time-start app-time-end]}]
      (with-open [pq (op/open-prepared-ra inner-query)]
        (let [^long update-app-time-from-µs (or (some-> app-time-start util/->instant util/instant->micros)
                                                Long/MIN_VALUE)
              ^long update-app-time-to-µs (or (some-> app-time-end util/->instant util/instant->micros)
                                              Long/MAX_VALUE)]
          (doseq [param-row param-rows
                  :let [param-row (->> param-row
                                       (into {} (map (juxt (comp symbol key) val))))]]
            (with-open [res (.openCursor pq (into {'$ scan-src} param-row) {:current-time current-time})]
              (.forEachRemaining res
                                 (reify Consumer
                                   (accept [_ in-rel]
                                     (let [^IIndirectRelation in-rel in-rel
                                           row-count (.rowCount in-rel)
                                           doc-row-copiers (vec
                                                            (for [^IIndirectVector in-col in-rel
                                                                  :when (not (temporal/temporal-column? (.getName in-col)))]
                                                              (doc-row-copier indexer in-col)))
                                           iid-rdr (.monoReader (.vectorForName in-rel "_iid"))
                                           app-time-start-rdr (.monoReader (.vectorForName in-rel "application_time_start"))
                                           app-time-end-rdr (.monoReader (.vectorForName in-rel "application_time_end"))]
                                       (dotimes [idx row-count]
                                         (let [row-id (.nextRowId indexer)
                                               iid (.readLong iid-rdr idx)
                                               start-app-time (Math/max (.readLong app-time-start-rdr idx) update-app-time-from-µs)
                                               end-app-time (Math/min (.readLong app-time-end-rdr idx) update-app-time-to-µs)]
                                           (doseq [^DocRowCopier doc-row-copier doc-row-copiers]
                                             (.copyDocRow doc-row-copier row-id idx))

                                           (.logPut log-op-idxer iid row-id start-app-time end-app-time)
                                           (.indexPut temporal-idxer iid row-id start-app-time end-app-time false))))))))))))))

(defn- ->sql-delete-indexer ^core2.indexer.SqlOpIndexer [^TransactionIndexer indexer, ^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer
                                                         ^Instant current-time, ^ScanSource scan-src]
  (reify SqlOpIndexer
    (indexOp [_ inner-query param-rows {:keys [app-time-start app-time-end]}]
      (with-open [pq (op/open-prepared-ra inner-query)]
        (let [^long delete-app-time-from-µs (or (some-> app-time-start util/->instant util/instant->micros)
                                                Long/MIN_VALUE)
              ^long delete-app-time-to-µs (or (some-> app-time-end util/->instant util/instant->micros)
                                              Long/MAX_VALUE)]
          (doseq [param-row param-rows
                  :let [param-row (->> param-row
                                       (into {} (map (juxt (comp symbol key) val))))]]
            (with-open [res (.openCursor pq (into {'$ scan-src} param-row) {:current-time current-time})]
              (.forEachRemaining res
                                 (reify Consumer
                                   (accept [_ in-rel]
                                     (let [^IIndirectRelation in-rel in-rel
                                           row-count (.rowCount in-rel)
                                           iid-rdr (.monoReader (.vectorForName in-rel "_iid"))
                                           app-time-start-rdr (.monoReader (.vectorForName in-rel "application_time_start"))
                                           app-time-end-rdr (.monoReader (.vectorForName in-rel "application_time_end"))]
                                       (dotimes [idx row-count]
                                         (let [row-id (.nextRowId indexer)
                                               iid (.readLong iid-rdr idx)
                                               start-app-time (Math/max (.readLong app-time-start-rdr idx) delete-app-time-from-µs)
                                               end-app-time (Math/min (.readLong app-time-end-rdr idx) delete-app-time-to-µs)]
                                           (.logDelete log-op-idxer iid start-app-time end-app-time)
                                           (.indexDelete temporal-idxer iid row-id start-app-time end-app-time false))))))))))))))

(defn- ->sql-indexer ^core2.indexer.OpIndexer [^TransactionIndexer indexer, ^IInternalIdManager iid-mgr
                                               ^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer
                                               ^DenseUnionVector tx-ops-vec, ^Instant current-time, ^ScanSource scan-src]
  (let [sql-vec (.getStruct tx-ops-vec 3)
        ^VarCharVector query-vec (.getChild sql-vec "query" VarCharVector)
        ^ListVector param-rows-vec (.getChild sql-vec "param-rows" ListVector)
        insert-idxer (->sql-insert-indexer indexer iid-mgr log-op-idxer temporal-idxer current-time scan-src)
        update-idxer (->sql-update-indexer indexer log-op-idxer temporal-idxer current-time scan-src)
        delete-idxer (->sql-delete-indexer indexer log-op-idxer temporal-idxer current-time scan-src)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [sql-offset (.getOffset tx-ops-vec tx-op-idx)
              query (read-string (t/get-object query-vec sql-offset))
              param-rows (if-not (.isEmpty param-rows-vec sql-offset)
                           (t/get-object param-rows-vec sql-offset)
                           [{}])]
          (zmatch query
            [:insert opts inner-query]
            (.indexOp insert-idxer inner-query param-rows opts)

            [:update opts inner-query]
            (.indexOp update-idxer inner-query param-rows opts)

            [:delete opts inner-query]
            (.indexOp delete-idxer inner-query param-rows opts)

            (throw (UnsupportedOperationException. "sql query"))))))))

(deftype Indexer [^BufferAllocator allocator
                  ^ObjectStore object-store
                  ^IMetadataManager metadata-mgr
                  ^IBufferPool buffer-pool
                  ^ITemporalManager temporal-mgr
                  ^IInternalIdManager iid-mgr
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

  (nextRowId [this]
    (let [row-id (+ chunk-idx chunk-row-count)]
      (set! (.chunk-row-count this) (inc chunk-row-count))
      row-id))

  (indexTx [this tx-key tx-root]
    (let [^DenseUnionVector tx-ops-vec (-> ^ListVector (.getVector tx-root "tx-ops")
                                           (.getDataVector))
          current-time (-> (.get ^TimeStampMicroTZVector (.getVector tx-root "current-time") 0)
                           (util/micros->instant))
          log-op-idxer (.startTx log-indexer tx-key)
          temporal-idxer (.startTx temporal-mgr tx-key)
          put-idxer (->put-indexer this iid-mgr log-op-idxer temporal-idxer tx-ops-vec)
          delete-idxer (->delete-indexer this iid-mgr log-op-idxer temporal-idxer tx-ops-vec)
          evict-idxer (->evict-indexer this iid-mgr log-op-idxer temporal-idxer tx-ops-vec)
          sql-idxer (->sql-indexer this iid-mgr log-op-idxer temporal-idxer tx-ops-vec current-time (.scanSource this tx-key))]

      (dotimes [tx-op-idx (.getValueCount tx-ops-vec)]
        (case (.getTypeId tx-ops-vec tx-op-idx)
          0 (.indexOp put-idxer tx-op-idx)
          1 (.indexOp delete-idxer tx-op-idx)
          2 (.indexOp evict-idxer tx-op-idx)
          3 (.indexOp sql-idxer tx-op-idx)))

      (.endTx log-op-idxer)

      (let [evicted-row-ids (.endTx temporal-idxer)]
        #_{:clj-kondo/ignore [:missing-body-in-when]}
        (when-not (.isEmpty evicted-row-ids)
          ;; TODO create work item
          ))

      (set! (.-latest-completed-tx this) tx-key)
      (.setWatermark watermark-mgr chunk-idx tx-key (snapshot-roots live-roots) (.getTemporalWatermark temporal-mgr))

      (when (>= chunk-row-count max-rows-per-chunk)
        (.finishChunk this))

      tx-key))

  (latestCompletedTx [_] latest-completed-tx)

  IndexerPrivate
  (scanSource [_ tx]
    (reify ScanSource
      (metadataManager [_] metadata-mgr)
      (bufferPool [_] buffer-pool)
      (txBasis [_] tx)
      (openWatermark [_]
        (wm/->Watermark nil live-roots temporal-mgr
                        chunk-idx max-rows-per-block))))

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
          :internal-id-mgr (ig/ref ::internal-id-manager)
          :buffer-pool (ig/ref ::bp/buffer-pool)
          :row-counts (ig/ref :core2/row-counts)}
         opts))

(defmethod ig/init-key ::indexer
  [_ {:keys [allocator object-store metadata-mgr buffer-pool ^ITemporalManager temporal-mgr, ^IWatermarkManager watermark-mgr, internal-id-mgr]
      {:keys [max-rows-per-chunk max-rows-per-block]} :row-counts
      :as deps}]

  (let [{:keys [latest-row-id latest-tx]} (latest-tx deps)
        chunk-idx (if latest-row-id
                    (inc (long latest-row-id))
                    0)]
    (.setWatermark watermark-mgr chunk-idx latest-tx nil (.getTemporalWatermark temporal-mgr))

    (Indexer. allocator object-store metadata-mgr buffer-pool temporal-mgr internal-id-mgr watermark-mgr
              max-rows-per-chunk max-rows-per-block
              (ConcurrentSkipListMap.)
              (->log-indexer allocator max-rows-per-block)
              chunk-idx latest-tx 0)))

(defmethod ig/halt-key! ::indexer [_ ^AutoCloseable indexer]
  (.close indexer))

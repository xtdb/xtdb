(ns xtdb.indexer.temporal-log-indexer
  (:require
   [juxt.clojars-mirrors.integrant.core :as ig]
   [xtdb.blocks :as blocks]
   [xtdb.types :as types]
   [xtdb.util :as util]
   [xtdb.vector.writer :as vw])
  (:import
   (java.io Closeable)
   (java.nio ByteBuffer)
   (java.util ArrayList)
   (java.util.concurrent.atomic AtomicInteger)
   (java.util.function Consumer)
   (org.apache.arrow.memory BufferAllocator)
   (org.apache.arrow.vector VectorLoader VectorSchemaRoot VectorUnloader)
   (org.apache.arrow.vector.types UnionMode)
   (org.apache.arrow.vector.types.pojo ArrowType$Union Schema)
   (xtdb ICursor)
   (xtdb.object_store ObjectStore)))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILogOpIndexer2
  (^void logPut [^bytes iid, ^long rowId, ^long app-timeStart, ^long app-timeEnd])
  (^void logDelete [^bytes iid, ^long app-timeStart, ^long app-timeEnd])
  (^void logEvict [^bytes iid])
  (^void commit [])
  (^void abort []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILogIndexer2
  (^xtdb.indexer.temporal_log_indexer.ILogOpIndexer2 startTx [^xtdb.api.protocols.TransactionInstant txKey])
  (^void finishBlock [])
  (^java.util.concurrent.CompletableFuture finishChunk [^long chunkIdx])
  (^void nextChunk [])
  (^void close []))

(defn- ->log-obj-key [chunk-idx]
  (format "chunk-%s/temporal-log.arrow" (util/->lex-hex-string chunk-idx)))

(defmethod ig/prep-key :xtdb.indexer/temporal-log-indexer [_ opts]
  (merge {:allocator (ig/ref :xtdb/allocator)
          :object-store (ig/ref :xtdb/object-store)}
         opts))

(def ^:private nullable-inst-type [:union #{:null [:timestamp-tz :micro "UTC"]}])

(def temporal-log-schema
  (Schema. [(types/col-type->field "tx-id" :i64)
            (types/col-type->field "system-time" types/temporal-col-type)
            ;; a null value indicates an abort
            (types/->field "tx-ops" types/list-type true
                           (types/->field "$data" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                                          (types/col-type->field "put" [:struct {'iid [:fixed-size-binary 16]
                                                                                 'row-id :i64
                                                                                 'valid-from nullable-inst-type
                                                                                 'valid-to nullable-inst-type}])
                                          (types/col-type->field "delete" [:struct {'iid [:fixed-size-binary 16]
                                                                                    'valid-from nullable-inst-type
                                                                                    'valid-to nullable-inst-type}])
                                          (types/col-type->field "evict" [:struct {'iid [:fixed-size-binary 16]}])))]))

(defmethod ig/init-key :xtdb.indexer/temporal-log-indexer [_ {:keys [^BufferAllocator allocator, ^ObjectStore object-store]}]
  (let [log-root (VectorSchemaRoot/create temporal-log-schema allocator)
        log-writer (vw/root->writer log-root)
        transcient-log-root (VectorSchemaRoot/create temporal-log-schema allocator)
        transient-log-writer (vw/root->writer transcient-log-root)

        tx-id-wtr (.writerForName transient-log-writer "tx-id" :i64)
        system-time-wtr (.writerForName transient-log-writer "system-time" [:timestamp-tz :micro "UTC"])

        tx-ops-wtr (.writerForName transient-log-writer "tx-ops")

        tx-ops-el-wtr (.listElementWriter tx-ops-wtr)

        put-wtr (.writerForTypeId tx-ops-el-wtr (byte 0))
        put-iid-wtr (.structKeyWriter put-wtr "iid")
        put-row-id-wtr (.structKeyWriter put-wtr "row-id")
        put-vf-wtr (.structKeyWriter put-wtr "valid-from")
        put-vt-wtr (.structKeyWriter put-wtr "valid-to")

        delete-wtr (.writerForTypeId tx-ops-el-wtr (byte 1))
        delete-iid-wtr (.structKeyWriter delete-wtr "iid")
        delete-vf-wtr (.structKeyWriter delete-wtr "valid-from")
        delete-vt-wtr (.structKeyWriter delete-wtr "valid-to")

        evict-wtr (.writerForTypeId tx-ops-el-wtr (byte 2))
        evict-iid-wtr (.structKeyWriter evict-wtr "iid")

        block-row-counts (ArrayList.)
        !block-row-count (AtomicInteger.)]

    (reify ILogIndexer2
      (startTx [_ tx-key]
        (.writeLong tx-id-wtr (.tx-id tx-key))
        (vw/write-value! (.system-time tx-key) system-time-wtr)

        (.startList tx-ops-wtr)
        (reify ILogOpIndexer2
          (logPut [_ iid row-id app-time-start app-time-end]
            (.startStruct put-wtr)
            (.writeBytes put-iid-wtr (ByteBuffer/wrap iid))
            (.writeLong put-row-id-wtr row-id)
            (.writeLong put-vf-wtr app-time-start)
            (.writeLong put-vt-wtr app-time-end)
            (.endStruct put-wtr))

          (logDelete [_ iid app-time-start app-time-end]
            (.startStruct delete-wtr)
            (.writeBytes delete-iid-wtr (ByteBuffer/wrap iid))
            (.writeLong delete-vf-wtr app-time-start)
            (.writeLong delete-vt-wtr app-time-end)
            (.endStruct delete-wtr))

          (logEvict [_ iid]
            (.startStruct evict-wtr)
            (.writeBytes evict-iid-wtr (ByteBuffer/wrap iid))
            (.endStruct evict-wtr))

          (commit [_]
            (.endList tx-ops-wtr)
            (.endRow transient-log-writer)
            (vw/append-rel log-writer (vw/rel-wtr->rdr transient-log-writer))

            (.clear transient-log-writer)
            (.getAndIncrement !block-row-count))

          (abort [_]
            (.clear tx-ops-wtr)
            (.writeNull tx-ops-wtr nil)
            (.endRow transient-log-writer)
            (vw/append-rel log-writer (vw/rel-wtr->rdr transient-log-writer))

            (.clear transient-log-writer)
            (.getAndIncrement !block-row-count))))

      (finishBlock [_]
        (.add block-row-counts (.getAndSet !block-row-count 0)))

      (finishChunk [_ chunk-idx]
        (.syncRowCount log-writer)

        (let [log-bytes (with-open [write-root (VectorSchemaRoot/create (.getSchema log-root) allocator)]
                          (let [loader (VectorLoader. write-root)]
                            (with-open [^ICursor slices (blocks/->slices log-root block-row-counts)]
                              (util/build-arrow-ipc-byte-buffer write-root :file
                                                                (fn [write-batch!]
                                                                  (.forEachRemaining slices
                                                                                     (reify Consumer
                                                                                       (accept [_ sliced-root]
                                                                                         (with-open [arb (.getRecordBatch (VectorUnloader. sliced-root))]
                                                                                           (.load loader arb)
                                                                                           (write-batch!))))))))))]

          (.putObject object-store (->log-obj-key chunk-idx) log-bytes)))

      (nextChunk [_]
        (.clear log-writer)
        (.clear block-row-counts))

      Closeable
      (close [_]
        (.close transcient-log-root)
        (.close log-root)))))

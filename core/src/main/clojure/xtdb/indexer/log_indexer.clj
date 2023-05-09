(ns xtdb.indexer.log-indexer
  (:require [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.blocks :as blocks]
            [xtdb.util :as util]
            [xtdb.vector :as vec]
            [xtdb.vector.writer :as vw])
  (:import (java.io Closeable)
           java.util.ArrayList
           java.util.concurrent.atomic.AtomicInteger
           (java.util.function Consumer)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector VectorLoader VectorSchemaRoot VectorUnloader)
           xtdb.ICursor
           xtdb.object_store.ObjectStore
           xtdb.vector.IVectorWriter))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILogOpIndexer
  (^void logPut [^long iid, ^long rowId, ^long app-timeStart, ^long app-timeEnd])
  (^void logDelete [^long iid, ^long app-timeStart, ^long app-timeEnd])
  (^void logEvict [^long iid])
  (^void commit [])
  (^void abort []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface ILogIndexer
  (^xtdb.indexer.log_indexer.ILogOpIndexer startTx [^xtdb.api.TransactionInstant txKey])
  (^void finishBlock [])
  (^java.util.concurrent.CompletableFuture finishChunk [^long chunkIdx])
  (^void nextChunk [])
  (^void close []))

(def ^:private log-ops-col-type
  '[:union #{:null
             [:list
              [:struct {iid :i64
                        row-id [:union #{:null :i64}]
                        application-time-start [:timestamp-tz :micro "UTC"]
                        application-time-end [:timestamp-tz :micro "UTC"]
                        evict? :bool}]]}])

(defn- ->log-obj-key [chunk-idx]
  (format "chunk-%s/log.arrow" (util/->lex-hex-string chunk-idx)))

(defmethod ig/prep-key :xtdb.indexer/log-indexer [_ opts]
  (merge {:allocator (ig/ref :xtdb/allocator)
          :object-store (ig/ref :xtdb/object-store)}
         opts))

(defmethod ig/init-key :xtdb.indexer/log-indexer [_ {:keys [^BufferAllocator allocator, ^ObjectStore object-store]}]
  (let [log-writer (vw/->rel-writer allocator)
        transient-log-writer (vw/->rel-writer allocator)

        tx-id-wtr (.writerForName transient-log-writer "tx-id" :i64)

        system-time-wtr (.writerForName transient-log-writer "system-time" [:timestamp-tz :micro "UTC"])

        ops-wtr (.writerForName transient-log-writer "ops" log-ops-col-type)
        op-wtr (.listElementWriter ops-wtr)

        row-id-wtr (.structKeyWriter op-wtr "row-id")
        iid-wtr (.structKeyWriter op-wtr "iid")

        valid-time-start-wtr (.structKeyWriter op-wtr "application-time-start")
        valid-time-end-wtr (.structKeyWriter op-wtr "application-time-end")

        evict-wtr (.structKeyWriter op-wtr "evict?")

        block-row-counts (ArrayList.)
        !block-row-count (AtomicInteger.)]

    (reify ILogIndexer
      (startTx [_ tx-key]
        (.writeLong tx-id-wtr (.tx-id tx-key))
        (vw/write-value! (.system-time tx-key) system-time-wtr)

        (.startList ops-wtr)
        (reify ILogOpIndexer
          (logPut [_ iid row-id app-time-start app-time-end]
            (.startStruct op-wtr)
            (.writeLong row-id-wtr row-id)
            (.writeLong iid-wtr iid)
            (.writeLong valid-time-start-wtr app-time-start)
            (.writeLong valid-time-end-wtr app-time-end)
            (.writeBoolean evict-wtr false)
            (.endStruct op-wtr))

          (logDelete [_ iid app-time-start app-time-end]
            (.startStruct op-wtr)
            (.writeNull row-id-wtr nil)
            (.writeLong iid-wtr iid)
            (.writeLong valid-time-start-wtr app-time-start)
            (.writeLong valid-time-end-wtr app-time-end)
            (.writeBoolean evict-wtr false)
            (.endStruct op-wtr))

          (logEvict [_ iid]
            (.startStruct op-wtr)
            (.writeNull row-id-wtr nil)
            (.writeLong iid-wtr iid)
            (.writeNull valid-time-start-wtr nil)
            (.writeNull valid-time-end-wtr nil)
            (.writeBoolean evict-wtr true)
            (.endStruct op-wtr))

          (commit [_]
            (.endList ops-wtr)
            (.endRow transient-log-writer)
            (vw/append-rel log-writer (vw/rel-wtr->rdr transient-log-writer))

            (.clear transient-log-writer)
            (.getAndIncrement !block-row-count))

          (abort [_]
            (.clear ops-wtr)
            (.writeNull ops-wtr nil)
            (.endRow transient-log-writer)
            (vw/append-rel log-writer (vw/rel-wtr->rdr transient-log-writer))

            (.clear transient-log-writer)
            (.getAndIncrement !block-row-count))))

      (finishBlock [_]
        (.add block-row-counts (.getAndSet !block-row-count 0)))

      (finishChunk [_ chunk-idx]
        (let [log-root (let [^Iterable vecs (for [^IVectorWriter w (seq log-writer)]
                                              (.getVector w))]
                         (VectorSchemaRoot. vecs))
              log-bytes (with-open [write-root (VectorSchemaRoot/create (.getSchema log-root) allocator)]
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
        (.close transient-log-writer)
        (.close log-writer)))))

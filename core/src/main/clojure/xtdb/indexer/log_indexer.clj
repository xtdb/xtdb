(ns xtdb.indexer.log-indexer
  (:require [xtdb.blocks :as blocks]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import xtdb.ICursor
           xtdb.object_store.ObjectStore
           (xtdb.vector IVectorWriter)
           (java.io Closeable)
           java.util.ArrayList
           (java.util.function Consumer)
           java.util.concurrent.atomic.AtomicInteger
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector BigIntVector BitVector TimeStampMicroTZVector VectorLoader VectorSchemaRoot VectorUnloader)
           (org.apache.arrow.vector.complex ListVector)) )

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

        ;; we're ignoring the writers for tx-id and sys-time, because they're simple primitive vecs and we're only writing to idx 0
        ^BigIntVector tx-id-vec (-> (.writerForName transient-log-writer "tx-id" :i64)
                                    (.getVector))

        ^TimeStampMicroTZVector sys-time-vec (-> (.writerForName transient-log-writer "system-time" [:timestamp-tz :micro "UTC"])
                                                 (.getVector))

        ops-writer (.asList (.writerForName transient-log-writer "ops" log-ops-col-type))
        ^ListVector ops-vec (.getVector ops-writer)
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
        ^BitVector evict-vec (.getVector evict-writer)

        block-row-counts (ArrayList.)
        !block-row-count (AtomicInteger.)]

    (reify ILogIndexer
      (startTx [_ tx-key]
        (.startValue ops-writer)
        (doto tx-id-vec
          (.setSafe 0 (.tx-id tx-key))
          (.setValueCount 1))
        (doto sys-time-vec
          (.setSafe 0 (util/instant->micros (.sys-time tx-key)))
          (.setValueCount 1))

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

          (commit [_]
            (.endValue ops-writer)
            (.setValueCount ops-vec 1)
            (vw/append-rel log-writer (vw/rel-writer->reader transient-log-writer))

            (.clear transient-log-writer)
            (.getAndIncrement !block-row-count))

          (abort [_]
            (.clear ops-vec)
            (.setNull ops-vec 0)
            (.setValueCount ops-vec 1)
            (vw/append-rel log-writer (vw/rel-writer->reader transient-log-writer))

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

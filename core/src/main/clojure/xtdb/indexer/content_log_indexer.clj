(ns xtdb.indexer.content-log-indexer
  (:require
   [juxt.clojars-mirrors.integrant.core :as ig]
   [xtdb.blocks :as blocks]
   [xtdb.types :as types]
   [xtdb.util :as util]
   [xtdb.vector.writer :as vw])
  (:import
   (java.io Closeable)
   (java.util ArrayList)
   (java.util.concurrent.atomic AtomicInteger)
   (java.util.function Consumer)
   (org.apache.arrow.memory BufferAllocator)
   (org.apache.arrow.vector VectorLoader VectorSchemaRoot VectorUnloader)
   (org.apache.arrow.vector.types.pojo Schema)
   (xtdb ICursor)
   (xtdb.object_store ObjectStore)))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IContentLogTx
  (^xtdb.vector.IRelationWriter contentWriter [])
  (^void commit [])
  (^void abort []))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IContentLog
  (^xtdb.indexer.content_log_indexer.IContentLogTx startTx [])

  (^void finishBlock [])
  (^java.util.concurrent.CompletableFuture finishChunk [^long chunkIdx])
  (^void nextChunk [])
  (^void close []))

(defn- ->log-obj-key [chunk-idx]
  (format "chunk-%s/content-log.arrow" (util/->lex-hex-string chunk-idx)))

(defmethod ig/prep-key :xtdb.indexer/content-log-indexer [_ opts]
  (merge {:allocator (ig/ref :xtdb/allocator)
          :object-store (ig/ref :xtdb/object-store)}
         opts))

(def content-log-schema
  (Schema. [(types/->field "documents" types/dense-union-type false)]))

(defn content-writer->table-writer ^xtdb.vector.IRelationWriter [^xtdb.vector.IRelationWriter content-wtr table-name]
  (->> (types/->field table-name types/struct-type false)
       (.writerForField (.writerForName content-wtr "documents"))
       (vw/struct-writer->rel-writer)))

(defmethod ig/init-key :xtdb.indexer/content-log-indexer [_ {:keys [^BufferAllocator allocator, ^ObjectStore object-store]}]
  (let [content-root (VectorSchemaRoot/create content-log-schema allocator)
        content-wtr (vw/root->writer content-root)
        transient-content-root (VectorSchemaRoot/create content-log-schema allocator)
        transient-content-wtr (vw/root->writer transient-content-root)

        block-row-counts (ArrayList.)
        !block-row-count (AtomicInteger.)
        transient-writer-pos (.writerPosition transient-content-wtr)]

    (reify IContentLog
      (startTx [_]
        (reify IContentLogTx
          (contentWriter [_] transient-content-wtr)

          (commit [_]
            (.syncRowCount transient-content-wtr)
            (when (pos? (.getPosition transient-writer-pos))
              (.syncSchema transient-content-root)
              (vw/append-rel content-wtr (vw/rel-wtr->rdr transient-content-wtr))

              (.addAndGet !block-row-count (.getPosition transient-writer-pos))
              (.clear transient-content-wtr)))

          (abort [_]
            (.clear transient-content-wtr))))

      (finishBlock [_]
        (let [current-row-count (.getAndSet !block-row-count 0)]
          (when (pos? current-row-count)
            (.add block-row-counts current-row-count))))

      (finishChunk [_ chunk-idx]
        (.syncRowCount content-wtr)
        (.syncSchema content-root)

        (let [content-bytes (with-open [write-root (VectorSchemaRoot/create (.getSchema content-root) allocator)]
                              (let [loader (VectorLoader. write-root)]
                                (with-open [^ICursor slices (blocks/->slices content-root block-row-counts)]
                                  (util/build-arrow-ipc-byte-buffer write-root :file
                                                                    (fn [write-batch!]
                                                                      (.forEachRemaining slices
                                                                                         (reify Consumer
                                                                                           (accept [_ sliced-root]
                                                                                             (with-open [arb (.getRecordBatch (VectorUnloader. sliced-root))]
                                                                                               (.load loader arb)
                                                                                               (write-batch!))))))))))]

          (.putObject object-store (->log-obj-key chunk-idx) content-bytes)))

      (nextChunk [_]
        (.clear content-wtr)
        (.clear block-row-counts))

      Closeable
      (close [_]
        (.close transient-content-root)
        (.close content-root)))))

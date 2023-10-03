(ns xtdb.indexer
  (:require [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [sci.core :as sci]
            [xtdb.await :as await]
            [xtdb.datalog :as d]
            [xtdb.error :as err]
            xtdb.indexer.live-index
            [xtdb.metadata :as meta]
            xtdb.object-store
            [xtdb.operator :as op]
            [xtdb.operator.scan :as scan]
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.sql :as sql]
            [xtdb.trie :as trie]
            [xtdb.tx-producer :as txp]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector :as vec]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw]
            [xtdb.watermark :as wm])
  (:import (java.io ByteArrayInputStream Closeable)
           java.lang.AutoCloseable
           java.nio.ByteBuffer
           (java.nio.channels ClosedByInterruptException)
           (java.time Instant ZoneId)
           (java.util.concurrent CompletableFuture PriorityBlockingQueue TimeUnit)
           (java.util.concurrent.locks StampedLock)
           (java.util.function Consumer)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector BitVector)
           (org.apache.arrow.vector.complex DenseUnionVector ListVector)
           (org.apache.arrow.vector.ipc ArrowStreamReader)
           (xtdb.api.protocols TransactionInstant)
           xtdb.types.ClojureForm
           (xtdb.indexer.live_index ILiveIndex ILiveIndexTx ILiveTableTx)
           xtdb.metadata.IMetadataManager
           xtdb.object_store.ObjectStore
           xtdb.operator.IRaQuerySource
           (xtdb.operator.scan IScanEmitter)
           xtdb.util.RowCounter
           (xtdb.vector IRowCopier IVectorReader RelationReader)
           (xtdb.watermark IWatermark IWatermarkSource)))

(set! *unchecked-math* :warn-on-boxed)

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IIndexer
  (^xtdb.api.protocols.TransactionInstant indexTx [^xtdb.api.protocols.TransactionInstant tx
                                                   ^org.apache.arrow.vector.VectorSchemaRoot txRoot])
  (^xtdb.api.protocols.TransactionInstant latestCompletedTx [])
  (^xtdb.api.protocols.TransactionInstant latestCompletedChunkTx [])
  (^java.util.concurrent.CompletableFuture #_<TransactionInstant> awaitTxAsync [^xtdb.api.protocols.TransactionInstant tx, ^java.time.Duration timeout])
  (^void forceFlush [^xtdb.api.protocols.TransactionInstant txKey ^long expected-last-chunk-tx-id])
  (^Throwable indexerError []))

(defprotocol Finish
  (^void finish-chunk! [_]))

(def ^:private abort-exn (err/runtime-err :abort-exn))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface OpIndexer
  (^org.apache.arrow.vector.complex.DenseUnionVector indexOp [^long tx-op-idx]
   "returns a tx-ops-vec of more operations (mostly for `:call`)"))

(defn- ->put-indexer ^xtdb.indexer.OpIndexer [^RowCounter row-counter, ^ILiveIndexTx live-idx-tx,
                                              ^IVectorReader tx-ops-rdr, ^Instant system-time]
  (let [put-leg (.legReader tx-ops-rdr :put)
        doc-rdr (.structKeyReader put-leg "document")
        valid-from-rdr (.structKeyReader put-leg "xt$valid_from")
        valid-to-rdr (.structKeyReader put-leg "xt$valid_to")
        system-time-µs (util/instant->micros system-time)
        tables (->> (.legs doc-rdr)
                    (mapv (fn [^IVectorReader table-rdr]
                            (let [table-name (.getName table-rdr)
                                  table-rel-rdr (vr/rel-reader (for [sk (.structKeys table-rdr)]
                                                                 (.structKeyReader table-rdr sk))
                                                               (.valueCount table-rdr))
                                  live-table (.liveTable live-idx-tx table-name)]
                              {:table-name table-name
                               :id-rdr (.structKeyReader table-rdr "xt$id")

                               :live-table live-table

                               :doc-copier (-> (.docWriter live-table)
                                               (vw/struct-writer->rel-copier table-rel-rdr))}))))]

    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [{:keys [^IVectorReader id-rdr, ^ILiveTableTx live-table, ^IRowCopier doc-copier]}
              (nth tables (.getTypeId doc-rdr tx-op-idx))

              eid (.getObject id-rdr tx-op-idx)

              valid-from (if (.isNull valid-from-rdr tx-op-idx)
                           system-time-µs
                           (.getLong valid-from-rdr tx-op-idx))
              valid-to (if (.isNull valid-to-rdr tx-op-idx)
                         util/end-of-time-μs
                         (.getLong valid-to-rdr tx-op-idx))]
          (when-not (> valid-to valid-from)
            (throw (err/runtime-err :xtdb.indexer/invalid-valid-times
                                    {:valid-from (util/micros->instant valid-from)
                                     :valid-to (util/micros->instant valid-to)})))

          (.logPut live-table (trie/->iid eid) valid-from valid-to #(.copyRow doc-copier tx-op-idx))
          (.addRows row-counter 1))

        nil))))

(defn- ->delete-indexer ^xtdb.indexer.OpIndexer [^RowCounter row-counter, ^ILiveIndexTx live-idx-tx, ^IVectorReader tx-ops-rdr, ^Instant current-time]
  (let [delete-leg (.legReader tx-ops-rdr :delete)
        table-rdr (.structKeyReader delete-leg "table")
        id-rdr (.structKeyReader delete-leg "xt$id")
        valid-from-rdr (.structKeyReader delete-leg "xt$valid_from")
        valid-to-rdr (.structKeyReader delete-leg "xt$valid_to")
        current-time-µs (util/instant->micros current-time)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [table (.getObject table-rdr tx-op-idx)
              eid (.getObject id-rdr tx-op-idx)
              valid-from (if (.isNull valid-from-rdr tx-op-idx)
                           current-time-µs
                           (.getLong valid-from-rdr tx-op-idx))
              valid-to (if (.isNull valid-to-rdr tx-op-idx)
                         util/end-of-time-μs
                         (.getLong valid-to-rdr tx-op-idx))]
          (when (> valid-from valid-to)
            (throw (err/runtime-err :xtdb.indexer/invalid-valid-times
                                    {:valid-from (util/micros->instant valid-from)
                                     :valid-to (util/micros->instant valid-to)})))

          (-> (.liveTable live-idx-tx table)
              (.logDelete (trie/->iid eid) valid-from valid-to))

          (.addRows row-counter 1))

        nil))))

(defn- ->evict-indexer ^xtdb.indexer.OpIndexer [^RowCounter row-counter, ^ILiveIndexTx live-idx-tx, ^IVectorReader tx-ops-rdr]

  (let [evict-leg (.legReader tx-ops-rdr :evict)
        table-rdr (.structKeyReader evict-leg "_table")
        id-rdr (.structKeyReader evict-leg "xt$id")]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [table (.getObject table-rdr tx-op-idx)
              eid (.getObject id-rdr tx-op-idx)]

          (-> (.liveTable live-idx-tx table)
              (.logEvict (trie/->iid eid)))

          (.addRows row-counter 1))

        nil))))

(defn- find-fn [allocator ^IRaQuerySource ra-src, wm-src, sci-ctx {:keys [basis default-tz]} fn-id]
  (let [lp '[:scan {:table xt/tx-fns} [{xt/id (= xt/id ?id)} xt/fn]]
        ^xtdb.operator.PreparedQuery pq (.prepareRaQuery ra-src lp)]
    (with-open [bq (.bind pq wm-src
                          {:params (vr/rel-reader [(-> (vw/open-vec allocator '?id [fn-id])
                                                       (vr/vec->reader))]
                                                  1)
                           :default-all-valid-time? false
                           :basis basis
                           :default-tz default-tz})
                res (.openCursor bq)]

      (let [!fn-doc (object-array 1)]
        (.tryAdvance res
                     (reify Consumer
                       (accept [_ in-rel]
                         (when (pos? (.rowCount ^RelationReader in-rel))
                           (aset !fn-doc 0 (first (vr/rel->rows in-rel)))))))

        (let [fn-doc (or (aget !fn-doc 0)
                         (throw (err/runtime-err :xtdb.call/no-such-tx-fn {:fn-id fn-id})))
              fn-body (:xt/fn fn-doc)]

          (when-not (instance? ClojureForm fn-body)
            (throw (err/illegal-arg :xtdb.call/invalid-tx-fn {:fn-doc fn-doc})))

          (let [fn-form (.form ^ClojureForm fn-body)]
            (try
              (sci/eval-form sci-ctx fn-form)

              (catch Throwable t
                (throw (err/runtime-err :xtdb.call/error-compiling-tx-fn {:fn-form fn-form} t))))))))))

(defn- tx-fn-q [allocator ra-src wm-src scan-emitter tx-opts]
  (fn tx-fn-q*
    ([q+args] (tx-fn-q* q+args {}))

    ([q+args opts]
     (let [[q args] (if (vector? q+args)
                      [(first q+args) (rest q+args)]
                      [q+args nil])]
       (with-open [res (d/open-datalog-query allocator ra-src wm-src scan-emitter q (-> (into tx-opts opts)
                                                                                        (assoc :args args)))]
         (vec (iterator-seq res)))))))

(defn- tx-fn-sql
  ([allocator, ^IRaQuerySource ra-src, wm-src tx-opts query]
   (tx-fn-sql allocator ra-src wm-src tx-opts query {}))

  ([allocator, ^IRaQuerySource ra-src, wm-src tx-opts query query-opts]
   (try
     (let [query-opts (into tx-opts query-opts)
           pq (.prepareRaQuery ra-src (sql/compile-query query query-opts))]
       (with-open [res (sql/open-sql-query allocator wm-src pq query-opts)]
         (vec (iterator-seq res))))
     (catch Throwable e
       (log/error e)
       (throw e)))))

(def ^:private !last-tx-fn-error (atom nil))

(defn reset-tx-fn-error! []
  (first (reset-vals! !last-tx-fn-error nil)))

(defn- ->call-indexer ^xtdb.indexer.OpIndexer [allocator, ra-src, wm-src, scan-emitter
                                               ^IVectorReader tx-ops-rdr, {:keys [tx-key] :as tx-opts}]
  (let [call-leg (.legReader tx-ops-rdr :call)
        fn-id-rdr (.structKeyReader call-leg "fn-id")
        args-rdr (.structKeyReader call-leg "args")

        ;; TODO confirm/expand API that we expose to tx-fns
        sci-ctx (sci/init {:bindings {'q (tx-fn-q allocator ra-src wm-src scan-emitter tx-opts)
                                      'sql-q (partial tx-fn-sql allocator ra-src wm-src tx-opts)
                                      'sleep (fn [n] (Thread/sleep n))
                                      '*current-tx* tx-key}})]

    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (try
          (let [fn-id (.getObject fn-id-rdr tx-op-idx)
                tx-fn (find-fn allocator ra-src wm-src (sci/fork sci-ctx) tx-opts fn-id)
                args (.getObject args-rdr tx-op-idx)

                res (try
                      (sci/binding [sci/out *out*
                                    sci/in *in*]
                        (apply tx-fn args))

                      (catch InterruptedException ie (throw ie))
                      (catch Throwable t
                        (log/warn t "unhandled error evaluating tx fn")
                        (throw (err/runtime-err :xtdb.call/error-evaluating-tx-fn
                                                {:fn-id fn-id, :args args}
                                                t))))]
            (when (false? res)
              (throw abort-exn))

            ;; if the user returns `nil` or `true`, we just continue with the rest of the transaction
            (when-not (or (nil? res) (true? res))
              (util/with-close-on-catch [tx-ops-vec (txp/open-tx-ops-vec allocator)]
                (txp/write-tx-ops! allocator (vw/->writer tx-ops-vec) res)
                (.setValueCount tx-ops-vec (count res))
                tx-ops-vec)))

          (catch Throwable t
            (reset! !last-tx-fn-error t)
            (throw t)))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface SqlOpIndexer
  (^void indexOp [^xtdb.vector.RelationReader inRelation, queryOpts]))

(defn- ->sql-upsert-indexer ^xtdb.indexer.SqlOpIndexer [^RowCounter row-counter, ^ILiveIndexTx live-idx-tx
                                                        {{:keys [^Instant current-time]} :basis}]

  (let [current-time-µs (util/instant->micros current-time)]
    (reify SqlOpIndexer
      (indexOp [_ in-rel {:keys [table]}]
        (let [row-count (.rowCount in-rel)
              content-rel (vr/rel-reader (->> in-rel
                                              (remove (comp types/temporal-column? #(.getName ^IVectorReader %)))
                                              (map (fn [^IVectorReader vec]
                                                     (.withName vec (util/str->normal-form-str (.getName vec))))))
                                         (.rowCount in-rel))
              table (util/str->normal-form-str table)
              id-col (.readerForName in-rel "xt$id")
              valid-from-rdr (.readerForName in-rel "xt$valid_from")
              valid-to-rdr (.readerForName in-rel "xt$valid_to")

              live-idx-table (.liveTable live-idx-tx table)
              live-idx-table-copier (-> (.docWriter live-idx-table)
                                        (vw/struct-writer->rel-copier content-rel))]

          (dotimes [idx row-count]
            (let [eid (.getObject id-col idx)
                  valid-from (if (and valid-from-rdr (not (.isNull valid-from-rdr idx)))
                               (.getLong valid-from-rdr idx)
                               current-time-µs)
                  valid-to (if (and valid-to-rdr (not (.isNull valid-to-rdr idx)))
                             (.getLong valid-to-rdr idx)
                             util/end-of-time-μs)]
              (when (> valid-from valid-to)
                (throw (err/runtime-err :xtdb.indexer/invalid-valid-times
                                        {:valid-from (util/micros->instant valid-from)
                                         :valid-to (util/micros->instant valid-to)})))

              (.logPut live-idx-table (trie/->iid eid) valid-from valid-to #(.copyRow live-idx-table-copier idx))))

          (.addRows row-counter row-count))))))

(defn- ->sql-delete-indexer ^xtdb.indexer.SqlOpIndexer [^RowCounter row-counter, ^ILiveIndexTx live-idx-tx]
  (reify SqlOpIndexer
    (indexOp [_ in-rel {:keys [table]}]
      (let [table (util/str->normal-form-str table)
            row-count (.rowCount in-rel)
            iid-rdr (.readerForName in-rel "xt$iid")
            valid-from-rdr (.readerForName in-rel "xt$valid_from")
            valid-to-rdr (.readerForName in-rel "xt$valid_to")]
        (dotimes [idx row-count]
          (let [iid (.getBytes iid-rdr idx)
                valid-from (.getLong valid-from-rdr idx)
                valid-to (.getLong valid-to-rdr idx)]
            (when (> valid-from valid-to)
              (throw (err/runtime-err :xtdb.indexer/invalid-valid-times
                                      {:valid-from (util/micros->instant valid-from)
                                       :valid-to (util/micros->instant valid-to)})))

            (-> (.liveTable live-idx-tx table)
                (.logDelete iid valid-from valid-to))))

        (.addRows row-counter row-count)))))

(defn- ->sql-erase-indexer ^xtdb.indexer.SqlOpIndexer [^RowCounter row-counter, ^ILiveIndexTx live-idx-tx]
  (reify SqlOpIndexer
    (indexOp [_ in-rel {:keys [table]}]
      (let [table (util/str->normal-form-str table)
            row-count (.rowCount in-rel)
            iid-rdr (.readerForName in-rel "xt$iid")]
        (dotimes [idx row-count]
          (let [iid (.getBytes iid-rdr idx)]
            (-> (.liveTable live-idx-tx table)
                (.logEvict iid))))

        (.addRows row-counter row-count)))))

(defn- ->sql-indexer ^xtdb.indexer.OpIndexer [^BufferAllocator allocator, ^RowCounter row-counter, ^ILiveIndexTx live-idx-tx
                                              ^IVectorReader tx-ops-rdr, ^IRaQuerySource ra-src, wm-src, ^IScanEmitter scan-emitter
                                              {:keys [default-all-valid-time? basis default-tz] :as tx-opts}]
  (let [sql-leg (.legReader tx-ops-rdr :sql)
        query-rdr (.structKeyReader sql-leg "query")
        params-rdr (.structKeyReader sql-leg "params")
        upsert-idxer (->sql-upsert-indexer row-counter live-idx-tx tx-opts)
        delete-idxer (->sql-delete-indexer row-counter live-idx-tx)
        erase-idxer (->sql-erase-indexer row-counter live-idx-tx)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (letfn [(index-op [^SqlOpIndexer op-idxer {:keys [all-app-time] :as query-opts} inner-query]
                  (let [^xtdb.operator.PreparedQuery pq (.prepareRaQuery ra-src inner-query)]
                    (letfn [(index-op* [^RelationReader params]
                              (with-open [res (-> (.bind pq wm-src {:params params, :basis basis, :default-tz default-tz
                                                                    :default-all-valid-time? (or all-app-time
                                                                                                 default-all-valid-time?)})
                                                  (.openCursor))]

                                (.forEachRemaining res
                                                   (reify Consumer
                                                     (accept [_ in-rel]
                                                       (.indexOp op-idxer in-rel query-opts))))))]
                      (if (.isNull params-rdr tx-op-idx)
                        (index-op* nil)

                        (with-open [is (ByteArrayInputStream. (.array ^ByteBuffer (.getObject params-rdr tx-op-idx))) ; could try to use getBytes
                                    asr (ArrowStreamReader. is allocator)]
                          (let [root (.getVectorSchemaRoot asr)]
                            (while (.loadNextBatch asr)
                              (let [rel (vr/<-root root)

                                    ^RelationReader
                                    rel (vr/rel-reader (->> rel
                                                            (map-indexed (fn [idx ^IVectorReader col]
                                                                           (.withName col (str "?_" idx)))))
                                                       (.rowCount rel))

                                    selection (int-array 1)]
                                (dotimes [idx (.rowCount rel)]
                                  (aset selection 0 idx)
                                  (index-op* (-> rel (.select selection))))))))))))]

          (let [query-str (.getObject query-rdr tx-op-idx)
                tables-with-cols (scan/tables-with-cols (:basis tx-opts) wm-src scan-emitter)]
            ;; TODO handle error
            (zmatch (sql/compile-query query-str (assoc tx-opts :table-info tables-with-cols))
              [:insert query-opts inner-query]
              (index-op upsert-idxer query-opts inner-query)

              [:update query-opts inner-query]
              (index-op upsert-idxer query-opts inner-query)

              [:delete query-opts inner-query]
              (index-op delete-idxer query-opts inner-query)

              [:erase query-opts inner-query]
              (index-op erase-idxer (assoc query-opts :all-app-time true) inner-query)

              (throw (UnsupportedOperationException. "sql query")))))

        nil))))

(def ^:private ^:const ^String txs-table
  "xt$txs")

(defn- add-tx-row! [^RowCounter row-counter, ^ILiveIndexTx live-idx-tx, ^TransactionInstant tx-key, ^Throwable t]
  (let [tx-id (.tx-id tx-key)
        system-time-µs (util/instant->micros (.system-time tx-key))

        live-table (.liveTable live-idx-tx txs-table)
        doc-writer (.docWriter live-table)]

    (.logPut live-table (trie/->iid tx-id) system-time-µs util/end-of-time-μs
             (fn write-doc! []
               (.startStruct doc-writer)
               (doto (.structKeyWriter doc-writer "xt$id" :i64)
                 (.writeLong tx-id))

               (doto (.structKeyWriter doc-writer "xt$tx_time" types/temporal-col-type)
                 (.writeLong system-time-µs))

               (doto (.structKeyWriter doc-writer "xt$committed?" :bool)
                 (.writeBoolean (nil? t)))

               (let [e-wtr (.structKeyWriter doc-writer "xt$error" [:union #{:null :clj-form}])]
                 (if (or (nil? t) (= t abort-exn))
                   (doto (.writerForType e-wtr :null)
                     (.writeNull nil))
                   (doto (.writerForType e-wtr :clj-form)
                     (.writeObject (pr-str t)))))
               (.endStruct doc-writer)))

    (.addRows row-counter 1)))

(deftype Indexer [^BufferAllocator allocator
                  ^ObjectStore object-store
                  ^IMetadataManager metadata-mgr
                  ^IScanEmitter scan-emitter
                  ^IRaQuerySource ra-src
                  ^ILiveIndex live-idx

                  ^:volatile-mutable indexer-error

                  ^:volatile-mutable ^TransactionInstant latest-completed-tx
                  ^:volatile-mutable ^TransactionInstant latest-completed-chunk-tx
                  ^PriorityBlockingQueue awaiters

                  ^RowCounter row-counter
                  ^long rows-per-chunk

                  ^:volatile-mutable ^IWatermark shared-wm
                  ^StampedLock wm-lock]

  IIndexer
  (indexTx [this {:keys [system-time] :as tx-key} tx-root]
    (try
      (if (and (not (nil? latest-completed-tx))
               (neg? (compare system-time
                              (.system-time latest-completed-tx))))
        (do
          (log/warnf "specified system-time '%s' older than current tx '%s'"
                     (pr-str tx-key)
                     (pr-str latest-completed-tx))

          (let [live-idx-tx (.startTx live-idx tx-key)]
            (add-tx-row! row-counter live-idx-tx tx-key
                         (err/illegal-arg :invalid-system-time
                                          {::err/message "specified system-time older than current tx"
                                           :tx-key tx-key
                                           :latest-completed-tx latest-completed-tx}))
            (.commit live-idx-tx)))

        (util/with-open [live-idx-tx (.startTx live-idx tx-key)]
          (let [^DenseUnionVector tx-ops-vec (-> ^ListVector (.getVector tx-root "tx-ops")
                                                 (.getDataVector))

                wm-src (reify IWatermarkSource
                         (openWatermark [_ _tx]
                           (wm/->wm nil (.openWatermark live-idx-tx))))

                tx-opts {:basis {:tx tx-key, :current-time system-time}
                         :default-tz (ZoneId/of (str (-> (.getVector tx-root "default-tz")
                                                         (.getObject 0))))
                         :default-all-valid-time? (== 1 (-> ^BitVector (.getVector tx-root "all-application-time?")
                                                            (.get 0)))
                         :tx-key tx-key}]

            (letfn [(index-tx-ops [^DenseUnionVector tx-ops-vec]
                      (let [tx-ops-rdr (vr/vec->reader tx-ops-vec)
                            !put-idxer (delay (->put-indexer row-counter live-idx-tx tx-ops-rdr system-time))
                            !delete-idxer (delay (->delete-indexer row-counter live-idx-tx tx-ops-rdr system-time))
                            !evict-idxer (delay (->evict-indexer row-counter live-idx-tx tx-ops-rdr))
                            !call-idxer (delay (->call-indexer allocator ra-src wm-src scan-emitter tx-ops-rdr tx-opts))
                            !sql-idxer (delay (->sql-indexer allocator row-counter live-idx-tx tx-ops-rdr ra-src wm-src scan-emitter tx-opts))]
                        (dotimes [tx-op-idx (.valueCount tx-ops-rdr)]
                          (when-let [more-tx-ops (case (.getTypeId tx-ops-rdr tx-op-idx)
                                                   0 (.indexOp ^OpIndexer @!sql-idxer tx-op-idx)
                                                   1 (.indexOp ^OpIndexer @!put-idxer tx-op-idx)
                                                   2 (.indexOp ^OpIndexer @!delete-idxer tx-op-idx)
                                                   3 (.indexOp ^OpIndexer @!evict-idxer tx-op-idx)
                                                   4 (.indexOp ^OpIndexer @!call-idxer tx-op-idx)
                                                   5 (throw abort-exn))]
                            (try
                              (index-tx-ops more-tx-ops)
                              (finally
                                (util/try-close more-tx-ops)))))))]
              (let [e (try
                        (index-tx-ops tx-ops-vec)
                        (catch xtdb.RuntimeException e e)
                        (catch xtdb.IllegalArgumentException e e)
                        (catch ClosedByInterruptException e
                          (throw (InterruptedException. (.toString e))))
                        (catch InterruptedException e
                          (throw e))
                        (catch Throwable t
                          (log/error t "error in indexer")
                          (throw t)))
                    wm-lock-stamp (.writeLock wm-lock)]
                (try
                  (if e
                    (do
                      (when (not= e abort-exn)
                        (log/debug e "aborted tx")
                        (.abort live-idx-tx))

                      (let [live-idx-tx (.startTx live-idx tx-key)]
                        (add-tx-row! row-counter live-idx-tx tx-key e)
                        (.commit live-idx-tx)))

                    (do
                      (add-tx-row! row-counter live-idx-tx tx-key nil)

                      (.commit live-idx-tx)))

                  (set! (.-latest-completed-tx this) tx-key)

                  (finally
                    (.unlock wm-lock wm-lock-stamp)))))

            (when (>= (.getChunkRowCount row-counter) rows-per-chunk)
              (finish-chunk! this))

            tx-key)))

      (await/notify-tx tx-key awaiters)

      (catch Throwable t
        (set! (.indexer-error this) t)
        (await/notify-ex t awaiters)
        (throw t))))

  (forceFlush [this tx-key expected-last-chunk-tx-id]
    (when (= (:tx-id latest-completed-chunk-tx -1) expected-last-chunk-tx-id)
      (finish-chunk! this))

    (set! (.-latest_completed_tx this) tx-key))

  IWatermarkSource
  (openWatermark [this tx-key]
    (letfn [(maybe-existing-wm []
              (when-let [^IWatermark wm (.shared-wm this)]
                (let [wm-tx-key (.txBasis wm)]
                  (when (or (nil? tx-key)
                            (and wm-tx-key
                                 (<= (.tx-id tx-key) (.tx-id wm-tx-key))))
                    (doto wm .retain)))))]
      (or (let [wm-lock-stamp (.readLock wm-lock)]
            (try
              (maybe-existing-wm)
              (finally
                (.unlock wm-lock wm-lock-stamp))))

          (let [wm-lock-stamp (.writeLock wm-lock)]
            (try
              (or (maybe-existing-wm)
                  (let [^IWatermark old-wm (.shared-wm this)]
                    (try
                      (let [^IWatermark shared-wm (wm/->wm latest-completed-tx (.openWatermark live-idx))]
                        (set! (.shared-wm this) shared-wm)
                        (doto shared-wm .retain))
                      (finally
                        (some-> old-wm .close)))))

              (finally
                (.unlock wm-lock wm-lock-stamp)))))))

  (latestCompletedTx [_] latest-completed-tx)
  (latestCompletedChunkTx [_] latest-completed-chunk-tx)

  (awaitTxAsync [this tx timeout]
    (-> (if tx
          (await/await-tx-async tx
                                #(or (some-> indexer-error throw)
                                     (.-latest-completed-tx this))
                                awaiters)
          (CompletableFuture/completedFuture latest-completed-tx))
        (cond-> timeout (.orTimeout (.toMillis timeout) TimeUnit/MILLISECONDS))))

  Finish
  (finish-chunk! [this]
    (let [chunk-idx (.getChunkIdx row-counter)
          next-chunk-idx (+ chunk-idx (.getChunkRowCount row-counter))
          table-metadata (.finishChunk live-idx chunk-idx next-chunk-idx)]

      (.finishChunk metadata-mgr chunk-idx
                    {:latest-completed-tx latest-completed-tx
                     :next-chunk-idx next-chunk-idx
                     :tables table-metadata})

      (.nextChunk row-counter)
      (set! (.-latest_completed_chunk_tx this) latest-completed-tx)

    (let [wm-lock-stamp (.writeLock wm-lock)]
      (try
        (.nextChunk live-idx)
        (when-let [^IWatermark shared-wm (.shared-wm this)]
          (set! (.shared-wm this) nil)
          (.close shared-wm))

          (finally
            (.unlock wm-lock wm-lock-stamp))))

      (log/debugf "finished chunk 'rf%s-nr%s'." (util/->lex-hex-string chunk-idx) (util/->lex-hex-string next-chunk-idx))))

  Closeable
  (close [_]
    (util/close allocator)
    (some-> shared-wm .close)))

(defmethod ig/prep-key :xtdb/indexer [_ opts]
  (merge {:allocator (ig/ref :xtdb/allocator)
          :object-store (ig/ref :xtdb/object-store)
          :metadata-mgr (ig/ref ::meta/metadata-manager)
          :scan-emitter (ig/ref :xtdb.operator.scan/scan-emitter)
          :live-index (ig/ref :xtdb.indexer/live-index)
          :ra-src (ig/ref ::op/ra-query-source)
          :rows-per-chunk 102400}
         opts))

(defmethod ig/init-key :xtdb/indexer
  [_ {:keys [allocator object-store metadata-mgr scan-emitter, ra-src, live-index, rows-per-chunk]}]

  (let [{:keys [latest-completed-tx next-chunk-idx], :or {next-chunk-idx 0}} (meta/latest-chunk-metadata metadata-mgr)]
    (util/with-close-on-catch [allocator (util/->child-allocator allocator "indexer")]
      (->Indexer allocator object-store metadata-mgr scan-emitter ra-src live-index

                 nil ;; indexer-error

                 latest-completed-tx
                 latest-completed-tx
                 (PriorityBlockingQueue.)

                 (RowCounter. next-chunk-idx)
                 rows-per-chunk

                 nil ;; watermark
                 (StampedLock.)))))

(defmethod ig/halt-key! :xtdb/indexer [_ ^AutoCloseable indexer]
  (.close indexer))

(ns xtdb.indexer
  (:require [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [sci.core :as sci]
            [xtdb.api :as xt]
            [xtdb.await :as await]
            [xtdb.error :as err]
            [xtdb.indexer.live-index :as li]
            [xtdb.log :as xt-log]
            [xtdb.metrics :as metrics]
            [xtdb.operator.scan :as scan]
            [xtdb.query :as q]
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.serde :as serde]
            [xtdb.sql :as sql]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.tx-ops :as tx-ops]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw]
            [xtdb.xtql :as xtql]
            [xtdb.protocols :as xtp])
  (:import (clojure.lang MapEntry)
           (io.micrometer.core.instrument Timer)
           (java.io ByteArrayInputStream Closeable)
           java.nio.ByteBuffer
           (java.nio.channels ClosedByInterruptException)
           (java.time Instant ZoneId)
           (java.util.concurrent CompletableFuture PriorityBlockingQueue TimeUnit)
           (java.util.function Consumer IntPredicate)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector BitVector)
           (org.apache.arrow.vector.complex DenseUnionVector ListVector)
           (org.apache.arrow.vector.ipc ArrowStreamReader)
           (org.apache.arrow.vector.types.pojo FieldType)
           xtdb.api.TransactionKey
           (xtdb.api.tx TxOp TxOp$XtqlOp)
           (xtdb.indexer.live_index ILiveIndex ILiveIndexTx ILiveTableTx)
           (xtdb.operator.scan IScanEmitter)
           (xtdb.query IQuerySource PreparedQuery)
           xtdb.types.ClojureForm
           (xtdb.vector IRowCopier IVectorReader RelationReader)
           (xtdb.watermark IWatermarkSource Watermark)))

(set! *unchecked-math* :warn-on-boxed)

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IIndexer
  (^xtdb.api.TransactionKey indexTx [^xtdb.api.TransactionKey tx
                                     ^org.apache.arrow.vector.VectorSchemaRoot txRoot])

  (^xtdb.api.TransactionKey latestCompletedTx [])
  (^xtdb.api.TransactionKey latestCompletedChunkTx [])

  (^xtdb.api.TransactionKey awaitTx [^xtdb.api.TransactionKey tx, ^java.time.Duration timeout])
  (^void forceFlush [^xtdb.api.TransactionKey txKey ^long expected-last-chunk-tx-id])
  (^Throwable indexerError []))

(def ^:private abort-exn (err/runtime-err :abort-exn))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface OpIndexer
  (^org.apache.arrow.vector.complex.DenseUnionVector indexOp [^long tx-op-idx]
   "returns a tx-ops-vec of more operations (mostly for `:call`)"))

(defn- ->put-docs-indexer ^xtdb.indexer.OpIndexer [^ILiveIndexTx live-idx-tx,
                                                   ^IVectorReader tx-ops-rdr, ^Instant system-time]
  (let [put-leg (.legReader tx-ops-rdr :put-docs)
        iids-rdr (.structKeyReader put-leg "iids")
        iid-rdr (.listElementReader iids-rdr)
        docs-rdr (.structKeyReader put-leg "documents")
        valid-from-rdr (.structKeyReader put-leg "xt$valid_from")
        valid-to-rdr (.structKeyReader put-leg "xt$valid_to")
        system-time-µs (time/instant->micros system-time)
        tables (->> (.legs docs-rdr)
                    (into {} (map (fn [table]
                                    (let [table-name (str (symbol table))
                                          table-docs-rdr (.legReader docs-rdr table)
                                          doc-rdr (.listElementReader table-docs-rdr)
                                          ^RelationReader table-rel-rdr (vr/rel-reader (for [sk (.structKeys doc-rdr)]
                                                                                         (.structKeyReader doc-rdr sk))
                                                                                       (.valueCount doc-rdr))
                                          live-table (.liveTable live-idx-tx table-name)]
                                      (MapEntry/create table
                                                       {:id-rdr (.structKeyReader doc-rdr "xt$id")

                                                        :live-table live-table

                                                        :docs-rdr table-docs-rdr

                                                        :doc-copier (-> (.docWriter live-table)
                                                                        (.rowCopier table-rel-rdr))}))))))]

    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [{:keys [^IVectorReader docs-rdr, ^IVectorReader id-rdr, ^ILiveTableTx live-table, ^IRowCopier doc-copier]}
              (get tables (.getLeg docs-rdr tx-op-idx))

              valid-from (if (.isNull valid-from-rdr tx-op-idx)
                           system-time-µs
                           (.getLong valid-from-rdr tx-op-idx))
              valid-to (if (.isNull valid-to-rdr tx-op-idx)
                         Long/MAX_VALUE
                         (.getLong valid-to-rdr tx-op-idx))]
          (when-not (> valid-to valid-from)
            (throw (err/runtime-err :xtdb.indexer/invalid-valid-times
                                    {:valid-from (time/micros->instant valid-from)
                                     :valid-to (time/micros->instant valid-to)})))

          (let [doc-start-idx (.getListStartIndex docs-rdr tx-op-idx)
                ^long iid-start-idx (or (some-> iids-rdr (.getListStartIndex tx-op-idx))
                                        Long/MIN_VALUE)]
            (dotimes [row-idx (.getListCount docs-rdr tx-op-idx)]
              (let [doc-idx (+ doc-start-idx row-idx)]
                (.logPut live-table
                         (if iid-rdr
                           (.getBytes iid-rdr (+ iid-start-idx row-idx))
                           (trie/->iid (.getObject id-rdr doc-idx)))
                         valid-from valid-to
                         #(.copyRow doc-copier doc-idx))))))

        nil))))

(defn- ->delete-docs-indexer ^xtdb.indexer.OpIndexer [^ILiveIndexTx live-idx-tx, ^IVectorReader tx-ops-rdr, ^Instant current-time]
  (let [delete-leg (.legReader tx-ops-rdr :delete-docs)
        table-rdr (.structKeyReader delete-leg "table")
        iids-rdr (.structKeyReader delete-leg "iids")
        iid-rdr (.listElementReader iids-rdr)
        valid-from-rdr (.structKeyReader delete-leg "xt$valid_from")
        valid-to-rdr (.structKeyReader delete-leg "xt$valid_to")
        current-time-µs (time/instant->micros current-time)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [table (.getObject table-rdr tx-op-idx)
              live-table (.liveTable live-idx-tx table)
              valid-from (if (.isNull valid-from-rdr tx-op-idx)
                           current-time-µs
                           (.getLong valid-from-rdr tx-op-idx))
              valid-to (if (.isNull valid-to-rdr tx-op-idx)
                         Long/MAX_VALUE
                         (.getLong valid-to-rdr tx-op-idx))]
          (when (> valid-from valid-to)
            (throw (err/runtime-err :xtdb.indexer/invalid-valid-times
                                    {:valid-from (time/micros->instant valid-from)
                                     :valid-to (time/micros->instant valid-to)})))

          (let [iids-start-idx (.getListStartIndex iids-rdr tx-op-idx)]
            (dotimes [iid-idx (.getListCount iids-rdr tx-op-idx)]
              (.logDelete live-table (.getBytes iid-rdr (+ iids-start-idx iid-idx))
                          valid-from valid-to))))

        nil))))

(defn- ->erase-docs-indexer ^xtdb.indexer.OpIndexer [^ILiveIndexTx live-idx-tx, ^IVectorReader tx-ops-rdr]
  (let [erase-leg (.legReader tx-ops-rdr :erase-docs)
        table-rdr (.structKeyReader erase-leg "table")
        iids-rdr (.structKeyReader erase-leg "iids")
        iid-rdr (.listElementReader iids-rdr)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [table (.getObject table-rdr tx-op-idx)
              live-table (.liveTable live-idx-tx table)

              iids-start-idx (.getListStartIndex iids-rdr tx-op-idx)]
          (dotimes [iid-idx (.getListCount iids-rdr tx-op-idx)]
            (.logErase live-table (.getBytes iid-rdr (+ iids-start-idx iid-idx)))))

        nil))))

(defn- find-fn [allocator ^IQuerySource q-src, wm-src, sci-ctx {:keys [basis default-tz]} fn-iid]
  (let [lp '[:scan {:table xt$tx_fns} [{xt$iid (= xt$iid ?iid)} xt$id xt$fn]]
        ^xtdb.query.PreparedQuery pq (.prepareRaQuery q-src lp wm-src)]
    (with-open [bq (.bind pq
                          {:params (vr/rel-reader [(-> (vw/open-vec allocator '?iid [fn-iid])
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

        (let [{fn-id :xt/id, fn-body :xt/fn, :as fn-doc} (or (aget !fn-doc 0)
                                                             (throw (err/runtime-err :xtdb.call/no-such-tx-fn
                                                                                     {:fn-iid (util/byte-buffer->uuid fn-iid)})))]

          (when-not (instance? ClojureForm fn-body)
            (throw (err/illegal-arg :xtdb.call/invalid-tx-fn {:fn-doc (dissoc fn-doc :xt/iid)})))

          (let [fn-form (.form ^ClojureForm fn-body)]
            (try
              {:tx-fn (sci/eval-form sci-ctx fn-form)
               :fn-id fn-id}

              (catch Throwable t
                (throw (err/runtime-err :xtdb.call/error-compiling-tx-fn {:fn-form fn-form} t))))))))))

(defn- tx-fn-q [^IQuerySource q-src wm-src tx-opts]
  (fn tx-fn-q*
    ([query] (tx-fn-q* query {}))

    ([query opts]
     (let [query-opts (-> (reduce into [{:key-fn :kebab-case-keyword} tx-opts opts])
                          (update :key-fn serde/read-key-fn))
           prepared-query (.prepareRaQuery q-src (.planQuery q-src query wm-src query-opts) wm-src)]

       (with-open [res (-> (.bind prepared-query query-opts)
                           (q/open-cursor-as-stream query-opts))]
         (vec (.toList res)))))))

(def ^:private !last-tx-fn-error (atom nil))

(defn reset-tx-fn-error! []
  (first (reset-vals! !last-tx-fn-error nil)))

(def ^:private xt-sci-ns
  (-> (sci/copy-ns xtdb.api (sci/create-ns 'xtdb.api))
      (select-keys ['put 'put-fn
                    'during 'starting-at 'until])))

(defn- ->call-indexer ^xtdb.indexer.OpIndexer [allocator, q-src, wm-src
                                               ^IVectorReader tx-ops-rdr, {:keys [tx-key] :as tx-opts}]
  (let [call-leg (.legReader tx-ops-rdr :call)
        fn-id-rdr (.structKeyReader call-leg "fn-id")
        fn-iid-rdr (.structKeyReader call-leg "fn-iid")
        args-rdr (.structKeyReader call-leg "args")

        ;; TODO confirm/expand API that we expose to tx-fns
        sci-ctx (sci/init {:bindings {'q (tx-fn-q q-src wm-src tx-opts)
                                      'sleep (fn [^long n] (Thread/sleep n))
                                      '*current-tx* tx-key}
                           :namespaces {'xt xt-sci-ns}})]

    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (try
          (let [fn-iid (if fn-iid-rdr
                         (.getBytes fn-iid-rdr tx-op-idx)
                         (trie/->iid (.getObject fn-id-rdr tx-op-idx)))
                {:keys [fn-id tx-fn]} (find-fn allocator q-src wm-src (sci/fork sci-ctx) tx-opts fn-iid)
                args (.form ^ClojureForm (.getObject args-rdr tx-op-idx))

                res (try
                      (sci/binding [sci/out *out*
                                    sci/in *in*]
                        (let [res (apply tx-fn args)]
                          (cond->> res
                            (seqable? res) (mapv (fn [tx-op]
                                                   (cond-> tx-op
                                                     (not (instance? TxOp tx-op)) tx-ops/parse-tx-op))))))
                      (catch InterruptedException ie (throw ie))
                      (catch xtdb.IllegalArgumentException e
                        (log/warn e "unhandled error evaluating tx fn")
                        (throw e))
                      (catch xtdb.RuntimeException e
                        (log/warn e "unhandled error evaluating tx fn")
                        (throw e))
                      (catch Throwable t
                        (log/warn t "unhandled error evaluating tx fn")
                        (throw (err/runtime-err :xtdb.call/error-evaluating-tx-fn
                                                {:fn-id fn-id, :args args}
                                                t))))]
            (when (false? res)
              (throw abort-exn))

            ;; if the user returns `nil` or `true`, we just continue with the rest of the transaction
            (when-not (or (nil? res) (true? res))
              (util/with-close-on-catch [tx-ops-vec (xt-log/open-tx-ops-vec allocator)]
                (xt-log/write-tx-ops! allocator (vw/->writer tx-ops-vec) res)
                (.setValueCount tx-ops-vec (count res))
                tx-ops-vec)))

          (catch Throwable t
            (reset! !last-tx-fn-error t)
            (throw t)))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface RelationIndexer
  (^void indexOp [^xtdb.vector.RelationReader inRelation, queryOpts]))

(defn- ->upsert-rel-indexer ^xtdb.indexer.RelationIndexer [^ILiveIndexTx live-idx-tx
                                                           {{:keys [^Instant current-time]} :basis}]

  (let [current-time-µs (time/instant->micros current-time)]
    (reify RelationIndexer
      (indexOp [_ in-rel {:keys [table]}]
        (let [row-count (.rowCount in-rel)
              ^RelationReader content-rel (vr/rel-reader (->> in-rel
                                                              (remove (comp types/temporal-column? #(.getName ^IVectorReader %))))
                                                         (.rowCount in-rel))
              table (str table)
              id-col (.readerForName in-rel "xt$id")
              valid-from-rdr (.readerForName in-rel "xt$valid_from")
              valid-to-rdr (.readerForName in-rel "xt$valid_to")

              live-idx-table (.liveTable live-idx-tx table)
              live-idx-table-copier (-> (.docWriter live-idx-table)
                                        (.rowCopier content-rel))]

          (when-not id-col
            (throw (err/runtime-err :xtdb.indexer/missing-xt-id-column
                                    {:column-names (vec (for [^IVectorReader col in-rel] (.getName col)))})))

          (dotimes [idx row-count]
            (let [eid (.getObject id-col idx)
                  valid-from (if (and valid-from-rdr (not (.isNull valid-from-rdr idx)))
                               (.getLong valid-from-rdr idx)
                               current-time-µs)
                  valid-to (if (and valid-to-rdr (not (.isNull valid-to-rdr idx)))
                             (.getLong valid-to-rdr idx)
                             Long/MAX_VALUE)]
              (when (> valid-from valid-to)
                (throw (err/runtime-err :xtdb.indexer/invalid-valid-times
                                        {:valid-from (time/micros->instant valid-from)
                                         :valid-to (time/micros->instant valid-to)})))

              ;; FIXME something in the generated SQL generates rows with `(= vf vt)`, which is also unacceptable
              (when (< valid-from valid-to)
                (.logPut live-idx-table (trie/->iid eid) valid-from valid-to #(.copyRow live-idx-table-copier idx))))))))))

(defn- ->delete-rel-indexer ^xtdb.indexer.RelationIndexer [^ILiveIndexTx live-idx-tx]
  (reify RelationIndexer
    (indexOp [_ in-rel {:keys [table]}]
      (let [row-count (.rowCount in-rel)
            iid-rdr (.readerForName in-rel "xt$iid")
            valid-from-rdr (.readerForName in-rel "xt$valid_from")
            valid-to-rdr (.readerForName in-rel "xt$valid_to")]
        (dotimes [idx row-count]
          (let [iid (.getBytes iid-rdr idx)
                valid-from (.getLong valid-from-rdr idx)
                valid-to (if (.isNull valid-to-rdr idx)
                           Long/MAX_VALUE
                           (.getLong valid-to-rdr idx))]
            (when-not (< valid-from valid-to)
              (throw (err/runtime-err :xtdb.indexer/invalid-valid-times
                                      {:valid-from (time/micros->instant valid-from)
                                       :valid-to (time/micros->instant valid-to)})))

            (-> (.liveTable live-idx-tx table)
                (.logDelete iid valid-from valid-to))))))))

(defn- ->erase-rel-indexer ^xtdb.indexer.RelationIndexer [^ILiveIndexTx live-idx-tx]
  (reify RelationIndexer
    (indexOp [_ in-rel {:keys [table]}]
      (let [row-count (.rowCount in-rel)
            iid-rdr (.readerForName in-rel "xt$iid")]
        (dotimes [idx row-count]
          (let [iid (.getBytes iid-rdr idx)]
            (-> (.liveTable live-idx-tx table)
                (.logErase iid))))))))

(defn- query-indexer [^IQuerySource q-src, wm-src, ^RelationIndexer rel-idxer, query, {:keys [basis default-tz default-all-valid-time?]} query-opts]
  (let [^PreparedQuery pq (.prepareRaQuery q-src query wm-src)]
    (fn eval-query [^RelationReader args]
      (with-open [res (-> (.bind pq {:params args, :basis basis, :default-tz default-tz
                                     :default-all-valid-time? default-all-valid-time?})
                          (.openCursor))]

        (.forEachRemaining res
                           (reify Consumer
                             (accept [_ in-rel]
                               (.indexOp rel-idxer in-rel query-opts))))))))

(defn- foreach-arg-row [^BufferAllocator allocator, ^IVectorReader args-rdr, ^long tx-op-idx, eval-query]
  (if (.isNull args-rdr tx-op-idx)
    (eval-query nil)

    (with-open [is (ByteArrayInputStream. (.array ^ByteBuffer (.getObject args-rdr tx-op-idx))) ; could try to use getBytes
                asr (ArrowStreamReader. is allocator)]
      (let [param-root (.getVectorSchemaRoot asr)]
        (while (.loadNextBatch asr)
          (let [param-rel (vr/<-root param-root)
                selection (int-array 1)]
            (dotimes [idx (.rowCount param-rel)]
              (aset selection 0 idx)
              (eval-query (-> param-rel (.select selection))))))))))

(defn- wrap-sql-args [f ^long param-count]
  (fn [^RelationReader args]
    (if (not args)
      (if (zero? param-count)
        (f nil)
        (throw (err/runtime-err :xtdb.indexer/missing-sql-args
                                {::err/message "Arguments list was expected but not provided"
                                 :param-count param-count}))) 
      
      (let [arg-count (count (seq args))]
        (if (not= arg-count param-count)
          (throw (err/runtime-err :xtdb.indexer/incorrect-sql-arg-count
                                  {::err/message (format "%s arguments were provided and %s arguments were provided" arg-count param-count)
                                   :param-count param-count
                                   :arg-count arg-count}))
          (f (vr/rel-reader (->> args
                                 (map-indexed (fn [idx ^IVectorReader col]
                                                (.withName col (str "?_" idx))))))))))))

(defn- ->sql-indexer ^xtdb.indexer.OpIndexer [^BufferAllocator allocator, ^ILiveIndexTx live-idx-tx
                                              ^IVectorReader tx-ops-rdr, ^IQuerySource q-src, wm-src, ^IScanEmitter scan-emitter
                                              tx-opts]
  (let [sql-leg (.legReader tx-ops-rdr :sql)
        query-rdr (.structKeyReader sql-leg "query")
        args-rdr (.structKeyReader sql-leg "args")
        upsert-idxer (->upsert-rel-indexer live-idx-tx tx-opts)
        delete-idxer (->delete-rel-indexer live-idx-tx)
        erase-idxer (->erase-rel-indexer live-idx-tx)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [query-str (.getObject query-rdr tx-op-idx)
              tables-with-cols (scan/tables-with-cols wm-src scan-emitter)
              compiled-query (sql/compile-query query-str (assoc tx-opts :table-info tables-with-cols))
              param-count (:param-count (meta compiled-query))]
          ;; TODO handle error
          (zmatch compiled-query
            [:insert query-opts inner-query]
            (foreach-arg-row allocator args-rdr tx-op-idx
                             (-> (query-indexer q-src wm-src upsert-idxer inner-query tx-opts query-opts)
                                 (wrap-sql-args param-count)))

            [:update query-opts inner-query]
            (foreach-arg-row allocator args-rdr tx-op-idx
                             (-> (query-indexer q-src wm-src upsert-idxer inner-query tx-opts query-opts)
                                 (wrap-sql-args param-count)))

            [:delete query-opts inner-query]
            (foreach-arg-row allocator args-rdr tx-op-idx
                             (-> (query-indexer q-src wm-src delete-idxer inner-query tx-opts query-opts)
                                 (wrap-sql-args param-count)))

            [:erase query-opts inner-query]
            (foreach-arg-row allocator args-rdr tx-op-idx
                             (-> (query-indexer q-src wm-src erase-idxer inner-query tx-opts (assoc query-opts :default-all-valid-time? true))
                                 (wrap-sql-args param-count)))

            (throw (UnsupportedOperationException. "sql query"))))

        nil))))

(defn- ->assert-idxer ^xtdb.indexer.RelationIndexer [mode ^IQuerySource q-src, wm-src
                                                     query, {:keys [basis default-tz default-all-valid-time?]}]
  (let [^PreparedQuery pq (.prepareRaQuery q-src query wm-src)
        ^IntPredicate valid-query-pred (case mode
                                         :assert-exists (reify IntPredicate
                                                          (test [_ i] (pos? i)))
                                         :assert-not-exists (reify IntPredicate
                                                              (test [_ i] (zero? i))))]
    (fn eval-query [^RelationReader args]
      (with-open [res (-> (.bind pq {:params args, :basis basis, :default-tz default-tz
                                     :default-all-valid-time? default-all-valid-time?})
                          (.openCursor))]

        (doto (.tryAdvance res
                           (reify Consumer
                             (accept [_ in-rel]
                               (let [^RelationReader in-rel in-rel]
                                 (assert (= 1 (.rowCount in-rel)))
                                 (assert (= 1 (count (seq in-rel))))

                                 (let [row-count (.getLong ^IVectorReader (first in-rel) 0)]
                                   (when-not (.test valid-query-pred row-count)
                                     (throw (err/runtime-err :xtdb/assert-failed
                                                             {::err/message (format "Precondition failed: %s" (name mode))
                                                              :row-count row-count}))))))))
          (assert "expecting a batch in assert"))

        (assert (not (.tryAdvance res nil))
                "only expecting one batch in assert")))))

(defn- wrap-xtql-args [f]
  (fn [^RelationReader args]
    (f (when args
         (vr/rel-reader (for [^IVectorReader col args]
                          (.withName col (str "?" (.getName col)))))))))

(defn- ->xtql-indexer ^xtdb.indexer.OpIndexer [^BufferAllocator allocator, ^ILiveIndexTx live-idx-tx
                                               ^IVectorReader tx-ops-rdr, ^IQuerySource q-src, wm-src, ^IScanEmitter scan-emitter
                                               tx-opts]
  (let [xtql-leg (.legReader tx-ops-rdr :xtql)
        op-rdr (.structKeyReader xtql-leg "op")
        args-rdr (.structKeyReader xtql-leg "args")
        upsert-idxer (->upsert-rel-indexer live-idx-tx tx-opts)
        delete-idxer (->delete-rel-indexer live-idx-tx)
        erase-idxer (->erase-rel-indexer live-idx-tx)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [xtql-op (.form ^ClojureForm (.getObject op-rdr tx-op-idx))
              tables-with-cols (scan/tables-with-cols wm-src scan-emitter)]
          (when-not (instance? TxOp$XtqlOp xtql-op)
            (throw (UnsupportedOperationException. "unknown XTQL DML op")))

          (zmatch (xtql/compile-dml xtql-op (assoc tx-opts :table-info tables-with-cols))
            [:insert query-opts inner-query]
            (foreach-arg-row allocator args-rdr tx-op-idx
                             (-> (query-indexer q-src wm-src upsert-idxer inner-query tx-opts query-opts)
                                 (wrap-xtql-args)))

            [:update query-opts inner-query]
            (foreach-arg-row allocator args-rdr tx-op-idx
                             (-> (query-indexer q-src wm-src upsert-idxer inner-query tx-opts query-opts)
                                 (wrap-xtql-args)))

            [:delete query-opts inner-query]
            (foreach-arg-row allocator args-rdr tx-op-idx
                             (-> (query-indexer q-src wm-src delete-idxer inner-query tx-opts query-opts)
                                 (wrap-xtql-args)))

            [:erase query-opts inner-query]
            (foreach-arg-row allocator args-rdr tx-op-idx
                             (-> (query-indexer q-src wm-src erase-idxer inner-query tx-opts query-opts)
                                 (wrap-xtql-args)))

            [:assert-not-exists _query-opts inner-query]
            (foreach-arg-row allocator args-rdr tx-op-idx
                             (-> (->assert-idxer :assert-not-exists q-src wm-src inner-query tx-opts)
                                 (wrap-xtql-args)))

            [:assert-exists _query-opts inner-query]
            (foreach-arg-row allocator args-rdr tx-op-idx
                             (-> (->assert-idxer :assert-exists q-src wm-src inner-query tx-opts)
                                 (wrap-xtql-args)))

            (throw (UnsupportedOperationException. "xtql query"))))

        nil))))

(def ^:private ^:const ^String txs-table
  "xt$txs")

(defn- add-tx-row! [^ILiveIndexTx live-idx-tx, ^TransactionKey tx-key, ^Throwable t]
  (let [tx-id (.getTxId tx-key)
        system-time-µs (time/instant->micros (.getSystemTime tx-key))

        live-table (.liveTable live-idx-tx txs-table)
        doc-writer (.docWriter live-table)]

    (.logPut live-table (trie/->iid tx-id) system-time-µs Long/MAX_VALUE
             (fn write-doc! []
               (.startStruct doc-writer)
               (doto (.structKeyWriter doc-writer "xt$id" (FieldType/notNullable #xt.arrow/type :i64))
                 (.writeLong tx-id))

               (doto (.structKeyWriter doc-writer "xt$tx_time" (FieldType/notNullable (types/->arrow-type types/temporal-col-type)))
                 (.writeLong system-time-µs))

               (doto (.structKeyWriter doc-writer "xt$committed?" (FieldType/notNullable  #xt.arrow/type :bool))
                 (.writeBoolean (nil? t)))

               (let [e-wtr (.structKeyWriter doc-writer "xt$error" (FieldType/nullable #xt.arrow/type :transit))]
                 (if (or (nil? t) (= t abort-exn))
                   (.writeNull e-wtr)
                   (.writeObject e-wtr t)))
               (.endStruct doc-writer)))))

(deftype Indexer [^BufferAllocator allocator
                  ^IScanEmitter scan-emitter
                  ^IQuerySource q-src
                  ^ILiveIndex live-idx

                  ^:volatile-mutable indexer-error

                  ^PriorityBlockingQueue awaiters
                  metrics]
  IIndexer
  (indexTx [this tx-key tx-root]
    (let [system-time (some-> tx-key (.getSystemTime))]
      (try
        (if (and (not (nil? (.latestCompletedTx this)))
                 (neg? (compare system-time
                                (.getSystemTime (.latestCompletedTx this)))))
          (do
            (log/warnf "specified system-time '%s' older than current tx '%s'"
                       (pr-str tx-key)
                       (pr-str (.latestCompletedTx this)))

            (util/with-open [live-idx-tx (.startTx live-idx tx-key)]
              (add-tx-row! live-idx-tx tx-key
                           (err/illegal-arg :invalid-system-time
                                            {::err/message "specified system-time older than current tx"
                                             :tx-key tx-key
                                             :latest-completed-tx (.latestCompletedTx this)}))
              (.commit live-idx-tx)))

          (util/with-open [live-idx-tx (.startTx live-idx tx-key)]
            (let [^DenseUnionVector tx-ops-vec (-> ^ListVector (.getVector tx-root "tx-ops")
                                                   (.getDataVector))

                  wm-src (reify IWatermarkSource
                           (openWatermark [_]
                             (Watermark. nil (.openWatermark live-idx-tx))))

                  tx-opts {:basis {:at-tx tx-key, :current-time system-time}
                           :default-tz (ZoneId/of (str (-> (.getVector tx-root "default-tz")
                                                           (.getObject 0))))
                           :default-all-valid-time? (== 1 (-> ^BitVector (.getVector tx-root "default-all-valid-time?")
                                                              (.get 0)))
                           :tx-key tx-key}]

              (letfn [(index-tx-ops [^DenseUnionVector tx-ops-vec]
                        (let [tx-ops-rdr (vr/vec->reader tx-ops-vec)
                              !put-docs-idxer (delay (->put-docs-indexer live-idx-tx tx-ops-rdr system-time))
                              !delete-docs-idxer (delay (->delete-docs-indexer live-idx-tx tx-ops-rdr system-time))
                              !erase-docs-idxer (delay (->erase-docs-indexer live-idx-tx tx-ops-rdr))
                              !call-idxer (delay (->call-indexer allocator q-src wm-src tx-ops-rdr tx-opts))
                              !xtql-idxer (delay (->xtql-indexer allocator live-idx-tx tx-ops-rdr q-src wm-src scan-emitter tx-opts))
                              !sql-idxer (delay (->sql-indexer allocator live-idx-tx tx-ops-rdr q-src wm-src scan-emitter tx-opts))]
                          (dotimes [tx-op-idx (.valueCount tx-ops-rdr)]
                            (when-let [more-tx-ops
                                       (.recordCallable ^Timer (:tx-timer metrics)
                                                        #(case (.getLeg tx-ops-rdr tx-op-idx)
                                                           :xtql (.indexOp ^OpIndexer @!xtql-idxer tx-op-idx)
                                                           :sql (.indexOp ^OpIndexer @!sql-idxer tx-op-idx)
                                                           :put-docs (.indexOp ^OpIndexer @!put-docs-idxer tx-op-idx)
                                                           :delete-docs (.indexOp ^OpIndexer @!delete-docs-idxer tx-op-idx)
                                                           :erase-docs (.indexOp ^OpIndexer @!erase-docs-idxer tx-op-idx)
                                                           :call (.indexOp ^OpIndexer @!call-idxer tx-op-idx)
                                                           :abort (throw abort-exn)))]
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
                            (throw t)))]
                  (if e
                    (do
                      (when (not= e abort-exn)
                        (log/debug e "aborted tx")
                        (.abort live-idx-tx))

                      (util/with-open [live-idx-tx (.startTx live-idx tx-key)]
                        (add-tx-row! live-idx-tx tx-key e)
                        (.commit live-idx-tx)))

                    (do
                      (add-tx-row! live-idx-tx tx-key nil)

                      (.commit live-idx-tx)))))

              tx-key)))

        (await/notify-tx tx-key awaiters)

        (catch Throwable t
          (set! (.indexer-error this) t)
          (await/notify-ex t awaiters)
          (throw t)))))

  (forceFlush [_ tx-key expected-last-chunk-tx-id]
    (li/force-flush! live-idx tx-key expected-last-chunk-tx-id))

  IWatermarkSource
  (openWatermark [_] (.openWatermark live-idx))

  (latestCompletedTx [_] (.latestCompletedTx live-idx))
  (latestCompletedChunkTx [_] (.latestCompletedChunkTx live-idx))

  (awaitTx [this tx timeout]
    @(-> (if tx
           (await/await-tx-async tx
                                 #(or (some-> (.indexer-error this) throw)
                                      (.latestCompletedTx this))
                                 awaiters)
           (CompletableFuture/completedFuture (.latestCompletedTx this)))
         (cond-> timeout (.orTimeout (.toMillis timeout) TimeUnit/MILLISECONDS))))

  Closeable
  (close [_]
    (util/close allocator)))

(defmethod ig/prep-key :xtdb/indexer [_ opts]
  (merge {:allocator (ig/ref :xtdb/allocator)
          :scan-emitter (ig/ref :xtdb.operator.scan/scan-emitter)
          :live-index (ig/ref :xtdb.indexer/live-index)
          :q-src (ig/ref ::q/query-source)
          :registry (ig/ref :xtdb/meter-registry)}
         opts))

(defmethod ig/init-key :xtdb/indexer [_ {:keys [allocator scan-emitter, q-src, live-index registry]}]
  (util/with-close-on-catch [allocator (util/->child-allocator allocator "indexer")]
    (->Indexer allocator scan-emitter q-src live-index

               nil ;; indexer-error

               (PriorityBlockingQueue.)
               {:tx-timer (metrics/add-timer registry "tx.op.timer"
                                             {:description "indicates the timing and number of transactions"})})))

(defmethod ig/halt-key! :xtdb/indexer [_ indexer]
  (util/close indexer))

(defn await-tx
  ([node] (await-tx (xtp/latest-submitted-tx node) node))
  ([tx node] (await-tx tx node nil))
  ([tx node timeout] (.awaitTx ^IIndexer (util/component node :xtdb/indexer) tx timeout)))

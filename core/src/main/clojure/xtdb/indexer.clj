(ns xtdb.indexer
  (:require [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [sci.core :as sci]
            [xtdb.buffer-pool :as bp]
            [xtdb.datalog :as d]
            [xtdb.error :as err]
            xtdb.indexer.internal-id-manager
            xtdb.indexer.live-tries
            xtdb.indexer.log-indexer
            xtdb.live-chunk
            [xtdb.metadata :as meta]
            xtdb.object-store
            [xtdb.operator :as op]
            [xtdb.operator.scan :as scan]
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.sql :as sql]
            [xtdb.temporal :as temporal]
            [xtdb.tx-producer :as txp]
            [xtdb.types :as t]
            [xtdb.util :as util]
            [xtdb.vector :as vec]
            [xtdb.vector.indirect :as iv]
            [xtdb.vector.writer :as vw]
            [xtdb.watermark :as wm])
  (:import (java.io ByteArrayInputStream Closeable)
           java.lang.AutoCloseable
           java.nio.ByteBuffer
           (java.nio.channels ClosedByInterruptException)
           java.security.MessageDigest
           (java.time Instant ZoneId)
           (java.util Arrays UUID)
           (java.util.concurrent.locks StampedLock)
           (java.util.function Consumer IntPredicate Supplier ToIntFunction)
           (java.util.stream IntStream StreamSupport)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector BigIntVector BitVector TimeStampVector VarBinaryVector VarCharVector VectorSchemaRoot)
           (org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector)
           (org.apache.arrow.vector.ipc ArrowStreamReader)
           org.roaringbitmap.RoaringBitmap
           (xtdb.api.protocols ClojureForm TransactionInstant)
           xtdb.buffer_pool.IBufferPool
           xtdb.indexer.internal_id_manager.IInternalIdManager
           (xtdb.indexer.live_tries ILiveTries ILiveTrieTx ILiveTriesTx)
           (xtdb.indexer.log_indexer ILogIndexer ILogOpIndexer)
           (xtdb.live_chunk ILiveChunk ILiveChunkTx ILiveTableTx)
           xtdb.metadata.IMetadataManager
           xtdb.object_store.ObjectStore
           xtdb.operator.IRaQuerySource
           (xtdb.operator.scan IScanEmitter)
           (xtdb.temporal ITemporalManager ITemporalTxIndexer)
           (xtdb.vector IIndirectRelation IIndirectVector IRelationWriter IRowCopier IVectorWriter)
           (xtdb.watermark IWatermark IWatermarkSource)))

(set! *unchecked-math* :warn-on-boxed)

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IIndexer
  (^xtdb.api.protocols.TransactionInstant indexTx [^xtdb.api.protocols.TransactionInstant tx
                                                   ^org.apache.arrow.vector.VectorSchemaRoot txRoot])
  (^xtdb.api.protocols.TransactionInstant latestCompletedTx []))

(defprotocol Finish
  (^void finish-block! [_])
  (^void finish-chunk! [_]))

(def ^:private abort-exn (err/runtime-err :abort-exn))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface OpIndexer
  (^org.apache.arrow.vector.complex.DenseUnionVector indexOp [^long tx-op-idx]
   "returns a tx-ops-vec of more operations (mostly for `:call`)"))

(def ^:private ^java.lang.ThreadLocal !msg-digest
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (MessageDigest/getInstance "SHA-256")))))

(defn- ->iid ^bytes [eid]
  (let [^bytes eid-bytes (cond
                           (uuid? eid) (util/uuid->bytes eid)
                           (string? eid) (.getBytes (str "s" eid))
                           (keyword? eid) (.getBytes (str "k" eid))
                           (integer? eid) (.getBytes (str "i" eid))
                           :else (throw (UnsupportedOperationException. (pr-str (class eid)))))]
    (-> ^MessageDigest (.get !msg-digest)
        (.digest eid-bytes)
        (Arrays/copyOfRange 0 16))))

(defn- ->put-indexer ^xtdb.indexer.OpIndexer [^IInternalIdManager iid-mgr ^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer,
                                              ^ILiveChunkTx live-chunk, ^ILiveTriesTx live-tries-tx
                                              ^DenseUnionVector tx-ops-vec, ^Instant system-time]
  (let [put-vec (.getStruct tx-ops-vec 1)
        ^DenseUnionVector doc-duv (.getChild put-vec "document" DenseUnionVector)
        ^TimeStampVector valid-from-vec (.getChild put-vec "xt$valid_from")
        ^TimeStampVector valid-to-vec (.getChild put-vec "xt$valid_to")
        system-time-µs (util/instant->micros system-time)
        tables (->> doc-duv
                    (mapv (fn [^StructVector table-vec]
                            (let [table-name (.getName table-vec)
                                  table-rdr (iv/->indirect-rel (map iv/->direct-vec table-vec)
                                                               (.getValueCount table-vec))]
                              {:table-name table-name
                               :id-duv (.getChild table-vec "xt$id" DenseUnionVector)

                               ;; dual write to live-chunk and live-trie for now
                               :live-table (let [live-table (.liveTable live-chunk table-name)]
                                             {:live-table live-table
                                              :table-copier (.rowCopier (.writer live-table) table-rdr)})

                               :live-trie (let [live-trie (.liveTrie live-tries-tx (format "tables/%s/t1-diff" table-name))
                                                rel-writer (.relationWriter live-trie)]
                                            {:live-trie live-trie
                                             :rel-writer rel-writer
                                             :iid-wtr (.writerForName rel-writer "xt$iid")
                                             :valid-from-wtr (.writerForName rel-writer "xt$valid_from")
                                             :sys-from-wtr (.writerForName rel-writer "xt$system_from")
                                             :doc-copier (.rowCopier (.writerForName rel-writer "xt$doc") table-vec)})}))))]

    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [row-id (.nextRowId live-chunk)
              put-offset (.getOffset tx-ops-vec tx-op-idx)

              {:keys [table-name, ^DenseUnionVector id-duv, live-table, live-trie]} (nth tables (.getTypeId doc-duv put-offset))
              doc-offset (.getOffset doc-duv put-offset)

              eid (t/get-object id-duv doc-offset)]

          (let [{:keys [^ILiveTableTx live-table, ^IRowCopier table-copier]} live-table]
            (.writeRowId live-table row-id)
            (.copyRow table-copier doc-offset))

          (let [new-entity? (not (.isKnownId iid-mgr table-name eid))
                iid (.getOrCreateInternalId iid-mgr table-name eid row-id)
                valid-from (if (.isNull valid-from-vec put-offset)
                             system-time-µs
                             (.get valid-from-vec put-offset))
                valid-to (if (.isNull valid-to-vec put-offset)
                           util/end-of-time-μs
                           (.get valid-to-vec put-offset))]
            (when-not (> valid-to valid-from)
              (throw (err/runtime-err :xtdb.indexer/invalid-valid-times
                                      {:valid-from (util/micros->instant valid-from)
                                       :valid-to (util/micros->instant valid-to)})))

            ;; TODO just T1 for now
            (let [{:keys [^ILiveTrieTx live-trie, ^IRelationWriter rel-writer, ^IRowCopier doc-copier,
                          ^IVectorWriter iid-wtr, ^IVectorWriter valid-from-wtr, ^IVectorWriter sys-from-wtr]} live-trie
                  pos (.getPosition (.writerPosition rel-writer))]
              (.writeBytes iid-wtr (ByteBuffer/wrap (->iid eid)))
              (.writeLong valid-from-wtr valid-from)
              (.writeLong sys-from-wtr system-time-µs)
              (.copyRow doc-copier doc-offset)
              (.endRow rel-writer)
              (.addRow live-trie pos))

            (.logPut log-op-idxer iid row-id valid-from valid-to)
            (.indexPut temporal-idxer iid row-id valid-from valid-to new-entity?)))

        nil))))

(defn- ->delete-indexer ^xtdb.indexer.OpIndexer [^IInternalIdManager iid-mgr, ^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer, ^ILiveChunkTx live-chunk
                                                 ^DenseUnionVector tx-ops-vec, ^Instant current-time]
  (let [delete-vec (.getStruct tx-ops-vec 2)
        ^VarCharVector table-vec (.getChild delete-vec "table" VarCharVector)
        ^DenseUnionVector id-vec (.getChild delete-vec "xt$id" DenseUnionVector)
        ^TimeStampVector valid-from-vec (.getChild delete-vec "xt$valid_from")
        ^TimeStampVector valid-to-vec (.getChild delete-vec "xt$valid_to")
        current-time-µs (util/instant->micros current-time)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [row-id (.nextRowId live-chunk)
              delete-offset (.getOffset tx-ops-vec tx-op-idx)
              table (t/get-object table-vec delete-offset)
              eid (t/get-object id-vec delete-offset)
              new-entity? (not (.isKnownId iid-mgr table eid))
              iid (.getOrCreateInternalId iid-mgr table eid row-id)
              valid-from (if (.isNull valid-from-vec delete-offset)
                           current-time-µs
                           (.get valid-from-vec delete-offset))
              valid-to (if (.isNull valid-to-vec delete-offset)
                         util/end-of-time-μs
                         (.get valid-to-vec delete-offset))]
          (when (> valid-from valid-to)
            (throw (err/runtime-err :xtdb.indexer/invalid-valid-times
                                    {:valid-from (util/micros->instant valid-from)
                                     :valid-to (util/micros->instant valid-to)})))

          (.logDelete log-op-idxer iid valid-from valid-to)
          (.indexDelete temporal-idxer iid row-id valid-from valid-to new-entity?))

        nil))))

(defn- ->evict-indexer ^xtdb.indexer.OpIndexer [^IInternalIdManager iid-mgr ^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer, ^ILiveChunkTx live-chunk
                                                ^DenseUnionVector tx-ops-vec]

  (let [evict-vec (.getStruct tx-ops-vec 3)
        ^VarCharVector table-vec (.getChild evict-vec "_table" VarCharVector)
        ^DenseUnionVector id-vec (.getChild evict-vec "xt$id" DenseUnionVector)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [row-id (.nextRowId live-chunk)
              evict-offset (.getOffset tx-ops-vec tx-op-idx)
              table (or (t/get-object table-vec evict-offset) "xt_docs")
              eid (t/get-object id-vec evict-offset)
              iid (.getOrCreateInternalId iid-mgr table eid row-id)]
          (.logEvict log-op-idxer iid)
          (.indexEvict temporal-idxer iid))

        nil))))

(defn- find-fn [allocator ^IRaQuerySource ra-src, wm-src, sci-ctx {:keys [basis default-tz]} fn-id]
  (let [lp '[:scan {:table xt/tx-fns} [{xt/id (= xt/id ?id)} xt/fn]]
        ^xtdb.operator.PreparedQuery pq (.prepareRaQuery ra-src lp)]
    (with-open [bq (.bind pq wm-src
                          {:params (iv/->indirect-rel [(-> (vw/open-vec allocator '?id [fn-id])
                                                           (iv/->direct-vec))]
                                                      1)
                           :default-all-valid-time? false
                           :basis basis
                           :default-tz default-tz})
                res (.openCursor bq)]

      (let [!fn-doc (object-array 1)]
        (.tryAdvance res
                     (reify Consumer
                       (accept [_ in-rel]
                         (when (pos? (.rowCount ^IIndirectRelation in-rel))
                           (aset !fn-doc 0 (first (iv/rel->rows in-rel)))))))

        (let [fn-doc (or (aget !fn-doc 0)
                         (throw (err/runtime-err :xtdb.call/no-such-tx-fn {:fn-id fn-id})))
              fn-body (:xt/fn fn-doc)]

          (when-not (instance? ClojureForm fn-body)
            (throw (err/illegal-arg :xtdb.call/invalid-tx-fn {:fn-doc fn-doc})))

          (let [fn-form (:form fn-body)]
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
                                               ^DenseUnionVector tx-ops-vec, {:keys [tx-key] :as tx-opts}]
  (let [call-vec (.getStruct tx-ops-vec 4)
        ^DenseUnionVector fn-id-vec (.getChild call-vec "fn-id" DenseUnionVector)
        ^ListVector args-vec (.getChild call-vec "args" ListVector)

        ;; TODO confirm/expand API that we expose to tx-fns
        sci-ctx (sci/init {:bindings {'q (tx-fn-q allocator ra-src wm-src scan-emitter tx-opts)
                                      'sql-q (partial tx-fn-sql allocator ra-src wm-src tx-opts)
                                      'sleep (fn [n] (Thread/sleep n))
                                      '*current-tx* tx-key}})]

    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (try
          (let [call-offset (.getOffset tx-ops-vec tx-op-idx)
                fn-id (t/get-object fn-id-vec call-offset)
                tx-fn (find-fn allocator ra-src wm-src (sci/fork sci-ctx) tx-opts fn-id)
                args (t/get-object args-vec call-offset)

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
  (^void indexOp [^xtdb.vector.IIndirectRelation inRelation, queryOpts]))

(defn- ->sql-insert-indexer ^xtdb.indexer.SqlOpIndexer [^IInternalIdManager iid-mgr, ^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer
                                                        ^ILiveChunkTx live-chunk, {{:keys [^Instant current-time]} :basis}]

  (let [current-time-µs (util/instant->micros current-time)]
    (reify SqlOpIndexer
      (indexOp [_ in-rel {:keys [table]}]
        (let [row-count (.rowCount in-rel)
              content-rel (iv/->indirect-rel (->> in-rel
                                                  (remove (comp temporal/temporal-column?
                                                                #(.getName ^IIndirectVector %)))
                                                  (map (fn [^IIndirectVector vec]
                                                         (.withName vec (util/str->normal-form-str (.getName vec))))))
                                             (.rowCount in-rel))
              table (util/str->normal-form-str table)
              live-table (.liveTable live-chunk table)
              table-copier (.rowCopier (.writer live-table) content-rel)
              id-col (.vectorForName in-rel "xt$id")
              valid-from-rdr (some-> (.vectorForName in-rel "xt$valid_from")
                                     (.polyReader [:union [:null t/temporal-col-type]]))
              valid-to-rdr (some-> (.vectorForName in-rel "xt$valid_to")
                                   (.polyReader [:union [:null t/temporal-col-type]]))]
          (dotimes [idx row-count]
            (let [row-id (.nextRowId live-chunk)]
              (.writeRowId live-table row-id)
              (.copyRow table-copier idx)

              (let [eid (t/get-object (.getVector id-col) (.getIndex id-col idx))
                    new-entity? (not (.isKnownId iid-mgr table eid))
                    iid (.getOrCreateInternalId iid-mgr table eid row-id)
                    valid-from (if (and valid-from-rdr (= 1 (.read valid-from-rdr idx)))
                                 (.readLong valid-from-rdr)
                                 current-time-µs)
                    valid-to (if (and valid-to-rdr (= 1 (.read valid-to-rdr idx)))
                               (.readLong valid-to-rdr)
                               util/end-of-time-μs)]
                (when (> valid-from valid-to)
                  (throw (err/runtime-err :xtdb.indexer/invalid-valid-times
                                          {:valid-from (util/micros->instant valid-from)
                                           :valid-to (util/micros->instant valid-to)})))

                (.logPut log-op-idxer iid row-id valid-from valid-to)
                (.indexPut temporal-idxer iid row-id valid-from valid-to new-entity?)))))))))

(defn- row-id->idx [buf, ^long block-idx, ^long row-id]
  (with-open [chunks (util/->chunks buf {:block-idxs (doto (RoaringBitmap.)
                                                       (.add block-idx))
                                         :close-buffer? true})]
    (-> (StreamSupport/stream chunks false)
        (.mapToInt (reify ToIntFunction
                     (applyAsInt [_ block-root]
                       (let [^BigIntVector row-id-vec (.getVector ^VectorSchemaRoot block-root "_row_id")]
                         (-> (IntStream/range 0 (.getValueCount row-id-vec))
                             (.filter (reify IntPredicate
                                        (test [_ idx]
                                          (= (.get row-id-vec idx) row-id))))
                             (.findFirst)
                             (.getAsInt))))))
        (.findFirst)
        (.getAsInt))))

(defn- ->sql-update-indexer ^xtdb.indexer.SqlOpIndexer [^IMetadataManager metadata-mgr, ^IBufferPool buffer-pool
                                                        ^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer
                                                        ^ILiveChunkTx live-chunk]
  (reify SqlOpIndexer
    (indexOp [_ in-rel {:keys [table]}]
      (let [row-count (.rowCount in-rel)
            ^IIndirectRelation in-rel (iv/->indirect-rel (map (fn [^IIndirectVector vec]
                                                                (.withName vec (util/str->normal-form-str (.getName vec)))) in-rel)
                                                         row-count)
            table (util/str->normal-form-str table)
            live-table (.liveTable live-chunk table)
            live-table-wtr (.writer live-table)]
        (letfn [(->live-row-copier ^xtdb.vector.IRowCopier [^IIndirectVector col]
                  (.rowCopier col (.writerForName live-table-wtr (.getName col))))]

          (let [update-col-copiers (->> (for [^IIndirectVector in-col in-rel
                                              :let [col-name (.getName in-col)]
                                              :when (not (temporal/temporal-column? col-name))]
                                          [col-name (->live-row-copier in-col)])
                                        (into {}))
                iid-rdr (.monoReader (.vectorForName in-rel "_iid") :i64)
                row-id-rdr (.monoReader (.vectorForName in-rel "_row_id") :i64)
                valid-from-rdr (-> (.vectorForName in-rel "xt$valid_from")
                                   (.monoReader t/temporal-col-type))
                valid-to-rdr (-> (.vectorForName in-rel "xt$valid_to")
                                 (.monoReader t/temporal-col-type))]

            ;; once the SQL planner has select-star we can likely re-use a lot of that instead of the below...
            (dotimes [idx row-count]
              (let [old-row-id (.readLong row-id-rdr idx)
                    new-row-id (.nextRowId live-chunk)
                    iid (.readLong iid-rdr idx)
                    valid-from (.readLong valid-from-rdr idx)
                    valid-to (.readLong valid-to-rdr idx)]
                (when (> valid-from valid-to)
                  (throw (err/runtime-err :xtdb.indexer/invalid-valid-times
                                          {:valid-from (util/micros->instant valid-from)
                                           :valid-to (util/micros->instant valid-to)})))

                (.writeRowId live-table new-row-id)

                (doseq [^IRowCopier update-col-copier (vals update-col-copiers)]
                  (.copyRow update-col-copier idx))

                (if-let [live-row (.liveRow live-table old-row-id)]
                  (doseq [^IIndirectVector live-col live-row
                          :when (not (contains? update-col-copiers (.getName live-col)))]
                    (doto ^IRowCopier (->live-row-copier live-col)
                      (.copyRow 0)))

                  (-> (when-let [{:keys [^long chunk-idx ^long block-idx, col-names]}
                                 (meta/row-id->chunk metadata-mgr table old-row-id)]
                        (let [idx @(-> (.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx table "_row_id"))
                                       (util/then-apply #(row-id->idx % block-idx old-row-id)))]

                          (doseq [^String col-name col-names
                                  :when (not (contains? update-col-copiers col-name))]
                            @(-> (.getBuffer buffer-pool (meta/->chunk-obj-key chunk-idx table col-name))
                                 (util/then-apply
                                   (fn [buf]
                                     (with-open [chunks (util/->chunks buf {:block-idxs (doto (RoaringBitmap.)
                                                                                          (.add block-idx))
                                                                            :close-buffer? true})]
                                       (-> chunks
                                           (.forEachRemaining (reify Consumer
                                                                (accept [_ block-root]
                                                                  (let [col (iv/->direct-vec (.getVector ^VectorSchemaRoot block-root col-name))]
                                                                    (doto ^IRowCopier (->live-row-copier col)
                                                                      (.copyRow idx))))))))))))))
                      (util/rethrowing-cause)))

                (.logPut log-op-idxer iid new-row-id valid-from valid-to)
                (.indexPut temporal-idxer iid new-row-id valid-from valid-to false)))))))))

(defn- ->sql-delete-indexer ^xtdb.indexer.SqlOpIndexer [^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer, ^ILiveChunkTx live-chunk]
  (reify SqlOpIndexer
    (indexOp [_ in-rel _query-opts]
      (let [row-count (.rowCount in-rel)
            iid-rdr (.monoReader (.vectorForName in-rel "_iid") :i64)
            valid-from-rdr (.monoReader (.vectorForName in-rel "xt$valid_from") t/temporal-col-type)
            valid-to-rdr (.monoReader (.vectorForName in-rel "xt$valid_to") t/temporal-col-type)]
        (dotimes [idx row-count]
          (let [row-id (.nextRowId live-chunk)
                iid (.readLong iid-rdr idx)
                valid-from (.readLong valid-from-rdr idx)
                valid-to (.readLong valid-to-rdr idx)]
            (when (> valid-from valid-to)
              (throw (err/runtime-err :xtdb.indexer/invalid-valid-times
                                      {:valid-from (util/micros->instant valid-from)
                                       :valid-to (util/micros->instant valid-to)})))

            (.logDelete log-op-idxer iid valid-from valid-to)
            (.indexDelete temporal-idxer iid row-id valid-from valid-to false)))))))

(defn- ->sql-erase-indexer ^xtdb.indexer.SqlOpIndexer [^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer]
  (reify SqlOpIndexer
    (indexOp [_ in-rel _query-opts]
      (let [row-count (.rowCount in-rel)
            iid-rdr (.monoReader (.vectorForName in-rel "_iid") :i64)]
        (dotimes [idx row-count]
          (let [iid (.readLong iid-rdr idx)]
            (.logEvict log-op-idxer iid)
            (.indexEvict temporal-idxer iid)))))))

(defn- ->sql-indexer ^xtdb.indexer.OpIndexer [^BufferAllocator allocator, ^IMetadataManager metadata-mgr, ^IBufferPool buffer-pool, ^IInternalIdManager iid-mgr
                                              ^ILogOpIndexer log-op-idxer, ^ITemporalTxIndexer temporal-idxer, ^ILiveChunk doc-idxer
                                              ^DenseUnionVector tx-ops-vec, ^IRaQuerySource ra-src, wm-src, ^IScanEmitter scan-emitter
                                              {:keys [default-all-valid-time? basis default-tz] :as tx-opts}]
  (let [sql-vec (.getStruct tx-ops-vec 0)
        ^VarCharVector query-vec (.getChild sql-vec "query" VarCharVector)
        ^VarBinaryVector params-vec (.getChild sql-vec "params" VarBinaryVector)
        insert-idxer (->sql-insert-indexer iid-mgr log-op-idxer temporal-idxer doc-idxer tx-opts)
        update-idxer (->sql-update-indexer metadata-mgr buffer-pool log-op-idxer temporal-idxer doc-idxer)
        delete-idxer (->sql-delete-indexer log-op-idxer temporal-idxer doc-idxer)
        erase-idxer (->sql-erase-indexer log-op-idxer temporal-idxer)]
    (reify OpIndexer
      (indexOp [_ tx-op-idx]
        (let [sql-offset (.getOffset tx-ops-vec tx-op-idx)]
          (letfn [(index-op [^SqlOpIndexer op-idxer query-opts inner-query]
                    (let [^xtdb.operator.PreparedQuery pq (.prepareRaQuery ra-src inner-query)]
                      (letfn [(index-op* [^IIndirectRelation params]
                                (with-open [res (-> (.bind pq wm-src {:params params, :basis basis, :default-tz default-tz :default-all-valid-time? default-all-valid-time?})
                                                    (.openCursor))]

                                  (.forEachRemaining res
                                                     (reify Consumer
                                                       (accept [_ in-rel]
                                                         (.indexOp op-idxer in-rel query-opts))))))]
                        (if (.isNull params-vec sql-offset)
                          (index-op* nil)

                          (with-open [is (ByteArrayInputStream. (.get params-vec sql-offset))
                                      asr (ArrowStreamReader. is allocator)]
                            (let [root (.getVectorSchemaRoot asr)]
                              (while (.loadNextBatch asr)
                                (let [rel (iv/<-root root)

                                      ^IIndirectRelation
                                      rel (iv/->indirect-rel (->> rel
                                                                  (map-indexed (fn [idx ^IIndirectVector col]
                                                                                 (.withName col (str "?_" idx)))))
                                                             (.rowCount rel))

                                      selection (int-array 1)]
                                  (dotimes [idx (.rowCount rel)]
                                    (aset selection 0 idx)
                                    (index-op* (-> rel (iv/select selection))))))))))))]

            (let [query-str (t/get-object query-vec sql-offset)
                  tables-with-cols (scan/tables-with-cols (:basis tx-opts) wm-src scan-emitter)]
              ;; TODO handle error
              (zmatch (sql/compile-query query-str (assoc tx-opts :table-info tables-with-cols))
                [:insert query-opts inner-query]
                (index-op insert-idxer query-opts inner-query)

                [:update query-opts inner-query]
                (index-op update-idxer query-opts inner-query)

                [:delete query-opts inner-query]
                (index-op delete-idxer query-opts inner-query)

                [:erase query-opts inner-query]
                (index-op erase-idxer query-opts inner-query)

                (throw (UnsupportedOperationException. "sql query"))))))

        nil))))

(def ^:private ^:const ^String txs-table
  "xt$txs")

(defn- add-tx-row! [^ILiveChunkTx live-chunk-tx, ^ITemporalTxIndexer temporal-tx, ^IInternalIdManager iid-mgr, ^TransactionInstant tx-key, ^Throwable t]
  (let [tx-id (.tx-id tx-key)
        system-time-µs (util/instant->micros (.system-time tx-key))
        live-table (.liveTable live-chunk-tx txs-table)
        row-id (.nextRowId live-chunk-tx)
        writer (.writer live-table)
        iid (.getOrCreateInternalId iid-mgr txs-table tx-id row-id)]

    (.writeRowId live-table row-id)

    (doto (.writerForName writer "xt$id" :i64)
      (.writeLong tx-id))

    (doto (.writerForName writer "xt$tx_time" t/temporal-col-type)
      (.writeLong system-time-µs))

    (doto (.writerForName writer "xt$committed?" :bool)
      (.writeBoolean (nil? t)))

    (let [e-wtr (.writerForName writer "xt$error" [:union #{:null :clj-form}])]
      (if (or (nil? t) (= t abort-exn))
        (doto (.writerForType e-wtr :null)
          (.writeNull nil))
        (doto (.writerForType e-wtr :clj-form)
          (.writeObject (pr-str t)))))

    (.indexPut temporal-tx iid row-id system-time-µs util/end-of-time-μs true)))

(deftype Indexer [^BufferAllocator allocator
                  ^ObjectStore object-store
                  ^IMetadataManager metadata-mgr
                  ^IScanEmitter scan-emitter
                  ^IBufferPool buffer-pool
                  ^ITemporalManager temporal-mgr
                  ^IInternalIdManager iid-mgr
                  ^IRaQuerySource ra-src
                  ^ILogIndexer log-indexer
                  ^ILiveChunk live-chunk
                  ^ILiveTries live-tries

                  ^:volatile-mutable ^TransactionInstant latest-completed-tx

                  ^:volatile-mutable ^IWatermark shared-wm
                  ^StampedLock wm-lock]

  IIndexer
  (indexTx [this {:keys [system-time] :as tx-key} tx-root]
    (util/with-open [live-chunk-tx (.startTx live-chunk)
                     live-tries-tx (.startTx live-tries)]
      (let [^DenseUnionVector tx-ops-vec (-> ^ListVector (.getVector tx-root "tx-ops")
                                             (.getDataVector))

            log-op-idxer (.startTx log-indexer tx-key)
            temporal-idxer (.startTx temporal-mgr tx-key)

            wm-src (reify IWatermarkSource
                     (openWatermark [_ _tx]
                       (wm/->wm nil (.openWatermark live-chunk-tx) temporal-idxer false)))

            tx-opts {:basis {:tx tx-key, :current-time system-time}
                     :default-tz (ZoneId/of (str (-> (.getVector tx-root "default-tz")
                                                     (.getObject 0))))
                     :default-all-valid-time? (== 1 (-> ^BitVector (.getVector tx-root "all-application-time?")
                                                        (.get 0)))
                     :tx-key tx-key}]

        (letfn [(index-tx-ops [^DenseUnionVector tx-ops-vec]
                  (let [!put-idxer (delay (->put-indexer iid-mgr log-op-idxer temporal-idxer live-chunk-tx live-tries-tx tx-ops-vec system-time))
                        !delete-idxer (delay (->delete-indexer iid-mgr log-op-idxer temporal-idxer live-chunk-tx tx-ops-vec system-time))
                        !evict-idxer (delay (->evict-indexer iid-mgr log-op-idxer temporal-idxer live-chunk-tx tx-ops-vec))
                        !call-idxer (delay (->call-indexer allocator ra-src wm-src scan-emitter tx-ops-vec tx-opts))
                        !sql-idxer (delay (->sql-indexer allocator metadata-mgr buffer-pool iid-mgr
                                                         log-op-idxer temporal-idxer live-chunk-tx
                                                         tx-ops-vec ra-src wm-src scan-emitter tx-opts))]
                    (dotimes [tx-op-idx (.getValueCount tx-ops-vec)]
                      (when-let [more-tx-ops (case (.getTypeId tx-ops-vec tx-op-idx)
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
                      (.abort temporal-idxer)
                      (throw (InterruptedException. (.toString e))))
                    (catch InterruptedException e
                      (.abort temporal-idxer)
                      (throw e))
                    (catch Throwable t
                      (log/error t "error in indexer")
                      (.abort temporal-idxer)
                      (throw t)))
                wm-lock-stamp (.writeLock wm-lock)]
            (try
              (if e
                (do
                  (when (not= e abort-exn)
                    (log/debug e "aborted tx"))
                  (.abort temporal-idxer)
                  (.abort log-op-idxer)

                  (with-open [live-chunk-tx (.startTx live-chunk)]
                    (let [temporal-tx (.startTx temporal-mgr tx-key)]
                      (add-tx-row! live-chunk-tx temporal-tx iid-mgr tx-key e)
                      (.commit live-chunk-tx)
                      (.commit temporal-tx))))

                (do
                  (add-tx-row! live-chunk-tx temporal-idxer iid-mgr tx-key nil)

                  (.commit live-chunk-tx)
                  (.commit live-tries-tx)

                  (let [evicted-row-ids (.commit temporal-idxer)]
                    #_{:clj-kondo/ignore [:missing-body-in-when]}
                    (when-not (.isEmpty evicted-row-ids)
                      ;; TODO create work item
                      ))

                  (.commit log-op-idxer)))

              (set! (.-latest-completed-tx this) tx-key)

              (finally
                (.unlock wm-lock wm-lock-stamp)))))

        (while (.isBlockFull live-chunk)
          (finish-block! this))

        (when (.isChunkFull live-chunk)
          (finish-chunk! this))

        tx-key)))

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
                      (let [^IWatermark shared-wm (wm/->wm latest-completed-tx (.openWatermark live-chunk) (.getTemporalWatermark temporal-mgr) true)]
                        (set! (.shared-wm this) shared-wm)
                        (doto shared-wm .retain))
                      (finally
                        (some-> old-wm .close)))))

              (finally
                (.unlock wm-lock wm-lock-stamp)))))))

  (latestCompletedTx [_] latest-completed-tx)

  Finish
  (finish-block! [this]
    (try
      (.finishBlock live-chunk)
      (.finishBlock log-indexer)

      (let [wm-lock-stamp (.writeLock wm-lock)]
        (try
          (when-let [^IWatermark shared-wm (.shared-wm this)]
            (set! (.shared-wm this) nil)
            (.close shared-wm))

          (.nextBlock live-chunk)

          (finally
            (.unlock wm-lock wm-lock-stamp))))

      (catch Throwable t
        (clojure.tools.logging/error t "fail")
        (throw t))))

  (finish-chunk! [this]
    (let [chunk-idx (.chunkIdx live-chunk)]
      (.registerNewChunk temporal-mgr chunk-idx)
      @(.finishChunk log-indexer chunk-idx)
      @(.finishChunk live-chunk latest-completed-tx)

      ;; TODO this should be row-id not tx-id, just throwing a number in for now
      (.finishChunk live-tries (:tx-id latest-completed-tx)))

    (let [wm-lock-stamp (.writeLock wm-lock)]
      (try
        (when-let [^IWatermark shared-wm (.shared-wm this)]
          (set! (.shared-wm this) nil)
          (.close shared-wm))

        (.nextChunk live-chunk)

        (finally
          (.unlock wm-lock wm-lock-stamp))))

    (.nextChunk log-indexer)

    (log/debug "finished chunk."))

  Closeable
  (close [_]
    (.close log-indexer)
    (some-> shared-wm .close)))

(defmethod ig/prep-key :xtdb/indexer [_ opts]
  (merge {:allocator (ig/ref :xtdb/allocator)
          :object-store (ig/ref :xtdb/object-store)
          :metadata-mgr (ig/ref ::meta/metadata-manager)
          :scan-emitter (ig/ref :xtdb.operator.scan/scan-emitter)
          :temporal-mgr (ig/ref ::temporal/temporal-manager)
          :internal-id-mgr (ig/ref :xtdb.indexer/internal-id-manager)
          :live-chunk (ig/ref :xtdb/live-chunk)
          :live-tries (ig/ref :xtdb.indexer/live-tries)
          :buffer-pool (ig/ref ::bp/buffer-pool)
          :log-indexer (ig/ref :xtdb.indexer/log-indexer)
          :ra-src (ig/ref ::op/ra-query-source)}
         opts))

(defmethod ig/init-key :xtdb/indexer
  [_ {:keys [allocator object-store metadata-mgr scan-emitter buffer-pool ^ITemporalManager temporal-mgr, ra-src
             internal-id-mgr log-indexer live-chunk live-tries]}]

  (let [{:keys [latest-completed-tx]} (meta/latest-chunk-metadata metadata-mgr)]

    (Indexer. allocator object-store metadata-mgr scan-emitter buffer-pool temporal-mgr internal-id-mgr
              ra-src log-indexer live-chunk live-tries

              latest-completed-tx

              nil ; watermark
              (StampedLock.))))

(defmethod ig/halt-key! :xtdb/indexer [_ ^AutoCloseable indexer]
  (.close indexer))

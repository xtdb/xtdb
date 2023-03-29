(ns xtdb.flight-sql
  (:require [clojure.tools.logging :as log]
            [xtdb.core.sql :as sql]
            xtdb.indexer
            xtdb.ingester
            [xtdb.operator :as op]
            [xtdb.sql :as xt]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.indirect :as iv]
            [xtdb.vector.writer :as vw]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import (com.google.protobuf Any ByteString)
           xtdb.indexer.IIndexer
           xtdb.ingester.Ingester
           (xtdb.operator BoundQuery PreparedQuery IRaQuerySource)
           java.lang.AutoCloseable
           (java.util ArrayList HashMap Map)
           (java.util.concurrent CompletableFuture ConcurrentHashMap)
           (java.util.function BiConsumer BiFunction Consumer)
           (org.apache.arrow.flight FlightEndpoint FlightInfo FlightProducer$ServerStreamListener FlightProducer$StreamListener
                                    FlightServer FlightServer$Builder FlightServerMiddleware FlightServerMiddleware$Factory FlightServerMiddleware$Key
                                    FlightStream Location PutResult Result Ticket)
           (org.apache.arrow.flight.sql FlightSqlProducer)
           (org.apache.arrow.flight.sql.impl FlightSql$ActionBeginTransactionResult
                                             FlightSql$ActionCreatePreparedStatementResult
                                             FlightSql$ActionEndTransactionRequest$EndTransaction
                                             FlightSql$CommandPreparedStatementQuery
                                             FlightSql$DoPutUpdateResult
                                             FlightSql$TicketStatementQuery)
           org.apache.arrow.memory.BufferAllocator
           (org.apache.arrow.vector FieldVector VectorSchemaRoot)
           org.apache.arrow.vector.types.pojo.Schema))

;;;; populate-root temporarily copied from test-util

(defn- populate-root ^xtdb.vector.IIndirectRelation [^VectorSchemaRoot root rows]
  (.clear root)

  (let [field-vecs (.getFieldVectors root)
        row-count (count rows)]
    (doseq [^FieldVector field-vec field-vecs]
      (vw/write-vec! field-vec (map (keyword (.getName (.getField field-vec))) rows)))

    (.setRowCount root row-count)
    root))

;;;; end populate-root

(defn- new-id ^com.google.protobuf.ByteString []
  (ByteString/copyFrom (util/uuid->bytes (random-uuid))))

(defn- pack-result ^org.apache.arrow.flight.Result [res]
  (Result. (.toByteArray (Any/pack res))))

(defn then-await-fn ^java.util.concurrent.CompletableFuture [cf {:keys [^Ingester ingester]}]
  (-> cf
      (util/then-compose
       (fn [tx]
         ;; HACK til we have the ability to await on the connection
         (.awaitTxAsync ingester tx nil)))))

(doto (def ^:private do-put-update-msg
        (let [^org.apache.arrow.flight.sql.impl.FlightSql$DoPutUpdateResult$Builder
              b (doto (FlightSql$DoPutUpdateResult/newBuilder)
                  (.setRecordCount -1))]
          (.toByteArray (.build b))))

  ;; for some reason, it doesn't work with ^bytes on the symbol :/
  (alter-meta! assoc :tag 'bytes))

(defn then-send-do-put-update-res
  ^java.util.concurrent.CompletableFuture
  [^CompletableFuture cf, ^FlightProducer$StreamListener ack-stream, ^BufferAllocator allocator]

  (.whenComplete cf
                 (reify BiConsumer
                   (accept [_ _ e]
                     (if e
                       (.onError ack-stream e)

                       (do
                         (with-open [res (PutResult/metadata
                                          (doto (.buffer allocator (alength do-put-update-msg))
                                            (.writeBytes do-put-update-msg)))]
                           (.onNext ack-stream res))

                         (.onCompleted ack-stream)))))))

(def ^:private dml?
  (comp #{:insert :delete :erase :merge} first))

(defn- flight-stream->rows [^FlightStream flight-stream]
  (let [root (.getRoot flight-stream)
        rows (ArrayList.)
        col-count (count (.getFieldVectors root))]
    (while (.next flight-stream)
      (dotimes [row-idx (.getRowCount root)]
        (let [row (ArrayList. col-count)]
          (dotimes [col-idx col-count]
            (.add row (types/get-object (.getVector root col-idx) row-idx)))

          (.add rows (vec row)))))

    (vec rows)))

(defn- flight-stream->bytes [^FlightStream flight-stream]
  (util/build-arrow-ipc-byte-buffer (.getRoot flight-stream) :stream
    (fn [write-batch!]
      (while (.next flight-stream)
        (write-batch!)))))

(defn- ->fsql-producer [{:keys [allocator node, ^IIndexer idxer, ^IRaQuerySource ra-src, wm-src, ^Map fsql-txs, ^Map stmts, ^Map tickets] :as svr}]
  (letfn [(col-types->schema ^org.apache.arrow.vector.types.pojo.Schema [col-types]
            (Schema. (for [[col-name col-type] col-types]
                       (types/col-type->field col-name col-type))))

          (exec-dml [dml fsql-tx-id]
            (if fsql-tx-id
              (if (.computeIfPresent fsql-txs fsql-tx-id
                                     (reify BiFunction
                                       (apply [_ _fsql-tx-id fsql-tx]
                                         (update fsql-tx :dml conj dml))))
                (CompletableFuture/completedFuture nil)
                (CompletableFuture/failedFuture (UnsupportedOperationException. "unknown tx")))

              (-> (xt/submit-tx& node [dml])
                  (then-await-fn svr))))

          (handle-get-stream [^BoundQuery bq, ^FlightProducer$ServerStreamListener listener]
            (with-open [res (.openCursor bq)
                        vsr (VectorSchemaRoot/create (col-types->schema (.columnTypes bq)) allocator)]
              (.start listener vsr)

              (.forEachRemaining res
                                 (reify Consumer
                                   (accept [_ in-rel]
                                     (.clear vsr)
                                     ;; HACK getting results in a Clojure data structure, putting them back in to a VSR
                                     ;; because we can get DUVs in the in-rel but the output just expects mono vecs.

                                     (populate-root vsr (iv/rel->rows in-rel))
                                     (.putNext listener))))

              (.completed listener)))]

    (reify FlightSqlProducer
      (acceptPutStatement [_ cmd _ctx _flight-stream ack-stream]
        (fn []
          @(-> (exec-dml [:sql (.getQuery cmd)]
                         (when (.hasTransactionId cmd)
                           (.getTransactionId cmd)))
               (then-send-do-put-update-res ack-stream allocator))))

      (acceptPutPreparedStatementQuery [_ cmd _ctx flight-stream ack-stream]
        (fn []
          ;; TODO in tx?
          (let [ps-id (.getPreparedStatementHandle cmd)]
            (or (.computeIfPresent stmts ps-id
                                   (reify BiFunction
                                     (apply [_ _ps-id {:keys [^PreparedQuery prepd-query] :as ^Map ps}]
                                       ;; TODO we likely needn't take these out and put them back.
                                       (let [new-params (-> (first (flight-stream->rows flight-stream))
                                                            (->> (sequence (map-indexed (fn [idx v]
                                                                                          (-> (vw/open-vec allocator (symbol (str "?_" idx)) [v])
                                                                                              (iv/->direct-vec))))))
                                                            (iv/->indirect-rel 1))]
                                         (try
                                           (doto ps
                                             (some-> (.put :bound-query
                                                           (.bind prepd-query wm-src
                                                                  {:node node, :params new-params,
                                                                   :basis {:tx (.latestCompletedTx idxer)}}))
                                                     util/try-close))
                                           (catch Throwable t
                                             (util/try-close new-params)
                                             (throw t)))))))
                (throw (UnsupportedOperationException. "invalid ps-id"))))

          (.onCompleted ack-stream)))

      (acceptPutPreparedStatementUpdate [_ cmd _ctx flight-stream ack-stream]
        ;; NOTE atm the PSs are either created within a tx and then assumed to be within that tx
        ;; my mental model would be that you could create a PS outside a tx and then use it inside, but this doesn't seem possible in FSQL.
        (fn []
          @(-> (let [{:keys [sql fsql-tx-id]} (or (get stmts (.getPreparedStatementHandle cmd))
                                                  (throw (UnsupportedOperationException. "invalid ps-id")))
                     dml [:sql sql (flight-stream->bytes flight-stream)]]
                 (exec-dml dml fsql-tx-id))

               (then-send-do-put-update-res ack-stream allocator))))

      (getFlightInfoStatement [_ cmd _ctx descriptor]
        (let [sql (.toStringUtf8 (.getQueryBytes cmd))
              ticket-handle (new-id)
              ^BoundQuery bq (-> (.prepareRaQuery ra-src (sql/compile-query sql {}))
                                 ;; HACK need to get the basis from somewhere...
                                 (.bind wm-src
                                        {:node node
                                         :basis {:tx (.latestCompletedTx idxer)}}))
              ticket (Ticket. (-> (doto (FlightSql$TicketStatementQuery/newBuilder)
                                    (.setStatementHandle ticket-handle))
                                  (.build)
                                  (Any/pack)
                                  (.toByteArray)))]
          (.put tickets ticket-handle bq)
          (FlightInfo. (col-types->schema (.columnTypes bq)) descriptor
                       [(FlightEndpoint. ticket (make-array Location 0))]
                       -1 -1)))

      (getStreamStatement [_ ticket _ctx listener]
        (let [bq (or (.remove tickets (.getStatementHandle ticket))
                     (throw (UnsupportedOperationException. "unknown ticket-id")))]
          (handle-get-stream bq listener)))

      (getFlightInfoPreparedStatement [_ cmd _ctx descriptor]
        (let [ps-id (.getPreparedStatementHandle cmd)

              {:keys [bound-query ^PreparedQuery prepd-query], :as ^Map ps}
              (or (get stmts ps-id)
                  (throw (UnsupportedOperationException. "invalid ps-id")))

              ticket (Ticket. (-> (doto (FlightSql$CommandPreparedStatementQuery/newBuilder)
                                    (.setPreparedStatementHandle ps-id))
                                  (.build)
                                  (Any/pack)
                                  (.toByteArray)))

              ^BoundQuery bound-query (or bound-query
                                          (.bind prepd-query wm-src {:node node}))]
          (.put ps :bound-query bound-query)
          (FlightInfo. (col-types->schema (.columnTypes bound-query)) descriptor
                       [(FlightEndpoint. ticket (make-array Location 0))]
                       -1 -1)))

      (getStreamPreparedStatement [_ ticket _ctx listener]
        (let [{:keys [bound-query]} (or (get stmts (.getPreparedStatementHandle ticket))
                                        (throw (UnsupportedOperationException. "invalid ps-id")))]
          (handle-get-stream bound-query listener)))

      (createPreparedStatement [_ req _ctx listener]
        (let [ps-id (new-id)
              sql (.toStringUtf8 (.getQueryBytes req))
              plan (sql/compile-query sql)
              {:keys [param-count]} (meta plan)
              ps (cond-> {:id ps-id, :sql sql
                          :fsql-tx-id (when (.hasTransactionId req)
                                        (.getTransactionId req))}
                   (not (dml? plan)) (assoc :prepd-query (.prepareRaQuery ra-src plan)))]
          (.put stmts ps-id (HashMap. ^Map ps))

          (.onNext listener
                   (pack-result (-> (doto (FlightSql$ActionCreatePreparedStatementResult/newBuilder)
                                      (.setPreparedStatementHandle ps-id)
                                      (.setParameterSchema (-> (Schema. (for [idx (range param-count)]
                                                                          (types/->field (str "?_" idx) types/dense-union-type false)))
                                                               (.toByteArray)
                                                               (ByteString/copyFrom))))
                                    (.build))))

          (.onCompleted listener)))

      (closePreparedStatement [_ req _ctx listener]
        (let [ps (.remove stmts (.getPreparedStatementHandle req))]
          (some-> (:bound-query ps) util/try-close)

          (.onCompleted listener)))

      (beginTransaction [_ _req _ctx listener]
        (let [fsql-tx-id (new-id)]
          (.put fsql-txs fsql-tx-id {:dml []})

          (.onNext listener
                   (-> (doto (FlightSql$ActionBeginTransactionResult/newBuilder)
                         (.setTransactionId fsql-tx-id))
                       (.build))))

        (.onCompleted listener))

      (endTransaction [_ req _ctx listener]
        (let [fsql-tx-id (.getTransactionId req)
              {:keys [dml]} (or (.remove fsql-txs fsql-tx-id)
                                (throw (UnsupportedOperationException. "unknown tx")))]

          (if (= FlightSql$ActionEndTransactionRequest$EndTransaction/END_TRANSACTION_COMMIT
                 (.getAction req))

            @(-> (xt/submit-tx& node dml)
                 (then-await-fn svr)
                 (.whenComplete (reify BiConsumer
                                  (accept [_ _v e]
                                    (if e
                                      (.onError listener e)
                                      (.onCompleted listener))))))

            (.onCompleted listener)))))))

#_{:clj-kondo/ignore [:unused-private-var]}
(defn- with-error-logging-middleware [^FlightServer$Builder builder]
  (.middleware builder
               (FlightServerMiddleware$Key/of "error-logger")
               (reify FlightServerMiddleware$Factory
                 (onCallStarted [_ _info _incoming-headers _req-ctx]
                   (reify FlightServerMiddleware
                     (onBeforeSendingHeaders [_ _headers])
                     (onCallCompleted [_ _call-status])
                     (onCallErrored [_ e]
                       (log/error e "FSQL server error")))))))

(defmethod ig/prep-key ::server [_ opts]
  (merge {:allocator (ig/ref :xtdb/allocator)
          :node (ig/ref :xtdb.node/node)
          :idxer (ig/ref :xtdb/indexer)
          :ingester (ig/ref :xtdb/ingester)
          :ra-src (ig/ref :xtdb.operator/ra-query-source)
          :wm-src (ig/ref :xtdb/indexer)
          :host "127.0.0.1"
          :port 9832}
         opts))

(defmethod ig/init-key ::server [_ {:keys [allocator node idxer ingester ra-src wm-src host ^long port]}]
  (let [fsql-txs (ConcurrentHashMap.)
        stmts (ConcurrentHashMap.)
        tickets (ConcurrentHashMap.)
        server (doto (-> (FlightServer/builder allocator (Location/forGrpcInsecure host port)
                                               (->fsql-producer {:allocator allocator, :node node, :idxer idxer, :ingester ingester, :ra-src ra-src, :wm-src wm-src
                                                                 :fsql-txs fsql-txs, :stmts stmts, :tickets tickets}))

                         #_(doto with-error-logging-middleware)

                         (.build))
                 (.start))]
    (log/infof "Flight SQL server started, port %d" port)
    (reify AutoCloseable
      (close [_]
        (util/try-close server)
        (run! util/try-close (vals stmts))
        (log/info "Flight SQL server stopped")))))

(defmethod ig/halt-key! ::server [_ server]
  (util/try-close server))

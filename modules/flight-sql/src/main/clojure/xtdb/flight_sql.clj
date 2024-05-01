(ns xtdb.flight-sql
  (:require [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.indexer]
            [xtdb.indexer :as idx]
            [xtdb.node :as xtn]
            [xtdb.query]
            [xtdb.sql :as sql]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import [com.google.protobuf Any ByteString]
           [java.nio ByteBuffer]
           [java.util ArrayList HashMap Map]
           [java.util.concurrent CompletableFuture ConcurrentHashMap]
           [java.util.function BiConsumer BiFunction Consumer]
           [org.apache.arrow.flight FlightEndpoint FlightInfo FlightProducer$ServerStreamListener FlightProducer$StreamListener FlightServer FlightServer$Builder FlightServerMiddleware FlightServerMiddleware$Factory FlightServerMiddleware$Key FlightStream Location PutResult Result Ticket]
           [org.apache.arrow.flight.sql FlightSqlProducer]
           [org.apache.arrow.flight.sql.impl FlightSql$ActionBeginTransactionResult FlightSql$ActionCreatePreparedStatementResult FlightSql$ActionEndTransactionRequest$EndTransaction FlightSql$CommandPreparedStatementQuery FlightSql$DoPutUpdateResult FlightSql$TicketStatementQuery]
           [org.apache.arrow.memory BufferAllocator]
           [org.apache.arrow.vector FieldVector VectorSchemaRoot]
           [org.apache.arrow.vector.types.pojo Schema]
           [xtdb.api FlightSqlServer FlightSqlServer$Factory Xtdb$Config]
           xtdb.api.module.XtdbModule
           [xtdb.api.tx TxOps]
           [xtdb.indexer IIndexer]
           [xtdb.query BoundQuery IQuerySource PreparedQuery]
           [xtdb.vector IVectorReader]))

;;;; populate-root temporarily copied from test-util

(defn- populate-root ^org.apache.arrow.vector.VectorSchemaRoot [^VectorSchemaRoot root rows]
  (.clear root)

  (let [field-vecs (.getFieldVectors root)
        row-count (count rows)]
    (doseq [^FieldVector field-vec field-vecs
            :let [field-name (.getName (.getField field-vec))]]
      (vw/write-vec! field-vec (map (fn [row] (get row field-name)) rows)))

    (.setRowCount root row-count)
    root))

;;;; end populate-root

(defn- new-id ^com.google.protobuf.ByteString []
  (ByteString/copyFrom (util/uuid->bytes (random-uuid))))

(defn- pack-result ^org.apache.arrow.flight.Result [res]
  (Result. (.toByteArray (Any/pack res))))

(doto (def ^:private do-put-update-msg
        (let [^org.apache.arrow.flight.sql.impl.FlightSql$DoPutUpdateResult$Builder
              b (doto (FlightSql$DoPutUpdateResult/newBuilder)
                  (.setRecordCount -1))]
          (.toByteArray (.build b))))

  ;; for some reason, it doesn't work with ^bytes on the symbol :/
  (alter-meta! assoc :tag 'bytes))

(defn send-do-put-update-res [^FlightProducer$StreamListener ack-stream, ^BufferAllocator allocator]
  (with-open [res (PutResult/metadata
                   (doto (.buffer allocator (alength do-put-update-msg))
                     (.writeBytes do-put-update-msg)))]
    (.onNext ack-stream res))

  (.onCompleted ack-stream))

(def ^:private dml?
  (comp #{:insert :delete :erase :merge} first))

(defn- flight-stream->rows [^FlightStream flight-stream]
  (let [root (.getRoot flight-stream)
        rows (ArrayList.)
        col-rdrs (vec (vr/<-root root))]
    (while (.next flight-stream)
      (dotimes [row-idx (.getRowCount root)]
        (.add rows (mapv #(.getObject ^IVectorReader % row-idx) col-rdrs))))

    (vec rows)))

(defn- flight-stream->bytes ^ByteBuffer [^FlightStream flight-stream]
  (util/build-arrow-ipc-byte-buffer (.getRoot flight-stream) :stream
                                    (fn [write-batch!]
                                      (while (.next flight-stream)
                                        (write-batch!)))))

(defn- ->fsql-producer [{:keys [allocator node, ^IIndexer idxer, ^IQuerySource q-src, wm-src, ^Map fsql-txs, ^Map stmts, ^Map tickets] :as svr}]
  (letfn [(fields->schema ^org.apache.arrow.vector.types.pojo.Schema [fields]
            (Schema. (map #(types/field-with-name (val (first %)) (key (first %))) fields)))

          (exec-dml [dml fsql-tx-id]
            (if fsql-tx-id
              (when-not (.computeIfPresent fsql-txs fsql-tx-id
                                     (reify BiFunction
                                       (apply [_ _fsql-tx-id fsql-tx]
                                         (update fsql-tx :dml conj dml))))
                (throw (UnsupportedOperationException. "unknown tx")))

              (xt/execute-tx node [dml])))

          (handle-get-stream [^BoundQuery bq, ^FlightProducer$ServerStreamListener listener]
            (with-open [res (.openCursor bq)
                        vsr (VectorSchemaRoot/create (fields->schema (.columnFields bq)) allocator)]
              (.start listener vsr)

              (.forEachRemaining res
                                 (reify Consumer
                                   (accept [_ in-rel]
                                     (.clear vsr)
                                     ;; HACK getting results in a Clojure data structure, putting them back in to a VSR
                                     ;; because we can get DUVs in the in-rel but the output just expects mono vecs.

                                     (populate-root vsr (vr/rel->rows in-rel #xt/key-fn :snake-case-string))
                                     (.putNext listener))))

              (.completed listener)))]

    (reify FlightSqlProducer
      (acceptPutStatement [_ cmd _ctx _flight-stream ack-stream]
        (fn []
          (try
            (exec-dml [:sql (.getQuery cmd)]
                      (when (.hasTransactionId cmd)
                        (.getTransactionId cmd)))

            (send-do-put-update-res ack-stream allocator)
            (catch Throwable t
              (.onError ack-stream t)))))

      (acceptPutPreparedStatementQuery [_ cmd _ctx flight-stream ack-stream]
        (fn []
          ;; TODO in tx?
          (let [ps-id (.getPreparedStatementHandle cmd)]
            (or (.computeIfPresent stmts ps-id
                                   (reify BiFunction
                                     (apply [_ _ps-id {:keys [^PreparedQuery prepd-query] :as ^Map ps}]
                                       ;; TODO we likely needn't take these out and put them back.
                                       (util/with-close-on-catch [new-params (-> (first (flight-stream->rows flight-stream))
                                                                                 (->> (sequence (map-indexed (fn [idx v]
                                                                                                               (-> (vw/open-vec allocator (symbol (str "?_" idx)) [v])
                                                                                                                   (vr/vec->reader))))))
                                                                                 (vr/rel-reader 1))]
                                         (doto ps
                                           (some-> (.put :bound-query
                                                         (.bind prepd-query {:params new-params, :basis {:at-tx (.latestCompletedTx idxer)}}))
                                                   util/try-close))))))
                (throw (UnsupportedOperationException. "invalid ps-id"))))

          (.onCompleted ack-stream)))

      (acceptPutPreparedStatementUpdate [_ cmd _ctx flight-stream ack-stream]
        ;; NOTE atm the PSs are either created within a tx and then assumed to be within that tx
        ;; my mental model would be that you could create a PS outside a tx and then use it inside, but this doesn't seem possible in FSQL.
        (fn []
          (let [{:keys [sql fsql-tx-id]} (or (get stmts (.getPreparedStatementHandle cmd))
                                             (throw (UnsupportedOperationException. "invalid ps-id")))
                dml (TxOps/sql sql (flight-stream->bytes flight-stream))]
            (try
              (exec-dml dml fsql-tx-id)
              (send-do-put-update-res ack-stream allocator)

              (catch Throwable t
                (.onError ack-stream t))))))

      (getFlightInfoStatement [_ cmd _ctx descriptor]
        (let [sql (.toStringUtf8 (.getQueryBytes cmd))
              ticket-handle (new-id)
              ^BoundQuery bq (-> (.prepareRaQuery q-src (sql/compile-query sql) wm-src)
                                 ;; HACK need to get the basis from somewhere...
                                 (.bind {:basis {:at-tx (.latestCompletedTx idxer)}}))
              ticket (Ticket. (-> (doto (FlightSql$TicketStatementQuery/newBuilder)
                                    (.setStatementHandle ticket-handle))
                                  (.build)
                                  (Any/pack)
                                  (.toByteArray)))]
          (.put tickets ticket-handle bq)
          (FlightInfo. (fields->schema (.columnFields bq)) descriptor
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
                                          (.bind prepd-query {}))]
          (.put ps :bound-query bound-query)
          (FlightInfo. (fields->schema (.columnFields bound-query)) descriptor
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
                   (not (dml? plan)) (assoc :prepd-query (.prepareRaQuery q-src plan wm-src)))]
          (.put stmts ps-id (HashMap. ^Map ps))

          (.onNext listener
                   (pack-result (-> (doto (FlightSql$ActionCreatePreparedStatementResult/newBuilder)
                                      (.setPreparedStatementHandle ps-id)
                                      (.setParameterSchema (-> (Schema. (for [idx (range param-count)]
                                                                          (types/->field (str "?_" idx) #xt.arrow/type :union false)))
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

            (try
              (xt/execute-tx node dml)
              (.onCompleted listener)

              (catch Throwable t
                (.onError listener t)))

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

(defmethod xtn/apply-config! ::server [^Xtdb$Config config, _ {:keys [host port]}]
  (.module config (cond-> (FlightSqlServer/flightSqlServer)
                    (some? host) (.host host)
                    (some? port) (.port port))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-server [{:keys [allocator indexer q-src wm-src] :as node}
                   ^FlightSqlServer$Factory factory]
  (let [host (.getHost factory)
        port (.getPort factory)
        fsql-txs (ConcurrentHashMap.)
        stmts (ConcurrentHashMap.)
        tickets (ConcurrentHashMap.)]
    (util/with-close-on-catch [allocator (util/->child-allocator allocator "flight-sql")
                               server (doto (-> (FlightServer/builder allocator (Location/forGrpcInsecure host port)
                                                                      (->fsql-producer {:allocator allocator, :node node, :idxer indexer, :q-src q-src, :wm-src wm-src
                                                                                        :fsql-txs fsql-txs, :stmts stmts, :tickets tickets}))

                                                #_(doto with-error-logging-middleware)

                                                (.build))
                                        (.start))]

      (log/infof "Flight SQL server started, port %d" port)
      (reify XtdbModule
        (close [_]
          (util/try-close server)
          (run! util/try-close (vals stmts))
          (util/close allocator)
          (log/info "Flight SQL server stopped"))))))


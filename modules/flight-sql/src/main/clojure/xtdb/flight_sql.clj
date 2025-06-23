(ns xtdb.flight-sql
  (:require [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.indexer]
            [xtdb.node :as xtn]
            [xtdb.protocols :as xtp]
            [xtdb.tx-ops :as tx-ops]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import [com.google.protobuf Any ByteString]
           [java.nio ByteBuffer]
           [java.util ArrayList HashMap Map]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.function BiFunction Consumer]
           [org.apache.arrow.flight FlightEndpoint FlightInfo FlightProducer$ServerStreamListener FlightProducer$StreamListener FlightServer FlightServer$Builder FlightServerMiddleware FlightServerMiddleware$Factory FlightServerMiddleware$Key FlightStream Location PutResult Result Ticket]
           [org.apache.arrow.flight.sql FlightSqlProducer]
           [org.apache.arrow.flight.sql.impl FlightSql$ActionBeginTransactionResult FlightSql$ActionCreatePreparedStatementResult FlightSql$ActionEndTransactionRequest$EndTransaction FlightSql$CommandPreparedStatementQuery FlightSql$DoPutUpdateResult FlightSql$TicketStatementQuery]
           [org.apache.arrow.memory BufferAllocator]
           [org.apache.arrow.vector VectorSchemaRoot]
           [org.apache.arrow.vector.types.pojo Schema]
           [xtdb.api FlightSqlServer FlightSqlServer$Factory Xtdb$Config]
           xtdb.arrow.Relation
           xtdb.IResultCursor
           [xtdb.query IQuerySource PreparedQuery]))

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

(defn- flight-stream->rows [^BufferAllocator allocator, ^FlightStream flight-stream]
  (let [root (.getRoot flight-stream)
        rows (ArrayList.)]
    (with-open [rel (Relation/fromRoot allocator root)]
      (while (.next flight-stream)
        (.loadFromArrow rel root)
        (.addAll rows (.toTuples rel))))

    (vec rows)))

(defn- flight-stream->bytes ^ByteBuffer [^FlightStream flight-stream]
  (util/build-arrow-ipc-byte-buffer (.getRoot flight-stream) :stream
                                    (fn [write-page!]
                                      (while (.next flight-stream)
                                        (write-page!)))))

(defn- ->fsql-producer [{:keys [allocator node, ^IQuerySource q-src, wm-src, ^Map fsql-txs, ^Map stmts, ^Map tickets]}]
  (letfn [(exec-dml [dml fsql-tx-id]
            (if fsql-tx-id
              (when-not (.computeIfPresent fsql-txs fsql-tx-id
                                     (reify BiFunction
                                       (apply [_ _fsql-tx-id fsql-tx]
                                         (update fsql-tx :dml conj dml))))
                (throw (UnsupportedOperationException. "unknown tx")))

              (xtp/execute-tx node [dml] {})))

          (handle-get-stream [^IResultCursor cursor, ^FlightProducer$ServerStreamListener listener]
            (try
              (with-open [vsr (VectorSchemaRoot/create (Schema. (.getResultFields cursor)) allocator)]
                (.start listener vsr)

                (let [out-wtr (vw/root->writer vsr)]
                  (.forEachRemaining cursor
                                     (fn [in-rel]
                                       (.clear out-wtr)
                                       (vw/append-rel out-wtr in-rel)
                                       (.syncRowCount out-wtr)
                                       (.putNext listener))))

                (.completed listener))
              (catch Throwable t
                (log/error t)
                (throw t))))]

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
                                       (util/with-close-on-catch [new-args (-> (first (flight-stream->rows allocator flight-stream))
                                                                               (->> (sequence (map-indexed (fn [idx v]
                                                                                                             (-> (vw/open-vec allocator (symbol (str "?_" idx)) [v])
                                                                                                                 (vr/vec->reader))))))
                                                                               (vr/rel-reader 1))]
                                         (doto ps
                                           (some-> (.put :cursor (.openQuery prepd-query {:args new-args}))
                                                   util/close))))))
                (throw (UnsupportedOperationException. "invalid ps-id"))))

          (.onCompleted ack-stream)))

      (acceptPutPreparedStatementUpdate [_ cmd _ctx flight-stream ack-stream]
        ;; NOTE atm the PSs are either created within a tx and then assumed to be within that tx
        ;; my mental model would be that you could create a PS outside a tx and then use it inside, but this doesn't seem possible in FSQL.
        (fn []
          (let [{:keys [sql fsql-tx-id]} (or (get stmts (.getPreparedStatementHandle cmd))
                                             (throw (UnsupportedOperationException. "invalid ps-id")))
                dml (tx-ops/->SqlByteArgs sql (flight-stream->bytes flight-stream))]
            (try
              (exec-dml dml fsql-tx-id)
              (send-do-put-update-res ack-stream allocator)

              (catch Throwable t
                (.onError ack-stream t))))))

      (getFlightInfoStatement [_ cmd _ctx descriptor]
        (let [sql (.toStringUtf8 (.getQueryBytes cmd))
              ticket-handle (new-id)
              plan (.planQuery q-src sql wm-src {})
              cursor (-> (.prepareRaQuery q-src plan wm-src {})
                         (.openQuery {}))
              ticket (Ticket. (-> (doto (FlightSql$TicketStatementQuery/newBuilder)
                                    (.setStatementHandle ticket-handle))
                                  (.build)
                                  (Any/pack)
                                  (.toByteArray)))]
          (.put tickets ticket-handle cursor)
          (FlightInfo. (Schema. (.getResultFields cursor)) descriptor
                       [(FlightEndpoint. ticket (make-array Location 0))]
                       -1 -1)))

      (getStreamStatement [_ ticket _ctx listener]
        (let [cursor (or (.remove tickets (.getStatementHandle ticket))
                         (throw (UnsupportedOperationException. "unknown ticket-id")))]
          (try
            (handle-get-stream cursor listener)
            (finally
              (util/close cursor)))))

      (getFlightInfoPreparedStatement [_ cmd _ctx descriptor]
        (let [ps-id (.getPreparedStatementHandle cmd)

              {:keys [cursor ^PreparedQuery prepd-query], :as ^Map ps}
              (or (get stmts ps-id)
                  (throw (UnsupportedOperationException. "invalid ps-id")))

              ticket (Ticket. (-> (doto (FlightSql$CommandPreparedStatementQuery/newBuilder)
                                    (.setPreparedStatementHandle ps-id))
                                  (.build)
                                  (Any/pack)
                                  (.toByteArray)))

              ^IResultCursor cursor (or cursor
                                        (.openQuery prepd-query {}))]
          (.put ps :cursor cursor)
          (FlightInfo. (Schema. (.getResultFields cursor)) descriptor
                       [(FlightEndpoint. ticket (make-array Location 0))]
                       -1 -1)))

      (getStreamPreparedStatement [_ ticket _ctx listener]
        (let [{:keys [cursor]} (or (get stmts (.getPreparedStatementHandle ticket))
                                        (throw (UnsupportedOperationException. "invalid ps-id")))]
          (handle-get-stream cursor listener)))

      (createPreparedStatement [_ req _ctx listener]
        (let [ps-id (new-id)
              sql (.toStringUtf8 (.getQueryBytes req))
              plan (.planQuery q-src sql wm-src {})
              {:keys [param-count]} (meta plan)
              ps (cond-> {:id ps-id, :sql sql
                          :fsql-tx-id (when (.hasTransactionId req)
                                        (.getTransactionId req))}
                   (not (dml? plan)) (assoc :prepd-query (.prepareRaQuery q-src plan wm-src {})))]
          (.put stmts ps-id (HashMap. ^Map ps))

          (.onNext listener
                   (pack-result (-> (doto (FlightSql$ActionCreatePreparedStatementResult/newBuilder)
                                      (.setPreparedStatementHandle ps-id)
                                      (.setParameterSchema (-> (Schema. (for [idx (range param-count)]
                                                                          (types/->field (str "$" idx) #xt.arrow/type :union false)))
                                                               (.serializeAsMessage)
                                                               (ByteString/copyFrom))))
                                    (.build))))

          (.onCompleted listener)))

      (closePreparedStatement [_ req _ctx listener]
        (let [ps (.remove stmts (.getPreparedStatementHandle req))]
          (some-> (:cursor ps) util/close)

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
              (xtp/execute-tx node dml {})
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
  (.module config (cond-> (FlightSqlServer$Factory.)
                    (some? host) (.host host)
                    (some? port) (.port port))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-server [{:keys [allocator q-src live-idx] :as node}
                   ^FlightSqlServer$Factory factory]
  (let [host (.getHost factory)
        port (.getPort factory)
        fsql-txs (ConcurrentHashMap.)
        stmts (ConcurrentHashMap.)
        tickets (ConcurrentHashMap.)]
    (util/with-close-on-catch [allocator (util/->child-allocator allocator "flight-sql")
                               server (doto (-> (FlightServer/builder allocator (Location/forGrpcInsecure host port)
                                                                      (->fsql-producer {:allocator allocator, :node node, :q-src q-src, :wm-src live-idx
                                                                                        :fsql-txs fsql-txs, :stmts stmts, :tickets tickets}))

                                                #_(doto with-error-logging-middleware)

                                                (.build))
                                        (.start))]

      (log/infof "Flight SQL server started, port %d" (.getPort server))
      (reify FlightSqlServer
        (getPort [_] (.getPort server))

        (close [_]
          (util/try-close server)
          (run! util/try-close (vals stmts))
          (util/close allocator)
          (log/info "Flight SQL server stopped"))))))

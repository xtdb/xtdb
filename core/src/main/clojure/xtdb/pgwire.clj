(ns xtdb.pgwire
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cognitect.anomalies :as-alias anom]
            [cognitect.transit :as transit]
            [integrant.core :as ig]
            [xtdb.authn :as authn]
            [xtdb.db-catalog :as db]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            [xtdb.log :as xt-log]
            [xtdb.metrics :as metrics]
            [xtdb.node :as xtn]
            [xtdb.pgwire.io :as pgio]
            [xtdb.serde :as serde]
            [xtdb.sql :as sql]
            [xtdb.tx-ops :as tx-ops]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw]
            [clojure.test :as t]
            [clojure.core :as c])
  (:import io.micrometer.core.instrument.Counter
           [java.io Closeable DataInputStream EOFException IOException PushbackInputStream]
           [java.lang Thread$State]
           [java.net InetAddress ServerSocket Socket SocketException]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]
           [java.nio.channels FileChannel]
           [java.nio.file Path]
           [java.security KeyStore]
           [java.time Clock Duration ZoneId]
           [java.util.concurrent ExecutorService Executors Future$State FutureTask TimeUnit]
           [javax.net.ssl KeyManagerFactory SSLContext]
           (org.apache.arrow.memory BufferAllocator)
           org.apache.arrow.vector.types.pojo.Field
           (xtdb.api Authenticator DataSource DataSource$ConnectionBuilder OAuthResult ServerConfig Xtdb Xtdb$Config Xtdb$Connection Xtdb$ExecutedTx Xtdb$SubmittedTx)
           xtdb.api.module.XtdbModule
           (xtdb.arrow Relation Relation$ILoader VectorType)
           xtdb.arrow.RelationReader
           xtdb.database.Database$Config
           (xtdb.error Incorrect Interrupted)
           xtdb.ResultCursor
           (xtdb.pgwire PgType PgTypes)
           xtdb.JsonSerde
           xtdb.JsonLdSerde
           xtdb.NodeBase
           (xtdb.query PreparedQuery SqlParser SqlPlanner
                       ParsedStatement ParsedStatement$Visitor ParsedStatement$Begin
                       ParsedStatement$Commit ParsedStatement$CommitMode ParsedStatement$Query ParsedStatement$Dml
                       ParsedStatement$ShowVariable ParsedStatement$CopyIn ParsedStatement$Execute)
           (xtdb.tx PutDocs PutRel TxOp$Sql)))

;; references
;; https://www.postgresql.org/docs/current/protocol-flow.html
;; https://www.postgresql.org/docs/current/protocol-message-formats.html

(defrecord Server [^BufferAllocator allocator
                   ^InetAddress host
                   port read-only? playground?

                   ^ServerSocket accept-socket
                   ^Thread accept-thread

                   ^ExecutorService thread-pool

                   !closing?

                   server-state

                   ^Authenticator authn

                   ^SqlPlanner sql-planner]
  DataSource
  (createConnectionBuilder [_]
    ;; connect back to the interface we actually bound, not a hardcoded
    ;; "localhost" — a node bound to a specific address (e.g. a docker bridge)
    ;; isn't reachable on loopback. A wildcard bind is reachable on loopback, so
    ;; "localhost" is right there.
    (let [client-host (if (and host (not (.isAnyLocalAddress host)))
                        (.getHostAddress host)
                        "localhost")]
      (DataSource$ConnectionBuilder. client-host port)))

  XtdbModule
  (close [_]
    (when (compare-and-set! !closing? false true)
      (when-not (#{Thread$State/NEW, Thread$State/TERMINATED} (.getState accept-thread))
        (log/trace "Closing accept thread")
        (.interrupt accept-thread)
        (when-not (.join accept-thread (Duration/ofSeconds 5))
          (log/error "Could not shut down accept-thread gracefully" {:port port, :thread (.getName accept-thread)})))

      (let [drain-wait (:drain-wait @server-state 5000)]
        (when-not (contains? #{0, nil} drain-wait)
          (log/trace "Server draining connections")
          (let [wait-until (+ (System/currentTimeMillis) drain-wait)]
            (loop []
              (cond
                (empty? (:connections @server-state)) nil

                (Thread/interrupted) (log/warn "Interrupted during drain, force closing")

                (< wait-until (System/currentTimeMillis))
                (log/warn "Could not drain connections in time, force closing")

                :else (do (Thread/sleep 10) (recur))))))

        ;; force stopping conns
        (util/try-close (vals (:connections @server-state)))

        (when-not (.isShutdown thread-pool)
          (log/trace "Closing thread pool")
          (.shutdownNow thread-pool)
          (when-not (.awaitTermination thread-pool 5 TimeUnit/SECONDS)
            (log/error "Could not shutdown thread pool gracefully" {:port port})))

        (util/close allocator)

        (log/infof "%s stopped." (if read-only? "Read-only server" "Server"))))))

;; all postgres client IO arrives as either an untyped (startup) or typed message

(defrecord Connection [^BufferAllocator allocator
                       ^Server server, frontend, node

                       ;; a positive integer that identifies the connection on this server
                       ;; we will use this as the pg Process ID for messages that require it (such as cancellation)
                       cid

                       !closing? conn-state]

  Closeable
  (close [_]
    (log/debug "Closing connection..." {:cid cid})
    (util/close frontend)

    (doseq [portal (vals (:portals @conn-state))]
      (util/close (:cursor portal)))

    (let [{:keys [server-state]} server]
      (swap! server-state update :connections dissoc cid))

    (util/close allocator)

    (log/debug "Connection closed" {:cid cid})))

;; best the server/conn records are opaque when printed as they contain mutual references
(defmethod print-method Server [wtr o] ((get-method print-method Object) wtr o))
(defmethod print-method Connection [wtr o] ((get-method print-method Object) wtr o))

(defmulti handle-msg*
  #_{:clj-kondo/ignore [:unused-binding]}
  (fn [conn {:keys [msg-name] :as msg}]
    msg-name)

  :default ::default)

;;; errors

;;TODO parse errors should return a PSQL parse error
;;this code is generic, but there are specific ones a well
#_
(defn- err-parse [parse-failure]
  (let [lines (str/split-lines (parser/failure->str parse-failure))]
    {:severity "ERROR"
     :localized-severity "ERROR"
     :sql-state "42601"
     :message (first lines)
     :detail (str/join "\n" (rest lines))
     :position (str (inc (:idx parse-failure)))}))

(defn- err-invalid-catalog [db-name]
  (ex-info (format "database '%s' does not exist" db-name)
           {::severity :fatal, ::error-code "3D000"}))

(defn- err-invalid-auth-spec [msg] (ex-info msg {::severity :error, ::error-code "28000"}))
(defn- err-query-cancelled [msg] (ex-info msg {::severity :error, ::error-code "57014"}))

(defn- notice [msg]
  {:severity "NOTICE"
   :localized-severity "NOTICE"
   :sql-state "00000"
   :message msg})

(defn- notice-warning [msg]
  {:severity "WARNING"
   :localized-severity "WARNING"
   :sql-state "01000"
   :message msg})

(defn cmd-cancel
  "Tells the connection to stop doing what its doing and return to idle"
  [{:keys [conn-state cancelled-connections-counter] :as conn}]
  (log/debug "Cancel request received for connection" (select-keys conn [:cid]))
  (metrics/inc-counter! cancelled-connections-counter)
  (when-let [cancel-query! (:cancel-query! @conn-state)]
    (cancel-query!)))

(defn- parse-session-params [params]
  ;; values are stored raw; param-specific interpretation (e.g. fallback_output_format) happens at read time.
  (->> params
       (into {} (mapcat (fn [[k v]]
                          (case k
                            "options" (parse-session-params (for [[_ k v] (re-seq #"-c ([\w_]*)=([\w_]*)" v)]
                                                              [k v]))
                            [[k v]]))))))

(def time-zone-nf-param-name "timezone")

(def pg-param-nf->display-format
  {time-zone-nf-param-name "TimeZone"
   "datestyle" "DateStyle"
   "intervalstyle" "IntervalStyle"})

(defn- set-session-parameter [{:keys [conn-state] :as conn} parameter value]
  ;;https://www.postgresql.org/docs/current/config-setting.html#CONFIG-SETTING-NAMES-VALUES
  ;;parameter names are case insensitive, choosing to lossily downcase them for now and store a mapping to a display format
  ;;for any name not typically displayed in lower case.
  ;; the connection owns the session-param store (its serialization env / SHOW readback); pgwire keeps only the wire echo.
  (let [^Xtdb$Connection node-conn (:node-conn @conn-state)]
    (doseq [[k v] (parse-session-params {parameter value})]
      (.setSessionParameter node-conn k (some-> v str))))
  (let [param (get pg-param-nf->display-format parameter parameter)]
    (pgio/cmd-write-msg conn pgio/msg-parameter-status {:parameter param,
                                                        :value (case param
                                                                 "standard_conforming_strings" "on"
                                                                 (str value))})))

(defn set-time-zone [{:keys [conn-state] :as conn} tz]
  ;; the connection owns the tz; mid-tx it's tx-local (session-scoped on COMMIT, reverted on ROLLBACK) — so
  ;; go through setTimeZone rather than the raw defaultTz setter, which would miss the open tx.
  (let [^ZoneId zone-id (if (instance? ZoneId tz) tz (ZoneId/of tz))]
    (.setTimeZone ^Xtdb$Connection (:node-conn @conn-state) zone-id))

  (set-session-parameter conn time-zone-nf-param-name tz))

(defn- fallback-output-format
  "Interpret the raw fallback_output_format session param — the format used for complex-type row values and
  error detail when the client hasn't requested a specific one. Defaults to :json."
  [session-params]
  (or (some-> (get session-params "fallback_output_format") util/->kebab-case-kw #{:json :json-ld :transit})
      :json))

(defn- ex->pgw-err [ex]
  (let [data (ex-data ex)]
    (if (::error-code data)
      data
      (case (::anom/category data)
        ::anom/conflict (case (::err/code data)
                          :xtdb/assert-failed {::error-code "P0004", ::severity :error}
                          :prepared-query-out-of-date {::error-code "0A000", ::severity :error,

                                                       ;; PG returns this, some drivers look for it
                                                       ;; and re-prepare the query on the user's behalf
                                                       ::routine "RevalidateCachedQuery"}

                          {::severity :error, ::error-code "XX000"})

        ::anom/incorrect (case (::err/code data)
                           :xtdb/unindexed-tx {::error-code "0B000", ::severity :error}
                           ::invalid-arg-representation {::severity :error
                                                         ::error-code (case (:arg-format data)
                                                                        :text "22P02"
                                                                        :binary "22P03")}
                           :xtdb/invalid-client-credentials {::severity :error, ::error-code "28P01"}
                           :xtdb/authn-failed {::severity :error, ::error-code "28P01"}

                           {::severity :error, ::error-code "08P01"})

        ::anom/unsupported {::severity :error, ::error-code "0A000"}

        (do
          (log/error ex "Uncaught exception processing message")
          {::severity :error, ::error-code "XX000"})))))

(defn validate-and-refresh-token [{:keys [conn-state ^Authenticator authn]}]
  (when-let [^OAuthResult authn-state (:authn-state @conn-state)]
    (when-let [revalidated-state (.revalidate authn authn-state)]
      (swap! conn-state assoc :authn-state revalidated-state))))

(defmacro with-auth-check
  [conn & body]
  `(do
     (validate-and-refresh-token ~conn)
     ~@body))

(defn- encode-error-detail
  "Encodes an anomaly into the `:detail` field of a pgwire ErrorResponse.
  Returns nil if encoding fails — losing the detail is preferable to the whole
  error frame failing to send, which would present to the client as a connection
  drop (EOFException) rather than a SQLException."
  [anomaly format]
  (try
    (case format
      :json (String. (JsonSerde/encodeToBytes anomaly) StandardCharsets/UTF_8)
      :json-ld (String. (JsonLdSerde/encodeJsonLdToBytes anomaly) StandardCharsets/UTF_8)
      :transit (serde/write-transit anomaly :json))
    (catch Throwable t
      (log/warn t "Failed to encode pgwire error detail; sending error frame without detail"
                {:format format})
      nil)))

(defn send-ex [{:keys [conn-state] :as conn}, ^Throwable ex]
  (let [ex-msg (ex-message ex)

        {::keys [severity error-code routine], :keys [detail]}
        (if (::error-code (ex-data ex))
          (ex-data ex)
          (let [anomaly (err/->anomaly ex {})
                format (fallback-output-format (.getSessionParameters ^Xtdb$Connection (:node-conn @conn-state)))]
            (-> (ex->pgw-err anomaly)
                (assoc :detail (encode-error-detail anomaly format)))))

        severity-str (str/upper-case (name severity))]
    (pgio/cmd-write-msg conn pgio/msg-error-response
                        {:error-fields (cond-> {:severity severity-str
                                                :localized-severity severity-str
                                                :sql-state error-code
                                                :message ex-msg}
                                         detail (assoc :detail detail)
                                         routine (assoc :routine routine))})))

;;; startup

(defn cmd-send-ready
  "Sends a msg-ready with the given status - eg (cmd-send-ready conn :idle).
  If the status is omitted, the status is determined from whether a transaction is currently open."
  ([conn]
   (let [^Xtdb$Connection node-conn (:node-conn @(:conn-state conn))]
     (cond
       (.isTxFailed node-conn) (cmd-send-ready conn :failed-transaction)
       (.isTxOpen node-conn) (cmd-send-ready conn :transaction)
       :else (cmd-send-ready conn :idle))))
  ([conn status]
   (pgio/cmd-write-msg conn pgio/msg-ready {:status status})))

(defn startup-ok [{:keys [server] :as conn} startup-opts]
  (let [{:keys [server-state]} server]

    (let [default-server-params (-> (:parameters @server-state)
                                    (update-keys str/lower-case))
          startup-opts-from-client (-> startup-opts
                                       (update-keys str/lower-case))]

      (doseq [[k v] (merge default-server-params startup-opts-from-client)]
        (if (= time-zone-nf-param-name k)
          (set-time-zone conn v)
          (set-session-parameter conn k v))))

    ;; thread the authenticated user onto the connection — it goes into every write's TxOpts (audit trail)
    (.setUser ^Xtdb$Connection (:node-conn @(:conn-state conn)) (get startup-opts "user"))

    (-> conn
        ;; backend key data (used to identify conn for cancellation)
        (doto (pgio/cmd-write-msg pgio/msg-backend-key-data {:process-id (:cid conn), :secret-key 0}))
        (doto (cmd-send-ready)))))

(defn parse-client-creds [^String credentials]
  (cond
    (nil? credentials)
    (throw (err/incorrect :xtdb/invalid-client-credentials "Client credentials were not provided")) 

    (str/blank? credentials)
    (throw (err/incorrect :xtdb/invalid-client-credentials "Client credentials were empty"))

    :else
    (let [parts (str/split credentials #":")]
      (if (= 2 (count parts))
        (let [[client-id client-secret] parts]
          {:client-id client-id :client-secret client-secret})
        (throw (err/incorrect :xtdb/invalid-client-credentials "Client credentials must be provided in the format 'client-id:client-secret'"))))))

(defn cmd-startup-pg30 [{:keys [frontend server] :as conn} startup-opts]
  (let [{:keys [node ^Authenticator authn playground?]} server
        ^String db-name (get startup-opts "database")
        db-cat (db/<-node node)
        db (or (.databaseOrNull db-cat db-name)
               (when playground?
                 (log/debug "creating playground database" (pr-str db-name))
                 (.attach db-cat db-name nil)
                 (.databaseOrNull db-cat db-name))
               (throw (err-invalid-catalog db-name)))
        user (get startup-opts "user")
        node-conn (let [^Xtdb$Connection node-conn (.connect ^Xtdb node db-name)]
                    ;; seed the connection's clock from the server's, when set (tests pin a fixed clock there);
                    ;; otherwise the connection keeps its own. It owns the clock thereafter, pinning
                    ;; current-time off it at BEGIN.
                    (when-let [clock (:clock @(:server-state server))]
                      (.setClock node-conn clock))
                    node-conn)
        conn (assoc conn :node node, :authn authn, :db db)
        _ (swap! (:conn-state conn) assoc :node-conn node-conn)]
    (if authn
      (condp = (.methodFor authn user (pgio/host-address frontend))
        #xt.authn/method :trust
        (do
          (pgio/cmd-write-msg conn pgio/msg-auth {:result 0})
          (startup-ok conn startup-opts))

        #xt.authn/method :password
        (do
          ;; asking for a password
          (pgio/cmd-write-msg conn pgio/msg-auth {:result 3})

          ;; we go idle until we receive a message
          (when-let [{:keys [msg-name] :as msg} (pgio/read-client-msg! frontend)]
            (if (not= :msg-password msg-name)
              (throw (err-invalid-auth-spec (str "password authentication failed for user: " user)))

              (let [auth-result (.verifyPassword authn user (:password msg))
                    user-id (.getUserId auth-result)]
                (pgio/cmd-write-msg conn pgio/msg-auth {:result 0})
                (when (instance? OAuthResult auth-result)
                  (swap! (:conn-state conn) assoc :authn-state auth-result))
                (startup-ok conn (assoc startup-opts "user" user-id))))))

        #xt.authn/method :device-auth
        (let [device-auth-resp (.startDeviceAuth authn user)]
          (pgio/cmd-write-msg conn pgio/msg-auth {:result 0})
          (pgio/cmd-send-notice conn (notice (str "\nHello! Please head over here to authenticate:"
                                                  (str "\n\n" (.getUrl device-auth-resp))
                                                  "\n")))
          (let [auth-result (.await device-auth-resp)
                user-id (.getUserId auth-result)]
            (swap! (:conn-state conn) assoc :authn-state auth-result)
            (startup-ok conn (assoc startup-opts "user" user-id))))

        #xt.authn/method :client-credentials
        (do
          ;; asking for client-id/client-secret
          (pgio/cmd-write-msg conn pgio/msg-auth {:result 3})

          ;; we go idle until we receive a message
          (when-let [{:keys [msg-name] :as msg} (pgio/read-client-msg! frontend)]
            (if (not= :msg-password msg-name)
              (throw (err-invalid-auth-spec (str "client credentials authentication failed for user: " user)))

              (let [{:keys [client-id client-secret]} (parse-client-creds (:password msg))
                    auth-result (.verifyClientCredentials authn client-id client-secret)
                    user-id (.getUserId auth-result)]
                (pgio/cmd-write-msg conn pgio/msg-auth {:result 0})
                (swap! (:conn-state conn) assoc :authn-state auth-result)
                (startup-ok conn (assoc startup-opts "user" user-id))))))

        (throw (err-invalid-auth-spec (str "no authentication record found for user: " user))))
      conn)))

(defn cmd-startup-cancel [conn msg-in]
  (let [{:keys [process-id]} ((:read pgio/io-cancel-request) msg-in)
        {:keys [server]} conn
        {:keys [server-state]} server
        {:keys [connections]} @server-state

        cancel-target (get connections process-id)]

    (when cancel-target (cmd-cancel cancel-target))

    (handle-msg* conn {:msg-name :msg-terminate})))

(defn- read-startup-opts [^DataInputStream in]
  (loop [in (PushbackInputStream. in)
         acc {}]
    (let [x (.read in)]
      (cond
        (neg? x) (throw (EOFException. "EOF in read-startup-opts"))
        (zero? x) acc
        :else (do (.unread in (byte x))
                  (recur in (assoc acc (pgio/read-c-string in) (pgio/read-c-string in))))))))

(defn cmd-startup [conn]
  (try
    (loop [{{:keys [in]} :frontend, :keys [server], :as conn} conn]
      (let [{:keys [version msg-in]} (pgio/read-version in)]
        (case version
          :gssenc (throw (pgio/err-protocol-violation "GSSAPI is not supported"))

          :ssl (let [{:keys [^SSLContext ssl-ctx]} server]
                 (recur (update conn :frontend pgio/upgrade-to-ssl ssl-ctx)))

          :cancel (doto conn
                    (cmd-startup-cancel msg-in))

          :30 (-> conn
                  (cmd-startup-pg30 (read-startup-opts msg-in)))

          (throw (pgio/err-protocol-violation "Unknown protocol version")))))

    (catch EOFException e
      (log/debug e "EOFException during startup")
      (doto conn
        (handle-msg* {:msg-name :msg-terminate})))
    (catch Exception e
      (doto conn
        (send-ex e)
        (handle-msg* {:msg-name :msg-terminate})))))

;;; close

(defn- close-portal
  [{:keys [conn-state, cid]} portal-name]
  (when-some [portal (get-in @conn-state [:portals portal-name])]
    (log/trace "Closing portal" {:cid cid, :portal portal-name})
    (util/close (:cursor portal))
    (swap! conn-state update-in [:prepared-statements (:stmt-name portal) :portals] disj portal-name)
    (swap! conn-state update :portals dissoc portal-name)))

(defn- close-all-portals [{:keys [conn-state] :as conn}]
  (doseq [portal-name (keys (:portals @conn-state))]
    (close-portal conn portal-name)))

(defmethod handle-msg* :msg-close [{:keys [conn-state, cid] :as conn} {:keys [close-type, close-name]}]
  ;; Closes a prepared statement or portal that was opened with bind / parse.
  (case close-type
    :prepared-stmt
    (do
      (log/trace "Closing prepared statement" {:cid cid, :stmt close-name})
      (when-some [stmt (get-in @conn-state [:prepared-statements close-name])]

        (doseq [portal-name (:portals stmt)]
          (close-portal conn portal-name))

        (swap! conn-state update :prepared-statements dissoc close-name)))

    :portal
    (close-portal conn close-name)

    nil)

  (pgio/cmd-write-msg conn pgio/msg-close-complete))

(defmethod handle-msg* :msg-terminate [{:keys [!closing?] :as conn} _]
  (close-all-portals conn)
  (reset! !closing? true))

;;; server impl

(defn- begin-implicit
  "Open an implicit transaction — one pgwire started itself (no client BEGIN), so pgwire will auto-commit it.
  [access-mode] is :read-only / :read-write / nil (bare — the connection takes its session default access mode).
  The connection pins the read basis (awaiting this connection's own writes) at BEGIN."
  [{:keys [conn-state]} access-mode]
  (let [^Xtdb$Connection node-conn (:node-conn @conn-state)]
    (case access-mode
      :read-only (.beginReadOnly node-conn)
      :read-write (.beginWriteOnly node-conn nil nil false)
      (.beginTx node-conn))
    (swap! conn-state assoc :implicit-tx? true)))

(defn- end-transaction
  "Tear down pgwire's per-tx wire state — open portals and the implicit-tx marker. The connection has
  already discarded its own transaction (commitSync / commitAsync / rollbackTx)."
  [{:keys [conn-state] :as conn}]
  (close-all-portals conn)
  (swap! conn-state dissoc :implicit-tx?))

(defn cmd-commit
  ([conn] (cmd-commit conn nil))
  ([{:keys [conn-state tx-error-counter tx-latency-timer] :as conn} commit-mode]
   ;; the connection owns the buffered ops + tx options; it submits (async) or submits-and-awaits (sync), clears
   ;; its transaction, and records lastSubmittedTx. A commit-time COMMIT SYNC / COMMIT ASYNC wins; a bare COMMIT
   ;; (nil mode) defers to the tx's begin-time async flag. The sync commit yields the awaited ExecutedTx, whose
   ;; error pgwire re-throws so the client sees it.
   (let [^Xtdb$Connection node-conn (:node-conn @conn-state)]
     (if (.isTxFailed node-conn)
       (throw (pgio/err-protocol-violation "transaction failed"))

       (try
         (let [async? (if (nil? commit-mode)
                        (.isTxAsync node-conn)
                        (= commit-mode ParsedStatement$CommitMode/ASYNC))]
           (metrics/record-callable! tx-latency-timer
                                     (if async?
                                       (with-auth-check conn (.commitAsync node-conn))
                                       (when-let [^Xtdb$ExecutedTx executed (with-auth-check conn (.commitSync node-conn))]
                                         (when-let [error (.getError executed)]
                                           (throw error))))))
         (catch Throwable t
           (metrics/inc-counter! tx-error-counter)
           (throw t))
         (finally
           (end-transaction conn)))))))

(defn cmd-rollback [{:keys [conn-state] :as conn}]
  (.rollbackTx ^Xtdb$Connection (:node-conn @conn-state))
  (end-transaction conn))

(defn- fallback-type [session-params]
  (case (fallback-output-format session-params)
    :json PgType/PG_JSON
    :json-ld PgType/PG_JSON_LD
    :transit PgType/PG_TRANSIT))

(defn- type->pg-col [col-name ^VectorType vec-type]
  {:pg-type (PgType/fromVectorType vec-type)
   :col-name col-name})

(defn- field->pg-col [^Field field]
  (type->pg-col (.getName field) (types/->type field)))

(defn- cmd-send-row-description [{:keys [conn-state] :as conn} pg-cols]
  (let [fallback (fallback-type (.getSessionParameters ^Xtdb$Connection (:node-conn @conn-state)))
        apply-defaults (fn [{:keys [^PgType pg-type col-name result-format]}]
                         (let [^PgType pg-type (if (or (nil? pg-type) (identical? pg-type PgType/PG_DEFAULT)) fallback pg-type)]
                           {:table-oid 0
                            :column-attribute-number 0
                            :column-oid (.getOid pg-type)
                            :typlen (.getTyplen pg-type)
                            :type-modifier -1
                            :column-name col-name
                            :result-format (or result-format :text)}))
        data {:columns (mapv apply-defaults pg-cols)}]

    (log/trace "sending row description - " (assoc data :input-cols pg-cols))
    (pgio/cmd-write-msg conn pgio/msg-row-description data)))

(defn cmd-describe-portal [conn {:keys [pg-cols]}]
  (if pg-cols
    (cmd-send-row-description conn pg-cols)
    (pgio/cmd-write-msg conn pgio/msg-no-data)))

(defn cmd-send-parameter-description [conn {:keys [parsed param-oids]}]
  (log/trace "sending parameter description - " {:param-oids param-oids})
  (pgio/cmd-write-msg conn pgio/msg-parameter-description
                      {:parameter-oids (if (instance? ParsedStatement$ShowVariable parsed)
                                         []
                                         (vec (for [^long param-oid param-oids]
                                                (if (zero? param-oid)
                                                  (.getOid PgType/PG_TEXT)
                                                  param-oid))))}))

(defn cmd-describe-canned-response [conn canned-response]
  (let [{:keys [cols]} canned-response]
    (cmd-send-row-description conn cols)))

;;; Sends description messages (e.g msg-row-description) to the client for a prepared statement or portal.
(defmethod handle-msg* :msg-describe [{:keys [conn-state] :as conn} {:keys [describe-type, describe-name]}]
  (let [coll-k (case describe-type
                 :portal :portals
                 :prepared-stmt :prepared-statements
                 (Object.))
        {:keys [parsed canned-response] :as describe-target} (get-in @conn-state [coll-k describe-name])]

    (letfn [(describe* [{:keys [pg-cols] :as describe-target}]
              (when (= :prepared-stmt describe-type)
                (cmd-send-parameter-description conn describe-target))

              (if pg-cols
                (cmd-send-row-description conn pg-cols)
                (pgio/cmd-write-msg conn pgio/msg-no-data)))]

      (cond
        canned-response (cmd-describe-canned-response conn canned-response)

        parsed
        (.accept ^ParsedStatement parsed
                 (reify ParsedStatement$Visitor
                   (visitQuery [_ _] (describe* describe-target))
                   (visitShowVariable [_ _] (describe* describe-target))
                   (visitDml [_ _] (describe* describe-target))
                   (visitBegin [_ _] (describe* describe-target))
                   (visitExecute [_ stmt]
                     (let [inner (get-in @conn-state [:prepared-statements (.getName stmt)])]
                       (describe* {:param-oids (:param-oids describe-target)
                                   :pg-cols (:pg-cols inner)})))
                   (visitOther [_ _] (pgio/cmd-write-msg conn pgio/msg-no-data))))

        :else (pgio/cmd-write-msg conn pgio/msg-no-data)))))

(defmethod handle-msg* :msg-sync [{:keys [conn-state] :as conn} _]
  ;; Sync commands are sent by the client to commit transactions
  ;; and to clear the error state of a :extended mode series of commands (e.g the parse/bind/execute dance)
  (let [^Xtdb$Connection node-conn (:node-conn @conn-state)]
    (try
      (when (:implicit-tx? @conn-state)
        (if (.isTxFailed node-conn)
          (cmd-rollback conn)
          (cmd-commit conn)))
      (catch Throwable t
        (send-ex conn t))))

  (cmd-send-ready conn)
  (swap! conn-state dissoc :skip-until-sync?, :protocol))

(defmethod handle-msg* :msg-flush [{:keys [frontend]} _]
  (pgio/flush! frontend))

(def replace-queries
  {; dbeaver, #4528 - remove if/when we support duplicate projections
   "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass ORDER BY nspname"
   "SELECT n.oid,n.nspname,n.nspowner,n.nspacl,d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass ORDER BY nspname"})



(def ^:private canned-responses
  "Some pre-baked responses to common queries issued as setup by Postgres drivers, e.g SQLAlchemy"
  [{:q ";"
    :cols []
    :rows (fn [_conn] [])}

   ;; jdbc meta getKeywords (hibernate)
   ;; I think this should work, but it causes some kind of low level issue, likely
   ;; because our query protocol impl is broken, or partially implemented.
   ;; java.lang.IllegalStateException: Received resultset tuples, but no field structure for them
   {:q "select string_agg(word, ',') from pg_catalog.pg_get_keywords()"
    :cols [{:col-name "col1" :pg-type PgType/PG_VARCHAR}]
    :rows (fn [_conn] [["xtdb"]])}])

(defn- trim-sql [s]
  (-> s (str/triml) (str/replace #";\s*$" "")))

(defn- comment-only?
  "Returns true if the SQL string contains only whitespace and single-line comments.
   Uses the same pattern as the ANTLR lexer's LINE_COMMENT rule."
  [s]
  (str/blank? (str/replace s #"--[^\r\n]*" "")))

;; yagni, is everything upper'd anyway by drivers / server?
(defn- probably-same-query? [s substr]
  ;; TODO I bet this may cause some amusement. Not sure what to do about non-parsed query matching, it'll do for now.
  (str/starts-with? (str/lower-case s) (str/lower-case substr)))

(defn get-canned-response
  "If the sql string is a canned response, returns the entry from the canned-responses that matches."
  [sql-str]
  (when sql-str (first (filter #(probably-same-query? sql-str (:q %)) canned-responses))))

(defn parse-sql [sql]
  (log/debug "Interpreting SQL:" sql)
  (err/wrap-anomaly {:sql sql}
    (loop [sql (trim-sql sql)]
      (if-let [replacement (replace-queries sql)]
        (recur replacement)
        (cond
          (or (str/blank? sql) (comment-only? sql)) [{:empty? true}]

          :else (if-let [canned-response (get-canned-response sql)]
                  [{:canned-response canned-response}]
                  (mapv (fn [ps] {:parsed ps}) (SqlParser/parseStatements sql))))))))

(defn- conn-db-name ^String [{:keys [conn-state]}]
  (.getDbName ^Xtdb$Connection (:node-conn @conn-state)))

(defn- prep-stmt [{:keys [conn-state] :as conn} {:keys [param-oids ^ParsedStatement parsed] :as stmt}]
  ;; queries, EXECUTE and SHOW all prepare through the connection; everything else passes through.
  (letfn [(prepare-parsed []
            (let [^PreparedQuery pq (with-auth-check conn
                                      (.prepare ^Xtdb$Connection (:node-conn @conn-state) parsed))]
              (when-let [warnings (.getWarnings pq)]
                (doseq [warning warnings]
                  (pgio/cmd-send-notice conn (notice-warning (sql/error-string warning)))))

              (let [param-oids (->> (concat param-oids (repeat 0))
                                    (into [] (take (.getParamCount pq))))
                    param-types (map #(some-> (PgType/fromOid %) .getXtType) param-oids)
                    pg-cols (if (some nil? param-types)
                              ;; if we're unsure on some of the col-types, return all output cols as the fallback type (#4455)
                              (for [col-name (map str (.getColumnNames pq))]
                                {:pg-type PgType/PG_DEFAULT, :col-name col-name})

                              (->> (.getColumnFields pq (->> param-types
                                                             (into [] (comp (map types/->nullable-type)
                                                                            (map-indexed (fn [idx vt]
                                                                                           (types/->field (str "?_" idx) vt)))))))
                                   (mapv field->pg-col)))]
                (assoc stmt
                       :prepared-query pq
                       :param-oids param-oids
                       :prepared-pg-cols pg-cols
                       :pg-cols pg-cols))))]

    (if (nil? parsed)                   ; :empty? / :canned-response markers carry no parsed statement
      stmt
      (try
       (.accept parsed
               (reify ParsedStatement$Visitor
                 (visitQuery [_ _] (prepare-parsed))
                 (visitExecute [_ _] (prepare-parsed))
                 (visitShowVariable [_ _] (prepare-parsed))
                 ;; DML executes by buffering (cmd-exec-dml) and is re-planned server-side, so we don't prepare
                 ;; it for execution — only, best-effort, to surface planning warnings for interactive
                 ;; (non-parameterized) statements. A planning failure here must not reject the statement.
                 (visitDml [_ _]
                   (when (empty? param-oids)
                     (try
                       (let [^PreparedQuery pq (with-auth-check conn
                                                 (.prepare ^Xtdb$Connection (:node-conn @conn-state) parsed))]
                         (when-let [warnings (.getWarnings pq)]
                           (doseq [warning warnings]
                             (pgio/cmd-send-notice conn (notice-warning (sql/error-string warning))))))
                       (catch Throwable e
                         (log/debug e "Error planning DML statement for warnings"))))
                   stmt)
                 (visitOther [_ _] stmt)))

      (catch IllegalArgumentException e
        (log/debug e "Error preparing statement")
        (throw e))
      (catch RuntimeException e
        (log/debug e "Error preparing statement")
        (throw e))
       (catch Throwable e
         (log/error e "Error preparing statement")
         (throw e))))))

(defmethod handle-msg* :msg-parse [{:keys [conn-state tx-error-counter] :as conn}
                                   {:keys [stmt-name param-oids] :as msg-data}]
  (swap! conn-state assoc :protocol :extended)

  (try
    (let [[stmt & more-stmts] (parse-sql (:query msg-data))]
      (assert (nil? more-stmts) (format "TODO: found %d statements in parse" (inc (count more-stmts))))

      (let [prepared-stmt (prep-stmt conn (-> stmt (assoc :param-oids param-oids)))]
        (swap! conn-state (fn [conn-state]
                            (-> conn-state
                                (assoc-in [:prepared-statements stmt-name] prepared-stmt)))))

      (pgio/cmd-write-msg conn pgio/msg-parse-complete))

    (catch Interrupted e (throw e))
    (catch Exception e
      (when (.isTxReadWrite ^Xtdb$Connection (:node-conn @conn-state))
        (metrics/inc-counter! tx-error-counter))

      (throw e))))

(defn cmd-prepare [{:keys [conn-state] :as conn} statement-name inner-ps]
  ;; the grammar allows a single inner statement, already parsed by the enclosing PREPARE
  (let [prepared-stmt (prep-stmt conn {:parsed inner-ps})]
    (swap! conn-state assoc-in [:prepared-statements statement-name]
           (assoc prepared-stmt
                  :statement-name statement-name))

    (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "PREPARE"})))

(defn- normalize-arg-formats [arg-format arg-count]
  (case (count arg-format)
    0 (repeat arg-count :text)
    1 (repeat arg-count (first arg-format))
    arg-format))

(defn- ->xtify-arg [{:keys [arg-format param-oids]}]
  (let [arg-formats (normalize-arg-formats arg-format (count param-oids))]
    (fn xtify-arg [arg-idx arg]
      (when (some? arg)
        (let [param-oid (nth param-oids arg-idx)
              arg-format (nth arg-formats arg-idx :text)
              ^PgType pg-type (or (PgType/fromOid param-oid)
                                  (throw (err/unsupported ::unsupported-param-type "Unsupported param type provided for read"
                                                          {:param-oid param-oid, :arg-format arg-format, :arg arg})))]

          (try
            (if (= :binary arg-format)
              (.readBinary pg-type arg)
              (.readText pg-type arg))
            (catch Exception e
              (let [ex-msg (or (ex-message e) (str "invalid arg representation - " e))]
                (throw (err/incorrect ::invalid-arg-representation ex-msg
                                      {:arg-format arg-format, :arg-idx arg-idx}))))))))))

(defn- xtify-args [_conn args stmt]
  (into [] (map-indexed (->xtify-arg stmt) args)))

(defn with-result-formats [pg-types result-format]
  (let [result-formats (let [type-count (count pg-types)]
                         (cond
                           (empty? result-format) (repeat type-count :text)
                           (= 1 (count result-format)) (repeat type-count (first result-format))
                           (= (count result-format) type-count) result-format
                           :else (throw (err/incorrect ::invalid-result-format "invalid result-format for query"
                                                       {:result-format result-format, :type-count type-count}))))]
    (mapv (fn [pg-type result-format]
            (assoc pg-type :result-format result-format))
          pg-types
          result-formats)))

(defn bind-stmt [{:keys [conn-state ^BufferAllocator allocator] :as conn} {:keys [^ParsedStatement parsed ^PreparedQuery prepared-query args result-format] :as stmt}]
  (let [{:keys [^Xtdb$Connection node-conn]} @conn-state
        xt-args (xtify-args conn args stmt)]

    ;; the connection owns the QueryOpts (basis / tz / tracer) and the read access-mode gate — pgwire drives its
    ;; own cursor but opens it through the connection. checked? reads through the gate (openQuery); an internal
    ;; args-eval opens unchecked. A user query in a write tx is the pgjdbc carve-out that verify-permissibility
    ;; let through, so it too opens unchecked (openQuery would reject it). SHOW opens checked but the connection
    ;; gate-exempts it off its parsed statement.
    (letfn [(->cursor ^xtdb.ResultCursor [^PreparedQuery pq xt-args checked?]
              (with-auth-check conn
                (util/with-close-on-catch [args-rel (vw/open-args allocator xt-args)]
                  (if (and checked? (not (.isTxReadWrite node-conn)))
                    (.openQuery node-conn pq args-rel)
                    (.openUncheckedQuery node-conn pq args-rel)))))

            (->pg-cols [prepared-pg-cols ^ResultCursor cursor]
              (let [resolved-pg-cols (mapv (fn [[col-name vec-type]] (type->pg-col col-name vec-type)) (.getResultTypes cursor))]
                (when-not (and (= (count prepared-pg-cols) (count resolved-pg-cols))
                               (->> (map vector prepared-pg-cols resolved-pg-cols)
                                    (every? (fn [[{prepared-pg-type :pg-type} {resolved-pg-type :pg-type}]]
                                              (or (identical? prepared-pg-type PgType/PG_DEFAULT)
                                                  (identical? resolved-pg-type PgType/PG_NULL)
                                                  (identical? prepared-pg-type resolved-pg-type))))))
                  (throw (err/conflict :prepared-query-out-of-date "cached plan must not change result type"
                                       #_ ; FIXME: these break because of PgType not being serializable
                                       {:prepared-cols prepared-pg-cols
                                        :resolved-cols resolved-pg-cols})))
                (with-result-formats prepared-pg-cols result-format)))

            (bind-query-portal []
              (util/with-close-on-catch [cursor (->cursor prepared-query xt-args true)]
                (-> stmt
                    (assoc :cursor cursor,
                           :pg-cols (->pg-cols (:pg-cols stmt) cursor)))))]

      (if (nil? parsed)                 ; :empty? / :canned-response markers carry no parsed statement
        (-> stmt (assoc :args xt-args))
        (.accept parsed
               (reify ParsedStatement$Visitor
                 ;; queries and SHOW open the same way — the connection gate-exempts SHOW off its parsed statement
                 (visitQuery [_ _] (bind-query-portal))
                 (visitShowVariable [_ _] (bind-query-portal))

                 (visitExecute [_ ps]
                   (util/with-open [^ResultCursor args-cursor (->cursor prepared-query xt-args false)]
                     ;; we've bound the args query rather than the inner query; now bind the inner query and
                     ;; pretend this was the one we were running all along
                     (let [{^PreparedQuery inner-pq :prepared-query, :as inner} (get-in @conn-state [:prepared-statements (.getName ps)])
                           ^ParsedStatement inner-parsed (:parsed inner)
                           !args (object-array 1)]

                       (.forEachRemaining args-cursor
                                          (fn [^RelationReader args-rel]
                                            (aset !args 0 (.openSlice args-rel allocator))))

                       (let [^RelationReader args-rel (aget !args 0)]
                         (cond
                           (instance? ParsedStatement$Query inner-parsed)
                           (with-auth-check conn
                             (util/with-close-on-catch [inner-cursor (if (.isTxReadWrite node-conn)
                                                                       (.openUncheckedQuery node-conn inner-pq args-rel)
                                                                       (.openQuery node-conn inner-pq args-rel))]
                               (-> inner
                                   (assoc :cursor inner-cursor
                                          :pg-cols (-> (->pg-cols (:pg-cols inner) inner-cursor)
                                                       (with-result-formats result-format))))))

                           (instance? ParsedStatement$Dml inner-parsed)
                           (let [arg-types (.getResultTypes args-cursor)]
                             (try
                               (-> inner
                                   (assoc :args (vec (for [col-name (keys arg-types)]
                                                       (-> (.vectorForOrNull args-rel col-name)
                                                           (.getObject 0))))
                                          :param-oids (->> arg-types
                                                           (mapv (fn [[_col-name ^VectorType vec-type]]
                                                                   (PgType/.getOid (PgType/fromVectorType vec-type)))))))
                               (finally
                                 (util/close args-rel))))

                           :else (throw (err/unsupported ::unsupported-execute "EXECUTE only supports a prepared query or DML statement")))))))

                 (visitOther [_ _]
                   (-> stmt
                       (assoc :args xt-args)))))))))

(defn unnamed-portal? [portal-name]
  (= "" portal-name))

(defmethod handle-msg* :msg-bind [{:keys [conn-state] :as conn} {:keys [portal-name stmt-name] :as bind-msg}]
  (swap! conn-state assoc :protocol :extended)
  (let [stmt (into (or (get-in @conn-state [:prepared-statements stmt-name])
                       (throw (pgio/err-protocol-violation "no prepared statement")))
                   bind-msg)]
    (when (unnamed-portal? portal-name)
      (close-portal conn portal-name))

    (when (get-in @conn-state [:portals portal-name])
      (throw (pgio/err-protocol-violation "Named portals must be explicit closed before they can be redefined")))

    (let [bound-stmt (bind-stmt conn stmt)]
      (swap! conn-state
             (fn [cs]
               (-> cs
                   (assoc-in [:portals portal-name] bound-stmt)
                   (update-in [:prepared-statements stmt-name :portals] (fnil conj #{}) portal-name)))))

    (pgio/cmd-write-msg conn pgio/msg-bind-complete)))

(defn- strip-semi-colon [s] (if (str/ends-with? s ";") (subs s 0 (dec (count s))) s))

(defn- statement-head [s]
  (-> s (str/split #"\s+") first str/upper-case strip-semi-colon))

(def ^:private pgjdbc-type-query
  ;; need to allow this one in RW transactions
  "SELECT pg_type.oid, typname FROM pg_catalog.pg_type LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r from pg_namespace as ns join ( select s.r, (current_schemas(false))[s.r] as nspname from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r using ( nspname ) ) as sp ON sp.nspoid = typnamespace WHERE typname = $1 ORDER BY sp.r, pg_type.oid DESC LIMIT 1")

(defn- verify-permissibility
  [{:keys [conn-state server]} {:keys [^ParsedStatement parsed]}]
  ;; the connection gates access-mode too (executeDml / resolveForQuery), but pgwire keeps these checks for
  ;; their own error codes/messages, the read-only-server rule (a server property, not connection state), and
  ;; the pgjdbc type-query special case — so it reads the tx's resolved access-mode off the connection.
  (let [^Xtdb$Connection node-conn (:node-conn @conn-state)
        dml? (instance? ParsedStatement$Dml parsed)
        query? (instance? ParsedStatement$Query parsed)]
    (when (and dml? (:read-only? server))
      (throw (err/incorrect :xtdb/dml-in-read-only-server
                            "DML is not allowed on the READ ONLY server"
                            {:query (.getOriginalSql parsed)})))

    (when (and dml? (.isTxReadOnly node-conn))
      (throw (err/incorrect :xtdb/dml-in-read-only-tx
                            "DML is not allowed in a READ ONLY transaction"
                            {:query (.getOriginalSql parsed)})))

    (when (and query? (.isTxReadWrite node-conn)
               (not= pgjdbc-type-query (str/replace (.getOriginalSql parsed) #"  +" " ")))
      (throw (err/incorrect :xtdb/queries-in-read-write-tx
                            "Queries are unsupported in a DML transaction"
                            {:query (.getOriginalSql parsed)})))))

(defn cmd-write-canned-response [conn {:keys [q rows] :as _canned-resp}]
  (let [rows (rows conn)]
    (doseq [row rows]
      (pgio/cmd-write-msg conn pgio/msg-data-row {:vals (mapv (fn [v] (if (bytes? v) v (PgTypes/utf8 v))) row)}))

    (pgio/cmd-write-msg conn pgio/msg-command-complete {:command (str (statement-head q) " " (count rows))})))

(defn cmd-set-session-parameter [conn parameter value]
  (if (= time-zone-nf-param-name parameter)
    (set-time-zone conn value)
    (set-session-parameter conn parameter value))
  (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "SET"}))

(defn cmd-set-transaction [conn _tx-opts]
  ;; no-op - can only set transaction isolation, and that
  ;; doesn't mean anything to us because we're always serializable
  (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "SET TRANSACTION"}))

(defn cmd-set-time-zone [{:keys [server] :as conn} zone-expr args]
  (set-time-zone conn (.evalLiteral ^SqlPlanner (:sql-planner server) zone-expr args))
  (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "SET TIME ZONE"}))

(defn cmd-set-await-token [{:keys [conn-state server] :as conn} token-expr args]
  (.setAwaitToken ^Xtdb$Connection (:node-conn @conn-state) (.evalLiteral ^SqlPlanner (:sql-planner server) token-expr args))
  (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "SET AWAIT_TOKEN"}))

(defn cmd-set-session-characteristics [{:keys [conn-state] :as conn} access-mode]
  ;; the connection holds the default access mode a subsequent bare BEGIN takes
  (.setSessionCharacteristics ^Xtdb$Connection (:node-conn @conn-state) access-mode)
  (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "SET SESSION CHARACTERISTICS"}))

(defn- cmd-exec-dml [{:keys [conn-state ^BufferAllocator allocator tx-error-counter] :as conn} {:keys [^ParsedStatement$Dml parsed args param-oids]}]
  (let [^Xtdb$Connection node-conn (:node-conn @conn-state)
        query (.getOriginalSql parsed)]
    (when (.isTxFailed node-conn)
      (throw (pgio/err-protocol-violation "current transaction is aborted, commands ignored until ROLLBACK is received")))

    (when (or (not= (count param-oids) (count args))
              (some (fn [idx]
                      (and (zero? (nth param-oids idx))
                           (some? (nth args idx))))
                    (range (count param-oids))))
      (metrics/inc-counter! tx-error-counter)
      (throw (err/incorrect ::missing-arg-types "Missing types for args - client must specify types for all non-null params in DML statements"
                            {:query query, :param-oids param-oids})))

    (when-not (.isTxOpen node-conn)
      (begin-implicit conn :read-write))

    ;; hand the op to the connection — it buffers, coalescing consecutive same-SQL ops, and submits at COMMIT.
    ;; no-param DML passes nil args (not an empty relation), matching the connection's own DML path.
    (.executeDml node-conn (TxOp$Sql. query (when (seq args) (vw/open-args allocator args))))

    ;; oid is always 0 (legacy pg); row count is 0 because we're async
    (pgio/cmd-write-msg conn pgio/msg-command-complete
                        {:command (.accept parsed
                                           (reify ParsedStatement$Visitor
                                             (visitInsert [_ _] "INSERT 0 0")
                                             (visitUpdate [_ _] "UPDATE 0")
                                             (visitDelete [_ _] "DELETE 0")
                                             (visitPatch [_ _] "PATCH 0")
                                             (visitErase [_ _] "ERASE 0")
                                             (visitAssert [_ _] "ASSERT")
                                             (visitGrantRole [_ _] "GRANT")
                                             (visitRevokeRole [_ _] "REVOKE")
                                             (visitCreateTable [_ _] "CREATE TABLE")))})))

(defn run-cancellable-query! [{:keys [conn-state] :as _conn} f]
  (let [task (FutureTask. f)] ; FutureTask used for cancellation
    (swap! conn-state assoc :cancel-query! #(when (.cancel task true)
                                              (log/debug "Query cancelled")))
    (try
      (.run task) ; in-thread execution
      (finally
        (swap! conn-state dissoc :cancel-query!)))
    (condp = (.state task)
      Future$State/SUCCESS (do)
      Future$State/CANCELLED (do
                               (Thread/interrupted) ; FutureTask may leak interruption status
                               (throw (err-query-cancelled "query cancelled during execution")))
      Future$State/FAILED (throw (.exceptionNow task)))))

(defn cmd-exec-query [{:keys [conn-state !closing? query-error-counter] :as conn}
                      {:keys [limit ^ParsedStatement parsed ^ResultCursor cursor pg-cols portal-name pending-rows total-rows-sent]
                       :as _portal}]
  ;; Create an implicit transaction if one hasn't already been started
  (let [^Xtdb$Connection node-conn (:node-conn @conn-state)]
    (when (.isTxFailed node-conn)
      (throw (pgio/err-protocol-violation "current transaction is aborted, commands ignored until ROLLBACK is received")))

    (when-not (or (.isTxOpen node-conn) (instance? ParsedStatement$ShowVariable parsed))
      (begin-implicit conn :read-only)))

  (try
    (let [!n-rows-out (volatile! 0)
          !pending (volatile! [])
          session-params (.getSessionParameters ^Xtdb$Connection (:node-conn @conn-state))
          fallback (fallback-type session-params)
          serialize-row (fn [^RelationReader rel idx]
                          (mapv (fn [{:keys [^String col-name pg-type result-format]}]
                                  (let [^PgType pg-type (if (or (nil? pg-type) (identical? pg-type PgType/PG_DEFAULT)) fallback pg-type)
                                        rdr (.vectorForOrNull rel col-name)]
                                    (when-not (.isNull rdr idx)
                                      (if (= :binary result-format)
                                        (.writeBinary pg-type session-params rdr idx)
                                        (.writeText pg-type session-params rdr idx)))))
                                pg-cols))
          send-row! (fn [row]
                      (pgio/cmd-write-msg conn pgio/msg-data-row {:vals row})
                      (vswap! !n-rows-out inc))]
      (run-cancellable-query!
       conn
       (fn []
         ;; Send any pending rows from previous Execute
         (when (not-empty pending-rows)
           (let [[to-send to-keep] (split-at (or limit (count pending-rows)) pending-rows)] 
             (run! send-row! to-send)
             (vreset! !pending (vec to-keep))))

         ;; If no pending rows left to process, continue with cursor
         (while (and (empty? @!pending)
                     (or (nil? limit) (< @!n-rows-out limit))
                     (.tryAdvance cursor
                                  (fn [^RelationReader rel]
                                    (log/trace "advancing cursor with rel count" (.getRowCount rel))
                                    (cond
                                      (Thread/interrupted) (throw (InterruptedException.))

                                      @!closing? (log/trace "query result stream stopping (conn closing)")

                                      :else (let [row-count (.getRowCount rel)
                                                  num-to-send (cond-> row-count
                                                                limit (min (- limit @!n-rows-out)))]
                                              ;; Send rows up to limit 
                                              (dotimes [idx num-to-send] 
                                                (send-row! (serialize-row rel idx)))
                                              ;; Buffer any remaining rows for next Execute 
                                              (dotimes [idx (- row-count num-to-send)] 
                                                (vswap! !pending conj (serialize-row rel (+ num-to-send idx))))))))))))

      ;; Save any pending rows and cumulative count back to portal
      (let [cumulative-rows (+ (or total-rows-sent 0) @!n-rows-out)]
        (when portal-name
          (swap! conn-state update-in [:portals portal-name]
                 assoc :pending-rows @!pending :total-rows-sent cumulative-rows))

        (if (= @!n-rows-out limit)
          (pgio/cmd-write-msg conn pgio/msg-portal-suspended)
          (pgio/cmd-write-msg conn pgio/msg-command-complete {:command (str (statement-head (.getOriginalSql parsed)) " " cumulative-rows)}))))

    (catch Interrupted e (throw e))
    (catch InterruptedException e (throw e))
    (catch Throwable e
      (metrics/inc-counter! query-error-counter)
      (throw e))))

(defn- attach-db [{:keys [conn-state] :as conn} db-name config-yaml sql]
  (let [default-db (conn-db-name conn)]
    (when-not (= default-db "xtdb")
      (throw (err/incorrect ::attach-db-on-secondary "Can only attach databases when connected to the primary 'xtdb' database."
                            {:db default-db}))))

  (when (and (.isTxOpen ^Xtdb$Connection (:node-conn @conn-state)) (not (:implicit-tx? @conn-state)))
    (throw (err/incorrect ::attach-db-in-tx "Cannot attach a database in a transaction."
                          {:db-name db-name})))

  (let [db-config (try
                    (or (some-> config-yaml (Database$Config/fromYaml)) (Database$Config.))
                    (catch Exception e
                      (throw (err/incorrect :xtdb/invalid-database-config
                                            (str "Invalid database config in `ATTACH DATABASE`: " (ex-message e))
                                            {:sql sql}))))]
    (with-auth-check conn
      (let [tx (.attachDb ^Xtdb$Connection (:node-conn @conn-state) db-name db-config)]
        (when-let [error (.getError tx)]
          (throw error))))))

(defn- detach-db [{:keys [conn-state] :as conn} db-name]
  (let [default-db (conn-db-name conn)]
    (when-not (= default-db "xtdb")
      (throw (err/incorrect ::detach-db-on-secondary "Can only detach databases when connected to the primary 'xtdb' database."
                            {:db default-db}))))

  (when (and (.isTxOpen ^Xtdb$Connection (:node-conn @conn-state)) (not (:implicit-tx? @conn-state)))
    (throw (err/incorrect ::detach-db-in-tx "Cannot detach a database in a transaction."
                          {:db-name db-name})))

  (with-auth-check conn
    (let [tx (.detachDb ^Xtdb$Connection (:node-conn @conn-state) db-name)]
      (when-let [error (.getError tx)]
        (throw error)))))

(defn execute-portal [{:keys [conn-state query-timer] :as conn} {:keys [^ParsedStatement parsed canned-response] :as portal}]
  (verify-permissibility conn portal)

  (cond
    canned-response (cmd-write-canned-response conn canned-response)
    (:empty? portal) (pgio/cmd-write-msg conn pgio/msg-empty-query)

    :else
    (.accept parsed
             (reify ParsedStatement$Visitor
               (visitQuery [_ _]
                 ;; the read gate + basis resolution happen when bind-stmt opens the cursor through the
                 ;; connection's openQuery (a read-write tx opens unchecked — the pgjdbc carve-out).
                 (metrics/record-callable! query-timer (cmd-exec-query conn portal)))
               (visitShowVariable [_ _] (metrics/record-callable! query-timer (cmd-exec-query conn portal)))
               (visitDml [_ _] (cmd-exec-dml conn portal))

               (visitBegin [_ stmt]
                 ;; pass the portal's bound args — a BEGIN option can be a parameter placeholder (e.g. AWAIT_TOKEN = $1)
                 (.beginParsedTx ^Xtdb$Connection (:node-conn @conn-state) (.getTxOptions stmt) (:args portal))
                 (swap! conn-state assoc :implicit-tx? false)
                 (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "BEGIN"}))

               (visitCommit [_ ^ParsedStatement$Commit stmt]
                 (cmd-commit conn (.getMode stmt))
                 (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "COMMIT"}))

               (visitRollback [_ _]
                 (cmd-rollback conn)
                 (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "ROLLBACK"}))

               (visitSetTransaction [_ _] (cmd-set-transaction conn nil))

               (visitSetSessionCharacteristics [_ stmt]
                 (cmd-set-session-characteristics conn (.getAccessMode stmt)))

               (visitSetTimeZone [_ stmt] (cmd-set-time-zone conn (.getZone stmt) (:args portal)))
               (visitSetAwaitToken [_ stmt] (cmd-set-await-token conn (.getToken stmt) (:args portal)))
               (visitSetSessionParameter [_ stmt]
                 (cmd-set-session-parameter conn (.getName stmt)
                                            (.evalLiteral ^SqlPlanner (:sql-planner (-> conn :server )) (.getValue stmt) nil)))
               (visitSetRole [_ _] nil)

               (visitCopyIn [_ stmt]
                 (when (nil? (.getFormat stmt))
                   (throw (err/incorrect ::invalid-copy-format
                                         "COPY IN requires a valid format: 'arrow-file', 'arrow-stream', 'transit-json', 'transit-msgpack'"
                                         {:format nil})))
                 (let [format (case (str (.getFormat stmt))
                                "TRANSIT_JSON" :transit-json
                                "TRANSIT_MSGPACK" :transit-msgpack
                                "ARROW_FILE" :arrow-file
                                "ARROW_STREAM" :arrow-stream)
                       table-name (let [s (.getSchema stmt) t (.getTable stmt)]
                                    (if s (symbol s t) (symbol t)))
                       copy-file (doto (util/->temp-file "copy-in" "")
                                   (-> .toFile (.deleteOnExit)))]

                   (swap! conn-state assoc
                          :copy {:table-name table-name
                                 :format format
                                 :copy-file copy-file
                                 :write-ch (util/->file-channel copy-file util/write-truncate-open-opts)})

                   (log/trace "Starting COPY IN" {:cid (:cid conn), :table-name table-name, :copy-file copy-file})

                   (pgio/cmd-write-msg conn pgio/msg-copy-in-response
                                       (let [copy-format (case format
                                                           :transit-json :text
                                                           (:transit-msgpack :arrow-file :arrow-stream) :binary)]
                                         {:copy-format copy-format
                                          :column-formats [copy-format]}))))

               (visitPrepare [_ stmt] (cmd-prepare conn (.getName stmt) (.getInner stmt)))

               (visitAttachDatabase [_ stmt]
                 (attach-db conn (.getDbName stmt) (.getConfigYaml stmt) (.getOriginalSql stmt))
                 (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "ATTACH DATABASE"}))

               (visitDetachDatabase [_ stmt]
                 (detach-db conn (.getDbName stmt))
                 (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "DETACH DATABASE"}))))))

(defmethod handle-msg* :msg-execute [{:keys [conn-state] :as conn} {:keys [portal-name limit]}]
  ;; Handles a msg-execute to run a previously bound portal (via msg-bind).
  (let [portal (or (get-in @conn-state [:portals portal-name])
                   (throw (pgio/err-protocol-violation "no such portal")))]
    (execute-portal conn (cond-> portal
                           (not (zero? limit)) (assoc :limit limit)
                           :always (assoc :portal-name portal-name)))))

(defmethod handle-msg* :msg-simple-query [{:keys [conn-state] :as conn} {:keys [query]}]
  (swap! conn-state assoc :protocol :simple)

  (close-portal conn "")

  (try
    (doseq [{:keys [^ParsedStatement parsed] :as stmt} (parse-sql query)]
      ;; an explicit BEGIN opens its own tx (and the connection rejects a double-begin), so don't auto-begin
      ;; for it; SHOW / COPY don't need a wrapping tx either.
      (when-not (or (.isTxOpen ^Xtdb$Connection (:node-conn @conn-state))
                    (instance? ParsedStatement$Begin parsed)
                    (instance? ParsedStatement$ShowVariable parsed)
                    (instance? ParsedStatement$CopyIn parsed))
        (begin-implicit conn nil))

      (try
        (let [{:keys [param-oids canned-response] :as prepared-stmt} (prep-stmt conn stmt)]
          (when (and (seq param-oids) (not (instance? ParsedStatement$ShowVariable parsed)))
            (throw (pgio/err-protocol-violation "Parameters not allowed in simple queries")))

          (let [portal (bind-stmt conn prepared-stmt)]
            (try
              (when (or canned-response
                        (instance? ParsedStatement$Query parsed)
                        (instance? ParsedStatement$ShowVariable parsed)
                        (and (instance? ParsedStatement$Execute parsed)
                             (instance? ParsedStatement$Query
                                        (:parsed (get-in @conn-state [:prepared-statements (.getName ^ParsedStatement$Execute parsed)])))))
                ;; client only expects a RowDescription (from cmd-describe) for certain statement types
                (cmd-describe-portal conn portal))

              (execute-portal conn portal)
              (finally
                (util/close (:cursor portal))))))

        (catch Interrupted e (throw e))
        (catch InterruptedException e (throw e))
        (catch Exception e
          (let [^Xtdb$Connection node-conn (:node-conn @conn-state)]
            (when (.isTxOpen node-conn)
              (if (:implicit-tx? @conn-state)
                (cmd-rollback conn)
                (.failTx node-conn e))))

          (send-ex conn e))))

    (let [^Xtdb$Connection node-conn (:node-conn @conn-state)]
      (when (:implicit-tx? @conn-state)
        (if (.isTxFailed node-conn)
          (cmd-rollback conn)
          (cmd-commit conn))))

    ;; here we catch explicitly because we need to send the error, then a ready message
    (catch Interrupted e (throw e))
    (catch InterruptedException e (throw e))
    (catch Throwable e
      (send-ex conn e)))

  (when-not (:copy @conn-state)
    (cmd-send-ready conn)))

(defmethod handle-msg* :msg-copy-data [{:keys [conn-state]} {:keys [data]}]
  (let [{:keys [^FileChannel write-ch]} (or (:copy @conn-state)
                                            (throw (err/incorrect ::copy-not-in-progress "COPY IN not in progress, cannot write data")))]
    (.write write-ch (ByteBuffer/wrap data))))

(defn- copy-exec-op
  "Buffer one converted COPY op on the connection (the client PutDocs/PutRel is converted to a core TxOp via
  open-tx-op, exactly as the tx submit path does). The connection owns the op once buffered."
  [{:keys [conn-state] :as conn} client-op allocator]
  (let [^Xtdb$Connection node-conn (:node-conn @conn-state)]
    (when (.isTxReadOnly node-conn)
      (throw (err/incorrect :xtdb/copy-in-read-only-tx
                            "COPY is not allowed in a READ ONLY transaction")))
    (.executeDml node-conn (xt-log/open-tx-op client-op allocator {:default-tz (.getDefaultTz node-conn)}))))

(defn- copy-transit-batch ^long [{:keys [conn-state ^BufferAllocator allocator] :as conn} {:keys [table-name format, ^Path copy-file]}]
  (let [^Xtdb$Connection node-conn (:node-conn @conn-state)
        started-tx? (when-not (.isTxOpen node-conn)
                      (begin-implicit conn :read-write)
                      true)]
    (try
      (let [docs (with-open [is (io/input-stream (.toFile copy-file))]
                   (vec (serde/transit-seq (transit/reader is
                                                           (case format
                                                             :transit-json :json
                                                             :transit-msgpack :msgpack)
                                                           {:handlers serde/transit-read-handler-map}))))]

        (copy-exec-op conn (PutDocs. table-name docs nil nil) allocator)

        (when started-tx?
          (cmd-commit conn))

        (count docs))

      (catch Throwable e
        (when started-tx?
          (cmd-rollback conn))
        (throw e)))))

(defn- copy-arrow-batches ^long [{:keys [^BufferAllocator allocator, conn-state] :as conn} {:keys [table-name format, ^Path copy-file]}]
  (let [^Xtdb$Connection node-conn (:node-conn @conn-state)
        !doc-count (atom 0)]
    (util/with-open [^Relation$ILoader ldr (case format
                                             :arrow-stream (Relation/streamLoader allocator copy-file)
                                             :arrow-file (Relation/loader allocator copy-file))
                     rel (Relation. allocator (.getSchema ldr))]
      (if (.isTxOpen node-conn)
        ;; inside an open tx: buffer every batch on it (the connection rejects a read-only tx, resolves an
        ;; unresolved one to read-write); the user's COMMIT submits them.
        (while (.loadNextPage ldr rel)
          (copy-exec-op conn (PutRel. table-name (.getAsArrowStream rel)) allocator)
          (swap! !doc-count + (.getRowCount rel)))

        ;; no active tx: the input is naturally batched, so we commit a transaction per record-batch.
        (while (.loadNextPage ldr rel)
          (begin-implicit conn :read-write)
          (try
            (copy-exec-op conn (PutRel. table-name (.getAsArrowStream rel)) allocator)
            (cmd-commit conn)
            (swap! !doc-count + (.getRowCount rel))
            (catch Throwable t
              (cmd-rollback conn)
              (throw t))))))
    @!doc-count))

(defmethod handle-msg* :msg-copy-done [{:keys [conn-state] :as conn} _msg]
  (let [{:keys [^Path copy-file, ^FileChannel write-ch, format] :as copy-stmt}
        (or (:copy @conn-state)
            (throw (err/incorrect ::copy-not-in-progress
                                  "COPY IN not in progress, cannot write data")))]
    (try
      (err/wrap-anomaly {}
        (.close write-ch)

        (let [doc-count (case format
                          (:transit-json :transit-msgpack) (copy-transit-batch conn copy-stmt)
                          (:arrow-file :arrow-stream) (copy-arrow-batches conn copy-stmt))]
          
          (pgio/cmd-write-msg conn pgio/msg-command-complete {:command (str "COPY " doc-count)})))

      (catch Incorrect e
        (log/debug e "Error writing COPY IN data")
        (send-ex conn e))

      (catch Throwable e
        (log/warn e "Error writing COPY IN data")
        (send-ex conn e))

      (finally
        (util/delete-file copy-file)))

    (swap! conn-state dissoc :copy)

    (cmd-send-ready conn)))

(defmethod handle-msg* :msg-copy-fail [{:keys [conn-state] :as conn} _msg]
  (when-let [{:keys [copy-file write-ch]} (:copy @conn-state)]
    (try
      (util/close write-ch)
      (catch Exception e
        (log/debug e "Error closing COPY write channel during failure")))

    (try
      (util/delete-file copy-file)
      (catch Exception e
        (log/debug e "Error deleting COPY file during failure")))

    (swap! conn-state dissoc :copy))

  (cmd-send-ready conn))

;; ignore password messages, we are authenticated when getting here
(defmethod handle-msg* :msg-password [_conn _msg])

(defmethod handle-msg* ::default [_conn {:keys [msg-name]}]
  (throw (err/unsupported ::unknown-client-msg (str "unknown client message: " msg-name)
                          {:msg-name msg-name})))

(defn handle-msg [{:keys [cid ^Authenticator authn conn-state] :as conn} {:keys [msg-name] :as msg}]
  (try
    (log/trace "Read client msg" {:cid cid, :msg msg})

    (if (and (:skip-until-sync? @conn-state) (not= :msg-sync msg-name))
      (log/trace "Skipping msg until next sync due to error in extended protocol" {:cid cid, :msg msg})
      (handle-msg* conn msg))

    (catch Interrupted e (throw e))
    (catch InterruptedException e (throw e))

    (catch Throwable e
      (log/debug e "error processing message: " (ex-message e))
      ;; error seen while in :extended mode — start skipping messages until sync received
      (when (= :extended (:protocol @conn-state))
        (swap! conn-state assoc :skip-until-sync? true))
      ;; mark an open tx failed (for now, any error does this)
      (let [^Xtdb$Connection node-conn (:node-conn @conn-state)]
        (when (.isTxOpen node-conn)
          (.failTx node-conn e)))

      (send-ex conn e))))

(defn- conn-loop [{:keys [cid, server, conn-state],
                   {:keys [^Socket socket] :as frontend} :frontend,
                   !conn-closing? :!closing?,
                   :as conn}]
  (let [{:keys [port], !server-closing? :!closing?} server]
    (loop []
      (cond
        @!conn-closing?
        (log/trace "Connection loop exiting (closing)" {:port port, :cid cid})

        (and @!server-closing?
             (not= :extended (:protocol @conn-state))
             ;; for now we allow buffered commands to be run
             ;; before closing - there probably needs to be limits to this
             ;; (for huge queries / result sets)
             (empty? (:cmd-buf @conn-state)))
        (do (log/trace "Connection loop exiting (draining)" {:port port, :cid cid})
            ;; TODO I think I should send an error, but if I do it causes a crash on the client?
            #_(throw (err-admin-shutdown "draining connections"))
            (reset! !conn-closing? true))

        ;; well, it won't have been us, as we would drain first
        (.isClosed socket)
        (do (log/trace "Connection closed unexpectedly" {:port port, :cid cid})
            (reset! !conn-closing? true))

        ;; go idle until we receive another msg from the client
        :else (do
                (when-let [msg (pgio/read-client-msg! frontend)]
                  (handle-msg conn msg))

                (recur))))))

(defn- connect
  "Starts and runs a connection on the current thread until it closes.

  The connection exiting for any reason (either because the connection, received a close signal, or the server is draining, or unexpected error) should result in connection resources being
  freed at the end of this function. So the connections lifecycle should be totally enclosed over the lifetime of a connect call.

  See comment 'Connection lifecycle'."
  [{:keys [node, ^Authenticator authn, server-state, port, allocator, query-error-counter, tx-error-counter, tx-latency-timer, ^Counter total-connections-counter, ^Counter cancelled-connections-counter, query-timer, query-tracer] :as server} ^Socket conn-socket]
  (let [close-promise (promise)
        {:keys [cid !closing?] :as conn} (util/with-close-on-catch [_ conn-socket]
                                           (let [cid (:next-cid (swap! server-state update :next-cid (fnil inc 0)))
                                                 ;; the connection owns the tx + clock + access-mode + session params now
                                                 !conn-state (atom {:close-promise close-promise})
                                                 !closing? (atom false)]
                                             (log/debug "New connection" {:cid cid})
                                             (try
                                               (-> (map->Connection {:cid cid, :node node, :authn authn, :server server,
                                                                     :frontend (pgio/->socket-frontend conn-socket),
                                                                     :!closing? !closing?
                                                                     :allocator (util/->child-allocator allocator (str "pg-conn-" cid))
                                                                     :conn-state !conn-state})
                                                   (cmd-startup))
                                               (catch EOFException _
                                                 (reset! !closing? true))
                                               (catch Throwable t
                                                 (log/warn t "error on conn startup")
                                                 (throw t)))))
        conn (assoc conn
                    :query-error-counter query-error-counter
                    :query-timer query-timer
                    :query-tracer query-tracer
                    :tx-error-counter tx-error-counter
                    :tx-latency-timer tx-latency-timer
                    :cancelled-connections-counter cancelled-connections-counter)]

    (try
      ;; the connection loop only gets initialized if we are not closing
      (when (not @!closing?)
        (when total-connections-counter
          (.increment total-connections-counter))
        (swap! server-state assoc-in [:connections cid] conn)

        (conn-loop conn))
      (catch SocketException e
        (when (or (= "Broken pipe (Write failed)" (.getMessage e))
                  (= "Connection reset by peer" (.getMessage e)))
          (log/debug "Client closed socket while we were writing" {:port port, :cid cid})
          (.close conn-socket))

        (when (= "Connection reset" (.getMessage e))
          (log/debug "Client closed socket while we were reading" {:port port, :cid cid})
          (.close conn-socket))

        ;; socket being closed is normal, otherwise log.
        (when-not (.isClosed conn-socket)
          (log/warn e "An exception was caught during connection" {:port port, :cid cid})))

      (catch EOFException _
        (log/debug "Connection closed by client" {:port port, :cid cid}))

      (catch IOException e
        (log/warn e "IOException in connection" {:port port, :cid cid}))

      (catch Interrupted _)
      (catch InterruptedException _)

      (finally
        (close-all-portals conn)
        (util/close conn)

        ;; can be used to co-ordinate waiting for close
        (when-not (realized? close-promise)
          (deliver close-promise true))))))

(defn- accept-loop [{:keys [^ServerSocket accept-socket, ^ExecutorService thread-pool] :as server}]
  (try
    (loop []
      (cond
        (Thread/interrupted) (throw (InterruptedException.))

        (.isClosed accept-socket)
        (log/trace "Accept socket closed, exiting accept loop")

        :else
        (do
          (try
            (let [conn-socket (.accept accept-socket)]
              (.setTcpNoDelay conn-socket true)
              ;; TODO fix buffer on tp? q gonna be infinite right now
              (.submit thread-pool ^Runnable (fn [] (connect server conn-socket))))
            (catch SocketException e
              (when (and (not (.isClosed accept-socket))
                         (not= "Socket closed" (.getMessage e)))
                (log/warn e "Accept socket exception")))
            (catch IOException e
              (log/warn e "Accept IO exception")))
          (recur))))

    (catch Interrupted _)
    (catch InterruptedException _)

    (finally
      (util/try-close accept-socket)))

  (log/trace "exiting accept loop"))

(defn serve
  "Creates and starts a PostgreSQL wire-compatible server.

  Options:

  :port (default 0, opening the socket on an unused port).
  :num-threads (bounds the number of client connections, default 42)
  :playground? (default false) - if true, the server will create a new in-memory database if it doesn't already exist.
  "
  (^Server [node] (serve node {}))
  (^Server [node {:keys [allocator host port num-threads drain-wait ssl-ctx metrics-registry tracer query-tracing? read-only? playground?]
                  :or {host (InetAddress/getLoopbackAddress)
                       port 0
                       num-threads 42
                       drain-wait 5000}}]
   (util/with-close-on-catch [accept-socket (ServerSocket. port 0 host)]
     (let [host (.getInetAddress accept-socket)
           port (.getLocalPort accept-socket) 
           query-error-counter (when metrics-registry (metrics/add-counter metrics-registry "query.error"))
           query-timer (when metrics-registry (metrics/add-timer metrics-registry "query.timer" {}))
           tx-error-counter (when metrics-registry (metrics/add-counter metrics-registry "tx.error"))
           tx-latency-timer (when metrics-registry (metrics/add-timer metrics-registry "pgwire.tx.latency" {}))
           total-connections-counter (when metrics-registry (metrics/add-counter metrics-registry "pgwire.total_connections"))
           cancelled-connections-counter (when metrics-registry (metrics/add-counter metrics-registry "pgwire.cancelled_connections")) 
           server (map->Server {:allocator allocator
                                :node node
                                :authn (authn/<-node node)
                                :sql-planner (sql/->sql-planner)
                                :host host
                                :port port
                                :read-only? read-only?
                                :accept-socket accept-socket
                                :thread-pool (Executors/newFixedThreadPool num-threads (-> (Thread/ofVirtual) (.name "pgwire-connection-" 0) (.factory)))
                                :!closing? (atom false)
                                ;;TODO use clock from node or at least take clock as a param for testing
                                :server-state (atom (let [default-clock (Clock/systemDefaultZone)]
                                                      {:clock default-clock
                                                       :drain-wait drain-wait
                                                       :parameters {"server_version" expr/postgres-server-version
                                                                    "server_encoding" "UTF8"
                                                                    "client_encoding" "UTF8"
                                                                    "search_path" "public"
                                                                    "DateStyle" "ISO"
                                                                    "IntervalStyle" "ISO_8601"
                                                                    "TimeZone" (str (.getZone ^Clock default-clock))
                                                                    "integer_datetimes" "on"
                                                                    "standard_conforming_strings" "on"}}))

                                :playground? playground?

                                :ssl-ctx ssl-ctx})

           server (assoc server
                         :query-error-counter query-error-counter
                         :query-timer query-timer
                         :query-tracer (when query-tracing? tracer)
                         :tx-error-counter tx-error-counter
                         :tx-latency-timer tx-latency-timer
                         :total-connections-counter total-connections-counter
                         :cancelled-connections-counter cancelled-connections-counter)
           accept-thread (-> (Thread/ofVirtual)
                             (.name (str "pgwire-server-accept-" port))
                             (.uncaughtExceptionHandler util/uncaught-exception-handler)
                             (.unstarted (fn []
                                           (accept-loop server))))

           server (assoc server :accept-thread accept-thread)]

       (when metrics-registry
         (metrics/add-gauge metrics-registry "pgwire.active_connections" server
                            (fn [server]
                              (count (:connections @(:server-state server))))))
       (.start accept-thread)
       server))))

(defmethod xtn/apply-config! ::server [^Xtdb$Config config, _ {:keys [port read-only-port num-threads ssl] :as server}]
  (if server
    (let [host (:host server ::absent)]
      (cond-> (.getServer config)
        ;; .host wants an InetAddress; "*" means "all interfaces" (nil). Accept
        ;; either a host string (coerced via getByName, as the YAML + healthz
        ;; paths do) or an already-constructed InetAddress.
        (not= host ::absent) (.host (cond
                                      (or (nil? host) (= "*" host)) nil
                                      (instance? InetAddress host) host
                                      :else (InetAddress/getByName host)))
        (some? port) (.port port)
        (some? read-only-port) (.readOnlyPort read-only-port)
        (some? num-threads) (.numThreads num-threads)
        (some? ssl) (.ssl (util/->path (:keystore ssl)) (:keystore-password ssl))))

    (.setServer config nil)))

(defn- ->ssl-ctx [^Path ks-path, ^String ks-password]
  (let [ks-password (.toCharArray ks-password)
        ks (with-open [ks-file (io/input-stream (.toFile ks-path))]
             (doto (KeyStore/getInstance "JKS")
               (.load ks-file ks-password)))
        kmf (doto (KeyManagerFactory/getInstance (KeyManagerFactory/getDefaultAlgorithm))
              (.init ks ks-password))]
    (doto (SSLContext/getInstance "TLS")
      (.init (.getKeyManagers kmf) nil nil))))

(defn- <-config [^ServerConfig config]
  {:host (.getHost config)
   :port (.getPort config)
   :ro-port (.getReadOnlyPort config)
   :num-threads (.getNumThreads config)
   :ssl-ctx (when-let [ssl (.getSsl config)]
              (->ssl-ctx (.getKeyStore ssl) (.getKeyStorePassword ssl)))})

(defmethod ig/expand-key ::server [k config]
  {k (into {:node (ig/ref :xtdb/node)
            :base (ig/ref :xtdb/base)}
           (<-config config))})

(defmethod ig/init-key ::server [_ {:keys [host node ^NodeBase base port ro-port] :as opts}]
  (let [opts (-> opts
                 (dissoc :port :ro-port :base)
                 (assoc :metrics-registry (.getMeterRegistry base)
                        :tracer (.getTracer base)
                        :query-tracing? (.getQueryTracing (.getTracer (.getConfig base)))))]
    (letfn [(start-server [port read-only?]
              (when-not (neg? port)
                (let [{:keys [^InetAddress host port] :as srv} (serve node (-> opts
                                                                               (assoc :host host
                                                                                      :port port
                                                                                      :read-only? read-only?
                                                                                      :allocator (util/->child-allocator (.getAllocator base) "pgwire"))))]
                  (log/infof "%s started at postgres://%s:%d"
                             (if read-only? "Read-only server" "Server")
                             (.getHostAddress host)
                             port)
                  srv)))]
      {:read-write (start-server port false)
       :read-only (start-server ro-port true)})))

(defmethod ig/halt-key! ::server [_ srv]
  (util/close srv))

(defn open-playground
  (^xtdb.pgwire.Server [] (open-playground nil))

  (^xtdb.pgwire.Server [opts]
   (util/with-close-on-catch [node (xtn/start-node)]
     (let [{:keys [^InetAddress host, port] :as srv} (serve node (merge {:host nil}
                                                                        opts
                                                                        {:allocator (util/->child-allocator (:allocator node) "playground")
                                                                         :playground? true}))]
       (log/infof "Playground started at postgres://%s:%d" (.getHostAddress host) port)
       srv))))

(comment
  (user/set-log-level! 'xtdb.pgwire :debug))

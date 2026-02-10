(ns xtdb.pgwire
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cognitect.anomalies :as-alias anom]
            [cognitect.transit :as transit]
            [integrant.core :as ig]
            [xtdb.antlr :as antlr]
            [xtdb.api :as xt]
            [xtdb.authn :as authn]
            [xtdb.db-catalog :as db]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            [xtdb.log :as xt-log]
            [xtdb.metrics :as metrics]
            [xtdb.node :as xtn]
            [xtdb.pgwire.io :as pgio]
            [xtdb.protocols :as xtp]
            [xtdb.serde :as serde]
            [xtdb.sql :as sql]
            [xtdb.time :as time]
            [xtdb.tx-ops :as tx-ops]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw]
            [clojure.test :as t])
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
           (org.antlr.v4.runtime ParserRuleContext)
           (org.apache.arrow.memory BufferAllocator)
           org.apache.arrow.vector.types.pojo.Field
           (xtdb.antlr Sql$DirectlyExecutableStatementContext SqlVisitor)
           (xtdb.api Authenticator DataSource DataSource$ConnectionBuilder OAuthResult ServerConfig Xtdb Xtdb$Config)
           xtdb.api.module.XtdbModule
           (xtdb.arrow Relation Relation$ILoader VectorType)
           xtdb.arrow.RelationReader
           xtdb.database.Database$Config
           (xtdb.error Incorrect Interrupted)
           xtdb.IResultCursor
           (xtdb.pgwire PgType PgTypes)
           xtdb.JsonSerde
           xtdb.JsonLdSerde
           (xtdb.query PreparedQuery)
           (xtdb.tx TxOpts)
           (xtdb.tx_ops Sql)))

;; references
;; https://www.postgresql.org/docs/current/protocol-flow.html
;; https://www.postgresql.org/docs/current/protocol-message-formats.html

(defrecord Server [^BufferAllocator allocator
                   port read-only? playground?

                   ^ServerSocket accept-socket
                   ^Thread accept-thread

                   ^ExecutorService thread-pool

                   !closing?

                   server-state

                   ^Authenticator authn]
  DataSource
  (createConnectionBuilder [_]
    (DataSource$ConnectionBuilder. "localhost" port))

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
    (util/close frontend)

    (let [{:keys [server-state]} server]
      (swap! server-state update :connections dissoc cid))

    (util/close allocator)

    (log/debug "Connection ended" {:cid cid})))

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
  (->> params
       (into {} (mapcat (fn [[k v]]
                          (case k
                            "options" (parse-session-params (for [[_ k v] (re-seq #"-c ([\w_]*)=([\w_]*)" v)]
                                                              [k v]))
                            [[k (case k
                                  "fallback_output_format" (#{:json :json-ld :transit} (util/->kebab-case-kw v))
                                  v)]]))))))

(def time-zone-nf-param-name "timezone")

(def pg-param-nf->display-format
  {time-zone-nf-param-name "TimeZone"
   "datestyle" "DateStyle"
   "intervalstyle" "IntervalStyle"})

(defn- set-session-parameter [conn parameter value]
  ;;https://www.postgresql.org/docs/current/config-setting.html#CONFIG-SETTING-NAMES-VALUES
  ;;parameter names are case insensitive, choosing to lossily downcase them for now and store a mapping to a display format
  ;;for any name not typically displayed in lower case.
  (swap! (:conn-state conn) update-in [:session :parameters] (fnil into {}) (parse-session-params {parameter value}))
  (let [param (get pg-param-nf->display-format parameter parameter)]
    (pgio/cmd-write-msg conn pgio/msg-parameter-status {:parameter param,
                                                        :value (case param
                                                                 "standard_conforming_strings" "on"
                                                                 (str value))})))

(defn set-time-zone [{:keys [conn-state] :as conn} tz]
  (swap! conn-state
         (fn [{:keys [transaction] :as conn-state}]
           (-> conn-state
               (update-in [:session :clock] (fn [^Clock clock]
                                              (.withZone clock (ZoneId/of tz))))
               (cond-> transaction (assoc-in [:transaction :default-tz] tz)))))

  (set-session-parameter conn time-zone-nf-param-name tz))

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

(defn send-ex [{:keys [conn-state] :as conn}, ^Throwable ex]
  (let [ex-msg (ex-message ex)

        {::keys [severity error-code routine], :keys [detail]}
        (if (::error-code (ex-data ex))
          (ex-data ex)
          (let [ex (err/->anomaly ex {})]
            (-> (ex->pgw-err ex)
                (assoc :detail (let [format (get-in @conn-state [:session :parameters "fallback_output_format"] :json)]
                                 (case format
                                   :json (String. (JsonSerde/encodeToBytes ex) StandardCharsets/UTF_8)
                                   :json-ld (String. (JsonLdSerde/encodeJsonLdToBytes ex) StandardCharsets/UTF_8)
                                   :transit (serde/write-transit ex :json)))))))

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
   ;; it would be good to look at ready status being automatically driven by pure conn state, this is a bit messy.
   (if-some [transaction (:transaction @(:conn-state conn))]
     (if (:failed transaction)
       (cmd-send-ready conn :failed-transaction)
       (cmd-send-ready conn :transaction))
     (cmd-send-ready conn :idle)))
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
                 (.attach db-cat db-name nil))
               (throw (err-invalid-catalog db-name)))
        user (get startup-opts "user")
        conn (assoc conn :node node, :authn authn, :default-db db-name, :db db)]
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

(defn- apply-args [expr args]
  (if (symbol? expr)
    (let [args-map (zipmap (map (fn [idx]
                                  (symbol (str "?_" idx)))
                                (range))
                           args)]
      (or (args-map expr)
          (throw (pgio/err-protocol-violation (str "missing arg: " expr)))))
    expr))

(defn- coerce->tz [tz]
  (cond
    (instance? ZoneId tz) tz
    (instance? String tz) (try
                            (ZoneId/of tz)
                            (catch Exception e
                              (throw (pgio/err-protocol-violation (format "invalid timezone '%s': %s" tz (ex-message e))))))

    :else (throw (pgio/err-protocol-violation (format "invalid timezone '%s'" (str tz))))))

(defn cmd-begin [{:keys [node conn-state]} tx-opts {:keys [args]}]
  (swap! conn-state
         (fn [{:keys [session await-token] :as st}]
           (let [await-token (if-let [[_ tok] (find tx-opts :await-token)]
                               (apply-args tok args)
                               await-token)
                 {:keys [^Clock clock]} session]

             (when-not (= :read-write (:access-mode tx-opts))
               (xt-log/await-node node await-token #xt/duration "PT30S"))

             (-> st
                 (update :transaction
                         (fn [{:keys [access-mode]}]
                           (if access-mode
                             (throw (pgio/err-protocol-violation "transaction already started"))

                             (-> {:current-time (.instant clock)
                                  :snapshot-token (xtp/snapshot-token node)
                                  :default-tz (.getZone clock)
                                  :implicit? false}
                                 (into (:characteristics session))
                                 (into (-> tx-opts
                                           (update :default-tz #(some-> % (apply-args args) (coerce->tz)))
                                           (update :system-time #(some-> % (apply-args args) (time/->instant {:default-tz (.getZone clock)})))
                                           (update :current-time #(some-> % (apply-args args) (time/->instant {:default-tz (.getZone clock)})))
                                           (update :snapshot-token #(some-> % (apply-args args)))
                                           (update :snapshot-time #(some-> % (apply-args args)))
                                           (update :user-metadata #(some-> % (apply-args args)))
                                           (->> (into {} (filter (comp some? val))))))
                                 (assoc :await-token await-token))))))))))

(defn close-transaction [{:keys [conn-state] :as conn}]
  (swap! conn-state dissoc :transaction)
  (close-all-portals conn))

(defn cmd-commit [{:keys [^Xtdb node conn-state default-db ^BufferAllocator allocator, tx-error-counter] :as conn}]
  (let [{:keys [transaction session]} @conn-state
        {:keys [failed dml-buf system-time access-mode default-tz async? user-metadata]} transaction
        {:keys [parameters]} session]
    (if failed
      (throw (pgio/err-protocol-violation "transaction failed"))

      (try
        (when (= :read-write access-mode)
          (let [tx-opts (TxOpts. default-tz
                                 (some-> system-time (time/->instant {:default-tz default-tz}))
                                 (get parameters "user")
                                 user-metadata)]
            (util/with-open [tx-ops (->> dml-buf
                                         (mapcat (fn [op]
                                                   (or (when (instance? Sql op)
                                                         (let [{:keys [sql arg-rows]} op]
                                                           (seq (sql/sql->static-ops sql arg-rows))))
                                                       [op])))
                                         (util/safe-mapv #(xt-log/open-tx-op % allocator {:default-tz default-tz})))]
              (if async?
                (let [tx-id (with-auth-check conn (.submitTx node default-db tx-ops tx-opts))]
                  (swap! conn-state assoc :await-token (xtp/await-token node), :latest-submitted-tx {:tx-id (.getTxId tx-id)}))

                (let [tx (with-auth-check conn (.executeTx node default-db tx-ops tx-opts))]
                  (swap! conn-state assoc
                         :await-token (xtp/await-token node),
                         :latest-submitted-tx {:tx-id (.getTxId tx)
                                               :system-time (.getSystemTime tx)
                                               :committed? (.getCommitted tx)
                                               :error (.getError tx)})

                  (when-let [error (.getError tx)]
                    (throw error)))))))
        (catch Throwable t
          (metrics/inc-counter! tx-error-counter)
          (throw t))
        (finally
          (close-transaction conn))))))

(defn cmd-rollback [conn]
  (close-transaction conn))

(defn- fallback-type [session]
  (case (get-in session [:parameters "fallback_output_format"] :json)
    :json PgType/PG_JSON
    :json-ld PgType/PG_JSON_LD
    :transit PgType/PG_TRANSIT))

(defn- type->pg-col [col-name ^VectorType vec-type]
  {:pg-type (PgType/fromVectorType vec-type)
   :col-name col-name})

(defn- field->pg-col [^Field field]
  (type->pg-col (.getName field) (types/->type field)))

(defn- cmd-send-row-description [{:keys [conn-state] :as conn} pg-cols]
  (let [fallback (fallback-type (:session @conn-state))
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

(defn cmd-send-parameter-description [conn {:keys [statement-type param-oids]}]
  (log/trace "sending parameter description - " {:param-oids param-oids})
  (pgio/cmd-write-msg conn pgio/msg-parameter-description
                      {:parameter-oids (case statement-type
                                         :show-variable []
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
        {:keys [statement-type canned-response] :as describe-target} (get-in @conn-state [coll-k describe-name])]

    (letfn [(describe* [{:keys [pg-cols] :as describe-target}]
              (when (= :prepared-stmt describe-type)
                (cmd-send-parameter-description conn describe-target))

              (if pg-cols
                (cmd-send-row-description conn pg-cols)
                (pgio/cmd-write-msg conn pgio/msg-no-data)))]

      (case statement-type
        :canned-response (cmd-describe-canned-response conn canned-response)
        (:begin :query :dml :show-variable) (describe* describe-target)

        :execute (let [inner (get-in @conn-state [:prepared-statements (:statement-name describe-target)])]
                   (describe* {:param-oids (:param-oids describe-target)
                               :pg-cols (:pg-cols inner)}))

        (pgio/cmd-write-msg conn pgio/msg-no-data)))))

(defmethod handle-msg* :msg-sync [{:keys [conn-state] :as conn} _]
  ;; Sync commands are sent by the client to commit transactions
  ;; and to clear the error state of a :extended mode series of commands (e.g the parse/bind/execute dance)
  (let [{:keys [implicit? failed]} (:transaction @conn-state)]
    (try
      (when implicit?
        (if failed
          (cmd-rollback conn)
          (cmd-commit conn)))
      (catch Throwable t
        (send-ex conn t))))

  (cmd-send-ready conn)
  (swap! conn-state dissoc :skip-until-sync?, :protocol))

(defmethod handle-msg* :msg-flush [{:keys [frontend]} _]
  (pgio/flush! frontend))

(defn session-param-name [^ParserRuleContext ctx]
  (some-> ctx
          (.accept (reify SqlVisitor
                     (visitRegularIdentifier [_ ctx] (.getText ctx))
                     (visitDelimitedIdentifier [_ ctx]
                       (let [di-str (.getText ctx)]
                         (subs di-str 1 (dec (count di-str)))))))
          (str/lower-case)))

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

;; yagni, is everything upper'd anyway by drivers / server?
(defn- probably-same-query? [s substr]
  ;; TODO I bet this may cause some amusement. Not sure what to do about non-parsed query matching, it'll do for now.
  (str/starts-with? (str/lower-case s) (str/lower-case substr)))

(defn get-canned-response
  "If the sql string is a canned response, returns the entry from the canned-responses that matches."
  [sql-str]
  (when sql-str (first (filter #(probably-same-query? sql-str (:q %)) canned-responses))))

(defn parse-sql [sql]
  (log/debug "Interpreting SQL: " sql)
  (err/wrap-anomaly {:sql sql}
    (loop [sql (trim-sql sql)]
      (if-let [replacement (replace-queries sql)]
        (recur replacement)

        (or (when (str/blank? sql)
              [{:statement-type :empty-query}])

            (when-some [canned-response (get-canned-response sql)]
              [{:statement-type :canned-response, :canned-response canned-response}])

            (try
              (letfn [(subsql [^ParserRuleContext ctx]
                        (subs sql (.getStartIndex (.getStart ctx)) (inc (.getStopIndex (.getStop ctx)))))]
                (let [env (sql/->env)]
                  (->> (antlr/parse-multi-statement sql)
                       (mapv (partial sql/accept-visitor
                                      (reify SqlVisitor
                                        (visitSetSessionVariableStatement [_ ctx]
                                          {:statement-type :set-session-parameter
                                           :parameter (session-param-name (.identifier ctx))
                                           :value (sql/plan-expr (.literal ctx) env)})

                                        (visitSetSessionCharacteristicsStatement [this ctx]
                                          {:statement-type :set-session-characteristics
                                           :session-characteristics
                                           (into {} (mapcat #(.accept ^ParserRuleContext % this)) (.sessionCharacteristic ctx))})

                                        (visitSessionTxCharacteristics [this ctx]
                                          (let [[^ParserRuleContext session-mode & more-modes] (.sessionTxMode ctx)]
                                            (assert (nil? more-modes) "pgwire only supports one for now")
                                            (.accept session-mode this)))

                                        (visitSetTransactionStatement [_ _]
                                          ;; no-op for us
                                          {:statement-type :set-transaction
                                           :tx-characteristics {}})

                                        (visitStartTransactionStatement [this ctx]
                                          {:statement-type :begin
                                           :tx-characteristics (some-> (.transactionCharacteristics ctx) (.accept this))})

                                        (visitTransactionCharacteristics [this ctx]
                                          (into {} (mapcat #(.accept ^ParserRuleContext % this)) (.transactionMode ctx)))

                                        (visitIsolationLevel [_ _] {})
                                        (visitSessionIsolationLevel [_ _] {})

                                        (visitReadWriteTransaction [this ctx]
                                          (into {:access-mode :read-write}
                                                (mapcat (partial sql/accept-visitor this) (.readWriteTxOption ctx))))

                                        (visitReadOnlyTransaction [this ctx]
                                          (into {:access-mode :read-only}
                                                (mapcat (partial sql/accept-visitor this) (.readOnlyTxOption ctx))))

                                        (visitTxTzOption0 [this ctx] (.accept (.txTzOption ctx) this))
                                        (visitTxTzOption1 [this ctx] (.accept (.txTzOption ctx) this))

                                        (visitTxTzOption [_ ctx]
                                          {:default-tz (sql/plan-expr (.tz ctx) env)})

                                        (visitAwaitTokenTxOption [_ ctx]
                                          {:await-token (sql/plan-expr (.awaitToken ctx) env)})

                                        (visitReadWriteSession [_ _] {:access-mode :read-write})

                                        (visitReadOnlySession [_ _] {:access-mode :read-only})

                                        (visitSystemTimeTxOption [_ ctx]
                                          {:system-time (sql/plan-expr (.systemTime ctx) env)})

                                        (visitAsyncTxOption [_ ctx]
                                          {:async? (boolean (sql/plan-expr (.async ctx) env))})

                                        (visitSnapshotTokenTxOption [_ ctx]
                                          {:snapshot-token (sql/plan-expr (.snapshotToken ctx) env)})

                                        (visitSnapshotTimeTxOption [_ ctx]
                                          {:snapshot-time (sql/plan-expr (.snapshotTime ctx) env)})

                                        (visitClockTimeTxOption [_ ctx]
                                          {:current-time (sql/plan-expr (.clockTime ctx) env)})

                                        (visitMetadataTxOption [_ ctx]
                                          {:user-metadata (sql/plan-expr (.metadata ctx) env)})

                                        (visitCommitStatement [_ _] {:statement-type :commit})
                                        (visitRollbackStatement [_ _] {:statement-type :rollback})

                                        (visitSetRoleStatement [_ _] {:statement-type :set-role})

                                        (visitSetTimeZoneStatement [_ ctx]
                                          ;; not sure if handlling time zone explicitly is the right approach
                                          ;; might be cleaner to handle it like any other session param
                                          {:statement-type :set-time-zone
                                           :tz (sql/plan-expr (.zone ctx) env)})

                                        (visitInsertStatement [_ ctx]
                                          {:statement-type :dml, :dml-type :insert, :query (subsql ctx)})

                                        (visitPatchStatement [_ ctx]
                                          {:statement-type :dml, :dml-type :patch, :query (subsql ctx)})

                                        (visitUpdateStatement [_ ctx]
                                          {:statement-type :dml, :dml-type :update, :query (subsql ctx)})

                                        (visitDeleteStatement [_ ctx]
                                          {:statement-type :dml, :dml-type :delete, :query (subsql ctx)})

                                        (visitEraseStatement [_ ctx]
                                          {:statement-type :dml, :dml-type :erase, :query (subsql ctx)})

                                        (visitAssertStatement [_ ctx]
                                          {:statement-type :dml, :dml-type :assert, :query (subsql ctx)})

                                        (visitQueryExpr [_ ctx]
                                          {:statement-type :query, :query (subsql ctx), :parsed-query ctx})

                                        (visitCopyInStmt [this ctx]
                                          (into {:statement-type :copy-in,
                                                 :table-name (sql/identifier-sym (.targetTable ctx))}
                                                (map (partial sql/accept-visitor this))
                                                (some-> (.opts ctx) (.copyOpt))))

                                        (visitCopyFormatOption [_ ctx]
                                          [:format (sql/plan-expr (.format ctx) env)])

                                        ;; could do pre-submit validation here
                                        (visitCreateUserStatement [_ ctx]
                                          {:statement-type :dml, :dml-type :create-role, :query (subsql ctx)})
                                        (visitAlterUserStatement [_ ctx]
                                          {:statement-type :dml, :dml-type :create-role, :query (subsql ctx)})

                                        (visitPrepareStatement [this ctx]
                                          (let [inner-ctx (.directlyExecutableStatement ctx)]
                                            {:statement-type :prepare
                                             :statement-name (str (sql/identifier-sym (.statementName ctx)))
                                             :inner (.accept inner-ctx this)}))

                                        (visitExecuteStatement [_ ctx]
                                          {:statement-type :execute,
                                           :statement-name (str (sql/identifier-sym (.statementName ctx))),
                                           :query (subsql ctx)
                                           :parsed-query ctx})

                                        (visitShowVariableStatement [_ ctx]
                                          {:statement-type :query, :query sql, :parsed-query ctx})

                                        (visitSetAwaitTokenStatement [_ ctx]
                                          (let [await-token (sql/plan-expr (.awaitToken ctx) env)]
                                            {:statement-type :set-await-token, :await-token await-token}))

                                        (visitShowAwaitTokenStatement [_ _]
                                          {:statement-type :show-variable, :query sql, :variable "await_token"})

                                        (visitShowSnapshotTokenStatement [_ ctx]
                                          {:statement-type :query, :query sql, :parsed-query ctx})

                                        (visitShowClockTimeStatement [_ ctx]
                                          {:statement-type :query, :query sql, :parsed-query ctx})

                                        (visitShowSessionVariableStatement [_ ctx]
                                          {:statement-type :show-variable
                                           :query sql
                                           :variable (session-param-name (.identifier ctx))})

                                        (visitAttachDatabaseStatement [_ ctx]
                                          {:statement-type :attach-db
                                           :db-name (str (sql/identifier-sym (.dbName ctx)))
                                           :db-config (try
                                                        (or (some-> (.configYaml ctx)
                                                                    (sql/plan-expr env)
                                                                    (Database$Config/fromYaml))
                                                            (Database$Config.))
                                                        (catch Exception e
                                                          (throw (err/incorrect :xtdb/invalid-database-config
                                                                                (str "Invalid database config in `ATTACH DATABASE`: " (ex-message e))))))})

                                        (visitDetachDatabaseStatement [_ ctx]
                                          {:statement-type :detach-db
                                           :db-name (str (sql/identifier-sym (.dbName ctx)))})))))))

              (catch Exception e
                (log/debug e "Error parsing SQL")
                (throw e))))))))

(defn- show-var-query [variable]
  (case variable
    ("latest_completed_txs" "latest_submitted_msg_ids" "latest_processed_msg_ids")
    (-> '[:table {:param ?_0}]
        (with-meta {:param-count 1}))

    "latest_submitted_tx" (-> '[:select {:predicate (not (nil? tx_id))}
                                [:table {:rows [{:tx_id ?_0, :system_time ?_1,
                                                 :committed ?_2, :error ?_3,
                                                 :await_token ?_4}]}]]
                              (with-meta {:param-count 5}))

    (-> (xt/template [:table {:rows [{~(keyword variable) ?_0}]}])
        (with-meta {:param-count 1}))))

(defn- show-var-param-types [variable]
  (case variable
    "latest_completed_txs" [#xt/type [:list [:struct {"db_name" :utf8, "part" :i32, "tx_id" :i64, "system_time" :instant}]]]
    ("latest_submitted_msg_ids" "latest_processed_msg_ids") [#xt/type [:list [:struct {"db_name" :utf8
                                                                                       "part" :i32
                                                                                       "msg_id" :i64}]]]
    "latest_submitted_tx" [#xt/type :i64, #xt/type :instant, #xt/type :bool, #xt/type :transit, #xt/type :utf8]
    "await_token" [#xt/type :utf8]
    "standard_conforming_strings" [#xt/type :bool]

    [#xt/type :utf8]))

(defn- prep-stmt [{:keys [node conn-state default-db] :as conn} {:keys [statement-type param-oids] :as stmt}]
  (case statement-type
    (:query :execute :show-variable)
    (try
      (let [{:keys [^Sql$DirectlyExecutableStatementContext parsed-query explain? explain-analyze?]} stmt

            {:keys [session await-token]} @conn-state
            {:keys [^Clock clock]} session

            query-opts {:await-token await-token
                        :tx-timeout (Duration/ofMinutes 1)
                        :default-tz (.getZone clock)
                        :explain? explain?
                        :explain-analyze? explain-analyze?
                        :default-db default-db}

            ^PreparedQuery pq (case statement-type
                                (:query :execute) (with-auth-check conn (xtp/prepare-sql node parsed-query query-opts))
                                :show-variable (with-auth-check conn (xtp/prepare-ra node (show-var-query (:variable stmt)) query-opts)))]

        (when-let [warnings (.getWarnings pq)]
          (doseq [warning warnings]
            (pgio/cmd-send-notice conn (notice-warning (sql/error-string warning)))))

        (let [param-oids (->> (concat param-oids (repeat 0))
                              (into [] (take (.getParamCount pq))))
              param-types (case statement-type
                            (:query :execute) (map #(some-> (PgType/fromOid %) .getXtType) param-oids)
                            :show-variable (show-var-param-types (:variable stmt)))
              pg-cols (if (some nil? param-types)
                        ;; if we're unsure on some of the col-types, return all of the output cols as the fallback type (#4455)
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
                 :pg-cols pg-cols)))

      (catch IllegalArgumentException e
        (log/debug e "Error preparing statement")
        (throw e))
      (catch RuntimeException e
        (log/debug e "Error preparing statement")
        (throw e))
      (catch Throwable e
        (log/error e "Error preparing statement")
        (throw e)))

    stmt))

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
      (when (= :read-write (get-in @conn-state [:transaction :access-mode]))
        (metrics/inc-counter! tx-error-counter))

      (throw e))))

(defn cmd-prepare [{:keys [conn-state] :as conn} {:keys [statement-name inner] :as _portal}]
  (let [{:keys [query]} inner
        [prepared-stmt & more-stmts] (parse-sql query)]
    (when (seq more-stmts)
      (throw (UnsupportedOperationException. "Multiple statements in a single PREPARE are not supported")))

    (let [prepared-stmt (prep-stmt conn prepared-stmt)]
      (swap! conn-state assoc-in [:prepared-statements statement-name]
             (assoc prepared-stmt
                    :statement-name statement-name))

      (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "PREPARE"}))))

(defn- normalize-arg-formats [arg-format arg-count]
  (case (count arg-format)
    0 (repeat arg-count :text)
    1 (repeat arg-count (first arg-format))
    arg-format))

(defn- ->xtify-arg [_session {:keys [arg-format param-oids]}]
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

(defn- xtify-args [{:keys [conn-state] :as _conn} args stmt]
  (into [] (map-indexed (->xtify-arg (:session @conn-state) stmt) args)))

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

(defn bind-stmt [{:keys [node conn-state ^BufferAllocator allocator query-tracer] :as conn} {:keys [statement-type ^PreparedQuery prepared-query args result-format] :as stmt}]
  (let [{:keys [session transaction await-token]} @conn-state
        {:keys [^Clock clock], session-params :parameters} session
        await-token (:await-token transaction await-token)

        query-opts {:snapshot-token (:snapshot-token stmt (:snapshot-token transaction))
                    :snapshot-time (:snapshot-time stmt (:snapshot-time transaction))
                    :current-time (or (:current-time stmt)
                                      (:current-time transaction)
                                      (.instant clock))
                    :default-tz (or (:default-tz transaction) (.getZone clock))
                    :await-token await-token
                    :tracer query-tracer}

        xt-args (xtify-args conn args stmt)]

    (letfn [(->cursor ^xtdb.IResultCursor [xt-args] 
              (with-auth-check conn
                (util/with-close-on-catch [args-rel (vw/open-args allocator xt-args)]
                  (.openQuery prepared-query (assoc query-opts :args args-rel :query-text (:query stmt))))))

            (->pg-cols [prepared-pg-cols ^IResultCursor cursor]
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
              (with-result-formats prepared-pg-cols result-format)))]

      (case statement-type
        :query (util/with-close-on-catch [cursor (->cursor xt-args)]
                 (-> stmt
                     (assoc :cursor cursor,
                            :pg-cols (->pg-cols (:pg-cols stmt) cursor))))

        :show-variable (let [{:keys [variable]} stmt]
                         (util/with-close-on-catch [cursor (->cursor (case variable
                                                                       "await_token" [await-token]
                                                                       "latest_completed_txs" [(for [[db-name parts] (xtp/latest-completed-txs node)
                                                                                                     [part-idx {:keys [tx-id system-time]}] (map vector (range) parts)]
                                                                                                 {:db_name db-name
                                                                                                  :part (int part-idx)
                                                                                                  :tx_id tx-id
                                                                                                  :system_time system-time})]
                                                                       "latest_submitted_msg_ids" [(for [[db-name parts] (xtp/latest-submitted-msg-ids node)
                                                                                                         [part-idx msg-id] (map vector (range) parts)]
                                                                                                     {:db_name db-name
                                                                                                      :part (int part-idx)
                                                                                                      :msg_id msg-id})]

                                                                       "latest_processed_msg_ids" [(for [[db-name parts] (xtp/latest-processed-msg-ids node)
                                                                                                         [part-idx msg-id] (map vector (range) parts)]
                                                                                                     {:db_name db-name
                                                                                                      :part (int part-idx)
                                                                                                      :msg_id msg-id})]

                                                                       "latest_submitted_tx" (mapv (into {} (assoc (:latest-submitted-tx @conn-state)
                                                                                                                   :await-token await-token))
                                                                                                   [:tx-id :system-time :committed? :error :await-token])
                                                                       [(get session-params variable)]))]

                           (-> stmt
                               (assoc :cursor cursor,
                                      :pg-cols (-> (:pg-cols stmt)
                                                   (with-result-formats result-format))))))

        :execute (util/with-open [^IResultCursor args-cursor (->cursor xt-args)]
                   ;; in the case of execute, we've just bound the args query rather than the inner query.
                   ;; so now we bind the inner query and pretend this was the one we were running all along
                   (let [{^PreparedQuery inner-pq :prepared-query, :as inner} (get-in @conn-state [:prepared-statements (:statement-name stmt)])
                         !args (object-array 1)]

                     (.forEachRemaining args-cursor
                                        (fn [^RelationReader args-rel]
                                          (aset !args 0 (.openSlice args-rel allocator))))

                     (let [^RelationReader args-rel (aget !args 0)]
                       (case (:statement-type inner)
                         :query (with-auth-check conn
                                  (util/with-close-on-catch [inner-cursor (.openQuery inner-pq (assoc query-opts :args args-rel :query-text (:query stmt)))]
                                    (-> inner
                                        (assoc :cursor inner-cursor
                                               :pg-cols (-> (->pg-cols (:pg-cols inner) inner-cursor)
                                                            (with-result-formats result-format))))))

                         :dml (let [arg-types (.getResultTypes args-cursor)]
                                (try
                                  (-> inner
                                      (assoc :args (vec (for [col-name (keys arg-types)]
                                                          (-> (.vectorForOrNull args-rel col-name)
                                                              (.getObject 0))))
                                             :param-oids (->> arg-types
                                                              (mapv (fn [[col-name ^VectorType vec-type]]
                                                                      (PgType/.getOid (PgType/fromVectorType vec-type)))))))
                                  (finally
                                    (util/close args-rel))))))))

        (-> stmt
            (assoc :args xt-args))))))

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
  [{:keys [conn-state server]} {:keys [statement-type] :as stmt}]
  (let [{:keys [access-mode]} (:transaction @conn-state)]
    (when (and (= :dml statement-type) (:read-only? server))
      (throw (err/incorrect :xtdb/dml-in-read-only-server
                            "DML is not allowed on the READ ONLY server"
                            {:query (:query stmt)})))

    (when (and (= :dml statement-type) (= :read-only access-mode))
      (throw (err/incorrect :xtdb/dml-in-read-only-tx
                            "DML is not allowed in a READ ONLY transaction"
                            {:query (:query stmt)})))

    (when (and (= :query statement-type) (= :read-write access-mode)
               (not= pgjdbc-type-query (str/replace (:query stmt) #"  +" " ")))
      (throw (err/incorrect :xtdb/queries-in-read-write-tx
                            "Queries are unsupported in a DML transaction"
                            {:query (:query stmt)})))))

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

(defn cmd-set-time-zone [conn {:keys [tz args]}]
  (let [tz (-> tz (apply-args args))]
    (set-time-zone conn tz))
  (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "SET TIME ZONE"}))

(defn cmd-set-await-token [{:keys [conn-state] :as conn} {:keys [await-token args]}]
  (let [await-token (-> await-token (apply-args args))]
    (swap! conn-state assoc :await-token await-token)
    (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "SET AWAIT_TOKEN"})))

(defn cmd-set-session-characteristics [{:keys [conn-state] :as conn} session-characteristics]
  (swap! conn-state update-in [:session :characteristics] (fnil into {}) session-characteristics)
  (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "SET SESSION CHARACTERISTICS"}))

(defn- cmd-exec-dml [{:keys [node conn-state tx-error-counter default-db] :as conn} {:keys [dml-type query args param-oids]}]
  (when (get-in @conn-state [:transaction :failed])
    (throw (pgio/err-protocol-violation "current transaction is aborted, commands ignored until ROLLBACK is received")))

  (when (or (not= (count param-oids) (count args))
            (some (fn [idx]
                    (and (zero? (nth param-oids idx))
                         (some? (nth args idx))))
                  (range (count param-oids))))
    (metrics/inc-counter! tx-error-counter)
    (throw (err/incorrect ::missing-arg-types "Missing types for args - client must specify types for all non-null params in DML statements"
                          {:query query, :param-oids param-oids})))

  ;; Extract warnings during interactive sessions (typically non-parameterized statements)
  (when (empty? param-oids)
    (try
      (let [^PreparedQuery pq (with-auth-check conn
                                (xtp/prepare-sql node
                                                 (antlr/parse-statement query)
                                                 {:default-db default-db}))]

        ;; Send any warnings to the client
        (when-let [warnings (.getWarnings pq)]
          (doseq [warning warnings]
            (pgio/cmd-send-notice conn (notice-warning (sql/error-string warning))))))

      (catch IllegalArgumentException e
        (log/debug e "Error planning DML statement for warnings"))
      (catch RuntimeException e
        (log/debug e "Error planning DML statement for warnings"))
      (catch Throwable e
        (log/debug e "Error planning DML statement for warnings"))))

  (when-not (:transaction @conn-state)
    (cmd-begin conn {:implicit? true, :access-mode :read-write} {}))

  (swap! conn-state update-in [:transaction :dml-buf]
         (fnil (fn [dml-ops]
                 (or (when-let [^Sql last-op (peek dml-ops)]
                       (when (and (instance? Sql last-op)
                                  (= (:sql last-op) query))
                         (conj (pop dml-ops)
                               (tx-ops/->Sql query (conj (or (:arg-rows last-op) []) args)))))
                     (conj dml-ops (tx-ops/->Sql query [args]))))
               []))

  (pgio/cmd-write-msg conn pgio/msg-command-complete
                      {:command (case dml-type
                                  ;; insert <oid> <rows>
                                  ;; oid is always 0 these days, its legacy thing in the pg protocol
                                  ;; rows is 0 for us cus async
                                  :insert "INSERT 0 0"
                                  ;; otherwise head <rows>
                                  :delete "DELETE 0"
                                  :update "UPDATE 0"
                                  :patch "PATCH 0"
                                  :erase "ERASE 0"
                                  :assert "ASSERT"
                                  :create-role "CREATE ROLE")}))

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
                      {:keys [limit statement-type query ^IResultCursor cursor pg-cols portal-name pending-rows total-rows-sent]
                       :as _portal}]
  ;; Create an implicit transaction if one hasn't already been started
  (let [transaction (get-in @conn-state [:transaction])]
    (when (:failed transaction)
      (throw (pgio/err-protocol-violation "current transaction is aborted, commands ignored until ROLLBACK is received")))

    (when-not (or transaction (= statement-type :show-variable))
      (cmd-begin conn {:implicit? true :access-mode :read-only} {})))

  (try
    (let [!n-rows-out (volatile! 0)
          !pending (volatile! [])
          {session-params :parameters, :as session} (:session @conn-state)
          fallback (fallback-type session)
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
          (pgio/cmd-write-msg conn pgio/msg-command-complete {:command (str (statement-head query) " " cumulative-rows)}))))

    (catch Interrupted e (throw e))
    (catch InterruptedException e (throw e))
    (catch Throwable e
      (metrics/inc-counter! query-error-counter)
      (throw e))))

(defn- attach-db [{:keys [node conn-state default-db] :as conn} {:keys [db-name db-config]}]
  (when-not (= default-db "xtdb")
    (throw (err/incorrect ::attach-db-on-secondary "Can only attach databases when connected to the primary 'xtdb' database."
                          {:db default-db})))

  (when (false? (get-in @conn-state [:transaction :implicit?]))
    (throw (err/incorrect ::attach-db-in-tx "Cannot attach a database in a transaction."
                          {:db-name db-name})))

  (with-auth-check conn
    (let [{:keys [error] :as tx} (xtp/attach-db node db-name db-config)]
      (swap! conn-state assoc :await-token (xtp/await-token node), :latest-submitted-tx tx)

      (when error
        (throw error)))))

(defn- detach-db [{:keys [node conn-state default-db] :as conn} {:keys [db-name]}]
  (when-not (= default-db "xtdb")
    (throw (err/incorrect ::detach-db-on-secondary "Can only detach databases when connected to the primary 'xtdb' database."
                          {:db default-db})))

  (when (false? (get-in @conn-state [:transaction :implicit?]))
    (throw (err/incorrect ::detach-db-in-tx "Cannot detach a database in a transaction."
                          {:db-name db-name})))

  (with-auth-check conn
    (let [{:keys [error] :as tx} (xtp/detach-db node db-name)]
      (swap! conn-state assoc :await-token (xtp/await-token node), :latest-submitted-tx tx)

      (when error
        (throw error)))))

(defn execute-portal [{:keys [conn-state query-timer] :as conn} {:keys [statement-type canned-response parameter value session-characteristics tx-characteristics] :as portal}]
  (verify-permissibility conn portal)

  (swap! conn-state (fn [{:keys [transaction] :as cs}]
                      (cond-> cs
                        transaction (update-in [:transaction :access-mode]
                                               (fnil identity
                                                     (case statement-type
                                                       :query :read-only
                                                       :dml :read-write
                                                       nil))))))

  (case statement-type
    :empty-query (pgio/cmd-write-msg conn pgio/msg-empty-query)
    :canned-response (cmd-write-canned-response conn canned-response)
    :set-session-parameter (cmd-set-session-parameter conn parameter value)
    :set-session-characteristics (cmd-set-session-characteristics conn session-characteristics)
    :set-role nil
    :set-transaction (cmd-set-transaction conn tx-characteristics)
    :set-time-zone (cmd-set-time-zone conn portal)
    :set-await-token (cmd-set-await-token conn portal)
    :ignore (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "IGNORED"})

    :begin (do
             (cmd-begin conn tx-characteristics portal)
             (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "BEGIN"}))

    :rollback (do
                (cmd-rollback conn)
                (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "ROLLBACK"}))

    :commit (do
              (cmd-commit conn)
              (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "COMMIT"}))

    (:query :show-variable) (metrics/record-callable! query-timer (cmd-exec-query conn portal))
    :prepare (cmd-prepare conn portal)
    :dml (cmd-exec-dml conn portal)

    :copy-in (let [format (case (:format portal)
                            "transit-json" :transit-json
                            "transit-msgpack" :transit-msgpack
                            "arrow-file" :arrow-file
                            "arrow-stream" :arrow-stream
                            (throw (err/incorrect ::invalid-copy-format "COPY IN requires a valid format: 'arrow-file', 'arrow-stream', 'transit-json', 'transit-msgpack'"
                                                  {:format (:format portal)})))
                   {:keys [table-name]} portal
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
                                      :column-formats [copy-format]})))

    :attach-db (do
                 (attach-db conn portal)
                 (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "ATTACH DATABASE"}))

    :detach-db (do
                 (detach-db conn portal)
                 (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "DETACH DATABASE"}))

    (throw (UnsupportedOperationException. (pr-str {:portal portal})))))

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
    (doseq [{:keys [statement-type] :as stmt} (parse-sql query)]
      (when-not (or (boolean (:transaction @conn-state))
                    (= statement-type :show-variable)
                    (= statement-type :copy-in))
        (cmd-begin conn {:implicit? true} {}))

      (try
        (let [{:keys [param-oids statement-type] :as prepared-stmt} (prep-stmt conn stmt)]
          (when (and (seq param-oids) (not= statement-type :show-variable))
            (throw (pgio/err-protocol-violation "Parameters not allowed in simple queries")))

          (let [portal (bind-stmt conn prepared-stmt)]
            (try
              (when (or (contains? #{:query :canned-response :show-variable} statement-type)
                        (and (= :execute statement-type)
                             (= :query (get-in @conn-state [:prepared-statements (:statement-name prepared-stmt) :statement-type]))))
                ;; Client only expects to see a RowDescription (result of cmd-descibe)
                ;; for certain statement types
                (cmd-describe-portal conn portal))

              (execute-portal conn portal)
              (finally
                (util/close (:cursor portal))))))

        (catch Interrupted e (throw e))
        (catch InterruptedException e (throw e))
        (catch Exception e
          (when-let [{:keys [implicit?]} (:transaction @conn-state)]
            (if implicit?
              (cmd-rollback conn)
              (swap! conn-state util/maybe-update :transaction assoc :failed true, :err e)))

          (send-ex conn e))))

    (let [{:keys [implicit? failed]} (:transaction @conn-state)]
      (when implicit?
        (if failed
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

(defn- copy-transit-batch ^long [{:keys [conn-state] :as conn} {:keys [format, ^Path copy-file]}]
  (let [started-tx? (when-not (:transaction @conn-state)
                      (cmd-begin conn {:implicit? true, :access-mode :read-write} {})
                      true)]
    (try
      (let [docs (with-open [is (io/input-stream (.toFile copy-file))]
                   (vec (serde/transit-seq (transit/reader is
                                                           (case format
                                                             :transit-json :json
                                                             :transit-msgpack :msgpack)
                                                           {:handlers serde/transit-read-handler-map}))))]

        (swap! conn-state
               (fn [{:keys [copy], :as conn-state}]
                 (-> conn-state
                     (update-in [:transaction :dml-buf] (fnil conj [])
                                (tx-ops/->PutDocs (:table-name copy) docs nil nil))
                     (dissoc :copy))))

        (when started-tx?
          (cmd-commit conn))

        (count docs))

      (catch Throwable e
        (when started-tx?
          (cmd-rollback conn))
        (throw e)))))

(defn- copy-arrow-batches ^long [{:keys [^BufferAllocator allocator, conn-state] :as conn} {:keys [format, ^Path copy-file]}]
  (let [!doc-count (atom 0)]
    (util/with-open [^Relation$ILoader ldr (case format
                                             :arrow-stream (Relation/streamLoader allocator copy-file)
                                             :arrow-file (Relation/loader allocator copy-file))
                     rel (Relation. allocator (.getSchema ldr))]
      (letfn [(add-batch! []
                (swap! conn-state
                       (fn [{:keys [copy], :as conn-state}]
                         (-> conn-state
                             (update-in [:transaction :dml-buf] (fnil conj [])
                                        (tx-ops/->PutRel (:table-name copy) (.getAsArrowStream rel)))))))]

        (if-let [{:keys [access-mode]} (:transaction @conn-state)]
          (do
            (when (= access-mode :read-only)
              (throw (err/incorrect :xtdb/copy-in-read-only-tx
                                    "COPY is not allowed in a READ ONLY transaction")))
            (swap! conn-state update-in [:transaction :access-mode] (fnil identity :read-write))

            (while (.loadNextPage ldr rel)
              (add-batch!)
              (swap! !doc-count + (.getRowCount rel))))

          ;; in arrow-file/arrow-stream, the input is naturally batched
          ;; therefore, if there's no active tx, we create a transaction for each record-batch.
          (while (.loadNextPage ldr rel)
            (cmd-begin conn {:implicit? true, :access-mode :read-write} {})
            (try
              (add-batch!)
              (cmd-commit conn)
              (swap! !doc-count + (.getRowCount rel))
              (catch Throwable t
                (cmd-rollback conn)
                (throw t)))))))
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
      (swap! conn-state
             (fn [{:keys [transaction protocol] :as cs}]
               (cond-> cs
                 ;; error seen while in :extended mode, start skipping messages until sync received
                 (= :extended protocol) (assoc :skip-until-sync? true)

                 ;; mark a transaction (if open as failed), for now we will consider all errors to do this
                 transaction (update :transaction assoc :failed true, :err e))))

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
  [{:keys [node, ^Authenticator authn, server-state, port, allocator, query-error-counter, tx-error-counter, ^Counter total-connections-counter, ^Counter cancelled-connections-counter, query-timer, query-tracer] :as server} ^Socket conn-socket]
  (let [close-promise (promise)
        {:keys [cid !closing?] :as conn} (util/with-close-on-catch [_ conn-socket]
                                           (let [cid (:next-cid (swap! server-state update :next-cid (fnil inc 0)))
                                                 !conn-state (atom {:close-promise close-promise
                                                                    :session {:access-mode :read-only
                                                                              :clock (:clock @server-state)}})
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
                    :cancelled-connections-counter cancelled-connections-counter)]

    (try
      ;; the connection loop only gets initialized if we are not closing
      (when (not @!closing?)
        (when total-connections-counter
          (.increment total-connections-counter))
        (swap! server-state assoc-in [:connections cid] conn)

        (log/trace "Starting connection loop" {:port port, :cid cid})
        (conn-loop conn))
      (catch SocketException e
        (when (= "Broken pipe (Write failed)" (.getMessage e))
          (log/trace "Client closed socket while we were writing" {:port port, :cid cid})
          (.close conn-socket))

        (when (= "Connection reset by peer" (.getMessage e))
          (log/trace "Client closed socket while we were writing" {:port port, :cid cid})
          (.close conn-socket))

        ;; socket being closed is normal, otherwise log.
        (when-not (.isClosed conn-socket)
          (log/error e "An exception was caught during connection" {:port port, :cid cid})))

      (catch EOFException _
        (log/trace "Connection closed by client" {:port port, :cid cid}))

      (catch IOException e
        (log/debug e "IOException in connection" {:port port, :cid cid}))

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
  (^Server [node {:keys [allocator host port num-threads drain-wait ssl-ctx metrics-registry tracer read-only? playground?]
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
           total-connections-counter (when metrics-registry (metrics/add-counter metrics-registry "pgwire.total_connections"))
           cancelled-connections-counter (when metrics-registry (metrics/add-counter metrics-registry "pgwire.cancelled_connections")) 
           server (map->Server {:allocator allocator
                                :node node
                                :authn (authn/<-node node)
                                :host host
                                :port port
                                :read-only? read-only?
                                :accept-socket accept-socket
                                :thread-pool (Executors/newFixedThreadPool num-threads (util/->prefix-thread-factory "pgwire-connection-"))
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
                         :query-tracer (when (:query-tracing? tracer) (:tracer tracer))
                         :tx-error-counter tx-error-counter
                         :total-connections-counter total-connections-counter
                         :cancelled-connections-counter cancelled-connections-counter)
           accept-thread (-> (Thread/ofVirtual)
                             (.name (str "pgwire-server-accept-" port))
                             (.uncaughtExceptionHandler util/uncaught-exception-handler)
                             (.unstarted (fn []
                                           (accept-loop server))))

           server (assoc server :accept-thread accept-thread)]

       (when metrics-registry
         (metrics/add-gauge metrics-registry "pgwire.active_connections"
                            (fn []
                              (count (:connections @(:server-state server))))))
       (.start accept-thread)
       server))))

(defmethod xtn/apply-config! ::server [^Xtdb$Config config, _ {:keys [port read-only-port num-threads ssl] :as server}]
  (if server
    (let [host (:host server ::absent)]
      (cond-> (.getServer config)
        (not= host ::absent) (.host (when (and host (not= "*" host))
                                      host))
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
            :allocator (ig/ref :xtdb/allocator)
            :metrics-registry (ig/ref :xtdb.metrics/registry)
            :tracer (ig/ref :xtdb/tracer)}
           (<-config config))})

(defmethod ig/init-key ::server [_ {:keys [host node allocator port ro-port] :as opts}]
  (let [opts (dissoc opts :port :ro-port)]
    (letfn [(start-server [port read-only?]
              (when-not (neg? port)
                (let [{:keys [^InetAddress host port] :as srv} (serve node (-> opts
                                                                               (assoc :host host
                                                                                      :port port
                                                                                      :read-only? read-only?
                                                                                      :allocator (util/->child-allocator allocator "pgwire"))))]
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

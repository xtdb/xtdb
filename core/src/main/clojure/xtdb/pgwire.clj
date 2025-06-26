(ns xtdb.pgwire
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cognitect.anomalies :as-alias anom]
            [cognitect.transit :as transit]
            [integrant.core :as ig]
            [xtdb.antlr :as antlr]
            [xtdb.api :as xt]
            [xtdb.authn :as authn]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            [xtdb.log :as xt-log]
            [xtdb.metrics :as metrics]
            [xtdb.node :as xtn]
            [xtdb.pgwire.io :as pgio]
            [xtdb.pgwire.types :as pg-types]
            [xtdb.protocols :as xtp]
            [xtdb.serde :as serde]
            [xtdb.sql :as sql]
            [xtdb.time :as time]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import io.micrometer.core.instrument.Counter
           [java.io Closeable DataInputStream EOFException IOException PushbackInputStream]
           [java.lang Thread$State]
           [java.net InetAddress ServerSocket Socket SocketException]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel]
           [java.nio.file Path]
           [java.security KeyStore]
           [java.time Clock Duration ZoneId]
           [java.util Map]
           [java.util.concurrent ConcurrentHashMap ExecutorService Executors TimeUnit]
           [javax.net.ssl KeyManagerFactory SSLContext]
           (org.antlr.v4.runtime ParserRuleContext)
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           org.apache.arrow.vector.types.pojo.Field
           (xtdb.antlr Sql$DirectlyExecutableStatementContext SqlVisitor)
           (xtdb.api DataSource DataSource$ConnectionBuilder ServerConfig Xtdb$Config)
           xtdb.api.module.XtdbModule
           xtdb.arrow.RelationReader
           (xtdb.error Anomaly Incorrect Interrupted)
           xtdb.IResultCursor
           (xtdb.query PreparedQuery)))

;; references
;; https://www.postgresql.org/docs/current/protocol-flow.html
;; https://www.postgresql.org/docs/current/protocol-message-formats.html

(defrecord Server [^BufferAllocator allocator
                   port read-only?

                   ^ServerSocket accept-socket
                   ^Thread accept-thread

                   ^ExecutorService thread-pool

                   !closing?

                   server-state

                   authn-rules

                   !tmp-nodes]
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

        (when !tmp-nodes
          (log/debug "closing tmp nodes")
          (util/close !tmp-nodes))

        (util/close allocator)

        (log/infof "Server%sstopped." (if read-only? " (read-only) " " "))))))

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
(defn- err-invalid-passwd [msg] (ex-info msg {::severity :error, ::error-code "28P01"}))
(defn- err-query-cancelled [msg] (ex-info msg {::severity :error, :error-code "57014"}))

(defn- notice-warning [msg]
  {:severity "WARNING"
   :localized-severity "WARNING"
   :sql-state "01000"
   :message msg})

(defn cmd-cancel
  "Tells the connection to stop doing what its doing and return to idle"
  [conn]
  ;; we might this want to be conditional on a 'working state' to avoid races (if you fire loads of cancels randomly), not sure whether
  ;; to use status instead
  ;;TODO need to interrupt the thread belonging to the conn
  (swap! (:conn-state conn) assoc :cancel true)
  nil)

(defn- parse-session-params [params]
  (->> params
       (into {} (mapcat (fn [[k v]]
                          (case k
                            "options" (parse-session-params (for [[_ k v] (re-seq #"-c ([\w_]*)=([\w_]*)" v)]
                                                              [k v]))
                            [[k (case k
                                  "fallback_output_format" (#{:json :transit} (util/->kebab-case-kw v))
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

                           {::severity :error, ::error-code "08P01"})

        ::anom/unsupported {::severity :error, ::error-code "0A000"}

        (do
          (log/error ex "Uncaught exception processing message")
          {::severity :error, ::error-code "XX000"})))))

(defn send-ex [{:keys [conn-state] :as conn}, ^Throwable ex]
  (let [ex-msg (ex-message ex)
        {::keys [severity error-code routine]} (if (::error-code (ex-data ex))
                                                 (ex-data ex)
                                                 (ex->pgw-err (err/->anomaly ex {})))
        severity-str (str/upper-case (name severity))]
    (pgio/cmd-write-msg conn pgio/msg-error-response
                        {:error-fields (cond-> {:severity severity-str
                                                :localized-severity severity-str
                                                :sql-state error-code
                                                :message ex-msg
                                                :detail (when (instance? Anomaly ex)
                                                          (case (get-in @conn-state [:session :parameters "fallback_output_format"])
                                                            :transit (serde/write-transit ex :json)
                                                            (pg-types/json-bytes ex)))}
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

(defn cmd-startup-pg30 [{:keys [frontend server] :as conn} startup-opts]
  (let [{:keys [->node]} server
        user (get startup-opts "user")
        db-name (get startup-opts "database")
        {:keys [node] :as conn} (assoc conn :node (->node db-name))
        authn (authn/<-node node)]
    (if node
      (condp = (.methodFor authn user (pgio/host-address frontend))
        #xt.authn/method :trust
        (do
          (pgio/cmd-write-msg conn pgio/msg-auth {:result 0})
          (startup-ok conn startup-opts))

        #xt.authn/method :password
        (do
          ;; asking for a password, we only have :trust and :password for now
          (pgio/cmd-write-msg conn pgio/msg-auth {:result 3})

          ;; we go idle until we receive a message
          (when-let [{:keys [msg-name] :as msg} (pgio/read-client-msg! frontend)]
            (if (not= :msg-password msg-name)
              (throw (err-invalid-auth-spec (str "password authentication failed for user: " user)))

              (if (.verifyPassword authn user (:password msg))
                (do
                  (pgio/cmd-write-msg conn pgio/msg-auth {:result 0})
                  (startup-ok conn startup-opts))

                (throw (err-invalid-passwd (str "password authentication failed for user: " user)))))))

        (throw (err-invalid-auth-spec (str "no authentication record found for user: " user))))

      (throw (err-invalid-catalog db-name)))))

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
         (fn [{:keys [session watermark-tx-id] :as st}]
           (let [watermark-tx-id (or (some-> (:watermark-tx-id tx-opts) (apply-args args))
                                     watermark-tx-id
                                     -1)
                 {:keys [^Clock clock]} session]

             (when-not (or (neg? watermark-tx-id)
                           (= :read-write (:access-mode tx-opts)))
               (xt-log/await-tx node watermark-tx-id #xt/duration "PT30S"))

             (-> st
                 (update :transaction
                         (fn [{:keys [access-mode]}]
                           (if access-mode
                             (throw (pgio/err-protocol-violation "transaction already started"))

                             (-> {:current-time (.instant clock)
                                  :snapshot-time (:system-time (xtp/latest-completed-tx node))
                                  :default-tz (.getZone clock)
                                  :implicit? false}
                                 (into (:characteristics session))
                                 (into (-> tx-opts
                                           (dissoc :watermark-tx-id)
                                           (update :default-tz #(some-> % (apply-args args) (coerce->tz)))
                                           (update :system-time #(some-> % (apply-args args) (time/->instant {:default-tz (.getZone clock)})))
                                           (update :current-time #(some-> % (apply-args args) (time/->instant {:default-tz (.getZone clock)})))
                                           (update :snapshot-time #(some-> % (apply-args args) (time/->instant {:default-tz (.getZone clock)})))
                                           (->> (into {} (filter (comp some? val))))))
                                 (assoc :after-tx-id watermark-tx-id))))))))))

(defn- inc-error-counter! [^Counter counter]
  (when counter
    (.increment counter)))

(defn cmd-commit [{:keys [node conn-state] :as conn}]
  (let [{:keys [transaction session]} @conn-state
        {:keys [failed dml-buf system-time access-mode default-tz async?]} transaction
        {:keys [parameters]} session]

    (if failed
      (throw (pgio/err-protocol-violation "transaction failed"))

      (try
        (when (= :read-write access-mode)
          (let [tx-opts {:default-tz default-tz
                         :system-time (some-> system-time (time/->instant {:default-tz default-tz}))
                         :authn {:user (get parameters "user")}}]
            (if async?
              (let [tx-id (xtp/submit-tx node dml-buf tx-opts)]
                (swap! conn-state assoc :watermark-tx-id tx-id, :latest-submitted-tx {:tx-id tx-id}))

              (let [{:keys [tx-id error] :as tx} (xtp/execute-tx node dml-buf tx-opts)]
                (swap! conn-state assoc :watermark-tx-id tx-id, :latest-submitted-tx tx)

                (when error
                  (throw error))))))
        (finally
          (swap! conn-state dissoc :transaction)
          (close-all-portals conn))))))

(defn cmd-rollback [{:keys [conn-state]}]
  (swap! conn-state dissoc :transaction))

(defn- cmd-send-row-description [{:keys [conn-state] :as conn} pg-cols]
  (let [defaults {:table-oid 0
                  :column-attribute-number 0
                  :typlen -1
                  :type-modifier -1
                  :result-format :text}
        types-with-default (-> pg-types/pg-types
                               (assoc :default (->> (get-in @conn-state [:session :parameters "fallback_output_format"] :json)
                                                    (get pg-types/pg-types))))
        apply-defaults (fn [{:keys [pg-type col-name result-format]}]
                         (-> (into defaults
                                   (-> (get types-with-default pg-type)
                                       (select-keys [:oid :typlen :type-modifier])
                                       (set/rename-keys {:oid :column-oid})))
                             (assoc :column-name col-name)
                             (cond-> result-format (assoc :result-format result-format))))
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
                                                  (get-in pg-types/pg-types [:text :oid])
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
        (:query :dml :show-variable) (describe* describe-target)

        :execute (let [inner (get-in @conn-state [:prepared-statements (:statement-name describe-target)])]
                   (describe* {:param-oids (:param-oids describe-target)
                               :pg-cols (:pg-cols inner)}))

        (pgio/cmd-write-msg conn pgio/msg-no-data)))))

(defmethod handle-msg* :msg-sync [{:keys [conn-state] :as conn} _]
  ;; Sync commands are sent by the client to commit transactions
  ;; and to clear the error state of a :extended mode series of commands (e.g the parse/bind/execute dance)

  (let [{:keys [implicit? failed]} (:transaction @conn-state)]
    (try
      (if implicit?
        (if failed
          (cmd-rollback conn)
          (cmd-commit conn))
        (close-all-portals conn))
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
    :cols [{:col-name "col1" :pg-type :varchar}]
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

                                        (visitWatermarkTxOption [_ ctx]
                                          {:watermark-tx-id (sql/plan-expr (.watermarkTx ctx) env)})

                                        (visitReadWriteSession [_ _] {:access-mode :read-write})

                                        (visitReadOnlySession [_ _] {:access-mode :read-only})

                                        (visitSystemTimeTxOption [_ ctx]
                                          {:system-time (sql/plan-expr (.systemTime ctx) env)})

                                        (visitAsyncTxOption [_ ctx]
                                          {:async? (boolean (sql/plan-expr (.async ctx) env))})

                                        (visitSnapshotTimeTxOption [_ ctx]
                                          {:snapshot-time (sql/plan-expr (.snapshotTime ctx) env)})

                                        (visitClockTimeTxOption [_ ctx]
                                          {:current-time (sql/plan-expr (.clockTime ctx) env)})

                                        (visitCommitStatement [_ _] {:statement-type :commit})
                                        (visitRollbackStatement [_ _] {:statement-type :rollback})

                                        (visitSetRoleStatement [_ _] {:statement-type :set-role})

                                        (visitSetTimeZoneStatement [_ ctx]
                                          ;; not sure if handlling time zone explicitly is the right approach
                                          ;; might be cleaner to handle it like any other session param
                                          {:statement-type :set-time-zone
                                           :tz (sql/plan-expr (.zone ctx) env)})

                                        (visitInsertStmt [this ctx] (-> (.insertStatement ctx) (.accept this)))

                                        (visitInsertStatement [_ ctx]
                                          {:statement-type :dml, :dml-type :insert, :query (subsql ctx)})

                                        (visitUpdateStmt [this ctx] (-> (.updateStatementSearched ctx) (.accept this)))

                                        (visitPatchStmt [_ ctx]
                                          {:statement-type :dml, :dml-type :patch, :query (subsql ctx)})

                                        (visitUpdateStatementSearched [_ ctx]
                                          {:statement-type :dml, :dml-type :update, :query (subsql ctx)})

                                        (visitDeleteStmt [this ctx] (-> (.deleteStatementSearched ctx) (.accept this)))

                                        (visitDeleteStatementSearched [_ ctx]
                                          {:statement-type :dml, :dml-type :delete, :query (subsql ctx)})

                                        (visitEraseStmt [this ctx] (-> (.eraseStatementSearched ctx) (.accept this)))

                                        (visitEraseStatementSearched [_ ctx]
                                          {:statement-type :dml, :dml-type :erase, :query (subsql ctx)})

                                        (visitAssertStatement [_ ctx]
                                          {:statement-type :dml, :dml-type :assert, :query (subsql ctx)})

                                        (visitQueryExpr [_ ctx]
                                          {:statement-type :query, :query (subsql ctx), :parsed-query ctx})

                                        (visitCopyInStmt [this ctx]
                                          (into {:statement-type :copy-in,
                                                 :table-name (sql/identifier-sym (.tableName ctx))}
                                                (map (partial sql/accept-visitor this))
                                                (some-> (.opts ctx) (.copyOpt))))

                                        (visitCopyFormatOption [_ ctx]
                                          [:format (sql/plan-expr (.format ctx) env)])

                                        ;; could do pre-submit validation here
                                        (visitCreateUserStatement [_ ctx]
                                          {:statement-type :dml, :dml-type :create-role, :query (subsql ctx)})
                                        (visitAlterUserStatement [_ ctx]
                                          {:statement-type :dml, :dml-type :create-role, :query (subsql ctx)})

                                        (visitPrepareStmt [this ctx] (-> (.prepareStatement ctx) (.accept this)))

                                        (visitPrepareStatement [this ctx]
                                          (let [inner-ctx (.directlyExecutableStatement ctx)]
                                            {:statement-type :prepare
                                             :statement-name (str (sql/identifier-sym (.statementName ctx)))
                                             :inner (.accept inner-ctx this)}))

                                        (visitExecuteStmt [_ ctx]
                                          {:statement-type :execute,
                                           :statement-name (str (sql/identifier-sym (.statementName (.executeStatement ctx)))),
                                           :query (subsql ctx)
                                           :parsed-query ctx})

                                        (visitShowVariableStatement [_ ctx]
                                          {:statement-type :query, :query sql, :parsed-query ctx})

                                        (visitSetWatermarkStatement [_ ctx]
                                          (let [wm-tx-id (sql/plan-expr (.literal ctx) env)]
                                            (if (number? wm-tx-id)
                                              {:statement-type :set-watermark, :watermark-tx-id wm-tx-id}
                                              (throw (pgio/err-protocol-violation "invalid watermark - expecting number")))))

                                        (visitShowWatermarkStatement [_ _]
                                          {:statement-type :show-variable, :query sql, :variable :watermark})

                                        (visitShowSnapshotTimeStatement [_ ctx]
                                          {:statement-type :query, :query sql, :parsed-query ctx})

                                        (visitShowClockTimeStatement [_ ctx]
                                          {:statement-type :query, :query sql, :parsed-query ctx})

                                        (visitShowSessionVariableStatement [_ ctx]
                                          {:statement-type :show-variable
                                           :query sql
                                           :variable (session-param-name (.identifier ctx))})))))))

              (catch Exception e
                (log/debug e "Error parsing SQL")
                (throw e))))))))

(defn- show-var-query [variable]
  (case variable
    "latest_completed_tx" (-> '[:select (not (nil? tx_id))
                                [:table [{:tx_id ?_0, :system_time ?_1}]]]
                              (with-meta {:param-count 2}))
    "latest_submitted_tx" (-> '[:select (not (nil? tx_id))
                                [:table [{:tx_id ?_0, :system_time ?_1,
                                          :committed ?_2, :error ?_3}]]]
                              (with-meta {:param-count 4}))
    (-> (xt/template [:table [{~(keyword variable) ?_0}]])
        (with-meta {:param-count 1}))))

(defn- show-var-param-types [variable]
  (case variable
    "latest_completed_tx" [:i64 types/temporal-col-type]
    "latest_submitted_tx" [:i64 types/temporal-col-type :bool :transit]
    :watermark [:i64]
    "standard_conforming_strings" [:bool]

    [:utf8]))

(defn- prep-stmt [{:keys [node, conn-state] :as conn} {:keys [statement-type param-oids] :as stmt}]
  (case statement-type
    (:query :execute :show-variable)
    (try
      (let [{:keys [^Sql$DirectlyExecutableStatementContext parsed-query explain?]} stmt

            {:keys [session watermark-tx-id]} @conn-state
            {:keys [^Clock clock]} session

            query-opts {:after-tx-id (or watermark-tx-id -1)
                        :tx-timeout (Duration/ofMinutes 1)
                        :default-tz (.getZone clock)
                        :explain? explain?}

            ^PreparedQuery pq (case statement-type
                                (:query :execute) (xtp/prepare-sql node parsed-query query-opts)
                                :show-variable (xtp/prepare-ra node (show-var-query (:variable stmt)) query-opts))]

        (when-let [warnings (.getWarnings pq)]
          (doseq [warning warnings]
            (pgio/cmd-send-notice conn (notice-warning (sql/error-string warning)))))

        (let [param-oids (->> (concat param-oids (repeat 0))
                              (into [] (take (.getParamCount pq))))
              param-types (case statement-type
                            (:query :execute) (map (comp :col-type pg-types/pg-types-by-oid) param-oids)
                            :show-variable (show-var-param-types (:variable stmt)))
              pg-cols (if (some nil? param-types)
                        ;; if we're unsure on some of the col-types, return all of the output cols as the fallback type (#4455)
                        (for [col-name (map str (.getColumnNames pq))]
                          {:pg-type :default, :col-name col-name})

                        (->> (.getColumnFields pq (->> param-types
                                                       (into [] (comp (map (comp types/col-type->field types/col-type->nullable-col-type))
                                                                      (map-indexed (fn [idx field]
                                                                                     (types/field-with-name field (str "?_" idx))))))))
                             (mapv pg-types/field->pg-col)))]
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
        (inc-error-counter! tx-error-counter))

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

(defn- ->xtify-arg [session {:keys [arg-format param-oids]}]
  (fn xtify-arg [arg-idx arg]
    (when (some? arg)
      (let [param-oid (nth param-oids arg-idx)
            arg-format (nth arg-format arg-idx :text)
            {:keys [read-binary, read-text]} (or (get pg-types/pg-types-by-oid param-oid)
                                                 (throw (err/unsupported ::unsupported-param-type "Unsupported param type provided for read"
                                                                         {:param-oid param-oid, :arg-format arg-format, :arg arg})))]

        (try
          (if (= :binary arg-format)
            (read-binary session arg)
            (read-text session arg))
          (catch Exception e
            (let [ex-msg (or (ex-message e) (str "invalid arg representation - " e))]
              (throw (err/incorrect ::invalid-arg-representation ex-msg
                                    {:arg-format arg-format, :arg-idx arg-idx})))))))))

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

(defn bind-stmt [{:keys [node conn-state ^BufferAllocator allocator] :as conn} {:keys [statement-type ^PreparedQuery prepared-query args result-format] :as stmt}]
  (let [{:keys [session transaction watermark-tx-id]} @conn-state
        {:keys [^Clock clock], session-params :parameters} session
        after-tx-id (or (:after-tx-id transaction) watermark-tx-id -1)

        query-opts {:snapshot-time (or (:snapshot-time stmt) (:snapshot-time transaction))
                    :current-time (or (:current-time stmt)
                                      (:current-time transaction)
                                      (.instant clock))
                    :default-tz (or (:default-tz transaction) (.getZone clock))
                    :after-tx-id after-tx-id}

        xt-args (xtify-args conn args stmt)]

    (letfn [(->cursor ^xtdb.IResultCursor [xt-args]
              (util/with-close-on-catch [args-rel (vw/open-args allocator xt-args)]
                (.openQuery prepared-query (assoc query-opts :args args-rel))))

            (->pg-cols [prepared-pg-cols ^IResultCursor cursor]
              (let [resolved-pg-cols (mapv pg-types/field->pg-col (.getResultFields cursor))]
                (-> (map (fn [{prepared-pg-type :pg-type, :as prepared-pg-col} {resolved-pg-type :pg-type}]
                           (when-not (or (= prepared-pg-type :default)
                                         (= resolved-pg-type :null)
                                         (= prepared-pg-type resolved-pg-type))
                             (throw (err/conflict :prepared-query-out-of-date "cached plan must not change result type"
                                                  {:prepared-cols prepared-pg-cols
                                                   :resolved-cols resolved-pg-cols})))
                           prepared-pg-col)

                         prepared-pg-cols
                         resolved-pg-cols)

                    (with-result-formats result-format))))]

      (case statement-type
        :query (util/with-close-on-catch [cursor (->cursor xt-args)]
                 (-> stmt
                     (assoc :cursor cursor,
                            :pg-cols (->pg-cols (:pg-cols stmt) cursor))))

        :show-variable (let [{:keys [variable]} stmt]
                         (util/with-close-on-catch [cursor (->cursor (case variable
                                                                       :watermark [(when-not (neg? after-tx-id)
                                                                                     after-tx-id)]
                                                                       "latest_completed_tx" (mapv (into {} (xtp/latest-completed-tx node))
                                                                                                   [:tx-id :system-time])
                                                                       "latest_submitted_tx" (mapv (into {} (:latest-submitted-tx @conn-state))
                                                                                                   [:tx-id :system-time :committed? :error])
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
                         :query (util/with-close-on-catch [inner-cursor (.openQuery inner-pq (assoc query-opts :args args-rel))]
                                  (-> inner
                                      (assoc :cursor inner-cursor
                                             :pg-cols (-> (->pg-cols (:pg-cols inner) inner-cursor)
                                                          (with-result-formats result-format)))))

                         :dml (let [arg-fields (.getResultFields args-cursor)]
                                (try
                                  (-> inner
                                      (assoc :args (vec (for [^Field field arg-fields]
                                                          (-> (.vectorForOrNull args-rel (.getName field))
                                                              (.getObject 0))))
                                             :param-oids (->> arg-fields
                                                              (mapv (comp :oid pg-types/pg-types :pg-type pg-types/field->pg-col)))))
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
      (pgio/cmd-write-msg conn pgio/msg-data-row {:vals (mapv (fn [v] (if (bytes? v) v (pg-types/utf8 v))) row)}))

    (pgio/cmd-write-msg conn pgio/msg-command-complete {:command (str (statement-head q) " " (count rows))})))

(defn cmd-set-session-parameter [conn parameter value]
  (set-session-parameter conn parameter value)
  (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "SET"}))

(defn cmd-set-transaction [conn _tx-opts]
  ;; no-op - can only set transaction isolation, and that
  ;; doesn't mean anything to us because we're always serializable
  (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "SET TRANSACTION"}))

(defn cmd-set-time-zone [conn {:keys [tz args]}]
  (let [tz (-> tz (apply-args args))]
    (set-time-zone conn tz))
  (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "SET TIME ZONE"}))

(defn cmd-set-watermark [{:keys [conn-state] :as conn} {:keys [watermark-tx-id]}]
  (swap! conn-state assoc :watermark-tx-id watermark-tx-id)
  (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "SET WATERMARK"}))

(defn cmd-set-session-characteristics [{:keys [conn-state] :as conn} session-characteristics]
  (swap! conn-state update-in [:session :characteristics] (fnil into {}) session-characteristics)
  (pgio/cmd-write-msg conn pgio/msg-command-complete {:command "SET SESSION CHARACTERISTICS"}))

(defn- cmd-exec-dml [{:keys [conn-state tx-error-counter] :as conn} {:keys [dml-type query args param-oids]}]
  (when (or (not= (count param-oids) (count args))
            (some (fn [idx]
                    (and (zero? (nth param-oids idx))
                         (some? (nth args idx))))
                  (range (count param-oids))))
    (inc-error-counter! tx-error-counter)
    (throw (err/incorrect ::missing-arg-types "Missing types for args - client must specify types for all non-null params in DML statements"
                          {:query query, :param-oids param-oids})))

  (when-not (:transaction @conn-state)
    (cmd-begin conn {:implicit? true, :access-mode :read-write} {}))

  (swap! conn-state update-in [:transaction :dml-buf]
         (fnil (fn [dml-ops]
                 (or (when-let [[_sql last-query :as last-op] (peek dml-ops)]
                       (when (= last-query query)
                         (conj (pop dml-ops)
                               (conj last-op args))))
                     (conj dml-ops [:sql query args])))
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

(defn cmd-exec-query [{:keys [conn-state !closing? query-error-counter] :as conn}
                      {:keys [limit query ^IResultCursor cursor pg-cols] :as _portal}]
  (try
    (let [cancelled-by-client? #(:cancel @conn-state)
          ;; please die as soon as possible (not the same as draining, which leaves conns :running for a time)

          n-rows-out (volatile! 0)

          session (:session @conn-state)
          types-with-default (-> pg-types/pg-types
                                 (assoc :default (->> (get-in session [:parameters "fallback_output_format"] :json)
                                                      (get pg-types/pg-types))))]

      (while (and (or (nil? limit) (< @n-rows-out limit))
                  (.tryAdvance cursor
                               (fn [^RelationReader rel]
                                 (cond
                                   (cancelled-by-client?)
                                   (do (log/trace "query cancelled by client")
                                       (swap! conn-state dissoc :cancel)
                                       (throw (err-query-cancelled "query cancelled during execution")))

                                   (Thread/interrupted) (throw (InterruptedException.))

                                   @!closing? (log/trace "query result stream stopping (conn closing)")

                                   :else (dotimes [idx (cond-> (.getRowCount rel)
                                                         limit (min (- limit @n-rows-out)))]
                                           (let [row (mapv
                                                      (fn [{:keys [^String col-name pg-type result-format]}]
                                                        (let [{:keys [write-binary write-text]} (get types-with-default pg-type)
                                                              rdr (.vectorForOrNull rel col-name)]
                                                          (when-not (.isNull rdr idx)
                                                            (if (= :binary result-format)
                                                              (write-binary session rdr idx)
                                                              (if write-text
                                                                (write-text session rdr idx)
                                                                (pg-types/write-json session rdr idx))))))
                                                      pg-cols)]
                                             (pgio/cmd-write-msg conn pgio/msg-data-row {:vals row})
                                             (vswap! n-rows-out inc))))))))

      (pgio/cmd-write-msg conn pgio/msg-command-complete {:command (str (statement-head query) " " @n-rows-out)}))

    (catch Interrupted e (throw e))
    (catch InterruptedException e (throw e))
    (catch Throwable e
      (inc-error-counter! query-error-counter)
      (throw e))))

(defn execute-portal [{:keys [conn-state] :as conn} {:keys [statement-type canned-response parameter value session-characteristics tx-characteristics] :as portal}]
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
    :set-watermark (cmd-set-watermark conn portal)
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

    (:query :show-variable) (cmd-exec-query conn portal)
    :prepare (cmd-prepare conn portal)
    :dml (cmd-exec-dml conn portal)

    :copy-in (let [format (case (:format portal)
                            "transit-json" :transit-json
                            "transit-msgpack" :transit-msgpack
                            (throw (err/incorrect ::invalid-copy-format "COPY IN requires a valid format: 'transit-json' or 'transit-msgpack'"
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
                                                       :transit-msgpack :binary)]
                                     {:copy-format copy-format
                                      :column-formats [copy-format]})))

    (throw (UnsupportedOperationException. (pr-str {:portal portal})))))

(defmethod handle-msg* :msg-execute [{:keys [conn-state] :as conn} {:keys [portal-name limit]}]
  ;; Handles a msg-execute to run a previously bound portal (via msg-bind).
  (let [portal (or (get-in @conn-state [:portals portal-name])
                   (throw (pgio/err-protocol-violation "no such portal")))]
    (execute-portal conn (cond-> portal
                           (not (zero? limit)) (assoc :limit limit)))))

(defmethod handle-msg* :msg-simple-query [{:keys [conn-state] :as conn} {:keys [query]}]
  (swap! conn-state assoc :protocol :simple)

  (close-portal conn "")

  (try
    (doseq [stmt (parse-sql query)]
      (when-not (boolean (:transaction @conn-state))
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

(defmethod handle-msg* :msg-copy-done [{:keys [conn-state] :as conn} _msg]
  (let [{:keys [^Path copy-file, ^FileChannel write-ch, format]} (or (:copy @conn-state)
                                                                     (throw (err/incorrect ::copy-not-in-progress
                                                                                           "COPY IN not in progress, cannot write data")))]
    (try
      (err/wrap-anomaly {}
        (.close write-ch)

        (let [started-tx? (when-not (:transaction @conn-state)
                            (cmd-begin conn {:implicit? true, :access-mode :read-write} {})
                            true)
              doc-count (try
                          (let [docs (with-open [is (util/open-input-stream copy-file)]
                                       (vec (serde/transit-seq (transit/reader is
                                                                               (case format
                                                                                 :transit-json :json
                                                                                 :transit-msgpack :msgpack)
                                                                               {:handlers serde/transit-read-handler-map}))))]

                            (swap! conn-state
                                   (fn [{:keys [copy], :as conn-state}]
                                     (-> conn-state
                                         (update-in [:transaction :dml-buf] (fnil conj [])
                                                    (into [:put-docs (keyword (:table-name copy))]
                                                          docs))
                                         (dissoc :copy))))

                            (when started-tx?
                              (cmd-commit conn))

                            (count docs))

                          (catch Throwable e
                            (when started-tx?
                              (cmd-rollback conn))
                            (throw e)))]

          (pgio/cmd-write-msg conn pgio/msg-command-complete {:command (str "COPY " doc-count)})))

      (catch Incorrect e
        (log/debug e "Error writing COPY IN data")
        (send-ex conn e))

      (catch Throwable e
        (log/warn e "Error writing COPY IN data")
        (send-ex conn e))

      (finally
        (util/delete-file copy-file)))

    (cmd-send-ready conn)))

;; ignore password messages, we are authenticated when getting here
(defmethod handle-msg* :msg-password [_conn _msg])

(defmethod handle-msg* ::default [_conn {:keys [msg-name]}]
  (throw (err/unsupported ::unknown-client-msg (str "unknown client message: " msg-name)
                          {:msg-name msg-name})))

(defn handle-msg [{:keys [cid conn-state] :as conn} {:keys [msg-name] :as msg}]
  (try
    (log/trace "Read client msg" {:cid cid, :msg msg})

    (err/wrap-anomaly {}
      (if (and (:skip-until-sync? @conn-state) (not= :msg-sync msg-name))
        (log/trace "Skipping msg until next sync due to error in extended protocol" {:cid cid, :msg msg})
        (handle-msg* conn msg)))

    (catch Interrupted e (throw e))

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

(defn- ->tmp-node [^Map !tmp-nodes, db-name]
  (.computeIfAbsent !tmp-nodes db-name
                    (fn [db-name]
                      (log/debug "starting tmp-node" (pr-str db-name))
                      (xtn/start-node {:server nil}))))

(defn- connect
  "Starts and runs a connection on the current thread until it closes.

  The connection exiting for any reason (either because the connection, received a close signal, or the server is draining, or unexpected error) should result in connection resources being
  freed at the end of this function. So the connections lifecycle should be totally enclosed over the lifetime of a connect call.

  See comment 'Connection lifecycle'."
  [{:keys [server-state, port, allocator, query-error-counter, tx-error-counter, ^Counter total-connections-counter] :as server} ^Socket conn-socket]
  (let [close-promise (promise)
        {:keys [cid !closing?] :as conn} (util/with-close-on-catch [_ conn-socket]
                                           (let [cid (:next-cid (swap! server-state update :next-cid (fnil inc 0)))
                                                 !conn-state (atom {:close-promise close-promise
                                                                    :session {:access-mode :read-only
                                                                              :clock (:clock @server-state)}})
                                                 !closing? (atom false)]
                                             (try
                                               (-> (map->Connection {:cid cid,
                                                                     :server server,
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
                    :tx-error-counter tx-error-counter)]

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

  node: if provided, uses the given node for all connections; otherwise, creates a transient, in-memory node for each new connection

  Options:

  :port (default 0, opening the socket on an unused port).
  :num-threads (bounds the number of client connections, default 42)
  "
  (^Server [node] (serve node {}))
  (^Server [node {:keys [allocator host port num-threads drain-wait ssl-ctx metrics-registry read-only?]
                  :or {host (InetAddress/getLoopbackAddress)
                       port 0
                       num-threads 42
                       drain-wait 5000}}]
   (util/with-close-on-catch [accept-socket (ServerSocket. port 0 host)]
     (let [host (.getInetAddress accept-socket)
           port (.getLocalPort accept-socket)
           query-error-counter (when metrics-registry (metrics/add-counter metrics-registry "query.error"))
           tx-error-counter (when metrics-registry (metrics/add-counter metrics-registry "tx.error"))
           total-connections-counter (when metrics-registry (metrics/add-counter metrics-registry "pgwire.total_connections"))
           !tmp-nodes (when-not node
                        (ConcurrentHashMap.))
           server (map->Server {:allocator allocator
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

                                :ssl-ctx ssl-ctx

                                :!tmp-nodes !tmp-nodes
                                :->node (fn [db-name]
                                          (cond
                                            (nil? node) (->tmp-node !tmp-nodes db-name)
                                            (= db-name "xtdb") node))})

           server (assoc server
                         :query-error-counter query-error-counter
                         :tx-error-counter tx-error-counter
                         :total-connections-counter total-connections-counter)
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
        ks (with-open [ks-file (util/open-input-stream ks-path)]
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

(defmethod ig/prep-key ::server [_ config]
  (into {:node (ig/ref :xtdb/node)
         :allocator (ig/ref :xtdb/allocator)
         :metrics-registry (ig/ref :xtdb.metrics/registry)}
        (<-config config)))

(defmethod ig/init-key ::server [_ {:keys [host node allocator port ro-port] :as opts}]
  (let [opts (dissoc opts :port :ro-port)]
    (letfn [(start-server [port read-only?]
              (when-not (neg? port)
                (let [{:keys [^InetAddress host port] :as srv} (serve node (-> opts
                                                                               (assoc :host host
                                                                                      :port port
                                                                                      :read-only? read-only?
                                                                                      :allocator (util/->child-allocator allocator "pgwire"))))]
                  (log/infof "Server%sstarted at postgres://%s:%d"
                             (if read-only? " (read-only) " " ")
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
   (let [{:keys [^InetAddress host, port] :as srv} (serve nil (merge opts
                                                                     {:host nil
                                                                      :allocator (RootAllocator.)}))]
     (log/infof "Playground started at postgres://%s:%d" (.getHostAddress host) port)
     srv)))


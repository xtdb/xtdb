(ns xtdb.pgwire
  (:require [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.antlr :as antlr]
            [xtdb.api :as xt]
            [xtdb.authn :as authn]
            [xtdb.expression :as expr]
            [xtdb.metrics :as metrics]
            [xtdb.node :as xtn]
            [xtdb.node.impl]
            [xtdb.protocols :as xtp]
            [xtdb.query]
            [xtdb.sql.plan :as plan]
            [xtdb.time :as time]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import [clojure.lang MapEntry]
           [java.io BufferedInputStream BufferedOutputStream ByteArrayInputStream ByteArrayOutputStream Closeable DataInputStream DataOutputStream EOFException IOException InputStream OutputStream PushbackInputStream]
           [java.lang AutoCloseable Thread$State]
           [java.net ServerSocket Socket SocketException URI]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]
           [java.nio.file Path]
           [java.security KeyStore]
           [java.time Clock Duration Instant LocalDate LocalDateTime LocalTime OffsetDateTime Period ZoneId ZonedDateTime]
           [java.util List Map Set UUID]
           [java.util.concurrent ConcurrentHashMap ExecutorService Executors TimeUnit]
           [javax.net.ssl KeyManagerFactory SSLContext SSLSocket]
           io.micrometer.core.instrument.Counter
           (org.antlr.v4.runtime ParserRuleContext)
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           [org.apache.arrow.vector PeriodDuration]
           org.apache.arrow.vector.types.pojo.Field
           [org.apache.commons.codec.binary Hex]
           (xtdb.antlr Sql$DirectlyExecutableStatementContext SqlVisitor)
           (xtdb.api Authenticator ServerConfig Xtdb$Config)
           xtdb.api.module.XtdbModule
           (xtdb.query BoundQuery PreparedQuery)
           [xtdb.types IntervalDayTime IntervalMonthDayNano IntervalYearMonth]
           [xtdb.vector IVectorReader RelationReader]))

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

(defprotocol Frontend
  (send-client-msg!
    [frontend msg-def]
    [frontend msg-def data])

  (read-client-msg! [frontend])
  (host-address [frontent])

  (upgrade-to-ssl [frontend ssl-ctx])

  (flush! [frontend]))

(defn- cmd-write-msg
  "Writes out a single message given a definition (msg-def) and optional data record."
  ([{:keys [frontend]} msg-def]
   (send-client-msg! frontend msg-def))

  ([{:keys [frontend]} msg-def data]
   (send-client-msg! frontend msg-def data)))

(def ^:private version-messages
  "Integer codes sent by the client to identify a startup msg"
  {
   ;; this is the normal 'hey I'm a postgres client' if ssl is not requested
   196608 :30
   ;; cancellation messages come in as special startup sequences (pgwire does not handle them yet!)
   80877102 :cancel
   ;; ssl messages are used when the client either requires, prefers, or allows ssl connections.
   80877103 :ssl
   ;; gssapi encoding is not supported by xt, and we tell the client that
   80877104 :gssenc})

;; all postgres client IO arrives as either an untyped (startup) or typed message

(defn- read-untyped-msg [^DataInputStream in]
  (let [size (- (.readInt in) 4)
        barr (byte-array size)
        _ (.readFully in barr)]
    (DataInputStream. (ByteArrayInputStream. barr))))

(defn- read-version [^DataInputStream in]
  (let [^DataInputStream msg-in (read-untyped-msg in)
        version (.readInt msg-in)]
    {:msg-in msg-in
     :version (version-messages version)}))

(declare client-msgs)
(declare client-err)
(declare err-protocol-violation)
(declare ->socket-frontend)
(declare flush-messages)

(defrecord SocketFrontend [^Socket socket, ^DataInputStream in, ^DataOutputStream out]
  Frontend
  (send-client-msg! [_ msg-def]
    (log/trace "Writing server message" (select-keys msg-def [:char8 :name]))

    (.writeByte out (byte (:char8 msg-def)))
    (.writeInt out 4)
    (when (flush-messages (:name msg-def))
      (.flush out)))

  (send-client-msg! [_ msg-def data]
    (log/trace "Writing server message (with body)" (select-keys msg-def [:char8 :name]))
    (let [bytes-out (ByteArrayOutputStream.)
          msg-out (DataOutputStream. bytes-out)
          _ ((:write msg-def) msg-out data)
          arr (.toByteArray bytes-out)]
      (.writeByte out (byte (:char8 msg-def)))
      (.writeInt out (+ 4 (alength arr)))
      (.write out arr)
      (when (flush-messages (:name msg-def))
        (.flush out))))

  (read-client-msg! [_]
    (let [type-char (char (.readUnsignedByte in))
          msg-var (or (client-msgs type-char)
                      (throw (client-err (str "Unknown client message " type-char))))
          rdr (:read @msg-var)]
      (try
        (-> (rdr (read-untyped-msg in))
            (assoc :msg-name (:name @msg-var)))
        (catch Exception e
          (throw (ex-info "error reading client message"
                          {::client-error (err-protocol-violation (str "Error reading client message " (ex-message e)))}
                          e))))))

  (host-address [_] (.getHostAddress (.getInetAddress socket)))

  (upgrade-to-ssl [this ssl-ctx]
    (if (and ssl-ctx (not (instance? SSLSocket socket)))
      ;; upgrade the socket, then wait for the client's next startup message

      (do
        (log/trace "upgrading to SSL")

        (.writeByte out (byte \S))
        (.flush out)

        (let [^SSLSocket ssl-socket (-> (.getSocketFactory ^SSLContext ssl-ctx)
                                        (.createSocket socket
                                                       (-> (.getInetAddress socket)
                                                           (.getHostAddress))
                                                       (.getPort socket)
                                                       true))]
          (try
            (.setUseClientMode ssl-socket false)
            (.startHandshake ssl-socket)
            (log/trace "SSL handshake successful")
            (catch Exception e
              (log/debug e "error in SSL handshake")
              (throw e)))

          (->socket-frontend ssl-socket)))

      ;; unsupported - recur and give the client another chance to say hi
      (do
        (.writeByte out (byte \N))
        (.flush out)
        this)))

  (flush! [_] (.flush out))

  AutoCloseable
  (close [_]
    (when-not (.isClosed socket)
      (util/try-close socket))))

(def ^:private socket-buffer-size 1024)

(defn ->socket-frontend [^Socket socket]
  (->SocketFrontend socket
                    (DataInputStream. (BufferedInputStream. (.getInputStream socket) socket-buffer-size))
                    (DataOutputStream. (BufferedOutputStream. (.getOutputStream socket) socket-buffer-size))))

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

(defn- read-c-string
  "Postgres strings are null terminated, reads a null terminated utf-8 string from in."
  ^String [^InputStream in]
  (loop [baos (ByteArrayOutputStream.)
         x (.read in)]
    (cond
      (neg? x) (throw (EOFException. "EOF in read-c-string"))
      (zero? x) (String. (.toByteArray baos) StandardCharsets/UTF_8)
      :else (recur (doto baos
                     (.write x))
                   (.read in)))))

(defn- write-c-string
  "Postgres strings are null terminated, writes a null terminated utf8 string to out."
  [^OutputStream out ^String s]
  (.write out (.getBytes s StandardCharsets/UTF_8))
  (.write out 0))

(def ^:private oid-varchar (get-in types/pg-types [:varchar :oid]))
(def ^:private oid-json (get-in types/pg-types [:json :oid]))

;;; errors

(defn- err-protocol-violation [msg]
  {:severity "ERROR"
   :localized-severity "ERROR"
   :sql-state "08P01"
   :message msg})

(defn- client-err
  ([client-msg] (client-err client-msg nil))
  ([client-msg data]
   (ex-info client-msg (assoc data ::client-error (err-protocol-violation client-msg)))))

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

(defn- err-internal [msg]
  {:severity "ERROR"
   :localized-severity "ERROR"
   :sql-state "XX000"
   :message msg})

(defn- err-invalid-catalog [db-name]
  {:severity "FATAL"
   :localized-severity "FATAL"
   :sql-state "3D000"
   :message (format "database '%s' does not exist" db-name)})

(defn- err-invalid-auth-spec [msg]
  {:severity "ERROR"
   :localized-severity "ERROR"
   :sql-state "28000"
   :message msg})

(defn- err-invalid-passwd [msg]
  {:severity "ERROR"
   :localized-severity "ERROR"
   :sql-state "28P01"
   :message msg})

(defn- err-query-cancelled [msg]
  {:severity "ERROR"
   :localized-severity "ERROR"
   :sql-state "57014"
   :message msg})

(defn- notice-warning [msg]
  {:severity "WARNING"
   :localized-severity "WARNING"
   :sql-state "01000"
   :message msg})

(defn- invalid-text-representation [msg]
  {:severity "ERROR"
   :localized-severity "ERROR"
   :sql-state "22P02"
   :message msg})

(defn- invalid-binary-representation [msg]
  {:severity "ERROR"
   :localized-severity "ERROR"
   :sql-state "22P03"
   :message msg})

(defn err-pg-exception
  "Returns a pg specific error for an XTDB exception"
  [^Throwable ex generic-msg]
  (if (or (instance? xtdb.IllegalArgumentException ex)
          (instance? xtdb.RuntimeException ex))
    (err-protocol-violation (.getMessage ex))
    (err-internal generic-msg)))

;;; sql processing

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
    :cols [{:column-name "col1" :column-oid oid-varchar}]
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

(defn- strip-semi-colon [s] (if (str/ends-with? s ";") (subs s 0 (dec (count s))) s))

(defn- statement-head [s]
  (-> s (str/split #"\s+") first str/upper-case strip-semi-colon))

(defn session-param-name [^ParserRuleContext ctx]
  (some-> ctx
          (.accept (reify SqlVisitor
                     (visitRegularIdentifier [_ ctx] (.getText ctx))
                     (visitDelimitedIdentifier [_ ctx]
                       (let [di-str (.getText ctx)]
                         (subs di-str 1 (dec (count di-str)))))))
          (str/lower-case)))

(defn date-time-visitor [^ZoneId default-tz]
  (reify SqlVisitor
    (visitDateLiteral [_ ctx]
      (-> (LocalDate/parse (.accept (.characterString ctx) plan/string-literal-visitor))
          (.atStartOfDay)
          (.atZone default-tz)))

    (visitTimestampLiteral [_ ctx]
      (let [ts (time/parse-sql-timestamp-literal (.accept (.characterString ctx) plan/string-literal-visitor))]
        (cond
          (instance? LocalDateTime ts) (.atZone ^LocalDateTime ts default-tz)
          (instance? ZonedDateTime ts) ts)))))

(defn- interpret-sql [sql {:keys [default-tz watermark-tx-id session-parameters]}]
  (log/debug "Interpreting SQL: " sql)
  (let [sql-trimmed (trim-sql sql)]
    (or (when (str/blank? sql-trimmed)
          [{:statement-type :empty-query}])

        (when-some [canned-response (get-canned-response sql-trimmed)]
          [{:statement-type :canned-response, :canned-response canned-response}])

        (try
          (letfn [(subsql [^ParserRuleContext ctx]
                    (subs sql (.getStartIndex (.getStart ctx)) (inc (.getStopIndex (.getStop ctx)))))]
            (->> (antlr/parse-multi-statement sql-trimmed)
                 (mapv (partial plan/accept-visitor
                                (reify SqlVisitor
                                  (visitSetSessionVariableStatement [_ ctx]
                                    {:statement-type :set-session-parameter
                                     :parameter (session-param-name (.identifier ctx))
                                     :value (-> (.literal ctx)
                                                (plan/plan-expr {:default-tz default-tz}))})

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
                                          (mapcat (partial plan/accept-visitor this) (.readWriteTxOption ctx))))

                                  (visitReadOnlyTransaction [this ctx]
                                    (into {:access-mode :read-only}
                                          (mapcat (partial plan/accept-visitor this) (.readOnlyTxOption ctx))))

                                  (visitReadWriteSession [_ _] {:access-mode :read-write})

                                  (visitReadOnlySession [_ _] {:access-mode :read-only})

                                  (visitSystemTimeTxOption [_ ctx]
                                    {:system-time (.accept (.dateTimeLiteral ctx) (date-time-visitor default-tz))})

                                  (visitSnapshotTimeTxOption [_ ctx]
                                    {:snapshot-time (.accept (.dateTimeLiteral ctx) (date-time-visitor default-tz))})

                                  (visitClockTimeTxOption [_ ctx]
                                    {:current-time (.accept (.dateTimeLiteral ctx) (date-time-visitor default-tz))})

                                  (visitCommitStatement [_ _] {:statement-type :commit})
                                  (visitRollbackStatement [_ _] {:statement-type :rollback})

                                  (visitSetRoleStatement [_ _] {:statement-type :set-role})

                                  (visitSetTimeZoneStatement [_ ctx]
                                    ;; not sure if handlling time zone explicitly is the right approach
                                    ;; might be cleaner to handle it like any other session param
                                    {:statement-type :set-time-zone
                                     :tz (let [v (.getText (.characterString ctx))]
                                           (subs v 1 (dec (count v))))})

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

                                  (visitQueryExpr [this ctx]
                                    (let [q {:statement-type :query, :explain? (boolean (.EXPLAIN ctx))
                                             :query (subsql ctx), :parsed-query ctx}]
                                      (->> (some-> (.settingQueryVariables ctx) (.settingQueryVariable))
                                           (transduce (keep (partial plan/accept-visitor this)) conj q))))

                                  ;; could do pre-submit validation here
                                  (visitCreateUserStatement [_ ctx]
                                    {:statement-type :dml, :dml-type :create-role, :query (subsql ctx)})
                                  (visitAlterUserStatement [_ ctx]
                                    {:statement-type :dml, :dml-type :create-role, :query (subsql ctx)})

                                  (visitPrepareStmt [this ctx] (-> (.prepareStatement ctx) (.accept this)))

                                  (visitPrepareStatement [this ctx]
                                    (let [inner-ctx (.directlyExecutableStatement ctx)]
                                      {:statement-type :prepare
                                       :statement-name (str (plan/identifier-sym (.statementName ctx)))
                                       :inner (.accept inner-ctx this)}))

                                  (visitExecuteStmt [_ ctx]
                                    {:statement-type :execute,
                                     :statement-name (str (plan/identifier-sym (.statementName (.executeStatement ctx)))),
                                     :query (subsql ctx)
                                     :parsed-query ctx})

                                  ;; handled in plan
                                  (visitSettingDefaultValidTime [_ _])
                                  (visitSettingDefaultSystemTime [_ _])

                                  (visitSettingClockTime [_ ctx]
                                    [:current-time (-> (.clockTime ctx)
                                                       (plan/plan-expr {:default-tz default-tz})
                                                       (time/->instant))])

                                  (visitSettingSnapshotTime [_ ctx]
                                    [:snapshot-time (-> (plan/plan-expr (.snapshotTime ctx) {:default-tz default-tz})
                                                        (time/->instant {:default-tz default-tz}))])

                                  (visitShowVariableStatement [_ ctx]
                                    {:statement-type :query, :query sql, :parsed-query ctx})

                                  (visitSetWatermarkStatement [_ ctx]
                                    (let [wm-tx-id (plan/plan-expr (.literal ctx))]
                                      (if (number? wm-tx-id)
                                        {:statement-type :set-watermark, :watermark-tx-id wm-tx-id}
                                        (throw (client-err "invalid watermark - expecting number")))))

                                  (visitShowWatermarkStatement [_ _]
                                    {:statement-type :query, :query sql
                                     :ra-plan [:table '[watermark]
                                               (if watermark-tx-id
                                                 [{:watermark watermark-tx-id}]
                                                 [])]})

                                  (visitShowSnapshotTimeStatement [_ ctx]
                                    {:statement-type :query, :query sql, :parsed-query ctx})

                                  (visitShowClockTimeStatement [_ ctx]
                                    {:statement-type :query, :query sql, :parsed-query ctx})

                                  ;; HACK: these values are fixed at prepare-time - if they were to change,
                                  ;; and the same prepared statement re-evaluated, the value would be stale.
                                  (visitShowSessionVariableStatement [_ ctx]
                                    (let [k (session-param-name (.identifier ctx))]
                                      {:statement-type :query, :query sql
                                       :ra-plan [:table (if-let [v (get session-parameters k)]
                                                          [{(keyword k) v}]
                                                          [])]})))))))

          (catch Exception e
            (log/debug e "Error parsing SQL")
            (throw (ex-info "error parsing sql"
                            {::client-error (err-pg-exception e "error parsing sql")}
                            e)))))))

(defn- json-clj
  "This function is temporary, the long term goal will be to walk arrow directly to generate the json (and later monomorphic
  results).

  Returns a clojure representation of the value, which - when printed as json will present itself as the desired json type string."
  [obj]

  ;; we do not extend any jackson/data.json/whatever because we want intentional printing of supported types and
  ;; text representation (e.g consistent scientific notation, dates and times, etc).

  ;; we can reduce the cost of this walk later by working directly on typed arrow vectors rather than dynamic clojure maps!
  ;; we lean on data.json for now for encoding, quote/escape, json compat floats etc

  (cond
    (nil? obj) nil
    (boolean? obj) obj
    (int? obj) obj

    ;; I am allowing string pass through for now but be aware data.json escapes unicode and that may not be
    ;; what we want at some point (e.g pass plain utf8 unless prompted as a param).
    (string? obj) obj

    ;; bigdec cast gets us consistent exponent notation E with a sign for doubles, floats and bigdecs.
    (float? obj) (bigdec obj)

    ;; java.time datetime-ish toString is already iso8601, we may want to tweak this later
    ;; as the string format often omits redundant components (like trailing seconds) which may make parsing harder
    ;; for clients, doesn't matter for now - json probably gonna die anyway
    (instance? LocalTime obj) (str obj)
    (instance? LocalDate obj) (str obj)
    (instance? LocalDateTime obj) (str obj)
    (instance? OffsetDateTime obj) (str obj)
    ;; print offset instead of zoneprefix  otherwise printed representation may change depending on client
    ;; we might later revisit this if json printing remains
    (instance? ZonedDateTime obj) (recur (.toOffsetDateTime ^ZonedDateTime obj))
    (instance? Instant obj) (recur (.atZone ^Instant obj #xt/zone "UTC"))
    (instance? Duration obj) (str obj)
    (instance? Period obj) (str obj)

    (instance? IntervalYearMonth obj) (str obj)
    (instance? IntervalDayTime obj) (str obj)
    (instance? IntervalMonthDayNano obj) (str obj)

    ;; represent period duration as an iso8601 duration string (includes period components)
    (instance? PeriodDuration obj)
    (let [^PeriodDuration obj obj
          period (.getPeriod obj)
          duration (.getDuration obj)]
      (cond
        ;; if either component is zero (likely in sql), we can just print the objects
        ;; not normalizing the period here, should we be?
        (.isZero period) (str duration)
        (.isZero duration) (str period)

        :else
        ;; otherwise the duration needs to be append to the period, with a T on front.
        ;; e.g P1DT3S - unfortunately, durations are printed with the ISO8601 PT header
        ;; Right now doesn't matter - so just string munging.
        (let [pstr (str period)
              dstr (str duration)]
          (str pstr (subs dstr 1)))))

    ;; returned to handle big utf8 bufs, right now we do not handle this well, later we will be writing json with less
    ;; copies and we may encode the quoted json string straight out of the buffer
    (instance? org.apache.arrow.vector.util.Text obj) (str obj)

    (or (instance? List obj) (instance? Set obj))
    (mapv json-clj obj)

    ;; maps, cannot be created from SQL yet, but working playground requires them
    ;; we are not dealing with the possibility of non kw/string keys, xt shouldn't return maps like that right now.
    (instance? Map obj) (update-vals obj json-clj)

    (or (instance? xtdb.RuntimeException obj)
        (instance? xtdb.IllegalArgumentException obj))
    (json-clj (-> (ex-data obj)
                  (assoc :message (ex-message obj))))

    (instance? clojure.lang.Keyword obj) (json-clj (str (symbol obj)))
    (instance? clojure.lang.Symbol obj) (json-clj (str (symbol obj)))
    (instance? UUID obj) (json-clj (str obj))
    (instance? ByteBuffer obj) (json-clj (str "0x" (Hex/encodeHexString (util/byte-buffer->byte-array obj))))
    (instance? URI obj) (json-clj (str obj))

    :else
    (throw (Exception. (format "Unexpected type encountered by pgwire (%s)" (class obj))))))


;;; server impl

(defn- parse-session-params [params]
  (->> params
       (into {} (mapcat (fn [[k v]]
                          (case k
                            "options" (parse-session-params (for [[_ k v] (re-seq #"-c ([\w_]*)=([\w_]*)" v)]
                                                              [k v]))
                            [[k (case k
                                  "fallback_output_format" (#{:json :transit} (util/->kebab-case-kw v))
                                  v)]]))))))

;;; pg i/o shared data types
;; our io maps just capture a paired :read fn (data-in)
;; and :write fn (data-out, val)
;; the goal is to describe the data layout of various pg messages, so they can be read and written from in/out streams
;; by generalizing a bit here, we gain a terser language for describing wire data types, and leverage
;; (e.g later we might decide to compile unboxed reader/writers using these)

(def ^:private no-read (fn [_in] (throw (UnsupportedOperationException.))))
(def ^:private no-write (fn [_out _] (throw (UnsupportedOperationException.))))

(def ^:private io-char8
  "A single byte character"
  {:read #(char (.readUnsignedByte ^DataInputStream %))
   :write #(.writeByte ^DataOutputStream %1 (byte %2))})

(def ^:private io-uint16
  "An unsigned short integer"
  {:read #(.readUnsignedShort ^DataInputStream %)
   :write #(.writeShort ^DataOutputStream %1 (short %2))})

(def ^:private io-uint32
  "An unsigned 32bit integer"
  {:read #(.readInt ^DataInputStream %)
   :write #(.writeInt ^DataOutputStream %1 (int %2))})

(def ^:private io-string
  "A postgres null-terminated utf8 string"
  {:read read-c-string
   :write write-c-string})

(defn- io-list
  "Returns an io data type for a dynamic list, takes an io def for the length (e.g io-uint16) and each element.

  e.g a list of strings (io-list io-uint32 io-string)"
  [io-len, io-el]
  (let [len-rdr (:read io-len)
        len-wtr (:write io-len)
        el-rdr (:read io-el)
        el-wtr (:write io-el)]
    {:read (fn [in]
             (vec (repeatedly (len-rdr in) #(el-rdr in))))
     :write (fn [out coll] (len-wtr out (count coll)) (run! #(el-wtr out %) coll))}))

(def ^:private io-format-code
  "Postgres format codes are integers, whose value is either

  0 (:text)
  1 (:binary)

  On read returns a keyword :text or :binary, write of these keywords will be transformed into the correct integer."
  {:read (fn [^DataInputStream in] ({0 :text, 1 :binary} (.readUnsignedShort in)))
   :write (fn [^DataOutputStream out v] (.writeShort out ({:text 0, :binary 1} v)))})

(def ^:private io-format-codes
  "A list of format codes of max len len(uint16). This is the format used by the msg-bind."
  (io-list io-uint16 io-format-code))

(defn- io-record
  "Returns an io data type for a record containing named fields. Useful for definining typed message contents.

  e.g

  (io-record
    :foo io-uint32
    :bar io-string)

  would define a record of two fields (:foo and :bar) to be read/written in the order defined. "
  [& fields]
  (let [pairs (partition 2 fields)
        ks (mapv first pairs)
        ios (mapv second pairs)
        rdrs (mapv :read ios)
        wtrs (mapv :write ios)]
    {:read (fn read-record [in]
             (loop [acc {}
                    i 0]
               (if (< i (count ks))
                 (let [k (nth ks i)
                       rdr (rdrs i)]
                   (recur (assoc acc k (rdr in))
                          (unchecked-inc-int i)))
                 acc)))
     :write (fn write-record [out m]
              (dotimes [i (count ks)]
                (let [k (nth ks i)
                      wtr (wtrs i)]
                  (wtr out (k m)))))}))

(defn- io-null-terminated-list
  "An io data type for a null terminated list of elements given by io-el."
  [io-el]
  (let [el-rdr (:read io-el)
        el-wtr (:write io-el)]
    {:read (fn read-null-terminated-list [^DataInputStream in]
             (loop [els []]
               (if-let [el (el-rdr in)]
                 (recur (conj els el))
                 (cond->> els
                   (instance? MapEntry (first els)) (into {})))))

     :write (fn write-null-terminated-list [^DataOutputStream out coll]
              (run! (partial el-wtr out) coll)
              (.writeByte out 0))}))

(defn- io-bytes-or-null
  "An io data type for a byte array (or null), in postgres you can write the bytes of say a column as either
  len (uint32) followed by the bytes OR in the case of null, a len whose value is -1. This is how row values are conveyed
  to the client."
  [io-len]
  (let [len-rdr (:read io-len)]
    {:read (fn read-bytes-or-null [^DataInputStream in]
             (let [len (len-rdr in)]
               (when (not= -1 len)
                 (let [arr (byte-array len)]
                   (.readFully in arr)
                   arr))))
     :write (fn write-bytes-or-null [^DataOutputStream out ^bytes arr]
              (if arr
                (do (.writeInt out (alength arr))
                    (.write out arr))
                (.writeInt out -1)))}))

(def ^:private error-or-notice-type->char
  {:localized-severity \S
   :severity \V
   :sql-state \C
   :message \M
   :detail \D
   :position \P
   :where \W})

(def ^:private char->error-or-notice-type (set/map-invert error-or-notice-type->char))

(def ^:private io-error-notice-field
  "An io-data type that writes a (vector/map-entry pair) k and v as an error field."
  {:read (fn read-error-or-notice-field [^DataInputStream in]
           (let [field-key (char->error-or-notice-type (char (.readByte in)))]
             ;; TODO this might fail if we don't implement some message type
             (when field-key
               (MapEntry/create field-key (read-c-string in)))))
   :write (fn write-error-or-notice-field [^DataOutputStream out [k v]]
            (let [field-char8 (error-or-notice-type->char k)]
              (when field-char8
                (.writeByte out (byte field-char8))
                (write-c-string out (str v)))))})

(def ^:private io-portal-or-stmt
  "An io data type that returns a keyword :portal or :prepared-statement given the next char8 in the buffer.
  This is useful for describe/close who name either a portal or statement."
  {:read (comp {\P :portal, \S :prepared-stmt} (:read io-char8)),
   :write no-write})

(def io-cancel-request
  (io-record :process-id io-uint32
             :secret-key io-uint32))

;;; msg definition

(def ^:private ^:redef client-msgs {})
(def ^:redef server-msgs {})

(defmacro ^:private def-msg
  "Defs a typed-message with the given kind (:client or :server) and fields.

  Installs the message var in the either client-msgs or server-msgs map for later retrieval by code."
  [sym kind char8 & fields]
  `(let [fields# [~@fields]
         kind# ~kind
         char8# ~char8
         {read# :read
          write# :write}
         (apply io-record fields#)]

     (def ~sym
       {:name ~(keyword sym)
        :kind kind#
        :char8 char8#
        :fields fields#
        :read read#
        :write write#})

     (if (= kind# :client)
       (alter-var-root #'client-msgs assoc char8# (var ~sym))
       (alter-var-root #'server-msgs assoc char8# (var ~sym)))

     ~sym))

;; client messages

(def-msg msg-bind :client \B
  :portal-name io-string
  :stmt-name io-string
  :arg-format io-format-codes
  :args (io-list io-uint16 (io-bytes-or-null io-uint32))
  :result-format io-format-codes)

(def-msg msg-close :client \C
  :close-type io-portal-or-stmt
  :close-name io-string)

(def-msg msg-copy-data :client \d)
(def-msg msg-copy-done :client \c)
(def-msg msg-copy-fail :client \f)

(def-msg msg-describe :client \D
  :describe-type io-portal-or-stmt
  :describe-name io-string)

(def-msg msg-execute :client \E
  :portal-name io-string
  :limit io-uint32)

(def-msg msg-flush :client \H)

(def-msg msg-parse :client \P
  :stmt-name io-string
  :query io-string
  :param-oids (io-list io-uint16 io-uint32))

(def-msg msg-password :client \p
  :password io-string)

(def-msg msg-simple-query :client \Q
  :query io-string)

(def-msg msg-sync :client \S)

(def-msg msg-terminate :client \X)

;;; server messages

(def ^:private flush-messages #{:msg-error-response :msg-notice-response
                                :msg-parameter-status :msg-auth :msg-ready
                                :msg-portal-suspended})


(def-msg msg-error-response :server \E
  :error-fields (io-null-terminated-list io-error-notice-field))

(def-msg msg-notice-response :server \N
  :notice-fields (io-null-terminated-list io-error-notice-field))

(def-msg msg-bind-complete :server \2)

(def-msg msg-close-complete :server \3)

(def-msg msg-command-complete :server \C
  :command io-string)

(def-msg msg-parameter-description :server \t
  :parameter-oids (io-list io-uint16 io-uint32))

(def-msg msg-parameter-status :server \S
  :parameter io-string
  :value io-string)

(def-msg msg-data-row :server \D
  :vals (io-list io-uint16 (io-bytes-or-null io-uint32)))

(def-msg msg-portal-suspended :server \s)

(def-msg msg-parse-complete :server \1)

(def-msg msg-no-data :server \n)

(def-msg msg-empty-query :server \I)


(def-msg msg-auth :server \R
  :result io-uint32)

(def-msg msg-backend-key-data :server \K
  :process-id io-uint32
  :secret-key io-uint32)

(def-msg msg-copy-in-response :server \G)

(def-msg msg-ready :server \Z
  :status {:read (fn [^DataInputStream in]
                   (case (char (.read in))
                     \I :idle
                     \T :transaction
                     \E :failed-transaction
                     :unknown))
           :write (fn [^DataOutputStream out status]
                    (.writeByte out (byte ({:idle \I
                                            :transaction \T
                                            :failed-transaction \E}
                                           status))))})

(def-msg msg-row-description :server \T
  :columns (->> (io-record
                 :column-name io-string
                 :table-oid io-uint32
                 :column-attribute-number io-uint16
                 :column-oid io-uint32
                 :typlen  io-uint16
                 :type-modifier io-uint32
                 :result-format io-format-code)
                (io-list io-uint16)))

;;; server commands
;; the commands represent actions the connection may take in response to some message
;; they are simple functions that can call each other directly, though they can also be enqueued
;; through the connections :cmd-buf queue (in :conn-state) this will later be useful
;; for shared concerns (e.g 'what is a connection doing') and to allow for termination mid query
;; in certain scenarios

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
    (cmd-write-msg conn msg-parameter-status {:parameter param,
                                              :value (case param
                                                       "standard_conforming_strings" "on"
                                                       (str value))})))

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
   (cmd-write-msg conn msg-ready {:status status})))

(defn cmd-send-error
  "Sends an error back to the client (e.g (cmd-send-error conn (err-protocol \"oops!\")).

  If the connection is operating in the :extended protocol mode, any error causes the connection to skip
  messages until a msg-sync is received."
  [{:keys [conn-state ^Counter query-error-counter] :as conn} err]

  ;; error seen while in :extended mode, start skipping messages until sync received
  (when (= :extended (:protocol @conn-state))
    (swap! conn-state assoc :skip-until-sync true))

  ;; mark a transaction (if open as failed), for now we will consider all errors to do this
  (swap! conn-state util/maybe-update :transaction assoc :failed true, :err err)

  (when query-error-counter
    (.increment ^Counter query-error-counter))

  (cmd-write-msg conn msg-error-response {:error-fields err}))

(defn- send-ex [conn, ^Throwable e]
  (if-let [client-err (::client-error (ex-data e))]
    (do
      (log/trace "Client error:" (ex-message e) (ex-data e))
      (cmd-send-error conn client-err))

    (log/error e "Uncaught exception processing message")))

(defn cmd-send-notice
  "Sends an notice message back to the client (e.g (cmd-send-notice conn (warning \"You are doing this wrong!\"))."
  [conn notice]
  (cmd-write-msg conn msg-notice-response {:notice-fields notice}))

(defn cmd-write-canned-response [conn {:keys [q rows] :as _canned-resp}]
  (let [rows (rows conn)]
    (doseq [row rows]
      (cmd-write-msg conn msg-data-row {:vals (mapv (fn [v] (if (bytes? v) v (types/utf8 v))) row)}))

    (cmd-write-msg conn msg-command-complete {:command (str (statement-head q) " " (count rows))})))

(defn- close-portal
  [{:keys [conn-state, cid]} portal-name]
  (log/trace "Closing portal" {:cid cid, :portal portal-name})
  (when-some [portal (get-in @conn-state [:portals portal-name])]

    (util/close (:bound-query portal))
    (swap! conn-state update-in [:prepared-statements (:stmt-name portal) :portals] disj portal-name)
    (swap! conn-state update :portals dissoc portal-name)))

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

  (cmd-write-msg conn msg-close-complete))

(defmethod handle-msg* :msg-terminate [{:keys [!closing?]} _]
  (reset! !closing? true))

(defn set-time-zone [{:keys [conn-state] :as conn} tz]
  (swap! conn-state update-in [:session :clock] (fn [^Clock clock]
                                                  (.withZone clock (ZoneId/of tz))))
  (set-session-parameter conn time-zone-nf-param-name tz))

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
        (doto (cmd-write-msg msg-backend-key-data {:process-id (:cid conn), :secret-key 0}))
        (doto (cmd-send-ready)))))

(defn cmd-startup-pg30 [{:keys [frontend server] :as conn} startup-opts]
  (let [{:keys [->node, ^Authenticator authn]} server
        user (get startup-opts "user")
        db-name (get startup-opts "database")
        {:keys [node] :as conn} (assoc conn :node (->node db-name))]
    (letfn [(killed-conn [err]
              (doto conn
                (cmd-send-error err)
                (handle-msg* {:msg-name :msg-terminate})))]

      (if node
        (condp = (.methodFor authn user (host-address frontend))
          #xt.authn/method :trust
          (do
            (cmd-write-msg conn msg-auth {:result 0})
            (startup-ok conn startup-opts))

          #xt.authn/method :password
          (do
            ;; asking for a password, we only have :trust and :password for now
            (cmd-write-msg conn msg-auth {:result 3})

            ;; we go idle until we receive a message
            (when-let [{:keys [msg-name] :as msg} (read-client-msg! frontend)]
              (if (not= :msg-password msg-name)
                (killed-conn (err-invalid-auth-spec (str "password authentication failed for user: " user)))

                (if (.verifyPassword authn node user (:password msg))
                  (do
                    (cmd-write-msg conn msg-auth {:result 0})
                    (startup-ok conn startup-opts))

                  (killed-conn (err-invalid-passwd (str "password authentication failed for user: " user)))))))

          (killed-conn (err-invalid-auth-spec (str "no authentication record found for user: " user))))

        (killed-conn (err-invalid-catalog db-name))))))

(defn cmd-cancel
  "Tells the connection to stop doing what its doing and return to idle"
  [conn]
  ;; we might this want to be conditional on a 'working state' to avoid races (if you fire loads of cancels randomly), not sure whether
  ;; to use status instead
  ;;TODO need to interrupt the thread belonging to the conn
  (swap! (:conn-state conn) assoc :cancel true)
  nil)

(defn cmd-startup-cancel [conn msg-in]
  (let [{:keys [process-id]} ((:read io-cancel-request) msg-in)
        {:keys [server]} conn
        {:keys [server-state]} server
        {:keys [connections]} @server-state

        cancel-target (get connections process-id)]

    (when cancel-target (cmd-cancel cancel-target))

    (handle-msg* conn {:msg-name :msg-terminate})))

(defn cmd-startup-err [conn err]
  (cmd-send-error conn err)
  (handle-msg* conn {:msg-name :msg-terminate}))

(defn- read-startup-opts [^DataInputStream in]
  (loop [in (PushbackInputStream. in)
         acc {}]
    (let [x (.read in)]
      (cond
        (neg? x) (throw (EOFException. "EOF in read-startup-opts"))
        (zero? x) acc
        :else (do (.unread in (byte x))
                  (recur in (assoc acc (read-c-string in) (read-c-string in))))))))

(defn cmd-startup [conn]
  (loop [{{:keys [in]} :frontend, :keys [server], :as conn} conn]
    (let [{:keys [version msg-in]} (read-version in)]
      (case version
        :gssenc (doto conn
                  (cmd-startup-err (err-protocol-violation "GSSAPI is not supported")))

        :ssl (let [{:keys [^SSLContext ssl-ctx]} server]
               (recur (update conn :frontend upgrade-to-ssl ssl-ctx)))

        :cancel (doto conn
                  (cmd-startup-cancel msg-in))

        :30 (-> conn
                (cmd-startup-pg30 (read-startup-opts msg-in)))

        (doto conn
          (cmd-startup-err (err-protocol-violation "Unknown protocol version")))))))

(def json-bytes (comp types/utf8 json/json-str json-clj))

(defn write-json [_env ^IVectorReader rdr idx]
  (json-bytes (.getObject rdr idx)))

(def supported-oids
  (set (map :oid (vals types/pg-types))))

(defn- execute-tx [{:keys [node read-only?]} dml-ops tx-opts]
  (try
    (xt/execute-tx node dml-ops tx-opts)
    (catch xtdb.IllegalArgumentException e
      (throw (client-err (ex-message e))))
    (catch Throwable e
      (log/debug e "Error on execute-tx")
      (let [msg "unexpected error on tx submit (report as a bug)"]
        (throw (ex-info msg {::client-error (err-pg-exception e msg)} e))))))

(defn- ->xtify-arg [session {:keys [arg-format param-fields]}]
  (fn xtify-arg [arg-idx arg]
    (when (some? arg)
      (let [param-oid (:oid (nth param-fields arg-idx))
            arg-format (or (nth arg-format arg-idx nil)
                           (nth arg-format arg-idx :text))
            {:keys [read-binary, read-text]} (or (get types/pg-types-by-oid param-oid)
                                                 (throw (Exception. "Unsupported param type provided for read")))]
        (if (= :binary arg-format)
          (read-binary session arg)
          (read-text session arg))))))

(defn- xtify-args [{:keys [conn-state] :as _conn} args {:keys [arg-format] :as stmt}]
  (try
    (vec (map-indexed (->xtify-arg (:session @conn-state) stmt) args))
    (catch Exception e
      (throw (ex-info "invalid arg representation"
                      {::client-error (if (= arg-format :binary)
                                        (invalid-binary-representation (ex-message e))
                                        (invalid-text-representation (ex-message e)))}
                      e)))))

(defn skip-until-sync? [{:keys [conn-state] :as _conn}]
  (:skip-until-sync @conn-state))

(defn- cmd-exec-dml [{:keys [conn-state] :as conn} {:keys [dml-type query args param-fields]}]
  (if (or (not= (count param-fields) (count args))
          (some #(= 0 (:oid %)) param-fields))
    (do (log/error "Missing types for params in DML statement")
        (cmd-send-error
         conn
         (err-protocol-violation "Missing types for args - client must specify types for all params in DML statements")))

    (let [{:keys [session transaction]} @conn-state
          ^Clock clock (:clock session)
          cmd-complete-msg {:command (case dml-type
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
                                       :create-role "CREATE ROLE")}]

      (cond
        (skip-until-sync? conn) nil

        transaction
        ;; we buffer the statement in the transaction (to be flushed with COMMIT)
        (do
          (swap! conn-state update-in [:transaction :dml-buf]
                 (fnil (fn [dml-ops]
                         (or (when-let [[_sql last-query :as last-op] (peek dml-ops)]
                               (when (= last-query query)
                                 (conj (pop dml-ops)
                                       (conj last-op args))))
                             (conj dml-ops [:sql query args])))
                       []))
          (cmd-write-msg conn msg-command-complete cmd-complete-msg))

        :else
        (let [{:keys [tx-id error]} (execute-tx conn [[:sql query args]]
                                                {:default-tz (.getZone clock)
                                                 :authn {:user (-> session :parameters (get "user"))}})]
          (when-not (skip-until-sync? conn)
            (if error
              (cmd-send-error conn (err-protocol-violation (ex-message error)))
              (cmd-write-msg conn msg-command-complete cmd-complete-msg))
            (swap! conn-state assoc :watermark-tx-id tx-id)))))))

(defn cmd-exec-query [{:keys [conn-state !closing?] :as conn} {:keys [limit query bound-query fields] :as _portal}]
  (try
    (with-open [result-cursor (.openCursor ^BoundQuery bound-query)]
      (let [cancelled-by-client? #(:cancel @conn-state)
            ;; please die as soon as possible (not the same as draining, which leaves conns :running for a time)

            n-rows-out (volatile! 0)

            session (:session @conn-state)]

        (while (and (or (nil? limit) (< @n-rows-out limit))
                    (.tryAdvance result-cursor
                                 (fn [^RelationReader rel]
                                   (cond
                                     (cancelled-by-client?)
                                     (do (log/trace "query cancelled by client")
                                         (swap! conn-state dissoc :cancel)
                                         (cmd-send-error conn (err-query-cancelled "query cancelled during execution")))

                                     (Thread/interrupted) (throw (InterruptedException.))

                                     @!closing? (log/trace "query result stream stopping (conn closing)")

                                     :else (dotimes [idx (cond-> (.rowCount rel)
                                                           limit (min (- limit @n-rows-out)))]
                                             (let [row (mapv
                                                        (fn [{:keys [field-name write-binary write-text result-format]}]
                                                          (let [rdr (.readerForName rel field-name)]
                                                            (when-not (.isNull rdr idx)
                                                              (if (= :binary result-format)
                                                                (write-binary session rdr idx)
                                                                (if write-text
                                                                  (write-text session rdr idx)
                                                                  (write-json session rdr idx))))))
                                                        fields)]
                                               (cmd-write-msg conn msg-data-row {:vals row})
                                               (vswap! n-rows-out inc))))))))

        (cmd-write-msg conn msg-command-complete {:command (str (statement-head query) " " @n-rows-out)})))

    (catch InterruptedException e (throw e))
    (catch Throwable e
      (log/error e)
      (cmd-send-error conn (err-pg-exception e "unexpected server error during query execution")))))

(defn- cmd-send-row-description [conn cols]
  (let [defaults {:table-oid 0
                  :column-attribute-number 0
                  :typlen -1
                  :type-modifier -1
                  :result-format :text}
        apply-defaults (fn [col]
                         (-> (merge defaults col)
                             (select-keys [:column-name :table-oid :column-attribute-number
                                           :column-oid :typlen :type-modifier :result-format])))
        data {:columns (mapv apply-defaults cols)}]

    (log/trace "sending row description - " (assoc data :input-cols cols))
    (cmd-write-msg conn msg-row-description data)))

(defn cmd-describe-canned-response [conn canned-response]
  (let [{:keys [cols]} canned-response]
    (cmd-send-row-description conn cols)))

(defn cmd-describe-portal [conn {:keys [fields]}]
  (if fields
    (cmd-send-row-description conn fields)
    (cmd-write-msg conn msg-no-data)))

(defn cmd-send-parameter-description [conn {:keys [param-fields]}]
  (log/trace "sending parameter description - " {:param-fields param-fields})
  (cmd-write-msg conn msg-parameter-description {:parameter-oids (mapv :oid param-fields)}))

(defn cmd-begin [{:keys [node conn-state]} tx-opts]
  (swap! conn-state
         (fn [{:keys [session watermark-tx-id] :as st}]
           (let [{:keys [^Clock clock]} session]
             (-> st
                 (update :transaction
                         (fn [{:keys [access-mode]}]
                           (if access-mode
                             (throw (client-err "transaction already started"))

                             (-> {:current-time (.instant clock)
                                  :snapshot-time (:system-time (:latest-completed-tx (xt/status node)))
                                  :after-tx-id (or watermark-tx-id -1)
                                  :implicit? false}
                                 (into (:characteristics session))
                                 (into tx-opts))))))))))

(defn cmd-commit [{:keys [conn-state] :as conn}]
  (let [{:keys [transaction session]} @conn-state
        {:keys [failed dml-buf system-time access-mode]} transaction
        {:keys [^Clock clock, parameters]} session]

    (if failed
      (throw (client-err "transaction failed"))

      (try
        (let [{:keys [tx-id error]} (when (= :read-write access-mode)
                                      (execute-tx conn dml-buf {:default-tz (.getZone clock)
                                                                :system-time system-time
                                                                :authn {:user (get parameters "user")}}))]
          (swap! conn-state (fn [conn-state]
                              (-> conn-state
                                  (dissoc :transaction)
                                  (cond-> tx-id (assoc :watermark-tx-id tx-id)))))

          (when error
            (throw (client-err (ex-message error)))))
        (catch InterruptedException e (throw e))
        (catch Exception e
          (swap! conn-state #(dissoc % :transaction))
          (throw e))))))

(defn cmd-rollback [{:keys [conn-state]}]
  (swap! conn-state dissoc :transaction))

;;; Sends description messages (e.g msg-row-description) to the client for a prepared statement or portal.
(defmethod handle-msg* :msg-describe [{:keys [conn-state] :as conn} {:keys [describe-type, describe-name]}]
  (let [coll-k (case describe-type
                 :portal :portals
                 :prepared-stmt :prepared-statements
                 (Object.))
        {:keys [statement-type canned-response] :as describe-target} (get-in @conn-state [coll-k describe-name])]

    (letfn [(describe* [{:keys [fields] :as describe-target}]
              (when (= :prepared-stmt describe-type)
                (cmd-send-parameter-description conn describe-target))

              (if fields
                (cmd-send-row-description conn fields)
                (cmd-write-msg conn msg-no-data)))]

      (case statement-type
        :canned-response (cmd-describe-canned-response conn canned-response)
        (:query :dml) (describe* describe-target)

        :execute (let [inner (get-in @conn-state [:prepared-statements (:statement-name describe-target)])]
                   (describe* {:param-fields (:param-fields describe-target)
                               :fields (:fields inner)}))

        (cmd-write-msg conn msg-no-data)))))

(defn cmd-set-session-parameter [conn parameter value]
  (set-session-parameter conn parameter value)
  (cmd-write-msg conn msg-command-complete {:command "SET"}))

(defn cmd-set-transaction [conn _tx-opts]
  ;; no-op - can only set transaction isolation, and that
  ;; doesn't mean anything to us because we're always serializable
  (cmd-write-msg conn msg-command-complete {:command "SET TRANSACTION"}))

(defn cmd-set-time-zone [conn {:keys [tz]}]
  (set-time-zone conn tz)
  (cmd-write-msg conn msg-command-complete {:command "SET TIME ZONE"}))

(defn cmd-set-watermark [{:keys [conn-state] :as conn} {:keys [watermark-tx-id]}]
  (swap! conn-state assoc :watermark-tx-id watermark-tx-id)
  (cmd-write-msg conn msg-command-complete {:command "SET WATERMARK"}))

(defn cmd-set-session-characteristics [{:keys [conn-state] :as conn} session-characteristics]
  (swap! conn-state update-in [:session :characteristics] (fnil into {}) session-characteristics)
  (cmd-write-msg conn msg-command-complete {:command "SET SESSION CHARACTERISTICS"}))

(defn- permissibility-err
  "Returns an error if the given statement, which is otherwise valid - is not permitted (say due to the access mode, transaction state)."
  [{:keys [conn-state server]} {:keys [statement-type]}]
  (let [{:keys [access-mode]} (:transaction @conn-state)]
    (cond
      (and (= :dml statement-type) (:read-only? server))
      (err-protocol-violation "DML is not allowed on the READ ONLY server")

      (and (= :dml statement-type) (= :read-only access-mode))
      (err-protocol-violation "DML is not allowed in a READ ONLY transaction")

      (and (= :query statement-type) (= :read-write access-mode))
      (err-protocol-violation "Queries are unsupported in a DML transaction"))))

(defmethod handle-msg* :msg-sync [{:keys [conn-state] :as conn} _]
  ;; Sync commands are sent by the client to commit transactions (we do not do anything here yet),
  ;; and to clear the error state of a :extended mode series of commands (e.g the parse/bind/execute dance)

  ;; TODO commit / rollback should be used here if not in an explicit tx?

  (when-not (:transaction @conn-state)
    ;;if outside an explicit transaction/transaction block (BEGIN/COMMIT) close any portals
    ;;as these are implicitly closed/cleaned up at the end of the transcation
    (doseq [portal-name (keys (:portals @conn-state))]
      (close-portal conn portal-name)))

  (cmd-send-ready conn)
  (swap! conn-state dissoc :skip-until-sync, :protocol))

(defmethod handle-msg* :msg-flush [conn _]
  (flush! (:frontend conn)))

(defn resolve-defaulted-params [declared-params inferred-params]
  (let [declared-params (vec declared-params)]
    (->> inferred-params
         (map-indexed (fn [idx inf-param]
                        (if-let [dec-param (nth declared-params idx nil)]
                          (if (= :default (:col-type dec-param))
                            inf-param
                            dec-param)
                          inf-param))))))

(defn parse
  "Responds to a msg-parse message that creates a prepared-statement."
  [{:keys [conn-state]} {:keys [query]}]

  (let [{:keys [session watermark-tx-id]} @conn-state
        {:keys [^Clock clock], session-parameters :parameters} session]

    (interpret-sql query {:default-tz (.getZone clock)
                          :watermark-tx-id watermark-tx-id
                          :session-parameters session-parameters})))

(defn- prep-stmt [{:keys [node, conn-state] :as conn} {:keys [statement-type] :as stmt} {:keys [param-oids]}]
  (let [{:keys [session watermark-tx-id]} @conn-state
        {:keys [^Clock clock]} session
        fallback-output-format (get-in session [:parameters "fallback_output_format"])
        param-types (map types/pg-types-by-oid param-oids)]

    (if (or (contains? #{:query :execute} statement-type)
            (and (= :prepare statement-type)
                 (= :query (:inner-statement-type stmt))))
      (let [param-col-types (mapv :col-type param-types)]
        (when (some nil? param-col-types)
          (throw (ex-info "unsupported param-types in query"
                          {::client-error (err-protocol-violation (str "Unsupported param-types in query: "
                                                                       (pr-str (->> param-types
                                                                                    (into [] (comp (filter (comp nil? :col-type))
                                                                                                   (map :typname)
                                                                                                   (distinct)))))))})))

        (try
          (let [{:keys [ra-plan, ^Sql$DirectlyExecutableStatementContext parsed-query explain?]} stmt
                query-opts {:after-tx-id (or watermark-tx-id -1)
                            :tx-timeout (Duration/ofSeconds 1)
                            :param-types param-col-types
                            :default-tz (.getZone clock)
                            :explain? explain?}

                ^PreparedQuery pq (if ra-plan
                                    (xtp/prepare-ra node ra-plan query-opts)
                                    (xtp/prepare-sql node parsed-query query-opts))]

            (when-let [warnings (.warnings pq)]
              (doseq [warning warnings]
                (cmd-send-notice conn (notice-warning (plan/error-string warning)))))

            (assoc stmt
                   :prepared-query pq
                   :fields (mapv (partial types/field->pg-type fallback-output-format) (.columnFields pq))
                   :param-fields (->> (.paramFields pq)
                                      (map types/field->pg-type)
                                      (map #(set/rename-keys % {:column-oid :oid}))
                                      (resolve-defaulted-params param-types))))
          (catch xtdb.IllegalArgumentException e
            (log/debug e "Error preparing statement")
            (throw (client-err (str "Error preparing statement: " (ex-message e)))))
          (catch xtdb.RuntimeException e
            (log/debug e "Error preparing statement")
            (throw (client-err (str "Error preparing statement: " (ex-message e)))))
          (catch Throwable e
            (log/error e "Error preparing statement")
            (throw (client-err (str "Error preparing statement: " (ex-message e)))))))

      ;; NOTE this means that for DML statments we assume the number and type of args is exactly
      ;; those specified by the client in param-types, irrelevant of the number featured in the query string.
      ;; If a client subsequently binds a different number of args we will send an error msg
      (assoc stmt :param-fields param-types))))

(defmethod handle-msg* :msg-parse [{:keys [conn-state] :as conn} {:keys [stmt-name param-oids] :as msg-data}]
  (swap! conn-state assoc :protocol :extended)

  (when-let [unsupported-param-oids (not-empty (into #{} (remove supported-oids) param-oids))]
    (throw (client-err (format "parameter type oids (%s) currently unsupported by xt" unsupported-param-oids))))

  (let [[stmt & more-stmts] (parse conn msg-data)]
    (assert (nil? more-stmts) (format "TODO: found %d statements in parse" (inc (count more-stmts))))

    (let [prepared-stmt (prep-stmt conn stmt {:param-oids param-oids})]
      (swap! conn-state (fn [conn-state]
                          (-> conn-state
                              (assoc-in [:prepared-statements stmt-name] prepared-stmt)))))

    (cmd-write-msg conn msg-parse-complete)))

(defn cmd-prepare [{:keys [conn-state] :as conn} {:keys [statement-name inner] :as _portal}]
  (let [{:keys [query]} inner
        [prepared-stmt & more-stmts] (parse conn {:query query})]
    (when (seq more-stmts)
      (throw (UnsupportedOperationException. "Multiple statements in a single PREPARE are not supported")))

    (let [prepared-stmt (prep-stmt conn prepared-stmt {})]
      (swap! conn-state assoc-in [:prepared-statements statement-name]
             (assoc prepared-stmt
                    :statement-name statement-name))

      (cmd-write-msg conn msg-command-complete {:command "PREPARE"}))))

(defn with-result-formats [pg-types result-format]
  (when-let [result-formats (let [type-count (count pg-types)]
                              (cond
                                (empty? result-format) (repeat type-count :text)
                                (= 1 (count result-format)) (repeat type-count (first result-format))
                                (= (count result-format) type-count) result-format))]
    (mapv (fn [pg-type result-format]
            (assoc pg-type :result-format result-format))
          pg-types
          result-formats)))

(defn bind-stmt [{:keys [conn-state allocator] :as conn} {:keys [statement-type prepared-query args result-format] :as stmt}]
  (let [{:keys [session transaction]} @conn-state
        {:keys [^Clock clock], {:strs [fallback_output_format]} :parameters} session

        query-opts {:snapshot-time (or (:snapshot-time stmt) (:snapshot-time transaction))
                    :current-time (or (:current-time stmt)
                                      (:current-time transaction)
                                      (.instant clock))
                    :default-tz (.getZone clock)}

        xt-args (xtify-args conn args stmt)]

    (letfn [(->bq []
              (util/with-close-on-catch [args-rel (vw/open-args allocator xt-args)]
                (.bind ^PreparedQuery prepared-query (assoc query-opts :args args-rel))))

            (->fields [^BoundQuery bq]
              (or (-> (map (partial types/field->pg-type fallback_output_format) (.columnFields bq))
                      (with-result-formats result-format))
                  (throw (client-err "invalid result format"))))]

      (case statement-type
        :query (let [bq (->bq)]
                 (-> stmt
                     (assoc :bound-query bq,
                            :fields (->fields bq))))

        :dml (-> stmt
                 (assoc :args xt-args))

        :execute (let [^BoundQuery bq (->bq)]
                   ;; in the case of execute, we've just bound the args query rather than the inner query.
                   ;; so now we bind the inner query and pretend this was the one we were running all along
                   (try
                     (let [{^PreparedQuery inner-pq :prepared-query, :as inner} (get-in @conn-state [:prepared-statements (:statement-name stmt)])
                           !args (object-array 1)]
                       (with-open [args-cursor (.openCursor bq)]
                         (.forEachRemaining args-cursor
                                            (fn [^RelationReader args-rel]
                                              (aset !args 0 (.copy args-rel allocator))))
                         (let [^RelationReader args-rel (aget !args 0)]
                           (case (:statement-type inner)
                             :query (let [inner-bq (.bind inner-pq (assoc query-opts :args args-rel))]
                                      (-> inner
                                          (assoc :bound-query inner-bq
                                                 :fields (->fields inner-bq))))
                             :dml (let [arg-fields (.columnFields bq)]
                                    (try
                                      (-> inner
                                          (assoc :args (vec (for [^Field field arg-fields]
                                                              (-> (.readerForName args-rel (.getName field))
                                                                  (.getObject 0))))
                                                 :param-fields arg-fields))
                                      (finally
                                        (util/close args-rel))))))))
                     (finally
                       (util/close bq))))

        stmt))))

(defmethod handle-msg* :msg-bind [{:keys [conn-state] :as conn} {:keys [portal-name stmt-name] :as bind-msg}]
  (let [stmt (into (or (get-in @conn-state [:prepared-statements stmt-name])
                       (throw (client-err "no prepared statement")))
                   bind-msg)
        portal (bind-stmt conn stmt)]
    (swap! conn-state assoc-in [:portals portal-name] portal)
    (swap! conn-state update-in [:prepared-statements stmt-name :portals] (fnil conj #{}) portal-name)
    (cmd-write-msg conn msg-bind-complete)))

(defn execute-portal [{:keys [conn-state] :as conn} {:keys [statement-type canned-response parameter value session-characteristics tx-characteristics] :as portal}]
  (when-let [err (permissibility-err conn portal)]
    (throw (ex-info "parsing error"
                    {::client-error err})))

  (swap! conn-state (fn [{:keys [transaction] :as cs}]
                      (cond-> cs
                        transaction (update-in [:transaction :access-mode]
                                               (fnil identity
                                                     (case statement-type
                                                       :query :read-only
                                                       :dml :read-write
                                                       nil))))))

  (case statement-type
    :empty-query (cmd-write-msg conn msg-empty-query)
    :canned-response (cmd-write-canned-response conn canned-response)
    :set-session-parameter (cmd-set-session-parameter conn parameter value)
    :set-session-characteristics (cmd-set-session-characteristics conn session-characteristics)
    :set-role nil
    :set-transaction (cmd-set-transaction conn tx-characteristics)
    :set-time-zone (cmd-set-time-zone conn portal)
    :set-watermark (cmd-set-watermark conn portal)
    :ignore (cmd-write-msg conn msg-command-complete {:command "IGNORED"})

    :begin (do
             (cmd-begin conn tx-characteristics)
             (cmd-write-msg conn msg-command-complete {:command "BEGIN"}))

    :rollback (do
                (cmd-rollback conn)
                (cmd-write-msg conn msg-command-complete {:command "ROLLBACK"}))

    :commit (do
              (cmd-commit conn)
              (cmd-write-msg conn msg-command-complete {:command "COMMIT"}))

    :query (cmd-exec-query conn portal)
    :prepare (cmd-prepare conn portal)
    :dml (cmd-exec-dml conn portal)

    (throw (UnsupportedOperationException. (pr-str {:portal portal})))))

(defmethod handle-msg* :msg-execute [{:keys [conn-state] :as conn} {:keys [portal-name limit]}]
  ;; Handles a msg-execute to run a previously bound portal (via msg-bind).
  (let [portal (or (get-in @conn-state [:portals portal-name])
                   (throw (ex-info "no such portal"
                                   {::client-error (err-protocol-violation "no such portal")})))]
    (execute-portal conn (cond-> portal
                           (not (zero? limit)) (assoc :limit limit)))))

(defmethod handle-msg* :msg-simple-query [{:keys [conn-state] :as conn} {:keys [query]}]
  (swap! conn-state assoc :protocol :simple)

  (try
    (when-not (boolean (:transaction @conn-state))
      (cmd-begin conn {:implicit? true}))

    (doseq [stmt (parse conn {:query query})]
      (when-not (boolean (:transaction @conn-state))
        (cmd-begin conn {:implicit? true}))

      (try
        (let [{:keys [param-fields statement-type] :as prepared-stmt} (prep-stmt conn stmt {})]
          (when (seq param-fields)
            (throw (client-err "Parameters not allowed in simple queries")))

          (let [portal (bind-stmt conn prepared-stmt)]
            (try
              (when (or (contains? #{:query :canned-response} statement-type)
                        (and (= :execute statement-type)
                             (= :query (get-in @conn-state [:prepared-statements (:statement-name prepared-stmt) :statement-type]))))
                ;; Client only expects to see a RowDescription (result of cmd-descibe)
                ;; for certain statement types
                (cmd-describe-portal conn portal))

              (execute-portal conn portal)
              (finally
                (util/close (:bound-query portal))))))

        (catch InterruptedException e (throw e))
        (catch Exception e
          (when (get-in @conn-state [:transaction :implicit?])
            (cmd-rollback conn))
          (send-ex conn e))))

    (let [{:keys [implicit? failed]} (:transaction @conn-state)]
      (when implicit?
        (if failed
          (cmd-rollback conn)
          (cmd-commit conn))))

    ;; here we catch explicitly because we need to send the error, then a ready message
    (catch InterruptedException e (throw e))
    (catch Throwable e (send-ex conn e)))

  (cmd-send-ready conn))

;; ignore password messages, we are authenticated when getting here
(defmethod handle-msg* :msg-password [_conn _msg])

(defmethod handle-msg* ::default [_conn _]
  (throw (client-err "unknown client message")))


(defn handle-msg [{:keys [cid] :as conn} {:keys [msg-name] :as msg}]
  (try
    (log/trace "Read client msg" {:cid cid, :msg msg})

    (if (and (skip-until-sync? conn) (not= :msg-sync msg-name))
      (log/trace "Skipping msg until next sync due to error in extended protocol" {:cid cid, :msg msg})
      (handle-msg* conn msg))

    (catch InterruptedException e (throw e))

    (catch Throwable e
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
            #_(cmd-send-error conn (err-admin-shutdown "draining connections"))
            (reset! !conn-closing? true))

        ;; well, it won't have been us, as we would drain first
        (.isClosed socket)
        (do (log/trace "Connection closed unexpectedly" {:port port, :cid cid})
            (reset! !conn-closing? true))

        ;; go idle until we receive another msg from the client
        :else (do
                (when-let [msg (read-client-msg! frontend)]
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
  [{:keys [server-state, port, allocator, query-error-counter, ^Counter total-connections-counter] :as server} ^Socket conn-socket]
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
                                                                     :frontend (->socket-frontend conn-socket),
                                                                     :!closing? !closing?
                                                                     :allocator (util/->child-allocator allocator (str "pg-conn-" cid))
                                                                     :conn-state !conn-state})
                                                   (cmd-startup))
                                               (catch EOFException _
                                                 (reset! !closing? true))
                                               (catch Throwable t
                                                 (log/warn t "error on conn startup")
                                                 (throw t)))))
        conn (assoc conn :query-error-counter query-error-counter)]

    (println "connect")
    (try
      ;; the connection loop only gets initialized if we are not closing
      (when (not @!closing?)
        (when total-connections-counter
          (println "when total-connections-counter")
          (prn 'total-connections-counter total-connections-counter)
          (.increment total-connections-counter)
          (prn 'total-connections-counter-count (.count total-connections-counter)))
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
  (^Server [node {:keys [allocator port num-threads drain-wait ssl-ctx authn metrics-registry read-only?]
                  :or {port 0
                       num-threads 42
                       drain-wait 5000}}]
   (util/with-close-on-catch [accept-socket (ServerSocket. port)]
     (let [port (.getLocalPort accept-socket)
           query-error-counter (when metrics-registry (metrics/add-counter metrics-registry "query.error"))
           _ (prn 'metrics-registry metrics-registry)
           total-connections-counter (when metrics-registry (metrics/add-counter metrics-registry "pgwire.total_connections"))
           !tmp-nodes (when-not node
                        (ConcurrentHashMap.))
           server (map->Server {:allocator allocator
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
                                :authn authn

                                :!tmp-nodes !tmp-nodes
                                :->node (fn [db-name]
                                          (cond
                                            (nil? node) (->tmp-node !tmp-nodes db-name)
                                            (= db-name "xtdb") node))})

           server (assoc server
                         :query-error-counter query-error-counter
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
                              (println 'pgwire.active-connections (count (:connections @(:server-state server))))
                              (prn 'connections (:connections @(:server-state server)))
                              (count (:connections @(:server-state server))))))
       (.start accept-thread)
       server))))

(defmethod xtn/apply-config! ::server [^Xtdb$Config config, _ {:keys [port read-only-port num-threads ssl] :as server}]
  (if server
    (cond-> (.getServer config)
      (some? port) (.port port)
      (some? read-only-port) (.readOnlyPort read-only-port)
      (some? num-threads) (.numThreads num-threads)
      (some? ssl) (.ssl (util/->path (:keystore ssl)) (:keystore-password ssl)))

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
  {:port (.getPort config)
   :ro-port (.getReadOnlyPort config)
   :num-threads (.getNumThreads config)
   :ssl-ctx (when-let [ssl (.getSsl config)]
              (->ssl-ctx (.getKeyStore ssl) (.getKeyStorePassword ssl)))})

(defmethod ig/prep-key ::server [_ config]
  (into {:node (ig/ref :xtdb/node)
         :authn (ig/ref :xtdb/authn)
         :allocator (ig/ref :xtdb/allocator)
         :metrics-registry (ig/ref :xtdb.metrics/registry)}
        (<-config config)))

(defmethod ig/init-key ::server [_ {:keys [node allocator authn port ro-port] :as opts}]
  (let [opts (dissoc opts :port :ro-port)]
    (letfn [(start-server [port read-only?]
              (when-not (neg? port)
                (let [{:keys [port] :as srv} (serve node (-> opts
                                                             (assoc :port port
                                                                    :read-only? read-only?
                                                                    :authn authn
                                                                    :allocator (util/->child-allocator allocator "pgwire"))))]
                  (log/infof "Server%sstarted on port: %d"
                             (if read-only? " (read-only) " " ")
                             port)
                  srv)))]
      {:read-write (start-server port false)
       :read-only (start-server ro-port true)})))

(defmethod ig/halt-key! ::server [_ srv]
  (util/close srv))

(defn open-playground
  (^xtdb.pgwire.Server [] (open-playground nil))

  (^xtdb.pgwire.Server [opts]
   (let [{:keys [port] :as srv} (serve nil (merge {:authn authn/default-authn}
                                                  opts
                                                  {:allocator (RootAllocator.)}))]
     (log/info "Playground started on port:" port)
     srv)))

(ns xtdb.pgwire
  (:require [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.node.impl]
            [xtdb.query]
            [xtdb.sql.plan :as plan]
            [xtdb.time :as time]
            [xtdb.util :as util])
  (:import [clojure.lang PersistentQueue]
           [java.io ByteArrayInputStream ByteArrayOutputStream Closeable DataInputStream DataOutputStream EOFException IOException InputStream OutputStream PushbackInputStream]
           [java.lang Thread$State]
           [java.net ServerSocket Socket SocketException]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]
           [java.time Clock Duration LocalDate LocalDateTime OffsetDateTime Period ZoneId ZonedDateTime]
           [java.util List Map]
           [java.util.concurrent ExecutorService Executors TimeUnit]
           [java.util.function Consumer]
           (org.antlr.v4.runtime ParserRuleContext)
           [org.apache.arrow.vector PeriodDuration]
           (xtdb.antlr SqlVisitor)
           [xtdb.api PgwireServer$Factory Xtdb$Config]
           xtdb.api.module.XtdbModule
           xtdb.IResultCursor
           xtdb.node.impl.IXtdbInternal
           (xtdb.query BoundQuery PreparedQuery)
           [xtdb.types IntervalDayTime IntervalMonthDayNano IntervalYearMonth]
           [xtdb.vector RelationReader]))

;; references
;; https://www.postgresql.org/docs/current/protocol-flow.html
;; https://www.postgresql.org/docs/current/protocol-message-formats.html

(defn- cleanup-connection-resources [conn]
  (let [{:keys [cid, server, in, out, ^Socket socket, conn-status]} conn

        {:keys [port server-state]} server]

    (reset! conn-status :cleaning-up)

    (when (realized? out)
      (try
        (.close ^OutputStream @out)
        (catch Throwable e
          (log/error e "Exception caught closing socket out" {:port port, :cid cid}))))

    (when (realized? in)
      (try
        (.close ^InputStream @in)
        (catch Throwable e
          (log/error e "Exception caught closing socket in" {:port port, :cid cid}))))

    (when-not (.isClosed socket)
      (try
        (.close socket)
        (catch Throwable e
          (log/error e "Exception caught closing conn socket"))))


    (when (compare-and-set! conn-status :cleaning-up :cleaned-up)
      (swap! server-state update :connections
             (fn [conns]
               (if (identical? conn (get conns cid))
                 (dissoc conns cid)
                 conns))))))

(defn stop-connection [conn]
  (let [{:keys [conn-status]} conn]
    (reset! conn-status :closing)
    ;; TODO wait for close?
    (cleanup-connection-resources conn)))

(defn- cleanup-server-resources [server]
  (let [{:keys [port
                accept-socket
                ^Thread accept-thread
                ^ExecutorService thread-pool
                server-state
                server-status]} server]

    ;; TODO revisit these transitions
    (or (compare-and-set! server-status :draining :cleaning-up)
        (compare-and-set! server-status :running :cleaning-up)
        (compare-and-set! server-status :error :cleaning-up)
        (compare-and-set! server-status :error-on-start :cleaning-up))

    (when (realized? accept-socket)
      (let [^ServerSocket socket @accept-socket]
        (when-not (.isClosed socket)
          (log/trace "Closing accept socket" port)
          (.close socket))))

    (when-not (#{Thread$State/NEW, Thread$State/TERMINATED} (.getState accept-thread))
      (log/trace "Closing accept thread")
      (.interrupt accept-thread)
      (.join accept-thread (* 5 1000))
      (when-not (= Thread$State/TERMINATED (.getState accept-thread))
        (log/error "Could not shut down accept-thread gracefully" {:port port, :thread (.getName accept-thread)})
        (compare-and-set! server-status :cleaning-up :error-on-cleanup)
        (swap! server-state assoc :accept-thread-misbehaving true)))

    ;; force stopping conns
    (doseq [conn (vals (:connections @server-state))]
      (stop-connection conn))

    (when-not (.isShutdown thread-pool)
      (log/trace "Closing thread pool")
      (.shutdownNow thread-pool)
      (when-not (.awaitTermination thread-pool 5 TimeUnit/SECONDS)
        (log/error "Could not shutdown thread pool gracefully" {:port port})
        (compare-and-set! server-status :cleaning-up :error-on-cleanup)
        (swap! server-state assoc :thread-pool-misbehaving true)))

    (compare-and-set! server-status :cleaning-up :cleaned-up)))


;;; Server
;; Represents a single postgres server on a particular port
;; the server currently is a blocking IO server (no nio / netty)
;; the server does not own the lifecycle of its associated node

;;; Server lifecycle
;; new -> starting        -> running -> draining -> cleaning-up -> cleaned-up
;;     -> error-on-start  -> error                              -> error-on-cleanup
;;
;; lifecycle changes are mediated through the :server-status field, which holds an atom that
;; can be used to control state transitions from any thread.
;; The :draining status is used to stop connections when they have finished their current query

(defrecord Server [port

                   ;; (delay) server socket and thread for accepting connections
                   accept-socket
                   ^Thread accept-thread

                   ;; a thread pool for handing off connections (currently using blocking io)
                   ^ExecutorService thread-pool

                   ;; atom to mediate lifecycle transitions (see Server lifecycle comment)
                   server-status

                   ;; atom to hold server state, such as current connections, and the connection :cid counter.
                   server-state]
  Closeable
  (close [this]
    (let [drain-wait (:drain-wait @server-state 5000)
          drain (not (contains? #{0, nil} drain-wait))]

      (when (and drain (compare-and-set! server-status :running :draining))
        (log/trace "Server draining connections")
        (let [wait-until (+ (System/currentTimeMillis) drain-wait)]
          (loop []
            (cond
              ;; connections all closed, proceed
              (empty? (:connections @server-state)) nil

              (Thread/interrupted)
              (do (log/warn "Interrupted during drain, force closing")
                  (swap! server-state assoc :force-close true))

              ;; timeout
              (< wait-until (System/currentTimeMillis))
              (do (log/warn "Could not drain connections in time, force closing")
                  (swap! server-state assoc :force-close true))

              :else (do (Thread/sleep 10) (recur))))))

      (cleanup-server-resources this)

      (when-not (= :cleaned-up @server-status)
        (log/fatal "Server resources could not be freed, please restart xtdb" port)))))

;;; Connection
;; Represents a single client connection to the server
;; identified by an integer 'cid'
;; each connection holds some state under :conn-state (such as prepared statements, session params and such)
;; and is registered under the :connections map under the servers :server-state.

;;; Connection lifecycle
;; new -> running        -> closing -> cleaning-up -> cleaned-up
;;     -> error-on-start
;;
;; Like the server, we use an atom :conn-status to mediate life-cycle state changes to control for concurrency
;; unlike the server, a connections lifetime is entirely bounded by a blocking (connect) call - so shutdowns should be more predictable.

(defrecord Connection [^Server server
                       node close-node?
                       socket

                       ;; a positive integer that identifies the connection on this server
                       ;; we will use this as the pg Process ID for messages that require it (such as cancellation)
                       cid
                       ;; atom to mediate lifecycle transitions (see Connection lifecycle comment)
                       conn-status
                       ;; atom to hold a map of session / connection state, such as :prepared-statements, :session, :transaction.
                       conn-state
                       ;; io
                       ^DataInputStream in
                       ^DataOutputStream out]

  Closeable
  (close [this]
    (stop-connection this)

    (when close-node?
      (util/close node))))

;; best the server/conn records are opaque when printed as they contain mutual references
(defmethod print-method Server [wtr o] ((get-method print-method Object) wtr o))
(defmethod print-method Connection [wtr o] ((get-method print-method Object) wtr o))

(defn- read-c-string
  "Postgres strings are null terminated, reads a null terminated utf-8 string from in."
  ^String [^InputStream in]
  (loop [baos (ByteArrayOutputStream.)
         x (.read in)]
    (if (zero? x)
      (String. (.toByteArray baos) StandardCharsets/UTF_8)
      (recur (doto baos
               (.write x))
             (.read in)))))

(defn- write-c-string
  "Postgres strings are null terminated, writes a null terminated utf8 string to out."
  [^OutputStream out ^String s]
  (.write out (.getBytes s StandardCharsets/UTF_8))
  (.write out 0))

(defn- utf8
  "Returns the utf8 byte-array for the given string"
  ^bytes [s]
  (.getBytes (str s) StandardCharsets/UTF_8))

(def oids
  "A map of postgres type (that we may support to some extent) to numeric oid.
  Mapping to oid is useful for result descriptions, as well as to determine the semantics of received parameters."
  {:undefined 0

   :int2 21
   :int2-array 1005

   :int4 23
   :int4-array 1007

   :int8 20
   :int8-array 1016

   :text 25
   :text-array 1009

   :numeric 1700
   :numeric-array 1231

   :float4 700
   :float4-array 1021

   :float8 701
   :float8-array 1022

   :bool 16
   :bool-array 1000

   :date 1082
   :date-array 1182

   :time 1083
   :time-array 1183

   :timetz 1266
   :timetz-array 1270

   :timestamp 1114
   :timestamp-array 1115

   :timestamptz 1184
   :timestamptz-array 1185

   :bytea 17
   :bytea-array 1001

   :varchar 1043
   :varchar-array 1015

   :bit 1560
   :bit-array 1561

   :interval 1186
   :interval-array 1187

   :char 18
   :char-array 1002

   :jsonb 3802
   :jsonb-array 3807
   :json 114
   :json-array 199})

(def oid->kw (set/map-invert oids))

(def ^:private oid-varchar (oids :varchar))
(def ^:private oid-json (oids :json))

(defn- read-utf8 [^bytes barr] (String. barr StandardCharsets/UTF_8))
(defn- read-ascii [^bytes barr] (String. barr StandardCharsets/US_ASCII))

(def type-mappings
  "Defines how we map from one type to another (pg, java/arrow) where possible."
  [{:pg :undefined
    :pg-read-binary (fn [x] (some-> x read-utf8))
    :pg-read-text (fn [x] (some-> x read-utf8))}

   {:pg :bool
    :pg-read-binary (fn [barr] (= 1 (nth barr 0 0)))
    :pg-read-text (fn [barr] (Boolean/parseBoolean (read-utf8 barr)))}

   ;; ints
   ;; not sure if bit should be mapped to bool?
   {:pg :bit
    :pg-read-binary (fn [barr] (-> barr ByteBuffer/wrap .get))
    :pg-read-text (fn [barr] (-> barr read-ascii Byte/parseByte))}
   {:pg :int2
    :pg-read-binary (fn [barr] (-> barr ByteBuffer/wrap .getShort))
    :pg-read-text (fn [barr] (-> barr read-ascii Short/parseShort))}
   {:pg :int4
    :pg-read-binary (fn [barr] (-> barr ByteBuffer/wrap .getInt))
    :pg-read-text (fn [barr] (-> barr read-ascii Integer/parseInt))}
   {:pg :int8
    :pg-read-binary (fn [barr] (-> barr ByteBuffer/wrap .getLong))
    :pg-read-text (fn [barr] (-> barr read-ascii Long/parseLong))}

   ;; floats
   {:pg :float4
    :pg-read-binary (fn [barr] (-> barr ByteBuffer/wrap .getFloat))
    :pg-read-text (fn [barr] (-> barr read-ascii Float/parseFloat))}
   {:pg :float8
    :pg-read-binary (fn [barr] (-> barr ByteBuffer/wrap .getDouble))
    :pg-read-text (fn [barr] (-> barr read-ascii Double/parseDouble))}

   ;; strings
   {:pg :varchar
    :pg-read-binary read-utf8
    :pg-read-text read-utf8}
   {:pg :text
    :pg-read-binary read-utf8
    :pg-read-text read-utf8}])

;; all postgres client IO arrives as either an untyped (startup) or typed message
(defn- read-untyped-msg [^DataInputStream in]
  (let [size (- (.readInt in) 4)
        barr (byte-array size)
        _ (.readFully in barr)]
    {:msg-in (DataInputStream. (ByteArrayInputStream. barr))
     :msg-len size}))

(defn- read-typed-msg [^DataInputStream in]
  (let [type-byte (.readUnsignedByte in)]
    (assoc (read-untyped-msg in)
      :msg-char8 (char type-byte))))

;;; errors

(defn- err-protocol-violation [msg]
  {:severity "ERROR"
   :localized-severity "ERROR"
   :sql-state "08P01"
   :message msg})

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

(defn- err-admin-shutdown [msg]
  {:severity "ERROR"
   :localized-severity "ERROR"
   :sql-state "57P01"
   :message msg})

(defn- err-cannot-connect-now [msg]
  {:severity "ERROR"
   :localized-severity "ERROR"
   :sql-state "57P03"
   :message msg})

(defn- err-query-cancelled [msg]
  {:severity "ERROR"
   :localized-severity "ERROR"
   :sql-state "57014"
   :message msg})

(defn err-pg-exception
  "Returns a pg specific error for an XTDB exception"
  [^Throwable ex generic-msg]
  (if (or (instance? xtdb.IllegalArgumentException ex)
          (instance? xtdb.RuntimeException ex))
    (err-protocol-violation (.getMessage ex))
    (err-internal generic-msg)))

;;; sql processing
;; because we are talking to postgres clients, we cannot simply fling sql at xt (shame!)
;; so these functions provide some utility on top of xt to figure out where we can 'fake it til we make it' as a pg server.

(def ^:private canned-responses
  "Some pre-baked responses to common queries issued as setup by Postgres drivers, e.g SQLAlchemy"
  [{:q ";"
    :cols []
    :rows []}
   {:q "select pg_catalog.version()"
    :cols [{:column-name "version" :column-oid oid-varchar}]
    :rows [["PostgreSQL 14.2"]]}
   {:q "show standard_conforming_strings"
    :cols [{:column-name "standard_conforming_strings" :column-oid oid-varchar}]
    :rows [["on"]]}

   {:q "show transaction isolation level"
    :cols [{:column-name "transaction_isolation" :column-oid oid-varchar}]
    :rows [["read committed"]]}

   ;; ODBC issues this query by default, you may be able to disable with an option
   {:q "select oid, typbasetype from pg_type where typname = 'lo'"
    :cols [{:column-name "oid", :column-oid oid-varchar} {:column-name "typebasetype", :column-oid oid-varchar}]
    :rows []}

   ;; jdbc meta getKeywords (hibernate)
   ;; I think this should work, but it causes some kind of low level issue, likely
   ;; because our query protocol impl is broken, or partially implemented.
   ;; java.lang.IllegalStateException: Received resultset tuples, but no field structure for them
   {:q "select string_agg(word, ',') from pg_catalog.pg_get_keywords()"
    :cols [{:column-name "col1" :column-oid oid-varchar}]
    :rows [["xtdb"]]}])

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

(defn- interpret-sql [sql]
  (let [sql-trimmed (trim-sql sql)]
    (or (when (str/blank? sql-trimmed)
          {:statement-type :empty-query})

        (when-some [canned-response (get-canned-response sql-trimmed)]
          {:statement-type :canned-response, :canned-response canned-response})

        (try
          (.accept (plan/parse-statement sql-trimmed)
                   (reify SqlVisitor
                     (visitDirectSqlStatement [this ctx] (.accept (.directlyExecutableStatement ctx) this))
                     (visitDirectlyExecutableStatement [this ctx] (-> (.getChild ctx 0) (.accept this)))

                     (visitSetSessionVariableStatement [_ ctx]
                       {:statement-type :set-session-parameter
                        :parameter (plan/identifier-sym (.identifier ctx))
                        :value (-> (.literal ctx)
                                   (.accept (plan/->ExprPlanVisitor nil nil)))})

                     (visitSetSessionCharacteristicsStatement [this ctx]
                       (let [[^ParserRuleContext sc & more-scs] (.sessionCharacteristic ctx)]
                         (assert (nil? more-scs) "pgwire only supports one for now")

                         (into {:statement-type :set-session-characteristics}
                               (.accept sc this))))

                     (visitSessionCharacteristic [this ctx]
                       (let [[^ParserRuleContext txc & more-txcs] (.transactionMode ctx)]
                         (assert (nil? more-txcs) "pgwire only supports one for now")
                         (.accept txc this)))

                     (visitSetTransactionStatement [this ctx]
                       (into {:statement-type :set-transaction}
                             (.accept (.transactionCharacteristics ctx) this)))

                     (visitStartTransactionStatement [this ctx]
                       (into {:statement-type :begin}
                             (some-> (.transactionCharacteristics ctx) (.accept this))))

                     (visitTransactionCharacteristics [this ctx]
                       (let [[^ParserRuleContext txc & more-txcs] (.transactionMode ctx)]
                         (assert (nil? more-txcs) "pgwire only supports one for now")
                         (.accept txc this)))

                     (visitIsolationLevel [_ _] {})

                     (visitReadWriteTransaction [_ _] {:access-mode :read-write})
                     (visitReadOnlyTransaction [_ _] {:access-mode :read-only})

                     (visitCommitStatement [_ _] {:statement-type :commit})
                     (visitRollbackStatement [_ _] {:statement-type :rollback})

                     (visitSetValidTimeDefaults [this ctx]
                       {:statement-type :set-session-parameter, :parameter :app-time-defaults
                        :value (.accept (.validTimeDefaults ctx) this)})

                     (visitValidTimeDefaultAsOfNow [_ _] :as-of-now)
                     (visitValidTimeDefaultIsoStandard [_ _] :iso-standard)

                     (visitSetTimeZoneStatement [_ ctx]
                       {:statement-type :set-time-zone
                        :tz (let [region (.getText (.characterString ctx))]
                              (ZoneId/of (subs region 1 (dec (count region)))))})

                     (visitInsertStatement [_ _]
                       (plan/plan-statement sql) ; plan to raise up any SQL errors pre tx-log
                       {:statement-type :dml, :dml-type :insert
                        :query sql, :transformed-query sql-trimmed})

                     (visitUpdateStatementSearched [_ _]
                       (plan/plan-statement sql) ; plan to raise up any SQL errors pre tx-log
                       {:statement-type :dml, :dml-type :update
                        :query sql, :transformed-query sql-trimmed})

                     (visitDeleteStatementSearched [_ _]
                       (plan/plan-statement sql) ; plan to raise up any SQL errors pre tx-log
                       {:statement-type :dml, :dml-type :delete
                        :query sql, :transformed-query sql-trimmed})

                     (visitEraseStatementSearched [_ _]
                       (plan/plan-statement sql) ; plan to raise up any SQL errors pre tx-log
                       {:statement-type :dml, :dml-type :erase
                        :query sql, :transformed-query sql-trimmed})

                     (visitQueryExpression [_ _]
                       {:statement-type :query, :query sql, :transformed-query sql-trimmed})))

          (catch Exception e
            {:err (err-pg-exception e "error parsing sql")})))))

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
    (instance? LocalDate obj) (str obj)
    (instance? LocalDateTime obj) (str obj)
    (instance? OffsetDateTime obj) (str obj)
    ;; print offset instead of zoneprefix  otherwise printed representation may change depending on client
    ;; we might later revisit this if json printing remains
    (instance? ZonedDateTime obj) (recur (.toOffsetDateTime ^ZonedDateTime obj))
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

    ;; java list, e.g arrow JsonStringArrayList, Vectors etc, walk its members (its a json array).
    (instance? List obj) (mapv json-clj obj)

    ;; maps, cannot be created from SQL yet, but working playground requires them
    ;; we are not dealing with the possibility of non kw/string keys, xt shouldn't return maps like that right now.
    (instance? Map obj) (update-vals obj json-clj)

    :else
    (throw (Exception. (format "Unexpected type encountered by pgwire (%s)" (class obj))))))

(defn- read-startup-parameters [^DataInputStream in]
  (loop [in (PushbackInputStream. in)
         acc {}]
    (let [x (.read in)]
      (if (zero? x)
        {:startup-parameters acc}
        (do (.unread in (byte x))
            (recur in (assoc acc (read-c-string in) (read-c-string in))))))))

;; startup negotiation utilities (see cmd-startup)

(def ^:private version-messages
  "Integer codes sent by the client to identify a startup msg"
  {
   ;; this is the normal 'hey I'm a postgres client' if ssl is not requested
   196608 :30
   ;; cancellation messages come in as special startup sequences (pgwire does not handle them yet!)
   80877102 :cancel
   ;; ssl messages are used when the client either requires, prefers, or allows ssl connections.
   ;; xt does not yet support ssl
   80877103 :ssl
   ;; gssapi encoding is not supported by xt, and we tell the client that
   80877104 :gssenc})

(def ^:private ssl-responses
  {:unsupported (byte \N)
   ;; for docs, xtdb does not yet support ssl
   :supported (byte \S)})

(defn- read-version [^DataInputStream in]
  (let [{:keys [^DataInputStream msg-in] :as msg} (read-untyped-msg in)
        version (.readInt msg-in)]
    (assoc msg :version (version-messages version))))

(defn- buggy-client-not-closed-after-ssl-unsupported-msg?
  "There appears to be a bug in the JDBC driver which causes it to leave its conn hanging
  if ssl is required and the server does not support it.

  For now, we timeout after 1 second if we do not receive a follow up to the ssl msg."
  [^DataInputStream in]
  (let [wait-until (+ (System/currentTimeMillis) 1000)]
    (loop []
      (cond
        ;; at least 4 bytes available
        (<= 4 (.available in))
        false

        ;; time left
        (< (System/currentTimeMillis) wait-until)
        ;; one nice thing about sleep is if we are interrupted
        ;; (as we've been asked to close)
        ;; we get an exception here
        (if (try
              (Thread/sleep 1)
              true
              (catch InterruptedException _ false))
          (recur)
          true)

        ;; timed out
        :else true))))

;;; server impl

(defn- start-server [server {:keys [accept-so-timeout]
                             :or {accept-so-timeout 5000}}]
  (let [{:keys [server-status, server-state, accept-thread, accept-socket, port]} server
        {:keys [injected-start-exc, silent-start]} @server-state
        start-exc (atom nil)]

    ;; sanity check its a new server
    (when-not (compare-and-set! server-status :new :starting)
      (throw (Exception. (format "Server must be :new to start, port %d" port))))

    (try

      ;; try and bind the socket (this throws if we cannot get the port)
      @accept-socket

      ;; set a socket timeout so we can interrupt the accept-thread
      ;; can be set to nil as an option if you want to allow no timeout
      (when accept-so-timeout
        (.setSoTimeout ^ServerSocket @accept-socket accept-so-timeout))

      ;; start the accept thread, which will listen for connections
      (let [is-running (promise)]

        ;; wait for the :running state
        (add-watch server-status [:accept-watch port]
                   (fn [_ _ _ ns]
                     (when (= :running ns)
                       (deliver is-running true))))

        (.start ^Thread accept-thread)

        ;; wait for a small amount of time for a :running state
        (when (= :timeout (deref is-running (* 5 1000) :timeout))
          (log/error "Server accept thread did not start properly" port)
          (reset! server-status :error-on-start))

        (remove-watch server-status [:accept-watch port]))

      ;; if we have injected an exception, throw it now (e.g for tests)
      (some-> injected-start-exc throw)

      (catch Throwable e
        (when-not silent-start
          (log/error e "Exception caught on server start" port))
        (reset! server-status :error-on-start)
        (reset! start-exc e)))

    ;; run clean up if we didn't start for any reason
    (when (not= :running @server-status)
      (cleanup-server-resources server))

    ;; check if we could not clean up due to a clean up error
    (when (compare-and-set! server-status :error-on-cleanup :error-on-start)
      (log/fatal "Server resources could not be freed, please restart xtdb" port))

    ;; we will have only cleaned up if there was some problem
    (compare-and-set! server-status :cleaned-up :error-on-start)

    (when-not silent-start
      (when-some [exc @start-exc]
        (throw exc))))

  nil)

(defn- set-session-parameter [conn parameter value]
  (let [parameter (-> (util/->kebab-case-kw parameter)
                      ((some-fn {:application-time-defaults :app-time-defaults} identity)))]
    (swap! (:conn-state conn) assoc-in [:session :parameters parameter] value)))

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
  (let [el-wtr (:write io-el)]
    {:read no-read
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

(def ^:private io-error-field
  "An io-data type that writes a (vector/map-entry pair) k and v as an error field."
  {:read no-read
   :write (fn write-error-field [^DataOutputStream out [k v]]
            (let [field-char8 ({:localized-severity \S
                                :severity \V
                                :sql-state \C
                                :message \M
                                :detail \D
                                :position \P
                                :where \W} k)]
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
(def ^:private ^:redef server-msgs {})

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
       {:name ~(name sym)
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
  :param-format io-format-codes
  :params (io-list io-uint16 (io-bytes-or-null io-uint32))
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
  :arg-types (io-list io-uint16 io-uint32))

(def-msg msg-password :client \p)

(def-msg msg-simple-query :client \Q
  :query io-string)

(def-msg msg-sync :client \S)

(def-msg msg-terminate :client \X)

;;; server messages

(def-msg msg-error-response :server \E
  :error-fields (io-null-terminated-list io-error-field))

(def-msg msg-bind-complete :server \2
  :result {:read no-read
           :write (fn [^DataOutputStream out _] (.writeInt out 4))})

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

(def-msg msg-notice-response :server \N)

(def-msg msg-auth :server \R
  :result io-uint32)

(def-msg msg-backend-key-data :server \K
  :process-id io-uint32
  :secret-key io-uint32)

(def-msg msg-copy-in-response :server \G)

(def-msg msg-ready :server \Z
  :status {:read no-read
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
                  :data-type-size io-uint16
                  :type-modifier io-uint32
                  :format-code io-format-code)
                (io-list io-uint16)))

;;; server commands
;; the commands represent actions the connection may take in response to some message
;; they are simple functions that can call each other directly, though they can also be enqueued
;; through the connections :cmd-buf queue (in :conn-state) this will later be useful
;; for shared concerns (e.g 'what is a connection doing') and to allow for termination mid query
;; in certain scenarios

(defn- cmd-write-msg
  "Writes out a single message given a definition (msg-def) and optional data record."
  ([conn msg-def]
   (log/trace "Writing server message" (select-keys msg-def [:char8 :name]))
   (let [^DataOutputStream out @(:out conn)]
     (.writeByte out (byte (:char8 msg-def)))
     (.writeInt out 4)))
  ([conn msg-def data]
   (log/trace "Writing server message (with body)" (select-keys msg-def [:char8 :name]))
   (let [^DataOutputStream out @(:out conn)
         bytes-out (ByteArrayOutputStream.)
         msg-out (DataOutputStream. bytes-out)
         _ ((:write msg-def) msg-out data)
         arr (.toByteArray bytes-out)]
     (.writeByte out (byte (:char8 msg-def)))
     (.writeInt out (+ 4 (alength arr)))
     (.write out arr))))

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
  [{:keys [conn-state] :as conn} err]

  ;; error seen while in :extended mode, start skipping messages until sync received
  (when (= :extended (:protocol @conn-state))
    (swap! conn-state assoc :skip-until-sync true))

  ;; mark a transaction (if open as failed), for now we will consider all errors to do this
  (swap! conn-state util/maybe-update :transaction assoc :failed true, :err err)

  (cmd-write-msg conn msg-error-response {:error-fields err}))

(defn cmd-write-canned-response [conn {:keys [q rows] :as _canned-resp}]
  (doseq [row rows]
    (cmd-write-msg conn msg-data-row {:vals (mapv (fn [v] (if (bytes? v) v (utf8 v))) row)}))

  (cmd-write-msg conn msg-command-complete {:command (str (statement-head q) " " (count rows))}))

(defn- close-portal
  [{:keys [conn-state, cid]} portal-name]
  (log/trace "Closing portal" {:cid cid, :portal portal-name})
  (when-some [portal (get-in @conn-state [:portals portal-name])]

    ;; close the portal/boundQuery (specifically any resources opened by binding params)
    (util/close (:bound-query portal))

    ;; remove portal from stmt
    (swap! conn-state update-in [:prepared-statements (:stmt-name portal) :portals] disj portal-name)

    ;; remove from root
    (swap! conn-state update :portals dissoc portal-name)))

(defn cmd-close
  "Closes a prepared statement or portal that was opened with bind / parse."
  [{:keys [conn-state, cid] :as conn} {:keys [close-type, close-name]}]

  (case close-type
    :prepared-stmt
    (do
      (log/trace "Closing prepared statement" {:cid cid, :stmt close-name})
      (when-some [stmt (get-in @conn-state [:prepared-statements close-name])]

        ;; close associated portals
        (doseq [portal-name (:portals stmt)]
          (close-portal conn portal-name))

        ;; remove from root
        (swap! conn-state update :prepared-statements dissoc close-name)))

    :portal
    (close-portal conn close-name)

    nil)

  (cmd-write-msg conn msg-close-complete))

(defn cmd-terminate
  "Causes the connection to start closing."
  [{:keys [conn-status]}]
  (compare-and-set! conn-status :running :closing))

(defn cmd-startup-pg30 [conn msg-in]
  (let [{:keys [server]} conn
        {:keys [server-state]} server]
    ;; send server parameters
    (cmd-write-msg conn msg-auth {:result 0})

    (doseq [[k v] (merge
                    ;; TimeZone derived from the servers :clock for now
                    ;; this may change
                    {"TimeZone" (str (.getZone ^Clock (:clock @server-state)))}
                    (:parameters @server-state))]
      (cmd-write-msg conn msg-parameter-status {:parameter k, :value v}))

    ;; set initial session parameters specified by client
    (let [{:keys [startup-parameters]} (read-startup-parameters msg-in)]
      (doseq [[k v] startup-parameters]
        (set-session-parameter conn k v)))

    ;; backend key data (used to identify conn for cancellation)
    (cmd-write-msg conn msg-backend-key-data {:process-id (:cid conn), :secret-key 0})
    (cmd-send-ready conn)))

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

    (cmd-terminate conn)))

(defn cmd-startup-err [conn err]
  (cmd-send-error conn err)
  (cmd-terminate conn))

(defn cmd-startup
  "A command that negotiates startup with the client, including ssl negotiation, and sending the state of the servers
  :parameters."
  [conn]
  (let [in @(:in conn)
        ^DataOutputStream out @(:out conn)

        {:keys [version] :as msg} (read-version in)

        err
        (case version
          :gssenc (err-protocol-violation "GSSAPI is not supported")
          nil)

        ;; tell the client ssl is unsupported if requested
        {:keys [err, version, msg-in]}
        (cond
          err (assoc msg :err err)

          (= :ssl version)
          (do (.writeByte out (ssl-responses :unsupported))
              ;; buggy jdbc driver fix
              (if (buggy-client-not-closed-after-ssl-unsupported-msg? in)
                (do
                  (cmd-terminate conn)
                  (assoc msg :err (err-protocol-violation "Took too long respond after SSL request")))
                ;; re-read version
                (read-version in)))

          :else msg)

        gsenc-err (when (= :gssenc version) (err-protocol-violation "GSSAPI is not supported"))
        err (or err gsenc-err)]

    (if err
      (cmd-startup-err conn err)
      (case version
        :cancel (cmd-startup-cancel conn msg-in)
        :30 (cmd-startup-pg30 conn msg-in)
        (cmd-startup-err conn (err-protocol-violation "Unknown protocol version"))))))

(defn cmd-enqueue-cmd
  "Enqueues another command for execution later (puts it at the back of the :cmd-buf queue).

  Commands can be queued with (non-conn) args using vectors like so (enqueue-cmd conn [#'cmd-simple-query {:query some-sql}])"
  [{:keys [conn-state]} & cmds]
  (swap! conn-state update :cmd-buf (fnil into PersistentQueue/EMPTY) cmds))

(defn cmd-send-query-result [{:keys [conn-status, conn-state] :as conn}
                             {:keys [query, ^IResultCursor result-cursor fields]}]

  (let [projection (mapv ffirst fields)
        json-bytes (comp utf8 json/json-str json-clj)

        ;; this query has been cancelled!
        cancelled-by-client? #(:cancel @conn-state)
        ;; please die as soon as possible (not the same as draining, which leaves conns :running for a time)
        closing? #(= :closing @conn-status)
        n-rows-out (volatile! 0)]


    (.forEachRemaining
     result-cursor
     (reify Consumer
       (accept [_ rel]
         (cond
           (cancelled-by-client?)
           (do (log/trace "Query cancelled by client")
               (swap! conn-state dissoc :cancel)
               (cmd-send-error conn (err-query-cancelled "query cancelled during execution")))

           (Thread/interrupted)
           (do
             (log/trace "query interrupted by server (forced shutdown)")
             (throw (InterruptedException.)))

           (closing?)
           (log/trace "query result stream stopping (conn closing)")

           :else
           (try
             (dotimes [idx (.rowCount ^RelationReader rel)]
               (let [row (mapv (fn [col-name] (.getObject (.readerForName ^RelationReader rel (str col-name)) idx)) projection)]
                 (cmd-write-msg conn msg-data-row {:vals (mapv json-bytes row)})
                 (vswap! n-rows-out inc)))

             ;; allow interrupts - this can happen if we are blocking during the row reduce and our conn is forced to close.
             (catch InterruptedException e
               (log/trace e "Interrupt thrown writing query results out")
               (throw e))

             ;; rethrow socket ex without logs (this is expected during any msg transfer
             ;; might later need to be more specific for storage
             ;; no point sending an error msg, the conn is probably dead.
             (catch SocketException e (throw e))

             ;; consider io error msg from storage (e.g AWS policy limit reached?)
             ;; not worried now, long term may sit elsewhere, but maybe not.

             ;; (ideally) unexpected (e.g bug in operator)
             (catch Throwable e
               (log/warn e "An exception was caught during query result set iteration")
               (cmd-send-error conn (err-internal "unexpected server error during query execution"))))))))

      (cmd-write-msg conn msg-command-complete {:command (str (statement-head query) " " @n-rows-out)})))

(defn- close-result-cursor [conn ^IResultCursor result-cursor]
  (try
    (.close result-cursor)
    (catch Throwable e
      (log/fatal e "Exception caught closing result cursor, resources may have been leaked - please restart XTDB")
      (.close ^Closeable conn))))

(def supported-param-oids
  (set (map (comp oids :pg) type-mappings)))

(defn- execute-tx [{:keys [node]} dml-buf {:keys [default-all-valid-time?]}]
  (let [tx-ops (mapv (fn [{:keys [transformed-query params]}]
                       [:sql transformed-query params])
                     dml-buf)]
    (try
      (some-> (ex-message (:error (xt/execute-tx node tx-ops {:default-all-valid-time? default-all-valid-time?})))
              err-protocol-violation)
      (catch Throwable e
        (log/debug e "Error on execute-tx")
        (err-pg-exception e "unexpected error on tx submit (report as a bug)")))))

(defn- ->xtify-param [{:keys [arg-types param-format]}]
  (fn xtify-param [param-idx param]
    (let [param-oid (nth arg-types param-idx nil)
          param-format (nth param-format param-idx nil)
          param-format (or param-format (nth param-format param-idx :text))
          pg-type (oid->kw param-oid)
          mapping (some #(when (= pg-type (:pg %)) %) type-mappings)
          _ (when-not mapping (throw (Exception. "Unsupported param type provided for read")))
          {:keys [pg-read-binary, pg-read-text]} mapping]
      (if (= :binary param-format)
        (pg-read-binary param)
        (pg-read-text param)))))

(defn- cmd-exec-dml [{:keys [conn-state] :as conn} {:keys [dml-type query transformed-query params] :as stmt}]
  (let [xtify-param (->xtify-param stmt)
        xt-params (vec (map-indexed xtify-param params))

        {:keys [transaction], {:keys [^Clock clock] :as session} :session} @conn-state

        default-all-valid-time? (not= :as-of-now (get-in session [:parameters :app-time-defaults]))

        stmt {:query query,
              :transformed-query transformed-query
              :params xt-params}]

    (if transaction
      ;; we buffer the statement in the transaction (to be flushed with COMMIT)
      (swap! conn-state update-in [:transaction :dml-buf] (fnil conj []) stmt)

      (execute-tx conn [stmt] {:default-all-valid-time? default-all-valid-time?
                              :default-tz (.getZone clock)}))

    (cmd-write-msg conn msg-command-complete
                   {:command (case dml-type
                               ;; insert <oid> <rows>
                               ;; oid is always 0 these days, its legacy thing in the pg protocol
                               ;; rows is 0 for us cus async
                               :insert "INSERT 0 0"
                               ;; otherwise head <rows>
                               :delete "DELETE 0"
                               :update "UPDATE 0"
                               :erase "ERASE 0")})))

(defn cmd-exec-query
  "Given a statement of type :query will execute it against the servers :node and send the results."
  [conn
   {:keys [query fields bound-query]}]
  (let [result-cursor
        (try
          (.openCursor ^BoundQuery bound-query)
          (catch InterruptedException e
            (log/trace e "Interrupt thrown opening result cursor")
            (throw e))
          (catch Throwable e
            (log/warn e)
            (cmd-send-error conn (err-pg-exception e "unexpected server error opening cursor for portal"))
            :failed-to-open-cursor))]


    (when-not (= result-cursor :failed-to-open-cursor)
      (try
        (cmd-send-query-result conn {:query query, :result-cursor result-cursor :fields fields})
        (catch InterruptedException e
          (log/trace e "Interrupt thrown sending query results")
          (throw e))
        (catch Throwable e
          (log/warn e)
          (cmd-send-error conn (err-pg-exception e "unexpected server error during query execution")))
        (finally
          ;; try and close the result-cursor (to warn on leak!)
          (close-result-cursor conn result-cursor))))))

(defn- cmd-send-row-description [conn cols]
  (let [defaults {:column-name ""
                  :table-oid 0
                  :column-attribute-number 0
                  :column-oid oid-json
                  :data-type-size -1
                  :type-modifier -1
                  :format-code :text}
        apply-defaults
        (fn [col]
          (if (map? col)
            (merge defaults col)
            (assoc defaults :column-name (str col))))
        data {:columns (mapv apply-defaults cols)}]
    (cmd-write-msg conn msg-row-description data)))

(defn cmd-describe-canned-response [conn canned-response]
  (let [{:keys [cols]} canned-response]
    (cmd-send-row-description conn cols)))

(defn cmd-describe-portal [conn {:keys [fields]}]
  (cmd-send-row-description conn (map ffirst fields)))

(defn cmd-begin [{:keys [node conn-state] :as conn} access-mode]
  (swap! conn-state
         (fn [{:keys [session] :as st}]
           (let [{:keys [^Clock clock latest-submitted-tx]} session]
             (-> st
                 (assoc :transaction
                        {:basis {:current-time (.instant clock)
                                 :at-tx (time/max-tx (:latest-completed-tx (xt/status node))
                                                     latest-submitted-tx)}

                         :access-mode (or access-mode
                                          (:access-mode (:next-transaction session))
                                          (:access-mode session))})

                 ;; clear :next-transaction variables for now
                 ;; aware right now this may not be spec compliant depending on interplay between START TRANSACTION and SET TRANSACTION
                 ;; thus TODO check spec for correct 'clear' behaviour of SET TRANSACTION vars
                 (update :session dissoc :next-transaction)))))

  (cmd-write-msg conn msg-command-complete {:command "BEGIN"}))

(defn cmd-commit [{:keys [conn-state] :as conn}]
  (let [{{:keys [failed err dml-buf]} :transaction, :keys [session]} @conn-state]
    (if failed
      (cmd-send-error conn (or err (err-protocol-violation "transaction failed")))

      (if-let [err (execute-tx conn dml-buf {:default-all-valid-time? (not (= :as-of-now (get-in session [:parameters :app-time-defaults])))})]
        (do
          (swap! conn-state update :transaction assoc :failed true, :err err)
          (cmd-send-error conn err))
        (do
          (swap! conn-state dissoc :transaction)
          (cmd-write-msg conn msg-command-complete {:command "COMMIT"}))))))

(defn cmd-rollback [{:keys [conn-state] :as conn}]
  (swap! conn-state dissoc :transaction)
  (cmd-write-msg conn msg-command-complete {:command "ROLLBACK"}))

(defn cmd-describe
  "Sends description messages (e.g msg-row-description) to the client for a prepared statement or portal."
  [{:keys [conn-state] :as conn} {:keys [describe-type, describe-name]}]
  (let [coll-k (case describe-type
                 :portal :portals
                 :prepared-stmt :prepared-statements
                 (Object.))
        {:keys [statement-type canned-response] :as describe-target} (get-in @conn-state [coll-k describe-name])]

    (when (= :prepared-statements describe-type)
      (throw (UnsupportedOperationException.
              ;;TODO consider if we can send some kind of describe statement for prepared statments
              ;;Not possible to return accurate param and return types but maybe we can return something
              ;;generic enough to not upset clients
              "XTDB does not support describing prepared statements,
               please bind the statement and describe the portal")))

    (case statement-type
      :empty-query (cmd-write-msg conn msg-no-data)
      :canned-response (cmd-describe-canned-response conn canned-response)
      :query (cmd-describe-portal conn describe-target)
      (cmd-write-msg conn msg-no-data))))

(defn cmd-set-session-parameter [conn parameter value]
  (set-session-parameter conn parameter value)
  (cmd-write-msg conn msg-command-complete {:command "SET"}))

(defn cmd-set-transaction [{:keys [conn-state] :as conn} {:keys [access-mode]}]
  ;; set the access mode for the next transaction
  ;; intention is BEGIN then can take these parameters as a preference to those in the session
  (when access-mode (swap! conn-state assoc-in [:session :next-transaction :access-mode] access-mode))
  (cmd-write-msg conn msg-command-complete {:command "SET TRANSACTION"}))

(defn cmd-set-time-zone [{:keys [conn-state] :as conn} ^ZoneId tz]
  (swap! conn-state update-in [:session :clock] (fn [^Clock clock] (.withZone clock tz)))
  (cmd-write-msg conn msg-command-complete {:command "SET TIME ZONE"}))

(defn cmd-set-session-characteristics [{:keys [conn-state] :as conn} [k v]]
  (swap! conn-state assoc-in [:session k] v)
  (cmd-write-msg conn msg-command-complete {:command "SET SESSION CHARACTERISTICS"}))

(defn- permissibility-err
  "Returns an error if the given statement, which is otherwise valid - is not permitted (say due to the access mode, transaction state)."
  [{:keys [conn-state]} {:keys [statement-type]}]
  (let [{:keys [transaction]} @conn-state

        ;; session access mode is ignored for now (wait for implicit transactions)
        access-mode (:access-mode transaction :read-only)

        access-mode-error
        (fn [msg wanted]
          (-> (with-out-str
                (println msg)
                (when (= :read-write wanted)
                  (println "READ WRITE transaction required for INSERT, UPDATE, and DELETE statements")
                  (when transaction (println "  - rollback the transaction: ROLLBACK"))
                  (println "  - start a transaction: START TRANSACTION READ WRITE")))
              err-protocol-violation))]
    (cond
      (and (= :set-transaction statement-type) transaction)
      (err-protocol-violation "invalid transaction state -- active SQL-transaction")

      (and (= :begin statement-type) transaction)
      (err-protocol-violation "invalid transaction state -- active SQL-transaction")

      ;; we currently only simulate partially direct sql opening transactions (spec behaviour)
      ;; that somewhat works as it should for reads, writes however are more difficult and its not clear
      ;; if we should follow spec (i.e autocommit blocking writes)
      ;; so we will to refuse DML unless we are in an explicit transaction
      (= :dml statement-type)
      (when (and transaction (= :read-only access-mode))
        (access-mode-error "DML is not allowed in a READ ONLY transaction" :read-write))

      (= :query statement-type)
      (when (and transaction (= :read-write access-mode))
        (access-mode-error "queries are unsupported in a READ WRITE transaction" :read-only)))))

(defn cmd-sync
  "Sync commands are sent by the client to commit transactions (we do not do anything here yet),
  and to clear the error state of a :extended mode series of commands (e.g the parse/bind/execute dance)"
  [{:keys [conn-state] :as conn}]
  ;; TODO commit / rollback should be used here if not in an explicit tx?

  (when-not (:transaction @conn-state)
    ;;if outside an explicit transaction/transaction block (BEGIN/COMMIT) close any portals
    ;;as these are implicitly closed/cleaned up at the end of the transcation
    (doseq [portal-name (keys (:portals @conn-state))]
      (close-portal conn portal-name)))

  (cmd-send-ready conn)
  (swap! conn-state dissoc :skip-until-sync, :protocol))

(defn cmd-flush
  "Flushes any pending output to the client."
  [conn]
  (.flush ^OutputStream @(:out conn)))

(defn cmd-parse
  "Responds to a msg-parse message that creates a prepared-statement."
  [{:keys [conn-state cid server] :as conn}
   {:keys [stmt-name query arg-types]}]
  ;; put the conn in the extend protocol
  ;; TODO if/when simple query mode calls through to this cmd, will need to move this/make it dynamic
  (swap! conn-state assoc :protocol :extended)

  (log/trace "Parsing" {:stmt-name stmt-name,
                        :query query
                        :port (:port server)
                        :cid cid})

  (let [{:keys [err] :as stmt} (interpret-sql query)
        unsupported-arg-types (remove supported-param-oids arg-types)
        stmt (when-not err (assoc stmt :arg-types arg-types))
        err (or err
                (when-some [oid (first unsupported-arg-types)]
                  (err-protocol-violation (format "parameter type (%s) currently unsupported by xt" (name (oid->kw oid (str oid))))))
                (permissibility-err conn stmt))]
    (if err
      (cmd-send-error conn err)
      (do
        (swap! conn-state assoc-in [:prepared-statements stmt-name] stmt)
        (cmd-write-msg conn msg-parse-complete)))))

(defn cmd-bind [{:keys [conn-state node] :as conn}
                {:keys [portal-name stmt-name params] :as bind-msg}]
  (let [{:keys [statement-type] :as stmt} (get-in @conn-state [:prepared-statements stmt-name])]
    (if stmt
      (let [stmt-with-bind-msg
            ;; add data from bind-msg to stmt, for queries params are bound now, for dml this happens later during execute-tx.
            (merge stmt bind-msg)
            {:keys [portal bind-outcome]}
            ;;if statement is a query, compile and bind it, else use the statment as a portal
            (if (= :query statement-type)
              (let [xtify-param (->xtify-param stmt-with-bind-msg)
                    xt-params (vec (map-indexed xtify-param params))

                    {{:keys [^Clock clock, latest-submitted-tx] :as session} :session
                     {:keys [basis]} :transaction} @conn-state

                    default-all-valid-time? (not (= :as-of-now (get-in session [:parameters :app-time-defaults])))

                    query-opts {:basis (or basis {:current-time (.instant clock)})
                                :after-tx latest-submitted-tx ;;TODO any need for this if we are sending explicit basis?
                                :tx-timeout (Duration/ofSeconds 1)
                                :default-tz (.getZone clock)
                                :args xt-params
                                :default-all-valid-time? default-all-valid-time?
                                :key-fn :snake-case-string}

                    ;;TODO explicit basis means its okay to send the same query-opts twice for now, rather than have to check what tables
                    ;;are reffed by compiled-query and see if they have changed by the time bound-query runs, as these 2 requests are fired in quick
                    ;;succession. Will nee changing when parse is moved to its own phase.

                    {:keys [prepared-query prep-outcome]}
                    (try
                      {:prepared-query (.prepareQuery ^IXtdbInternal node ^String (:transformed-query stmt-with-bind-msg) query-opts)
                       :prep-outcome :success}
                      (catch InterruptedException e
                        (log/trace e "Interrupt thrown compiling query")
                        (throw e))
                      (catch Throwable e
                        (log/warn e)
                        (cmd-send-error
                         conn
                         (err-pg-exception e "unexpected server error compiling query"))))]

                (when (= :success prep-outcome)
                  (try
                    (let [^BoundQuery bound-query (.bind ^PreparedQuery prepared-query query-opts)]
                      {:portal (assoc stmt-with-bind-msg :bound-query bound-query :fields (.columnFields bound-query))
                       :bind-outcome :success})
                    (catch InterruptedException e
                      (log/trace e "Interrupt thrown binding prepared statement")
                      (throw e))
                    (catch Throwable e
                      (log/warn e)
                      (cmd-send-error conn (err-pg-exception (.getCause e) "unexpected server error binding prepared statement"))))))

              {:portal stmt-with-bind-msg
               :bind-outcome :success})]

        ;; add the portal
        (when (= :success bind-outcome)
          (swap! conn-state assoc-in [:portals portal-name] portal))

        ;; track the portal name to the prepared stmt (for close)
        (when (= :success bind-outcome)
          (swap! conn-state update-in [:prepared-statements stmt-name :portals] (fnil conj #{}) portal-name))

        (if (and (= :success bind-outcome) (= :extended (:protocol @conn-state)))
          (cmd-write-msg conn msg-bind-complete)
          bind-outcome))

      (cmd-send-error conn (err-protocol-violation "no prepared statement")))))

(defn cmd-execute
  "Handles a msg-execute to run a previously bound portal (via msg-bind)."
  [{:keys [conn-state] :as conn}
   {:keys [portal-name _limit]}]
  ;;TODO implement limit for queries that return rows
  (if-some [{:keys [statement-type canned-response access-mode parameter tz value] :as portal} (get-in @conn-state [:portals portal-name])]

    (case statement-type
      :empty-query (cmd-write-msg conn msg-empty-query)
      :canned-response (cmd-write-canned-response conn canned-response)
      :set-session-characteristics (cmd-set-session-characteristics conn (first (dissoc portal :statement-type)))
      :set-session-parameter (cmd-set-session-parameter conn parameter value)
      :set-transaction (cmd-set-transaction conn {:access-mode access-mode})
      :set-time-zone (cmd-set-time-zone conn tz)
      :ignore (cmd-write-msg conn msg-command-complete {:command "IGNORED"})
      :begin (cmd-begin conn access-mode)
      :rollback (cmd-rollback conn)
      :commit (cmd-commit conn)
      :query (cmd-exec-query conn portal)
      :dml (cmd-exec-dml conn portal)

      (throw (UnsupportedOperationException. (pr-str {:portal portal}))))
    (cmd-send-error conn (err-protocol-violation "no such portal"))))

(defn cmd-simple-query [{:keys [conn-state] :as conn} {:keys [query]}]
  (let [{:keys [err statement-type] :as stmt} (interpret-sql query)
        stmt-name ""
        portal-name ""]
    (swap! conn-state assoc :protocol :simple)
    (if-some [err (or err (permissibility-err conn stmt))]
      (do
        (cmd-send-error conn err)
        (cmd-send-ready conn))
      (do
        (swap! conn-state assoc-in [:prepared-statements stmt-name] stmt)
        (if (= :success (cmd-bind conn {:portal-name portal-name
                                        :stmt-name stmt-name}))

          (do (when (#{:query :canned-response} statement-type)
                ;; Client only expects to see a RowDescription (result of cmd-descibe)
                ;; for certain statement types
                (cmd-describe conn {:describe-type :portal
                                    :describe-name portal-name}))

              (cmd-execute conn {:portal-name portal-name})
              (close-portal conn portal-name)

              (cmd-send-ready conn))

          ;;parse/bind failed, cmd-bind already sent the error message on our behalf
          ;;so now be ready to accept a new query
          (cmd-send-ready conn))))))

;; connection loop
;; we run a blocking io server so a connection is simple a loop sitting on some thread

(defn- conn-loop [conn]
  (let [{:keys [cid, server, ^Socket socket, in, conn-status, conn-state]} conn
        {:keys [port server-status]} server]

    (when-not (compare-and-set! conn-status :new :running)
      (log/error "Connect loop requires a newly initialised connection" {:cid cid, :port port}))

    (cmd-startup conn)

    (loop []

      (cond
        ;; the connection is closing right now
        ;; let it close.
        (not= :running @conn-status)
        (log/trace "Connection loop exiting (closing)" {:port port, :cid cid})

        ;; if the server is draining, we may later allow connections to finish their queries
        ;; (consider policy - server has a drain limit but maybe better to be explicit here as well)
        (and (= :draining @server-status)
             (not= :extended (:protocol @conn-state))
             ;; for now we allow buffered commands to be run
             ;; before closing - there probably needs to be limits to this
             ;; (for huge queries / result sets)
             (empty? (:cmd-buf @conn-state)))
        (do (log/trace "Connection loop exiting (draining)" {:port port, :cid cid})
            ;; TODO I think I should send an error, but if I do it causes a crash on the client?
            #_(cmd-send-error conn (err-admin-shutdown "draining connections"))
            (compare-and-set! conn-status :running :closing))

        ;; well, it won't have been us, as we would drain first
        (.isClosed socket)
        (do (log/trace "Connection closed unexpectedly" {:port port, :cid cid})
            (compare-and-set! conn-status :running :closing))

        ;; we have queued a command to be processed
        ;; a command represents something we want the server to do for the client
        ;; such as error, or send data.
        (seq (:cmd-buf @conn-state))
        (let [cmd-buf (:cmd-buf @conn-state)
              [cmd-fn & cmd-args] (peek cmd-buf)]
          (if (seq cmd-buf)
            (swap! conn-state assoc :cmd-buf (pop cmd-buf))
            (swap! conn-state dissoc :cmd-buf))

          (when cmd-fn (apply cmd-fn conn cmd-args))

          (recur))

        ;; go idle until we receive another msg from the client
        :else
        (let [{:keys [msg-char8, msg-in]} (read-typed-msg @in)
              msg-var (client-msgs msg-char8)]

          (log/trace "Read client msg"
                     {:port port, :cid cid, :msg (or msg-var msg-char8), :char8 msg-char8})

          (when (:skip-until-sync @conn-state)
            (if (= msg-var #'msg-sync)
              (cmd-enqueue-cmd conn [#'cmd-sync])
              (log/trace "Skipping msg until next sync due to error in extended protocol"
                         {:port port, :cid cid, :msg (or msg-var msg-char8), :char8 msg-char8})))

          (when-not msg-var
            (cmd-send-error conn (err-protocol-violation "unknown client message")))

          (when (and msg-var (not (:skip-until-sync @conn-state)))
            (let [msg-data ((:read @msg-var) msg-in)]
              (condp = msg-var
                #'msg-simple-query (cmd-simple-query conn msg-data)
                #'msg-terminate (cmd-terminate conn)
                #'msg-close (cmd-close conn msg-data)
                #'msg-parse (cmd-parse conn msg-data)
                #'msg-bind (cmd-bind conn msg-data)
                #'msg-sync (cmd-sync conn)
                #'msg-execute (cmd-execute conn msg-data)
                #'msg-describe (cmd-describe conn msg-data)
                #'msg-flush (cmd-flush conn)

                ;; ignored by xt
                #'msg-password nil

                (cmd-send-error conn (err-protocol-violation "unknown client message")))))

          (recur))))))

(defn- connect
  "Starts and runs a connection on the current thread until it closes.

  The connection exiting for any reason (either because the connection, received a close signal, or the server is draining, or unexpected error) should result in connection resources being
  freed at the end of this function. So the connections lifecycle should be totally enclosed over the lifetime of a connect call.

  See comment 'Connection lifecycle'."
  [server, ^Socket conn-socket, node]
  (let [{:keys [server-state, port]} server
        cid (:next-cid (swap! server-state update :next-cid (fnil inc 0)))
        in (delay (DataInputStream. (.getInputStream conn-socket)))
        out (delay (DataOutputStream. (.getOutputStream conn-socket)))
        conn-status (atom :new)
        conn-state (atom {:close-promise (promise)
                          :session {:access-mode :read-only
                                    :clock (:clock @server-state)}})

        create-node? (nil? node)

        node (or node
                 (do
                   (log/debug "starting in-memory node")
                   (xtn/start-node)))

        conn (map->Connection {:server server,
                               :node node
                               :close-node? create-node?
                               :socket conn-socket,
                               :cid cid,
                               :conn-status conn-status
                               :conn-state conn-state
                               :in in
                               :out out})]

    ;; first try and initialise the connection
    (try
      ;; try make in/out
      @in
      @out

      ;; registering the connection in allows us to address it from the server
      ;; important for shutdowns.
      (let [conn-in-map (-> (swap! server-state update-in [:connections cid] (fn [e] (or e conn)))
                            (get-in [:connections cid]))]
        (when-not (identical? conn-in-map conn)
          (compare-and-set! conn-status :new :error-on-start)))

      (catch Throwable e
        (compare-and-set! conn-status :new :error-on-start)
        (log/error e "An exception was caught on connection start" {:port port, :cid cid})))

    ;; if we succeeded, start the conn loop
    (when (= :new @conn-status)
      (try
        (log/trace "Starting connection loop" {:port port, :cid cid})
        (conn-loop conn)
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
        (catch Throwable e
          (log/error e "An exception was caught during connection" {:port port, :cid cid}))))

    ;; right now we'll deregister and cleanup regardless, but later we may
    ;; want to leave conns around for a bit, so you can look at their state and debug
    (cleanup-connection-resources conn)
    (when create-node?
      (log/debug "closing in-memory node")
      (util/close node))

    (log/trace "Connection ended" {:cid cid, :port port})

    ;; can be used to co-ordinate waiting for close
    (when-some [close-promise (:close-promise @conn-state)]
      (when-not (realized? close-promise)
        (deliver close-promise true)))))

(defn- accept-loop
  "Runs an accept loop on the current thread (intended to be the Server's :accept-thread).

  While the server is running, tries to .accept connections on its :accept-socket. Once accepted,
  a connection is created on the servers :thread-pool via the (connect) function."
  [{:keys [^ServerSocket accept-socket,
           server-status,
           server-state
           ^ExecutorService thread-pool,
           port]
    :as server}

   node]

  (when-not (compare-and-set! server-status :starting :running)
    (throw (Exception. "Accept loop requires a newly initialised server")))

  (log/info "PGWire server listening on" port)

  (try
    (loop []
      (cond
        (Thread/interrupted) (throw (InterruptedException.))

        (.isClosed ^ServerSocket @accept-socket)
        (log/trace "Accept socket closed, exiting accept loop")

        (#{:draining :running} @server-status)
        (do
          (try
            ;; set a low timeout to leave accept early if needed
            (when (= :draining @server-status)
              (.setSoTimeout ^ServerSocket @accept-socket 10))

            ;; accept next connection (blocks until interrupt (with so-timeout) or close)
            (let [conn-socket (.accept ^ServerSocket @accept-socket)]
              (when-some [exc (:injected-accept-exc @server-state)]
                (swap! server-state dissoc :injected-accept-exc)
                (.close conn-socket)
                (throw exc))

              (.setTcpNoDelay conn-socket true)
              (if (= :running @server-status)
                ;; TODO fix buffer on tp? q gonna be infinite right now
                (.submit thread-pool ^Runnable (fn [] (connect server conn-socket node)))
                (do
                  (err-cannot-connect-now "server shutting down")
                  (.close conn-socket))))
            (catch SocketException e
              (when (and (not (.isClosed ^ServerSocket @accept-socket))
                         (not= "Socket closed" (.getMessage e)))
                (log/warn e "Accept socket exception" {:port port})))
            (catch java.net.SocketTimeoutException e
              (when (not= "Accept timed out" (.getMessage e))
                (log/warn e "Accept time out" {:port port})))
            (catch IOException e
              (log/warn e "Accept IO exception" {:port port})))
          (recur))))

    (catch InterruptedException _
      (swap! server-state assoc :accept-interrupted true))

    ;; for the unknowns, do not stop - leave in an error state
    ;; so it can be inspected.
    (catch Throwable e
      (compare-and-set! server-status :running :error)
      (util/try-close @accept-socket)
      (when-not (:silent-accept @server-state)
        (log/error e "Exception caught on accept thread" {:port port}))))

  (log/trace "exiting accept loop"))

(defn- create-server
  "node: if provided, uses the given node for all connections; otherwise, creates a transient, in-memory node for each new connection"
  [node {:keys [port num-threads drain-wait unsafe-init-state]}]

  (let [self (atom nil)
        clock (Clock/systemDefaultZone)
        default-state {:clock clock
                       :drain-wait drain-wait
                       :parameters {"server_version" "14"
                                    "server_encoding" "UTF8"
                                    "client_encoding" "UTF8"}}
        srvr (map->Server {:port port
                           :accept-socket (delay (ServerSocket. port))
                           :accept-thread (Thread. ^Runnable (fn [] (accept-loop @self node)) (str "pgwire-server-accept-" port))
                           :thread-pool (Executors/newFixedThreadPool num-threads (util/->prefix-thread-factory "pgwire-connection-"))
                           :server-status (atom :new)
                           :server-state (atom (merge unsafe-init-state default-state))})]
    (reset! self srvr)
    srvr))

(defn serve
  "Creates and starts a pgwire server.

  Options:

  :port (default 5432)
  :num-threads (bounds the number of client connections, default 42)

  Returns the opaque server object.

  Stop with .close (its Closeable)"
  ([node] (serve node {}))
  ([node opts]
   (let [defaults {:port 5432
                   :num-threads 42
                   ;; 5 seconds of draining
                   :drain-wait 5000}
         cfg (merge defaults opts)]
     (doto (create-server node cfg)
       (start-server cfg)))))

(defmethod xtn/apply-config! ::server [^Xtdb$Config config, _ {:keys [port num-threads]}]
  (.module config (cond-> (PgwireServer$Factory.)
                    (some? port) (.port port)
                    (some? num-threads) (.numThreads num-threads))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn open-server [node ^PgwireServer$Factory module]
  (let [port (.getPort module)
        num-threads (.getNumThreads module)
        srv (serve node {:port port, :num-threads num-threads})]
    (log/info "PGWire server started on port:" port)
    (reify XtdbModule
      (close [_]
        (util/try-close ^Closeable srv)
        (log/info "PGWire server stopped")))))


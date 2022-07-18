(ns core2.sql.pgwire
  "Defines a postgres wire protocol (pgwire) server for xtdb.

  Start with (serve node)
  Stop with (.close server) or (stop-all)"
  (:require [core2.api :as c2]
            [core2.sql.plan :as plan]
            [core2.sql.analyze :as sem]
            [core2.sql.parser :as parser]
            [core2.rewrite :as r]
            [core2.local-node :as node]
            [core2.util :as util]
            [clojure.string :as str]
            [clojure.data.json :as json]
            [core2.api :as api]
            [clojure.tools.logging :as log])
  (:import (java.nio.charset StandardCharsets)
           (java.io Closeable ByteArrayOutputStream DataInputStream DataOutputStream InputStream IOException OutputStream PushbackInputStream EOFException ByteArrayInputStream)
           (java.net Socket ServerSocket SocketTimeoutException SocketException)
           (java.util.concurrent Executors ExecutorService ConcurrentHashMap RejectedExecutionException TimeUnit)
           (java.util HashMap)
           (org.apache.arrow.vector PeriodDuration)
           (java.time LocalDate)
           (java.nio ByteBuffer)
           (com.fasterxml.jackson.databind.util ByteBufferBackedInputStream)
           (java.util.function Function BiFunction)
           (java.lang Thread$State)
           (clojure.lang PersistentQueue)))

;; references
;; https://www.postgresql.org/docs/current/protocol-flow.html
;; https://www.postgresql.org/docs/current/protocol-message-formats.html

;; important to not worry until we've measured!
(set! *warn-on-reflection* false)

;; unchecked math is unnecessary (and perhaps dangerous) for this ns
(set! *unchecked-math* false)

;; Server
;
;; Represents a single postgres server on a particular port
;; the server currently is a blocking IO server (no nio / netty)
;; the server does not own the lifecycle of its associated node

;; Server lifecycle
;
;; new -> starting        -> running -> draining -> cleaning-up -> cleaned-up
;;     -> error-on-start  -> error                              -> error-on-cleanup
;;
;; lifecycle changes are mediated through the :server-status field, which holds an atom that
;; can be used to control state transitions from any thread.
;; The :draining status is used to stop connections when they have finished their current query

(declare stop-server stop-connection)

(defrecord Server
  [port
   node

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
  (close [this] (stop-server this)))

;; Connection
;
;; Represents a single client connection to the server
;; identified by an integer 'cid'
;; each connection holds some state under :conn-state (such as prepared statements, session params and such)
;; and is registered under the :connections map under the servers :server-state.

;; Connection lifecycle
;
;; new -> running        -> closing -> cleaning-up -> cleaned-up
;;     -> error-on-start
;;
;; Like the server, we use an atom :conn-status to mediate life-cycle state changes to control for concurrency
;; unlike the server, a connections lifetime is entirely bounded by a blocking (connect) call - so shutdowns should be more predictable.

(defrecord Connection
  [^Server server
   socket

   ;; a positive integer that identifies the connection on this server
   ;; we will use this as the pg Process ID for messages that require it (such as cancellation)
   cid
   ;; atom to mediate lifecycle transitions (see Connection lifecycle comment)
   conn-status
   ;; atom to hold a map of session / connection state, such as :prepared-statements and :session-parameters
   conn-state
   ;; io
   ^DataInputStream in
   ^DataOutputStream out]
  Closeable
  (close [this] (stop-connection this)))

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

(def ^:private oids
  "A map of postgres type (that we may support to some extent) to numeric oid.
  Mapping to oid is useful for result descriptions, as well as to determine the semantics of received parameters."
  {:int2 21
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

(def ^:private oid-varchar (oids :varchar))
(def ^:private oid-json (oids :json))

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

;; errors
;

(defn- err-protocol-violation [msg]
  {:severity "ERROR"
   :localized-severity "ERROR"
   :sql-state "08P01"
   :message msg})

(defn- err-parse [parse-failure]
  {:severity "ERROR"
   :localized-severity "ERROR"
   :sql-state "42601"
   :message (first (str/split-lines (pr-str parse-failure)))
   :detail (parser/failure->str parse-failure)
   :position (str (inc (:idx parse-failure)))})

(defn- err-internal [ex]
  {:severity "ERROR"
   :localized-severity "ERROR"
   :sql-state "XX000"
   ;; todo I don't think we want to do this unless we are in debug mode or something
   :message (first (str/split-lines (or (.getMessage ex) "")))
   :detail (str ex)
   :where (with-out-str
            (binding [*err* *out*]
              (.printStackTrace ex)))})

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

;; sql processing
;
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
   {:q "select current_schema"
    :cols [{:column-name "current_schema" :column-oid oid-varchar}]
    :rows [["public"]]}
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
    :rows [["xtdb"]]}

   ;; while testing this stops me typing so much
   {:q "select ping"
    :cols [{:column-name "ping", :column-oid oid-varchar}]
    :rows [["pong"]]}])

;; this is hopefully temporary
;; I would like to use some custom parse rules for this (and actually canned responses too)
(defn- transform-query
  "Pre-processes a sql string to remove semi-colons and replaces the parameter convention to
  that which XT supports."
  [s]
  (-> s
      (str/replace #"\$\d+" "?")
      (str/replace #";\s*$" "")))

;; yagni, is everything upper'd anyway by drivers / server?
(defn- probably-same-query? [s substr]
  ;; todo I bet this may cause some amusement. Not sure what to do about non-parsed query matching, it'll do for now.
  (str/starts-with? (str/lower-case s) (str/lower-case substr)))

(defn get-canned-response
  "If the sql string is a canned response, returns the entry from the canned-responses that matches."
  [sql-str]
  (when sql-str (first (filter #(probably-same-query? sql-str (:q %)) canned-responses))))

(defn- statement-head [s]
  (-> s (str/split #"\s+") first str/upper-case))

(defn- interpret-sql
  "Takes a sql string and returns a map of data about it.

  :statement-type (:canned-response, :set-session-parameter, :ignore, :empty-query or :query)

  if :canned-response
  :canned-response (the map from the canned-responses)

  if :set-session-parameter
  :parameter (the param name)
  :value (the value)

  if :ignore
  :ignore the kind of statement to ignore (useful to emit the correct msg-command-complete)

  if :empty-query (nothing more to say!)

  if :query
  :query (the original sql string)
  :transformed-query (the pre-processed for xt sql string)
  :num-placeholders (the number of parameter placeholders in the query)
  :ast (the result of parse, possibly a parse failure)
  :err (if there was a parse failure, the pgwire error to send)"
  [sql]
  (cond
    (get-canned-response sql)
    {:statement-type :canned-response
     :canned-response (get-canned-response sql)}

    (= "SET" (statement-head sql))
    (let [[_ k v] (re-find #"SET\s+(\w+)\s*=\s*'?(.*)'?" sql)]
      {:statement-type :set-session-parameter
       :parameter k
       :value v})

    (#{"BEGIN" "COMMIT" "ROLLBACK"} (statement-head sql))
    {:statement-type :ignore
     :ignore (statement-head sql)}

    (str/blank? sql)
    {:statement-type :empty-query}

    :else
    (let [transformed-query (transform-query sql)
          num-placeholders (count (re-seq #"\?" transformed-query))
          parse-result (parser/parse transformed-query)]
      {:statement-type :query
       :query sql
       :transformed-query transformed-query
       :ast parse-result
       :err (when (parser/failure? parse-result) (err-parse parse-result))
       :num-placeholders num-placeholders})))

(defn- query-projection
  "Returns a vec of column names returned by the query (ast)."
  [ast]
  (binding [r/*memo* (HashMap.)]
    (->> (sem/projected-columns (r/$ (r/vector-zip ast) 1))
         (first)
         (mapv (comp name plan/unqualified-projection-symbol)))))

;; json printing for xt data types

(extend-protocol json/JSONWriter
  PeriodDuration
  (-write [pd out options]
    (json/-write (str pd) out options))
  LocalDate
  (-write [ld out options]
    (json/-write (str ld) out options))
  org.apache.arrow.vector.util.Text
  (-write [ld out options]
    (json/-write (str ld) out options)))

(defn- json-str ^String [obj]
  (json/write-str obj))

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
    (try
      (loop []
        (cond
          ;; at least 4 bytes available
          (<= 4 (.available in))
          (do
            false)

          ;; time left
          (< (System/currentTimeMillis) wait-until)
          (do
            ;; one nice thing about sleep is if we are interrupted
            ;; (as we've been asked to close)
            ;; we get an exception here
            (if (try (Thread/sleep 1) true (catch InterruptedException _ false))
              (recur)
              true))

          ;; timed out
          :else true)))))

;; server impl
;

(defn- cleanup-server-resources [server]
  (let [{:keys [port
                accept-socket
                ^Thread accept-thread
                ^ExecutorService thread-pool
                server-state
                server-status]} server]

    ;; todo revisit these transitions
    (or (compare-and-set! server-status :draining :cleaning-up)
        (compare-and-set! server-status :running :cleaning-up)
        (compare-and-set! server-status :error :cleaning-up)
        (compare-and-set! server-status :error-on-start :cleaning-up))

    (when (realized? accept-socket)
      (let [^ServerSocket socket @accept-socket]
        (when-not (.isClosed socket)
          (log/debug "Closing accept socket" port)
          (.close socket))))

    (when-not (#{Thread$State/NEW, Thread$State/TERMINATED} (.getState accept-thread))
      (log/debug "Closing accept thread")
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
      (log/debug "Closing thread pool")
      (.shutdownNow thread-pool)
      (when-not (.awaitTermination thread-pool 5 TimeUnit/SECONDS)
        (log/error "Could not shutdown thread pool gracefully" {:port port})
        (compare-and-set! server-status :cleaning-up :error-on-cleanup)
        (swap! server-state assoc :thread-pool-misbehaving true)))

    (compare-and-set! server-status :cleaning-up :cleaned-up)))

;; we keep the servers in a global
;; sockets, thread pools and so on are global resources, no point pretending.
;; we want to be able to address sockets, threads, and such
;; from the repl, and possibly from telemetry and monitoring code
(defonce ^{:doc "Server instances by port"
           :private true
           :tag ConcurrentHashMap}
  servers
  (ConcurrentHashMap.))

(defn- deregister-server
  "If the server obj is currently registered as the server for its port (this is usually the case), then remove it."
  [server]
  (let [port (:port server)]
    (->> (reify BiFunction
           (apply [_ _ v]
             (if (identical? v server)
               nil
               v)))
         (.compute servers port))))

(defn- start-server [server {:keys [accept-so-timeout]
                             :or {accept-so-timeout 5000}}]
  (let [{:keys [server-status, server-state, accept-thread, accept-socket, port]} server
        {:keys [injected-start-exc, silent-start]} @server-state
        start-exc (atom nil)]

    ;; sanity check its a new server
    (when-not (compare-and-set! server-status :new :starting)
      (throw (Exception. (format "Server must be :new to start" port))))

    (try

      ;; try and bind the socket (this throws if we cannot get the port)
      @accept-socket

      ;; set a socket timeout so we can interrupt the accept-thread
      ;; can be set to nil as an option if you want to allow no timeout
      (when accept-so-timeout
        (.setSoTimeout @accept-socket accept-so-timeout))

      ;; lets register ourselves now we have acquired a global resource (port).
      (->> (reify BiFunction
             (apply [_ _ v]
               ;; if we have a value, something has gone wrong
               ;; we've bound the port, but we think we have already got a server
               ;; listening at port
               (when v
                 (log/error "Server was already registered at port" port)
                 (reset! server-status :error-on-start))

               (or v server)))
           (.compute servers port))

      ;; start the accept thread, which will listen for connections
      (let [is-running (promise)]

        ;; wait for the :running state
        (->> (fn [_ _ _ ns]
               (when (= :running ns)
                 (deliver is-running true)))
             (add-watch server-status [:accept-watch port]))

        (.start accept-thread)

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
    (when (compare-and-set! server-status :cleaned-up :error-on-start)
      ;; if we have no dangling resources, unregister
      ;; (if we have dangling, we might be able to fix our repl session still!)
      (deregister-server server))

    (when-not silent-start
      (when-some [exc @start-exc]
        (throw exc))))

  nil)

(defn stop-server [server]
  (let [{:keys [server-status, server-state, port]} server
        drain-wait (:drain-wait @server-state 5000)
        interrupted (.isInterrupted (Thread/currentThread))
        drain (not (or interrupted (contains? #{0, nil} drain-wait)))]

    (when (and drain (compare-and-set! server-status :running :draining))
      (log/debug "server draining connections")
      (loop [wait-until (+ (System/currentTimeMillis) drain-wait)]
        (cond
          ;; connections all closed, proceed
          (empty? (:connections @server-state)) nil

          ;; timeout
          (< wait-until (System/currentTimeMillis))
          (do (log/warn "could not drain connections in time, force closing")
              (swap! server-state assoc :force-close true))

          ;; we are interrupted, hurry up!
          (.isInterrupted (Thread/currentThread))
          (do (log/warn "interupted during drain, force closing")
              (swap! server-state assoc :force-close true))

          :else (do (Thread/sleep 10) (recur wait-until)))))

    (cleanup-server-resources server)

    (if (= :cleaned-up @server-status)
      (deregister-server server)
      (log/fatal "Server resources could not be freed, please restart xtdb" port))))

(defn- set-session-parameter [conn parameter value]
  (swap! (:conn-state conn) assoc-in [:session-parameters parameter] value))

(defn- cleanup-connection-resources [conn]
  (let [{:keys [cid, server, in, out, ^Socket socket, conn-status]} conn
        {:keys [port server-state]} server]

    (reset! conn-status :cleaning-up)

    (when (realized? out)
      (try
        (.close @out)
        (catch Throwable e
          (log/error e "Exception caught closing socket out" {:port port, :cid cid}))))

    (when (realized? in)
      (try
        (.close @in)
        (catch Throwable e
          (log/error e "Exception caught closing socket in" {:port port, :cid cid}))))

    (when-not (.isClosed socket)
      (try
        (.close socket)
        (catch Throwable e
          (log/error e "Exception caught closing conn socket"))))


    (when (compare-and-set! conn-status :cleaning-up :cleaned-up)
      (->> (fn [conns]
             (if (identical? conn (get conns cid))
               (dissoc conns cid)
               conns))
           (swap! server-state update :connections)))))

(defn stop-connection [conn]
  (let [{:keys [conn-status]} conn]
    (reset! conn-status :closing)
    ;; todo wait for close?
    (cleanup-connection-resources conn)))

;; pg i/o shared data types
;
;; our io maps just capture a paired :read fn (data-in)
;; and :write fn (data-out, val)
;; the goal is to describe the data layout of various pg messages, so they can be read and written from in/out streams
;; by generalizing a bit here, we gain a terser language for describing wire data types, and leverage
;; (e.g later we might decide to compile unboxed reader/writers using these)

(def ^:private no-read (fn [in] (throw (UnsupportedOperationException.))))
(def ^:private no-write (fn [out _] (throw (UnsupportedOperationException.))))

(def ^:private io-char8
  "A single byte character"
  {:read #(char (.readUnsignedByte %))
   :write #(.writeByte %1 (byte %2))})

(def ^:private io-uint16
  "An unsigned short integer"
  {:read #(.readUnsignedShort %)
   :write #(.writeShort %1 (short %2))})

(def ^:private io-uint32
  "An unsigned 32bit integer"
  {:read #(.readInt %)
   :write #(.writeInt %1 (int %2))})

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
    {:read (fn [in] (vec (repeatedly (len-rdr in) #(el-rdr in))))
     :write (fn [out coll] (len-wtr out (count coll)) (run! #(el-wtr out %) coll))}))

(defn ^:private io-bytes
  "Returns an io data type for a pg byte array, whose len is given by the io-len, e.g a byte array whose size
  is bounded to max(uint16) bytes - (io-bytes io-uint16)"
  [io-len]
  (let [len-rdr (:read io-len)]
    {:read (fn [in]
             (let [arr (byte-array (len-rdr in))]
               (.readFully in arr)
               arr))
     :write no-write}))

(def ^:private io-format-code
  "Postgres format codes are integers, whose value is either

  0 (:text)
  1 (:binary)

  On read returns a keyword :text or :binary, write of these keywords will be transformed into the correct integer."
  {:read (fn [in] ({0 :text, 1 :binary} (.readUnsignedShort in)))
   :write (fn [out v] (.writeShort out ({:text 0, :binary 1} v)))})

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

(def ^:private io-bytes-or-null
  "An io data type for a byte array (or null), in postgres you can write the bytes of say a column as either
  len (uint32) followed by the bytes OR in the case of null, a len whose value is -1. This is how row values are conveyed
  to the client."
  {:read no-read
   :write (fn write-bytes-or-null [^DataOutputStream out ^bytes arr]
            (if arr
              (do (.writeInt out (alength arr))
                  (.write out arr))
              (.writeInt out -1)))})

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

;; msg definition

;
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
  :params (io-list io-uint16 (io-bytes io-uint32))
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

;; server messages
;

(def-msg msg-error-response :server \E
  :error-fields (io-null-terminated-list io-error-field))

(def-msg msg-bind-complete :server \2
  :result {:read no-read
           :write (fn [out _] (.writeInt out 4))})

(def-msg msg-close-complete :server \3)

(def-msg msg-command-complete :server \C
  :command io-string)

(def-msg msg-parameter-description :server \t
  :parameter-oids (io-list io-uint16 io-uint32))

(def-msg msg-parameter-status :server \S
  :parameter io-string
  :value io-string)

(def-msg msg-data-row :server \D
  :vals (io-list io-uint16 io-bytes-or-null))

(def-msg msg-portal-suspended :server \s)

(def-msg msg-parse-complete :server \1)

(def-msg msg-no-data :server \n)

(def-msg msg-empty-query :server \I)

(def-msg msg-notice-response :server \N)

(def-msg msg-auth :server \R
  :result io-uint32)

(def-msg msg-backend-key-data :server \K)

(def-msg msg-copy-in-response :server \G)

(def-msg msg-ready :server \Z
  :status {:read no-read
           :write (fn [out status]
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

;; server commands
;
;; the commands represent actions the connection may take in response to some message
;; they are simple functions that can call each other directly, though they can also be enqueued
;; through the connections :cmd-buf queue (in :conn-state) this will later be useful
;; for shared concerns (e.g 'what is a connection doing') and to allow for termination mid query
;; in certain scenarios

(defn- cmd-write-msg
  "Writes out a single message given a definition (msg-def) and optional data record."
  ([conn msg-def]
   (log/debug "Writing server message" (select-keys msg-def [:char8 :name]))
   (.writeByte @(:out conn) (byte (:char8 msg-def)))
   (.writeInt @(:out conn) 4))
  ([conn msg-def data]
   (log/debug "Writing server message (with body)" (select-keys msg-def [:char8 :name]))
   (.writeByte @(:out conn) (byte (:char8 msg-def)))
   (let [bytes-out (ByteArrayOutputStream.)
         msg-out (DataOutputStream. bytes-out)
         _ ((:write msg-def) msg-out data)
         arr (.toByteArray bytes-out)]
     (.writeInt @(:out conn) (+ 4 (alength arr)))
     (.write @(:out conn) arr))))

(defn cmd-send-ready
  "Sends a msg-ready with the given status - eg (cmd-send-ready conn :ok)"
  [conn status]
  (cmd-write-msg conn msg-ready {:status status}))

(defn cmd-send-error
  "Sends an error back to the client (e.g (cmd-send-error conn (err-protocol \"oops!\")).

  If the connection is operating in the :extended protocol mode, any error causes the connection to skip
  messages until a msg-sync is received."
  [{:keys [conn-status, conn-state] :as conn} err]

  ;; error seen while in :extended mode, start skipping messages until sync received
  (when (= :extended (:protocol @conn-state))
    (swap! conn-state assoc :skip-until-sync true))

  (cmd-write-msg conn msg-error-response {:error-fields err})

  (when (= :running @conn-status)
    (cmd-send-ready conn :idle)))

(defn cmd-write-canned-response [conn canned-response]
  (let [{:keys [q, rows]} canned-response]
    (doseq [row rows]
      (cmd-write-msg conn msg-data-row {:vals (mapv (fn [v] (if (bytes? v) v (utf8 v))) row)}))
    (cmd-write-msg conn msg-command-complete {:command (str (statement-head q) " " (count rows))})))

(defn cmd-close
  "Closes a prepared statement or portal that was opened with bind / parse."
  [{:keys [conn-state] :as conn} {:keys [close-type, close-name]}]
  (let [coll-k (case close-type
                 :portal :portals
                 :prepared-stmt :prepared-statements
                 (Object.))]
    ;; todo if removing a portal, do I need also remove the prepared statement, or vice versa?
    (swap! conn-state [coll-k] dissoc close-name)
    (cmd-write-msg conn msg-close-complete)))

(defn cmd-terminate
  "Causes the connection to start closing."
  [{:keys [conn-status]}]
  (compare-and-set! conn-status :running :closing))

(defn cmd-startup
  "A command that negotiates startup with the client, including ssl negotiation, and sending the state of the servers
  :server-parameters."
  [conn]
  (let [in @(:in conn)
        out @(:out conn)
        server-state (:server-state (:server conn))

        {:keys [version] :as msg} (read-version in)

        err
        (case version
          :cancel (err-protocol-violation "Cancellation is not supported")
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

        err
        (cond
          err err
          (= :gssenc version) (err-protocol-violation "GSSAPI is not supported")
          (not= :30 version) (err-protocol-violation "Unknown protocol version"))]


    (when-not err
      (cmd-write-msg conn msg-auth {:result 0})
      (doseq [[k v] (:parameters @server-state)]
        (cmd-write-msg conn msg-parameter-status {:parameter k, :value v}))
      (cmd-send-ready conn :idle)

      (let [{:keys [startup-parameters]} (read-startup-parameters msg-in)]
        (doseq [[k v] startup-parameters]
          (set-session-parameter conn k v))))

    (when err
      (cmd-send-error conn err)
      (cmd-terminate conn))))

(defn cmd-exec-query
  "Given a statement of type :query will execute it against the servers :node and send the results."
  [{:keys [server] :as conn}
   {:keys [ast
           query
           transformed-query
           arg-types
           params
           param-format
           _result-format]}]
  (let [node (:node server)

        xtify-param
        (fn [param-idx param]
          (let [_param-oid (nth arg-types param-idx nil)
                param-format (nth param-format param-idx nil)
                param-format (or param-format (nth param-format param-idx :text))

                parsed (case param-format
                         :text (String. ^bytes param StandardCharsets/UTF_8)
                         (throw (Exception. "Binary encoded parameters not implemented yet")))]
            parsed))

        xt-params (vec (map-indexed xtify-param params))

        projection (mapv keyword (query-projection ast))

        ;; todo result format

        jsonfn (comp utf8 json-str)
        tuplefn (if (seq projection) (apply juxt (map (partial comp jsonfn) projection)) (constantly []))
        [err rows]
        (try
          [nil (c2/sql-query node transformed-query (if (seq xt-params) {:? xt-params} {}))]
          (catch Throwable e
            [(err-internal e)]))]
    (if err
      (cmd-send-error conn err)
      (do
        (doseq [row rows]
          (cmd-write-msg conn msg-data-row {:vals (tuplefn row)}))
        (cmd-write-msg conn msg-command-complete {:command (str (statement-head query) " " (count rows))})))))

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
            (assoc defaults :column-name col)))
        data {:columns (mapv apply-defaults cols)}]
    (cmd-write-msg conn msg-row-description data)))

(defn cmd-describe-canned-response [conn canned-response]
  (let [{:keys [cols]} canned-response]
    (cmd-send-row-description conn cols)))

(defn cmd-describe-query [conn ast]
  (cmd-send-row-description conn (query-projection ast)))

(defn cmd-describe
  "Sends description messages (e.g msg-row-description) to the client for a prepared statement or portal."
  [{:keys [conn-state] :as conn} {:keys [describe-type, describe-name] :as msg}]
  (let [coll-k (case describe-type
                 :portal :portals
                 :prepared-stmt :prepared-statements
                 (Object.))
        {:keys [statement-type
                ast
                canned-response] :as stmt}
        (get-in @conn-state [coll-k describe-name])]

    (when (= :prepared-statements describe-type)
      ;; todo send param desc
      )

    (case statement-type
      :empty-query (cmd-write-msg conn msg-no-data)
      :canned-response (cmd-describe-canned-response conn canned-response)
      :set-session-parameter (cmd-write-msg conn msg-no-data)
      :ignore (cmd-write-msg conn msg-no-data)
      :query (cmd-describe-query conn ast)
      (cmd-send-error conn (err-protocol-violation "no such description possible")))))

(defn cmd-exec-stmt
  "Given some kind of statement (from interpret-sql), will execute it. For some statements, this does not mean
  the xt node gets hit - e.g SET some_session_parameter = 42 modifies the connection, not the database."
  [{:keys [conn-state] :as conn}
   {:keys [query
           ast
           statement-type
           canned-response
           parameter
           value]
    :as stmt}]

  (when (= :simple (:protocol @conn-state))
    (case statement-type
      :canned-response (cmd-describe-canned-response conn canned-response)
      :query (cmd-describe-query conn ast)
      nil))

  (case statement-type
    :empty-query (cmd-write-msg conn msg-empty-query)
    :canned-response (cmd-write-canned-response conn canned-response)
    :set-session-parameter (set-session-parameter conn parameter value)
    :ignore (cmd-write-msg conn msg-command-complete {:command (statement-head query)})
    :query (cmd-exec-query conn stmt))

  (when (= :simple (:protocol @conn-state))
    (cmd-send-ready conn :idle)))

(defn cmd-simple-query [{:keys [conn-state] :as conn} {:keys [query]}]
  (let [{:keys [err] :as stmt} (interpret-sql query)]
    (swap! conn-state assoc :protocol :simple)
    (if err
      (cmd-send-error conn err)
      (cmd-exec-stmt conn stmt))))

(defn cmd-sync
  "Sync commands are sent by the client to commit transactions (we do not do anything here yet),
  and to clear the error state of a :extended mode series of commands (e.g the parse/bind/execute dance)"
  [{:keys [conn-state] :as conn}]
  (swap! conn-state dissoc :skip-until-sync, :protocol)
  (cmd-send-ready conn :idle))

(defn cmd-flush
  "Flushes any pending output to the client."
  [conn]
  (.flush @(:out conn)))

(defn cmd-enqueue-cmd
  "Enqueues another command for execution later (puts it at the back of the :cmd-buf queue).

  Commands can be queued with (non-conn) args using vectors like so (enqueue-cmd conn [#'cmd-simple-query {:query some-sql}])"
  [{:keys [conn-state]} & cmds]
  (swap! conn-state update :cmd-buf (fnil into PersistentQueue/EMPTY) cmds))

(defn cmd-parse
  "Responds to a msg-parse message that creates a prepared-statement."
  [{:keys [conn-state
           cid
           server] :as conn}
   {:keys [stmt-name
           query
           arg-types]}]

  ;; put the conn in the extend protocol
  (swap! conn-state assoc :protocol :extended)

  (log/debug "Parsing" {:stmt-name stmt-name,
                        :query query
                        :port (:port server)
                        :cid cid})
  (let [{:keys [err] :as stmt} (interpret-sql query)
        stmt (assoc stmt :arg-types arg-types)
        _ (when err (cmd-send-error conn err))
        _ (when-not err (swap! conn-state assoc-in [:prepared-statements stmt-name] stmt))
        _ (when-not err (cmd-write-msg conn msg-parse-complete))]))

(defn cmd-bind [{:keys [conn-state] :as conn}
                {:keys [portal-name
                        stmt-name
                        param-format
                        params
                        result-format]}]
  (let [stmt (get-in @conn-state [:prepared-statements stmt-name])
        _ (when-not stmt (cmd-send-error conn (err-protocol-violation "no prepared statement")))
        _ (when stmt (swap! conn-state assoc-in [:portals portal-name]
                            (assoc stmt :param-format param-format
                                        :params params
                                        :result-format result-format)))
        _ (when stmt (cmd-write-msg conn msg-bind-complete))]))

(defn cmd-execute
  "Handles a msg-execute to run a previously bound portal (via msg-bind)."
  [{:keys [conn-state] :as conn}
   {:keys [portal-name
           limit]}]
  (if-some [stmt (get-in @conn-state [:portals portal-name])]
    (cmd-exec-stmt conn (assoc stmt :limit limit))
    (cmd-send-error conn (err-protocol-violation "no such portal"))))


;; connection loop
;; we run a blocking io server so a connection is simple a loop sitting on some thread

(defn- conn-loop [conn]
  (let [{:keys [cid, server, ^Socket socket, in, conn-status, conn-state]} conn
        {:keys [port, server-status]} server]

    (when-not (compare-and-set! conn-status :new :running)
      (log/error "Connect loop requires a newly initialised connection" {:cid cid, :port port}))

    (cmd-enqueue-cmd conn [#'cmd-startup])

    (loop []
      (cond
        ;; the connection is closing right now
        ;; let it close.
        (not= :running @conn-status)
        (->> {:port port
              :cid cid}
             (log/debug "Connection loop exiting (closing)"))

        ;; if the server is draining, we may later allow connections to finish their queries
        ;; (consider policy - server has a drain limit but maybe better to be explicit here as well)
        (and (= :draining @server-status) (not= :extended (:protocol @conn-state)))
        (do (->> {:port port
                  :cid cid}
                 (log/debug "Connection loop exiting (draining)"))
            ;; TODO I think I should send an error, but if I do it causes a crash on the client?
            #_(cmd-send-error conn (err-admin-shutdown "draining connections"))
            (compare-and-set! conn-status :running :closing))

        ;; well, it won't have been us, as we would drain first
        (.isClosed socket)
        (do (log/debug "Connection closed unexpectedly" {:port port, :cid cid})
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

          (when cmd-fn
            (->> {:port port
                  :cid cid
                  :cmd cmd-fn}
                 (log/debug "Executing buffered command"))
            (apply cmd-fn conn cmd-args))

          (recur))

        ;; go idle until we receive another msg from the client
        :else
        (let [{:keys [msg-char8, msg-in]} (read-typed-msg @in)
              msg-var (client-msgs msg-char8)

              _
              (->> {:port port,
                    :cid cid,
                    :msg (or msg-var msg-char8)
                    :char8 msg-char8}
                   (log/debug "Read client msg"))]

          (when (:skip-until-sync @conn-state)
            (if (= msg-var #'msg-sync)
              (cmd-enqueue-cmd conn [#'cmd-sync])
              (->> {:port port,
                    :cid cid,
                    :msg (or msg-var msg-char8)
                    :char8 msg-char8}
                   (log/warn "Skipping msg until next sync due to error in extended protocol"))))

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
  [server conn-socket]
  (let [{:keys [node, server-state, port]} server
        cid (:next-cid (swap! server-state update :next-cid (fnil inc 0)))
        in (delay (DataInputStream. (.getInputStream conn-socket)))
        out (delay (DataOutputStream. (.getOutputStream conn-socket)))
        conn-status (atom :new)
        conn-state (atom {:close-promise (promise)})
        conn
        (map->Connection
          {:server server,
           :node node,
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
        (log/debug "Starting connection loop" {:port port, :cid cid})
        (conn-loop conn)
        (catch SocketException e
          (when (= "Broken pipe (Write failed)" (.getMessage e))
            (log/debug "Client closed socket while we were writing" {:port port, :cid cid})
            (.close conn-socket))
          ;; socket being closed is normal, otherwise log.
          (when-not (.isClosed conn-socket)
            (log/error e "An exception was caught during connection" {:port port, :cid cid})))
        (catch EOFException _
          (log/debug "Connection closed by client" {:port port, :cid cid}))
        (catch Throwable e
          (log/error e "An exception was caught during connection" {:port port, :cid cid}))))

    ;; right now we'll deregister and cleanup regardless, but later we may
    ;; want to leave conns around for a bit, so you can look at their state and debug
    (cleanup-connection-resources conn)

    (log/debug "Connection ended" {:cid cid, :port port})

    ;; can be used to co-ordinate waiting for close
    (when-some [close-promise (:close-promise @conn-state)]
      (when-not (realized? close-promise)
        (deliver close-promise true)))))

(defn- accept-loop
  "Runs an accept loop on the current thread (intended to be the Server's :accept-thread).

  While the server is running, tries to .accept connections on its :accept-socket. Once accepted,
  a connection is created on the servers :thread-pool via the (connect) function."
  [server]
  (let [{:keys [^ServerSocket accept-socket,
                server-status,
                server-state
                ^ExecutorService thread-pool,
                port]} server]
    (when-not (compare-and-set! server-status :starting :running)
      (throw (Exception. "Accept loop requires a newly initialised server")))

    (log/debug "Pgwire server listening on" port)

    (try
      (loop []

        (cond
          (.isInterrupted (Thread/currentThread)) nil

          (.isClosed @accept-socket)
          (log/debug "Accept socket closed, exiting accept loop")

          (#{:draining :running} @server-status)
          (do

            ;; set a low timeout to leave accept early if needed
            (when (= :draining @server-status)
              (.setSoTimeout @accept-socket 10))

            (try
              (let [conn-socket (.accept @accept-socket)]

                (when-some [exc (:injected-accept-exc @server-state)]
                  (swap! server-state dissoc :injected-accept-exc)
                  (.close conn-socket)
                  (throw exc))

                (.setTcpNoDelay conn-socket true)
                (if (= :running @server-status)
                  ;; todo fix buffer on tp? q gonna be infinite right now
                  (.submit thread-pool ^Runnable (fn [] (connect server conn-socket)))
                  (do
                    (err-cannot-connect-now "server shutting down")
                    (.close conn-socket))))
              (catch SocketException e
                (when (and (not (.isClosed @accept-socket))
                           (not= "Socket closed" (.getMessage e)))
                  (log/warn e "Accept socket exception" {:port port})))
              (catch java.net.SocketTimeoutException e
                (when (not= "Accept timed out" (.getMessage e))
                  (log/warn e "Accept time out" {:port port})))
              (catch IOException e
                (log/warn e "Accept IO exception" {:port port})))
            (recur))

          :else nil))
      ;; should already be stopping if interrupted
      (catch InterruptedException _ (.interrupt (Thread/currentThread)))
      ;; for the unknowns, do not stop - leave in an error state
      ;; so it can be inspected.
      (catch Throwable e
        (compare-and-set! server-status :running :error)
        (util/try-close @accept-socket)
        (when-not (:silent-accept @server-state)
          (log/error e "Exception caught on accept thread" {:port port}))))

    (when (.isInterrupted (Thread/currentThread))
      (swap! server-state assoc :accept-interrupted true)
      (log/debug "Accept loop interrupted, exiting accept loop"))))

(defn- create-server [{:keys [node,
                              port,
                              num-threads,
                              drain-wait
                              unsafe-init-state]}]
  (assert node ":node is required")
  (let [self (atom nil)
        srvr (map->Server
               {:port port
                :node node
                :accept-socket (delay (ServerSocket. port))
                :accept-thread (Thread. ^Runnable (fn [] (accept-loop @self)) (str "pgwire-server-accept-" port))
                :thread-pool (Executors/newFixedThreadPool num-threads (util/->prefix-thread-factory "pgwire-connection-"))
                :server-status (atom :new)
                :server-state (atom (merge unsafe-init-state {:drain-wait drain-wait
                                                              :parameters {"server_version" "14"
                                                                           "server_encoding" "UTF8"
                                                                           "client_encoding" "UTF8"
                                                                           "TimeZone" "UTC"}}))})]
    (reset! self srvr)
    srvr))

(defn serve
  "Creates and starts a pgwire server.

  Options:

  :port (default 5432)
  :num-threads (bounds the number of client connections, default 42)

  Returns the opaque server object.

  Stop with .close (its Closeable)

  It will be addressable in an emergency by port with (get @#'pgwire/servers port).
  OR nuke everything with (stop-all)."
  ([node] (serve node {}))
  ([node opts]
   (let [defaults {:port 5432
                   :num-threads 42
                   ;; 5 seconds of draining
                   :drain-wait 5000}
         cfg (assoc (merge defaults opts) :node node)]
     (doto (create-server cfg)
       (start-server cfg)))))

(defn stop-all
  "Stops all running pgwire servers (from orbit, its the only way to be sure)."
  []
  (run! stop-server (vec (.values servers))))

(comment

  ;; pre-req
  (do
    (require '[juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc])
    (require 'dev)

    (dev/go)

    (def server (or (.get servers 5432) (serve dev/node {:port 5432})))

    (defn i [& rows]
      (assert (:node server) "No running pgwire!")
      (->> (for [row rows
                 :let [auto-id (str "pgwire-" (random-uuid))]]
             [:put (merge {:_id auto-id} row)])
           (api/submit-tx (:node server))
           deref))

    (defn read-xtdb [o]
      (if (instance? org.postgresql.util.PGobject o)
        (json/read-str (str o))
        o))

    (defn try-connect []
      (with-open [c (jdbc/get-connection "jdbc:postgresql://:5432/test?user=test&password=test")]
        :ok))

    (defn q [sql]
      (with-open [c (jdbc/get-connection "jdbc:postgresql://:5432/test?user=test&password=test")]
        (mapv #(update-vals % read-xtdb) (jdbc/execute! c (if (vector? sql) sql [sql])))))
    )
  ;; eval for setup ^

  server

  (.close server)

  (def server (serve dev/node {:port 5432}))

  ;; insert something, be aware as we haven't set an :_id, it will keep creating rows each time it is evaluated!
  (i {:user "wot", :bio "Hello, world", :age 34})

  ;; query with aliases otherwise XT explodes, and no *.
  (q "SELECT u.user, u.bio, u.age FROM users u where u.user = 'wot'")

  ;; you can use values for inline SQL tables
  (q "SELECT * FROM (VALUES (1 YEAR, true), (3.14, 'foo')) AS x (a, b)")

  )

(ns core2.sql.pgwire
  (:require [core2.api :as c2]
            [core2.sql.plan :as plan]
            [core2.sql.analyze :as sem]
            [core2.sql.parser :as parser]
            [core2.rewrite :as r]
            [core2.local-node :as node]
            [core2.util :as util]
            [clojure.string :as str]
            [clojure.data.json :as json])
  (:import java.nio.charset.StandardCharsets
           [java.io Closeable ByteArrayOutputStream DataInputStream DataOutputStream InputStream IOException OutputStream PushbackInputStream EOFException]
           [java.net Socket ServerSocket]
           [java.util.concurrent Executors ExecutorService]
           [java.util HashMap]
           [org.apache.arrow.vector PeriodDuration]
           [java.time LocalDate]))

(set! *warn-on-reflection* true)

(defn- read-c-string ^String [^InputStream in]
  (loop [baos (ByteArrayOutputStream.)
         x (.read in)]
    (if (zero? x)
      (String. (.toByteArray baos) StandardCharsets/UTF_8)
      (recur (doto baos
               (.write x))
             (.read in)))))

(defn- write-c-string [^OutputStream out ^String s]
  (.write out (.getBytes s StandardCharsets/UTF_8))
  (.write out 0))

(defn- send-message-with-body [^DataOutputStream out ^long message-type build-message-fn]
  (let [baos (ByteArrayOutputStream.)
        _ (build-message-fn (DataOutputStream. baos))
        message (.toByteArray baos)]
    (doto out
      (.writeByte (byte message-type))
      (.writeInt (+ Integer/BYTES (alength message)))
      (.write message))))

(defn- send-authentication-ok [^DataOutputStream out]
  (doto out
    (.writeByte (byte \R))
    (.writeInt 8)
    (.writeInt 0)))

(def ^:private backend-status->indicator-code {:idle \I
                                               :transaction \T
                                               :failed-transaction \E})

(defn- send-ready-for-query [^DataOutputStream out status]
  (doto out
    (.writeByte (byte \Z))
    (.writeInt 5)
    (.writeByte (byte (get backend-status->indicator-code status)))))

(defn- send-message-without-body [^DataOutputStream out ^long message-type]
  (doto out
    (.writeByte (byte message-type))
    (.writeInt 4)))

(defn- send-empty-query-response [^DataOutputStream out]
  (send-message-without-body out (byte \I)))

(defn- send-no-data [^DataOutputStream out]
  (send-message-without-body out (byte \n)))

(defn- send-parse-complete [^DataOutputStream out]
  (send-message-without-body out (byte \1)))

(defn- send-bind-complete [^DataOutputStream out]
  (send-message-without-body out (byte \2)))

(defn- send-close-complete [^DataOutputStream out]
  (send-message-without-body out (byte \3)))

(defn- send-portal-suspended [^DataOutputStream out]
  (send-message-without-body out (byte \s)))

(defn- send-data-row [^DataOutputStream out row]
  (send-message-with-body out
                          (byte \D)
                          (fn [^DataOutputStream out]
                            (.writeShort out (count row))
                            (doseq [^bytes column row]
                              (if column
                                (do (.writeInt out (alength column))
                                    (.write out column))
                                (.writeInt out -1))))))

(defn- send-parameter-description [^DataOutputStream out parameter-oids]
  (send-message-with-body out
                          (byte \t)
                          (fn [^DataOutputStream out]
                            (.writeShort out (count parameter-oids))
                            (doseq [^long oid parameter-oids]
                              (.writeInt out oid)))))

(defn- send-parameter-status [^DataOutputStream out ^String k ^String v]
  (send-message-with-body out
                          (byte \S)
                          (fn [out]
                            (doto out
                              (write-c-string k)
                              (write-c-string v)))))

(defn- send-command-complete [^DataOutputStream out ^String command]
  (send-message-with-body out
                          (byte \C)
                          (fn [out]
                            (write-c-string out command))))

(def ^:private field-type->field-code {:localized-severity \S
                                       :severity \V
                                       :sql-state \C
                                       :message \M
                                       :detail \D
                                       :position \P
                                       :where \W})

(defn- send-error-response [^DataOutputStream out field-type->field-value]
  (send-message-with-body out
                          (byte \E)
                          (fn [^DataOutputStream out]
                            (doseq [[k v] field-type->field-value]
                              (.writeByte out (byte (get field-type->field-code k)))
                              (write-c-string out v))
                            (.writeByte out 0))))

(def ^:private ^:const ^long varchar-oid 1043)
(def ^:private ^:const ^long json-oid 114)

(def ^:private ^:const ^long typlen-varlena -1)
(def ^:private ^:const ^long typmod-none -1)

(def ^:private ^:const ^long text-format-code 0)

(defn- col-desc [col]
  (if (string? col)
    {:column-name col
     :table-oid 0
     :column-attribute-number 0
     :oid json-oid
     :data-type-size typlen-varlena
     :type-modifier typmod-none
     :format-code text-format-code}
    {:column-name (:column-name col)
     :table-oid 0
     :column-attribute-number 0
     :oid (:oid col json-oid)
     :data-type-size typlen-varlena
     :type-modifier typmod-none
     :format-code text-format-code}))

(defn- send-row-description [^DataOutputStream out columns]
  (send-message-with-body out
                          (byte \T)
                          (fn [^DataOutputStream out]
                            (.writeShort out (count columns))
                            (doseq [{:keys [column-name
                                            table-oid
                                            column-attribute-number
                                            oid
                                            data-type-size
                                            type-modifier
                                            format-code]}
                                    (map col-desc columns)]
                              (doto out
                                (write-c-string column-name)
                                (.writeInt table-oid)
                                (.writeShort column-attribute-number)
                                (.writeInt oid)
                                (.writeShort data-type-size)
                                (.writeInt type-modifier)
                                (.writeShort format-code))))))

(def ^:private message-code->type '{\P :pgwire/parse
                                    \B :pgwire/bind
                                    \Q :pgwire/query
                                    \H :pgwire/flush
                                    \S :pgwire/sync
                                    \X :pgwire/terminate
                                    \D :pgwire/describe
                                    \E :pgwire/execute})

(defmulti ^:private parse-message (fn [message-type in] message-type))

(defmethod parse-message :pgwire/parse [_ ^DataInputStream in]
  {:pgwire.parse/prepared-statement (read-c-string in)
   :pgwire.parse/query-string (read-c-string in)
   :pgwire.parse/parameter-oids (vec (repeatedly (.readShort in) #(.readInt in)))})

(defmethod parse-message :pgwire/bind [_ ^DataInputStream in]
  {:pgwire.bind/portal (read-c-string in)
   :pgwire.bind/prepared-statement (read-c-string in)
   :pgwire.bind/parameter-format-codes (vec (repeatedly (.readShort in) #(.readInt in)))
   :pgwire.bind/parameters (vec (repeatedly (.readShort in)
                                            #(let [size (.readInt in)]
                                               (when-not (= -1 size)
                                                 (let [body (byte-array size)]
                                                   (.readFully in body)
                                                   body)))))
   :pgwire.bind/result-format-codes (vec (repeatedly (.readShort in) #(.readShort in)))})

(defmethod parse-message :pgwire/query [_ ^DataInputStream in]
  {:pgwire.query/query-string (read-c-string in)})

(defmethod parse-message :pgwire/execute [_ ^DataInputStream in]
  {:pgwire.execute/portal (read-c-string in)
   :pgwire.execute/max-rows (.readInt in)})

(defmethod parse-message :pgwire/describe [_ ^DataInputStream in]
  (case (char (.readByte in))
    \P {:pgwire.describe/portal (read-c-string in)}
    \S {:pgwire.describe/statement (read-c-string in)}))

(defmethod parse-message :default [_ _]
  {})

(defn- statement-command [parse-message]
  (first (str/split (:pgwire.parse/query-string parse-message) #"\s+")))

(defn- empty-query? [parse-message]
  (str/blank? (:pgwire.parse/query-string parse-message)))

;; TODO: should really just use direct_sql_statement rule, but that
;; requires some hacks.
(defn- strip-query-semi-colon [sql]
  (str/replace sql #";\s*$" ""))

(defmulti handle-message (fn [session message out]
                           (:pgwire/type message)))

(def ^:private sql-state-syntax-error "42601")

(defn utf8 [s] (.getBytes (str s) StandardCharsets/UTF_8))

(def canned-responses
  "Some pre-baked responses to common queries issued as setup by Postgres drivers, e.g SQLAlchemy"
  [{:q "select pg_catalog.version()"
    :cols [{:column-name "version" :oid varchar-oid}]
    :rows [["PostgreSQL 14.2"]]}
   {:q "show standard_conforming_strings"
    :cols [{:column-name "standard_conforming_strings" :oid varchar-oid}]
    :rows [["on"]]}
   {:q "select current_schema"
    :cols [{:column-name "current_schema" :oid varchar-oid}]
    :rows [["public"]]}
   {:q "show transaction isolation level"
    :cols [{:column-name "transaction_isolation" :oid varchar-oid}]
    :rows [["read committed"]]}
   ;; jdbc meta getKeywords (hibernate)
   ;; I think this should work, but it causes some kind of low level issue, likely
   ;; because our query protocol impl is broken, or partially implemented.
   ;; java.lang.IllegalStateException: Received resultset tuples, but no field structure for them
   {:q "select string_agg(word, ',') from pg_catalog.pg_get_keywords()"
    :cols [{:column-name "col1" :oid varchar-oid}]
    :rows [["xtdb"]]}])

;; yagni, is everything upper'd anyway by drivers / server?
(defn- probably-same-query? [s substr]
  ;; todo I bet this may cause some amusement. Not sure what to do about non-parsed query matching, it'll do for now.
  (str/starts-with? (str/lower-case s) (str/lower-case substr)))

(defn get-canned-response [sql-str]
  (when sql-str (first (filter #(probably-same-query? sql-str (:q %)) canned-responses))))

(def ignore-parse-bind-strings
  (-> ["set"
       "begin"
       "commit"
       "rollback"]
      (concat (map :q canned-responses))
      set))

(defn- ignore-parse-bind? [message]
  (boolean (or (empty-query? message)
               (some (partial probably-same-query? (:pgwire.parse/query-string message)) ignore-parse-bind-strings))))

(defmethod handle-message :pgwire/parse [session message out]
  (let [message (if (ignore-parse-bind? message)
                  message
                  (with-meta message {:tree (parser/parse (strip-query-semi-colon (:pgwire.parse/query-string message)))}))]
    (if-let [parse-failure (when (parser/failure? (:tree (meta message))) (:tree (meta message)))]
      (do (send-error-response out {:severity "ERROR"
                                    :localized-severity "ERROR"
                                    :sql-state sql-state-syntax-error
                                    :message (first (str/split-lines (pr-str parse-failure)))
                                    :detail (parser/failure->str parse-failure)
                                    :position (str (inc (:idx parse-failure)))})
          session)
      (do (send-parse-complete out)
          (assoc-in session
                    [:prepared-statements (get message :pgwire.parse/prepared-statement)]
                    message)))))

(defmethod handle-message :pgwire/bind [session message out]
  (let [parse-message (get-in session [:prepared-statements (:pgwire.bind/prepared-statement message)])
        message (if (ignore-parse-bind? parse-message)
                  message
                  (with-meta message (plan/plan-query (:tree (meta parse-message)))))]
    (if-let [error (first (:errs (meta message)))]
      (do (send-error-response out {:severity "ERROR"
                                    :localized-severity "ERROR"
                                    :sql-state sql-state-syntax-error
                                    :message (first (str/split-lines error))
                                    :detail error})
          session)
      (do (send-bind-complete out)
          (assoc-in session
                    [:portals (get message :pgwire.bind/portal)]
                    message)))))

(defmethod handle-message :pgwire/flush [session message ^DataOutputStream out]
  (.flush out)
  session)

(defmethod handle-message :pgwire/sync [session message ^DataOutputStream out]
  (send-ready-for-query out :idle)
  session)

(defmethod handle-message :pgwire/terminate [session message out]
  (util/try-close out)
  session)

(defn- query-projection [tree]
  (binding [r/*memo* (HashMap.)]
    (->> (sem/projected-columns (r/$ (r/vector-zip tree) 1))
         (first)
         (mapv (comp name plan/unqualified-projection-symbol)))))

(defmethod handle-message :pgwire/describe [session message out]

  (cond
    (:pgwire.describe/portal message)
    (let [bind-message (get-in session [:portals (:pgwire.describe/portal message)])
          parse-message (get-in session [:prepared-statements (:pgwire.bind/prepared-statement bind-message)])]
      (cond
        (get-canned-response (:pgwire.parse/query-string parse-message))
        (let [{:keys [cols]} (get-canned-response (:pgwire.parse/query-string parse-message))]
          (send-row-description out cols))

        (= "SELECT" (str/upper-case (statement-command parse-message)))
        (send-row-description out (query-projection (:tree (meta parse-message))))

        :else (send-no-data out))
      session)

    (:pgwire.describe/statement message)
    (let [parse-message (get-in session [:prepared-statements (:pgwire.describe/statement message)])]
      (send-parameter-description out (:pgwire.parse/parameter-oids parse-message))
      (cond

        (get-canned-response (:pgwire.parse/query-string parse-message))
        (let [{:keys [cols]} (get-canned-response (:pgwire.parse/query-string parse-message))]
          (send-row-description out cols))

        (= "SELECT" (str/upper-case (statement-command parse-message)))
        (send-row-description out (query-projection (:tree (meta parse-message))))

        :else (send-no-data out))
      session)))

(extend-protocol json/JSONWriter
  PeriodDuration
  (-write [pd out options]
    (json/-write (str pd) out options))
  LocalDate
  (-write [ld out options]
    (json/-write (str ld) out options)))

(defn- json-str [obj]
  (json/write-str obj))

(defmethod handle-message :pgwire/execute [session message out]
  (let [bind-message (get-in session [:portals (:pgwire.execute/portal message)])
        parse-message (get-in session [:prepared-statements (:pgwire.bind/prepared-statement bind-message)])
        canned-response (get-canned-response (:pgwire.parse/query-string parse-message))]
    (cond
      (empty-query? parse-message)
      (do (send-empty-query-response out)
          session)

      canned-response
      (let [{:keys [rows]} canned-response]
        (doseq [row rows]
          (send-data-row out (mapv (fn [v] (if (bytes? v) v (utf8 v))) row)))
        (send-command-complete out (str (statement-command parse-message) " " (count rows)))
        session)

      (= "SET" (statement-command parse-message))
      (let [[_ k v] (re-find #"SET\s+(\w+)\s*=\s*'?(.*)'?" (strip-query-semi-colon (:pgwire.parse/query-string parse-message)))]
        (send-command-complete out (statement-command parse-message))
        (assoc-in session [:parameters k] v))

      (#{"BEGIN" "COMMIT" "ROLLBACK"} (statement-command parse-message))
      (do (send-command-complete out (statement-command parse-message))
          session)

      :else
      (let [node (:node (meta session))
            projection (mapv keyword (query-projection (:tree (meta parse-message))))
            result (c2/sql-query node (strip-query-semi-colon (:pgwire.parse/query-string parse-message)) {})]
        (doseq [row result]
          (send-data-row out (for [column (mapv row projection)]
                               (.getBytes (json-str column) StandardCharsets/UTF_8))))
        (send-command-complete out (str (statement-command parse-message) " " (count result)))
        session))))

(defn- error-message-buffer? [^bytes message]
  (and (pos? (alength message))
       (= \E (char (aget message 0)))))

(defmethod handle-message :pgwire/query [session message ^DataOutputStream out]
  (let [query-string (:pgwire.query/query-string message)
        baos (ByteArrayOutputStream.)
        null-out (DataOutputStream. baos)
        parse-message {:pgwire/type :pgwire/parse
                       :pgwire.parse/query-string query-string
                       :pgwire.parse/prepared-statement ""
                       :pgwire.parse/parameters []}
        session (handle-message session parse-message null-out)]
    (if (error-message-buffer? (.toByteArray baos))
      (do (.write out (.toByteArray baos))
          (send-ready-for-query out :idle)
          session)
      (let [baos (ByteArrayOutputStream.)
            null-out (DataOutputStream. baos)
            bind-message {:pgwire/type :pgwire/bind
                          :pgwire.bind/portal ""
                          :pgwire.bind/prepared-statement ""}
            session (handle-message session bind-message null-out)]
        (if (error-message-buffer? (.toByteArray baos))
          (do (.write out (.toByteArray baos))
              (send-ready-for-query out :idle)
              session)
          (let [session (cond->
                          session

                          (#{"SELECT" "SHOW"} (str/upper-case (statement-command parse-message)))
                          (handle-message
                            {:pgwire/type :pgwire/describe, :pgwire.describe/portal ""}
                            out)

                          true
                          (handle-message
                            {:pgwire/type :pgwire/execute, :pgwire.execute/portal ""}
                            out))]
            (send-ready-for-query out :idle)
            session))))))

(defmethod handle-message :default [session message _]
  session)

(def ^:private sql-state-internal-error "XX000")

(defn- pg-message-exchange [session ^Socket socket ^DataInputStream in ^DataOutputStream out]
  (loop [session session]
    (if (.isClosed socket)
      session
      (do
          (recur (try
                   (let [message-code (char (.readByte in))]
                     (if-let [message-type (get message-code->type message-code)]
                       (let [size (- (.readInt in) Integer/BYTES)
                             message (assoc (parse-message message-type in) :pgwire/type message-type)]
                         #_(prn message)
                         (handle-message session message out))
                       (throw (IllegalArgumentException. (str "unknown message code: " message-code)))))
                   (catch EOFException e
                     (send-ready-for-query out :idle)
                     session)
                   (catch Exception e
                     (send-error-response out {:severity "ERROR"
                                               :localized-severity "ERROR"
                                               :sql-state sql-state-internal-error
                                               :message (first (str/split-lines (or (.getMessage e) "")))
                                               :detail (str e)
                                               :where (with-out-str
                                                        (binding [*err* *out*]
                                                          (.printStackTrace e)))})
                     (send-ready-for-query out :idle)
                     session)))))))

(defn- parse-startup-message [^DataInputStream in]
  (loop [in (PushbackInputStream. in)
         acc {}]
    (let [x (.read in)]
      (if (zero? x)
        {:pgwire.startup-message/parameters acc}
        (do (.unread in (byte x))
            (recur in (assoc acc (read-c-string in) (read-c-string in))))))))

(def ^:private ssl-request 80877103)
(def ^:private gssenc-request 80877104)
(def ^:private startup-message 196608)

(defn- pg-establish-connection [{:keys [parameters] :as session} ^DataInputStream in ^DataOutputStream out]
  (loop []
    (let [size (- (.readInt in) (* 2 Integer/BYTES))
          handshake (.readInt in)]
      (cond
        (or (= ssl-request handshake)
            (= gssenc-request handshake))
        (do (.writeByte out (byte \N))
            (recur))

        (= startup-message handshake)
        (let [startup-message (parse-startup-message in)]
          #_(prn startup-message)
          (send-authentication-ok out)

          (doseq [[k v] parameters]
            (send-parameter-status out k v))

          (send-ready-for-query out :idle)

          (update session :parameters merge (:pgwire.startup-message/parameters startup-message)))

        :else
        (throw (IllegalArgumentException. (str "unknown handshake: " handshake)))))))

(defn- pg-conn [server ^Socket socket]
  (try
    (with-open [in (DataInputStream. (.getInputStream socket))
                out (DataOutputStream. (.getOutputStream socket))]
      (let [session (with-meta
                      {:parameters (:parameters server)
                       :prepared-statements {}
                       :portals {}}
                      {:node (:node server)})
            session (pg-establish-connection session in out)]
        (pg-message-exchange session socket in out)))
    (catch Throwable t
      #_(prn t)
      (throw t))))

(defn- pg-accept [{:keys [^ServerSocket server-socket ^ExecutorService pool] :as server}]
  (while (not (.isClosed server-socket))
    (try
      (let [socket (.accept server-socket)]
        (try
          (.setTcpNoDelay socket true)
          (.submit pool ^Runnable #(pg-conn server socket))
          (catch Throwable t
            (util/try-close socket)
            (throw t))))
      (catch IOException e
        (when-not (.isClosed server-socket)
          (throw e))))))

(defrecord PgWireServer [node ^ServerSocket server-socket ^ExecutorService pool parameters]
  Closeable
  (close [_]
    (util/shutdown-pool pool)
    (util/try-close server-socket)))

(defn ->pg-wire-server ^core2.sql.pgwire.PgWireServer [node {:keys [server-parameters ^long port ^long num-threads]}]
  (let [server-socket (ServerSocket. port)
        pool (Executors/newFixedThreadPool num-threads (util/->prefix-thread-factory "pgwire-connection-"))
        server (->PgWireServer node server-socket pool server-parameters)]
    (doto (Thread. #(pg-accept server))
      (.start))
    server))

(comment

  ;; pre-req
  (do
    (require '[juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc])
    (require 'dev)

    (when-not (bound? #'dev/node) (dev/go))

    (defonce server nil)

    (defn stop-server []
      (when server
        (some-> (:server server) .close)
        (println "Server stopped")
        (alter-var-root #'server (constantly nil))))

    (defn start-server
      ([] (start-server 5432))
      ([port]
       (stop-server)
       (let [node dev/node
             server (->pg-wire-server node
                                      {:server-parameters {"server_version" "14"
                                                           "server_encoding" "UTF8"
                                                           "client_encoding" "UTF8"
                                                           "TimeZone" "UTC"}
                                       :port port
                                       :num-threads 16})
             srv {:port port
                  :node node
                  :server server}]
         (println "Listening on" port)
         (alter-var-root #'server (constantly srv))
         srv))))
  ;; eval for setup ^

  server

  (start-server 5432)

  (stop-server)

  ;;;; clojure JDBC
  ;;

  (defn read-xtdb [o]
    (if (instance? org.postgresql.util.PGobject o)
      (json/read-str (str o))
      o))

  (defn q [s]
    (with-open [c (jdbc/get-connection "jdbc:postgresql://:5432/test?user=test&password=test")]
      (mapv #(update-vals % read-xtdb) (jdbc/execute! c [s]))))

  (q "SELECT * FROM (VALUES (1 YEAR, true), (3.14, 'foo')) AS x (a, b)")

  ;; canned query example (hibernate calls something like this (with a big where clause) via JDBC db metadata.
  (q "SELECT string_agg(word, ',') from pg_catalog.pg_get_keywords()")

  (q "SELECT x.a from x where x.a = 42")

  )

(ns core2.sql.pgwire
  (:require [core2.sql.plan :as plan]
            [core2.sql.analyze :as sem]
            [core2.sql :as sql]
            [core2.rewrite :as r]
            [core2.util :as util]
            [clojure.string :as str])
  (:import java.nio.charset.StandardCharsets
           [java.io Closeable ByteArrayOutputStream
            DataInputStream DataOutputStream InputStream IOException OutputStream PushbackInputStream]
           [java.net Socket ServerSocket]
           [java.util.concurrent Executors ExecutorService]))

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

(defn- send-ready-for-query [^DataOutputStream out ^long status]
  (doto out
    (.writeByte (byte \Z))
    (.writeInt 5)
    (.writeByte (byte status))))

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

(defn- send-row-description [^DataOutputStream out columns]
  (send-message-with-body out
                          (byte \T)
                          (fn [^DataOutputStream out]
                            (.writeShort out (count columns))
                            (doseq [column columns
                                    :let [table-oid 0
                                          column-attribute-number 0
                                          oid 0
                                          data-type-size 0
                                          type-modifier 0
                                          format-code 0]]
                              (doto out
                                (write-c-string column)
                                (.writeInt table-oid)
                                (.writeShort column-attribute-number)
                                (.writeInt oid)
                                (.writeShort data-type-size)
                                (.writeInt type-modifier)
                                (.writeShort format-code))))))

(def ^:private message-code->type '{\P :pgwire/parse
                                    \B :pgwire/bind
                                    \Q :pgwire/query
                                    \F :pgwire/flush
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

(defmulti handle-message (fn [session message out]
                           (:pgwire/type message)))

(defmethod handle-message :pgwire/parse [session message out]
  (send-parse-complete out)
  (assoc-in session
            [:prepared-statements (get message :pgwire.parse/prepared-statement)]
            (if (= "SET" (statement-command message))
              message
              (with-meta message {:tree (sql/parse (:pgwire.parse/query-string message))}))))

(defmethod handle-message :pgwire/bind [session message out]
  (send-bind-complete out)
  (let [parse-message (get-in session [:prepared-statements (:pgwire.bind/prepared-statement message)])]
    (assoc-in session
              [:portals (get message :pgwire.bind/portal)]
              (if (= "SET" (statement-command parse-message))
                message
                (with-meta message (plan/plan-query (:tree (meta parse-message))))))))

(defmethod handle-message :pgwire/flush [session message ^DataOutputStream out]
  (.flush out)
  session)

(defmethod handle-message :pgwire/terminate [session message out]
  (util/try-close out)
  session)

(defn- query-projection [tree]
  (->> (sem/projected-columns (r/$ (r/->zipper tree) 1))
       (first)
       (mapv (comp name plan/unqualified-projection-symbol))))

(defmethod handle-message :pgwire/describe [session message out]
  (cond
    (:pgwire.describe/portal message)
    (let [bind-message (get-in session [:portals (:pgwire.describe/portal message)])
          parse-message (get-in session [:prepared-statements (:pgwire.bind/prepared-statement bind-message)])]
      (if (= "SELECT" (statement-command parse-message))
        (send-row-description out (query-projection (:tree (meta parse-message))))
        (send-no-data out))
      session)

    (:pgwire.describe/statement message)
    (let [parse-message (get-in session [:prepared-statements (:pgwire.describe/statement message)])]
      (send-parameter-description out (:pgwire.parse/parameter-oids parse-message))
      (if (= "SELECT" (statement-command parse-message))
        (send-row-description out (query-projection (:tree (meta parse-message))))
        (send-no-data out))
      session)))

(defmethod handle-message :pgwire/execute [session message out]
  (let [bind-message (get-in session [:portals (:pgwire.execute/portal message)])
        parse-message (get-in session [:prepared-statements (:pgwire.bind/prepared-statement bind-message)])]
    (if  (= "SET" (statement-command parse-message))
      (let [[_ k v] (re-find #"SET\s+(\w+)\s*=\s*'?(.*)'?" (:pgwire.parse/query-string parse-message))]
        (send-command-complete out (statement-command parse-message))
        (assoc-in session [:parameters k] v))

      ;; HACK: not running query and returning the data.
      (let [{:keys [plan]} (meta bind-message)]
        (doseq [x [1 2]
                y [3 4]]
          (send-data-row out [(.getBytes (str x) StandardCharsets/UTF_8)
                              (.getBytes (str y) StandardCharsets/UTF_8)]))
        (send-command-complete out (str (statement-command parse-message) " 2"))
        session))))

(defmethod handle-message :pgwire/query [session message out]
  (let [query-string (:pgwire.query/query-string message)
        null-out (DataOutputStream. (ByteArrayOutputStream.))]
    (-> session
        (handle-message {:pgwire/type :parse
                         :pgwire.parse/query-string query-string
                         :pgwire.parse/prepared-statement ""
                         :pgwire.parse/parameters []}
                        null-out)
        (handle-message {:pgwire/type :bind
                         :pgwire.bind/portal ""
                         :pgwire.bind/prepared-statement ""}
                        null-out)
        (handle-message {:pgwire/type :execute
                         :pgwire.execute/portal ""}))))

(defmethod handle-message :default [session message _]
  session)

(defn- pg-message-exchange [session ^Socket socket ^DataInputStream in ^DataOutputStream out]
  (loop [session session]
    (if (.isClosed socket)
      session
      (do (prn session)
          (send-ready-for-query out (byte \I))

          (let [message-code (char (.readByte in))]
            (if-let [message-type (get message-code->type message-code)]
              (let [size (- (.readInt in) Integer/BYTES)
                    message (assoc (parse-message message-type in) :pgwire/type message-type)]
                (prn message)
                (recur (handle-message session message out)))
              (throw (IllegalArgumentException. (str "unknown message code: " message-code)))))))))

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
          (prn startup-message)
          (send-authentication-ok out)

          (doseq [[k v] parameters]
            (send-parameter-status out k v))

          (update session :parameters merge (:pgwire.startup-message/parameters startup-message)))

        :else
        (throw (IllegalArgumentException. (str "unknown handshake: " handshake)))))))

(defn- pg-conn [server ^Socket socket]
  (try
    (with-open [in (DataInputStream. (.getInputStream socket))
                out (DataOutputStream. (.getOutputStream socket))]
      (let [session {:parameters (:parameters server)
                     :prepared-statements {}
                     :portals {}}
            session (pg-establish-connection session in out)]
        (pg-message-exchange session socket in out)))
    (catch Throwable t
      (prn t)
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

(defrecord PgWireServer [parameters ^ServerSocket server-socket ^ExecutorService pool]
  Closeable
  (close [_]
    (util/try-close server-socket)
    (util/shutdown-pool pool)))

(defn ->pg-wire-server ^core2.sql.pgwire.PgWireServer [parameters ^long port ^long num-threads]
  (let [server-socket (ServerSocket. port)
        pool (Executors/newFixedThreadPool num-threads (util/->prefix-thread-factory "pgwire-connection-"))
        server (->PgWireServer parameters server-socket pool)]
    (doto (Thread. #(pg-accept server))
      (.start))
    server))

(comment
  (do (require '[juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc])
      (with-open [server (->pg-wire-server {"server_version" "14"
                                            "server_encoding" "UTF8"
                                            "TimeZone" "UTC"}
                                           5432
                                           16)]
        (Thread/sleep 1000)
        (with-open [c (jdbc/get-connection "jdbc:postgresql://:5432/test?user=test&password=test")]
          (Thread/sleep 1000)
          (prn (jdbc/execute! c ["SELECT * FROM (VALUES 1, 2) AS x (a), (VALUES 3, 4) AS y (b)"]))
          (Thread/sleep 1000)))))

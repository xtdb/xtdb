(ns core2.sql.pgwire
  (:require [clojure.string :as str]
            [core2.util :as util])
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
      (.write message)
      (.flush))))

(defn- send-authentication-ok [^DataOutputStream out]
  (doto out
    (.writeByte (byte \R))
    (.writeInt 8)
    (.writeInt 0)
    (.flush)))

(defn- send-ready-for-query [^DataOutputStream out ^long status]
  (doto out
    (.writeByte (byte \Z))
    (.writeInt 5)
    (.writeByte (byte status))
    (.flush)))

(defn- send-parse-complete [^DataOutputStream out]
  (doto out
    (.writeByte (byte \1))
    (.writeInt 4)
    (.flush)))

(defn- send-bind-complete [^DataOutputStream out]
  (doto out
    (.writeByte (byte \2))
    (.writeInt 4)
    (.flush)))

(defn- send-parameter-status [^DataOutputStream out ^String k ^String v]
  (send-message-with-body out
                          (byte \S)
                          (fn [out]
                            (write-c-string out k)
                            (write-c-string out v))))

(defn- send-command-complete [^DataOutputStream out ^String command]
  (send-message-with-body out
                          (byte \C)
                          (fn [out]
                            (write-c-string out command))))

(def ^:private message-code->type '{\P :pgwire/parse
                                    \B :pgwire/bind
                                    \Q :pgwire/query
                                    \S :pgwire/sync
                                    \X :pgwire/terminate
                                    \D :pgwire/describe
                                    \E :pgwire/execute})

(defmulti ^:private parse-message (fn [message-type in] message-type))

(defmethod parse-message :pgwire/parse [_ ^DataInputStream in]
  {:pgwire.parse/prepared-statement (read-c-string in)
   :pgwire.parse/query-string (read-c-string in)
   :pgwire.parse/parameters (vec (repeatedly (.readShort in) #(.readInt in)))})

(defmethod parse-message :pgwire/bind [_ ^DataInputStream in]
  {:pgwire.bind/portal (read-c-string in)
   :pgwire.bind/prepared-statement (read-c-string in)
   :pgwire.bind/parameter-format-codes (vec (repeatedly (.readShort in) #(.readInt in)))
   :pgwire.bind/parameters (vec (repeatedly (.readShort in)
                                            #(let [body (byte-array (.readInt in))]
                                               (.readFully in body)
                                               body)))
   :pgwire.bind/parameter-result-codes (vec (repeatedly (.readShort in) #(.readShort in)))})

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

(defmulti handle-message (fn [session message out]
                           (:pgwire/type message)))

(defmethod handle-message :pgwire/parse [session message out]
  (send-parse-complete out)
  (assoc-in session [:prepared-statements (get message :pgwire.parse/prepared-statement)] message))

(defmethod handle-message :pgwire/bind [session message out]
  (send-bind-complete out)
  (assoc-in session [:portals (get message :pgwire.bind/portal)] message))

(defmethod handle-message :pgwire/terminate [session message out]
  (util/try-close out)
  session)

(defmethod handle-message :pgwire/describe [session message out]
  (cond
    (:pgwire.describe/portal message)
    (let [bind-message (get-in session [:portals (:pgwire.describe/portal message)])]
      session)

    (:pgwire.describe/statement message)
    (let [parse-message (get-in session [:prepared-statements (:pgwire.describe/statement message)])]
      session)))

(defmethod handle-message :pgwire/execute [session message out]
  (let [bind-message (get-in session [:portals (:pgwire.execute/portal message)])
        parse-message (get-in session [:prepared-statements (:pgwire.bind/prepared-statement bind-message)])
        command (first (str/split (:pgwire.parse/query-string parse-message) #"\s+"))]
    (send-command-complete out command)
    session))

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
  (with-open [in (DataInputStream. (.getInputStream socket))
              out (DataOutputStream. (.getOutputStream socket))]
    (let [session {:parameters (:parameters server)
                   :prepared-statements {}
                   :portals {}}
          session (pg-establish-connection session in out)]
      (pg-message-exchange session socket in out))))

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
          (prn (jdbc/execute! c ["SELECT * FROM (VALUES 1, 2) AS x (a), (VALUES 3, 4) AS y (b)"]))
          (Thread/sleep 1000)))))

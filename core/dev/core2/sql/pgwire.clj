(ns core2.sql.pgwire
  (:require [core2.util :as util])
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
    (.writeByte out (byte message-type))
    (.writeInt out (+ Integer/BYTES (alength message)))
    (.write out message)))

(defn- send-ready-for-query [^DataOutputStream out ^long status]
  (.writeByte out (byte \Z))
  (.writeInt out 5)
  (.writeByte out (byte status)))

(defn- send-parse-complete [^DataOutputStream out]
  (.writeByte out (byte \1))
  (.writeInt out 4))

(defn- send-bind-complete [^DataOutputStream out]
  (.writeByte out (byte \2))
  (.writeInt out 4))

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

(def ^:private message-code->type '{\P :parse
                                    \B :bind
                                    \Q :query
                                    \S :sync
                                    \X :terminate
                                    \D :describe
                                    \E :execute})

(defmulti ^:private parse-message (fn [message-type in] message-type))

(defmethod parse-message :parse [_ ^DataInputStream in]
  {:parse/prepared-statement (read-c-string in)
   :parse/query-string (read-c-string in)
   :parse/parameters (vec (repeatedly (.readShort in) #(.readInt in)))})

(defmethod parse-message :bind [_ ^DataInputStream in]
  {:bind/portal (read-c-string in)
   :bind/prepared-statement (read-c-string in)
   :bind/parameter-format-codes (vec (repeatedly (.readShort in) #(.readInt in)))
   :bind/parameters (vec (repeatedly (.readShort in)
                                     #(let [body (byte-array (.readInt in))]
                                        (.readFully in body)
                                        body)))
   :bind/parameter-result-codes (vec (repeatedly (.readShort in) #(.readShort in)))})

(defmethod parse-message :query [_ ^DataInputStream in]
  {:query/query-string (read-c-string in)})

(defmethod parse-message :execute [_ ^DataInputStream in]
  {:execute/portal (read-c-string in)
   :execute/max-rows (.readInt in)})

(defmethod parse-message :describe [_ ^DataInputStream in]
  (case (char (.readByte in))
    \P {:describe/portal (read-c-string in)}
    \S {:describe/statement (read-c-string in)}))

(defmethod parse-message :default [_ _]
  {})

(defmulti handle-message (fn [message out]
                           (:pgwire/type message)))

(defmethod handle-message :parse [message out]
  (send-parse-complete out))

(defmethod handle-message :bind [message out]
  (send-bind-complete out))

(defmethod handle-message :terminate [message out]
  (util/try-close out))

(defmethod handle-message :execute [message out]
  (send-command-complete out "SET"))

(defmethod handle-message :default [message _])

(defn- pg-message-exchange [server parameters ^Socket socket ^DataInputStream in ^DataOutputStream out]
  (prn server)
  (while (not (.isClosed socket))
    (send-ready-for-query out (byte \I))

    (let [message-code (char (.readByte in))]
      (if-let [message-type (get message-code->type message-code)]
        (let [size (.readInt in)
              message (assoc (parse-message message-type in) :pgwire/type message-type)]
          (prn message)
          (handle-message message out))
        (throw (IllegalArgumentException. (str "unknown message code: " message-code)))))))

(defn- send-authentication-ok [^DataOutputStream out]
  (.writeByte out (byte \R))
  (.writeInt out 8)
  (.writeInt out 0))

(defn- parse-startup-message [^DataInputStream in]
  (loop [in (PushbackInputStream. in)
         acc {}]
    (let [x (.read in)]
      (if (zero? x)
        {:startup-message/parameters acc}
        (do (.unread in (byte x))
            (recur in (assoc acc (read-c-string in) (read-c-string in))))))))

(def ^:private ssl-request 80877103)
(def ^:private gssenc-request 80877104)
(def ^:private startup-message 196608)

(defn- pg-establish-connection [{:keys [parameters] :as server} ^Socket socket ^DataInputStream in ^DataOutputStream out]
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
          (send-authentication-ok out)

          (doseq [[k v] parameters]
            (send-parameter-status out k v))

          startup-message)

        :else
        (throw (IllegalArgumentException. (str "unknown handshake: " handshake)))))))

(defn- pg-conn [server ^Socket socket]
  (with-open [in (DataInputStream. (.getInputStream socket))
              out (DataOutputStream. (.getOutputStream socket))]
    (let [startup-message (pg-establish-connection server socket in out)]
      (prn startup-message)
      (pg-message-exchange server (:startup-message/parameters startup-message) socket in out))))

(defn- pg-accept [{:keys [^ServerSocket server-socket ^ExecutorService pool] :as server}]
  (while (not (.isClosed server-socket))
    (try
      (let [socket (.accept server-socket)]
        (try
          (.submit pool ^Runnable (fn pg-handle []
                                    (with-open [socket socket]
                                      (pg-conn server socket))))
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

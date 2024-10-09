(ns xtdb.pgwire-protocol-test
  (:require [clojure.test :as t :refer [deftest]]
            [clojure.tools.logging :as log]
            [xtdb.pgwire :as pgwire]
            [xtdb.test-util :as tu]
            [xtdb.types :as types])
  (:import [java.io ByteArrayOutputStream DataInputStream DataOutputStream IOException InputStream]
           [java.lang AutoCloseable]
           [java.net Socket]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]))

(def ^:dynamic ^:private *port* nil)

(defn with-port [f]
  (let [server (-> tu/*node* :system :xtdb.pgwire/server)]
    (binding [*port* (:port server)]
      (f))))

(t/use-fixtures :each tu/with-mock-clock tu/with-node with-port)

;; Helper Functions

(defn int-to-bytes ^bytes [value]
  (let [buffer (ByteBuffer/allocate 4)]
    (.putInt buffer value)
    (.array buffer)))

(defrecord WrappedSocket [^Socket socket in out]
  AutoCloseable
  (close [_] (.close socket)))

(defn wrap-socket [^Socket socket]
  (let [in (DataInputStream. (.getInputStream socket))
        out (DataOutputStream. (.getOutputStream socket))]
    (->WrappedSocket socket in out)))

(defn connect ^AutoCloseable [^long port]
  (wrap-socket (Socket. "localhost" port)))

(defn write-string-parameter [^DataOutputStream out ^String key ^String value]
  (doto out
    (.writeBytes key)
    (.write 0)
    (.writeBytes value)
    (.write 0)))

(defn bytes->str [^bytes arr]
  (String. arr StandardCharsets/UTF_8))

(defn read-message [^DataInputStream in]
  (let [response-type (.read in)]
    (when (= response-type -1)
      (throw (IOException. "Unexpected end of stream")))
    (let [response-char (char response-type)
          _length (.readInt in)]

      (if-let [{:keys [read name]} @(get pgwire/server-msgs response-char)]

        (let [{auth-code :result :as message} (-> (read in)
                                                  (assoc :message-type name))]
          (case response-char
            ;; 'R' indicates an Authentication request
            \R
            (cond
              (= auth-code 0)
              message

              :else ;; TODO add other authentication methods if available
              (throw (IOException. (str "Unsupported authentication method: " auth-code))))


            ;; 'D' indicates a DataRow message (contains a row of query result data)
            ;; we are not actually employing the send decoding types here, but string is more readable
            \D (update message :vals (partial mapv bytes->str))

            message))

        (throw (IllegalStateException. (str "Unhandled message type: " response-char)))))))

(defn read-response
  ([^InputStream in]
   (loop [responses []]
     (let [{:keys [message-type] :as  response} (read-message in)]
       (log/info response)
       (if (= message-type "msg-ready")
         (conj responses response)
         (recur (cond-> responses response (conj response))))))))

(defn startup [{:keys [in ^DataOutputStream out]} user database]
  (let [startup-message-ba (ByteArrayOutputStream.)
        startup-message (DataOutputStream. startup-message-ba)]
    ;; Reserve 4 bytes for the message length (to be set later)
    (.writeInt startup-message 0)
    ;; this is the normal 'hey I'm a postgres client' if ssl is not requested
    (.writeInt startup-message 196608)
    (write-string-parameter startup-message "user" user)
    (write-string-parameter startup-message "database" database)
    (write-string-parameter startup-message "timezone" "UTC")
    (.write startup-message 0)
    (let [^bytes message-bytes (.toByteArray startup-message-ba)
          length (alength message-bytes)]
      (System/arraycopy (int-to-bytes length) 0 message-bytes 0 4)
      (.write out message-bytes)
      (.flush out)
      (read-response in))))

(defn simple-query [{:keys [in ^DataOutputStream out]} ^String query]
  (let [message-ba (ByteArrayOutputStream.)
        message (DataOutputStream. message-ba)]
    (.write message (byte \Q))
    (.writeInt message (+ 4 (alength (.getBytes query StandardCharsets/UTF_8)) 1))
    (.writeBytes message query)
    (.write message 0)
    (.write out (.toByteArray message-ba))
    (.flush out)
    (read-response in)))

(deftest test-startup
  (let [port *port*
        user "xtdb"
        database "xtdb"]
    (with-open [^AutoCloseable socket (connect port) #_(ssl-negotiation (connect port))]

      (t/is (= [{:message-type "msg-auth"
                 :result 0}
                {:parameter "client_encoding",
                 :value "UTF8",
                 :message-type "msg-parameter-status"}
                {:parameter "TimeZone",
                 :value "UTC",
                 :message-type "msg-parameter-status"}
                {:parameter "user",
                 :value "xtdb",
                 :message-type "msg-parameter-status"}
                {:parameter "integer_datetimes",
                 :value "on",
                 :message-type "msg-parameter-status"}
                {:parameter "database",
                 :value "xtdb",
                 :message-type "msg-parameter-status"}
                {:parameter "server_encoding",
                 :value "UTF8",
                 :message-type "msg-parameter-status"}
                {:parameter "datestyle",
                 :value "ISO",
                 :message-type "msg-parameter-status"}
                {:parameter "server_version",
                 :value "16",
                 :message-type "msg-parameter-status"}
                {:parameter "intervalstyle",
                 :value "ISO_8601",
                 :message-type "msg-parameter-status"}
                {:message-type "msg-backend-key-data" :process-id 2, :secret-key 0}
                {:message-type "msg-ready", :status :idle}]
               (startup socket user database))))))

(deftest test-simple-query
  (let [port *port*
        user "xtdb"
        database "xtdb"
        query "SELECT 42 AS V"]
    (with-open [^AutoCloseable socket (connect port)]
      (startup socket user database)

      (t/is (= [{:message-type "msg-row-description",
                 :columns [{:column-name "v",
                            :table-oid 0,
                            :column-attribute-number 0,
                            :column-oid 20,
                            :typlen 8,
                            :type-modifier -1,
                            :result-format :text}]}
                {:message-type "msg-data-row" :vals ["42"]}
                {:message-type "msg-command-complete", :command "SELECT 1"}
                {:message-type "msg-ready", :status :idle}]
               (simple-query socket query))))))


(defn send-parse-message
  ([out query] (send-parse-message out query []))
  ([^DataOutputStream out ^String query param-types]
   (let [body-ba (ByteArrayOutputStream.)
         body (DataOutputStream. body-ba)
         message-ba (ByteArrayOutputStream.)
         message (DataOutputStream. message-ba)]
     ;; Write an empty statement name (null-terminated string)
     (.write body 0)
     ;; Write the query string (null-terminated)
     (.writeBytes body query)
     (.write body 0)
     ;; Write the number of parameter data types (int16)
     (.writeShort body (short (count param-types)))
     ;; Write each parameter data type OID (int32)
     (doseq [param-type param-types]
       (.writeInt body param-type))
     (.write message (byte \P))
     (.writeInt message (+ 4 (.size body-ba)))
     (.write message (.toByteArray body-ba))
     (.write out (.toByteArray message-ba))
     (.flush out))))

(defn send-bind-message
  ([out] (send-bind-message out []))
  ([^DataOutputStream out param-values]
   (let [body-ba (ByteArrayOutputStream.)
         body (DataOutputStream. body-ba)
         num-params (count param-values)
         message-ba (ByteArrayOutputStream.)
         message (DataOutputStream. message-ba)]
     ;; Write an empty portal name (null-terminated string)
     (.write body 0)
     ;; Write an empty prepared statement name (null-terminated string)
     (.write body 0)
     ;; Write the number of parameter format codes and each parameter format code (only using text)
     (.writeShort body (short num-params))
     (doseq [_ (range (count param-values))]
       (.writeShort body (short 0)))
     ;; Write the number of parameter values (int16)
     (.writeShort body (short num-params))
     ;; Write each parameter value
     (doseq [value param-values]
       (if (nil? value)
         ;; Write -1 for NULL value
         (.writeInt body -1)
         (let [value-bytes (.getBytes (str value) StandardCharsets/UTF_8) ]
           (.writeInt body (count value-bytes))
           (.write body value-bytes))))
     ;; Write the number of result format codes (int16)
     (.writeShort body 1)
     ;; Write each result format code (int16)
     (.writeShort body 0)
     (.write message (byte \B))
     (.writeInt message (+ 4 (.size body-ba)))
     (.write message (.toByteArray body-ba))
     (.write out (.toByteArray message-ba))
     (.flush out))))

(defn send-describe-message [^DataOutputStream out]
  (let [body (ByteArrayOutputStream.)
        message-ba (ByteArrayOutputStream.)
        message (DataOutputStream. message-ba)]
    ;; using the portal
    (.write body (byte \P))
    (.write body 0)
    (.write message (byte \D))
    (.writeInt message (+ 4 (.size body)))
    (.write message (.toByteArray body))
    (.write out (.toByteArray message-ba))
    (.flush out)))

(defn send-execute-message [^DataOutputStream out]
  (let [body-ba (ByteArrayOutputStream.)
        body (DataOutputStream. body-ba)
        message-ba (ByteArrayOutputStream.)
        message (DataOutputStream. message-ba)]
    ;; write an empty string for the portal
    (.write body 0)
    ;; no limit to the number of rows returned
    (.writeInt body 0)
    (.write message (byte \E))
    (.writeInt message (+ 4 (.size body-ba)))
    (.write message (.toByteArray body-ba))
    (.write out (.toByteArray message-ba))
    (.flush out)))

(defn send-sync-message [^DataOutputStream out]
  (let [message-ba (ByteArrayOutputStream.)
        message (DataOutputStream. message-ba)]
    (.write message (byte \S))
    (.writeInt message 4)
    (.write out (.toByteArray message-ba))
    (.flush out)))

(defn extended-query
  ([socket query] (extended-query socket query [] []))
  ([{:keys [in out]} query param-types param-values]
   (send-parse-message out query param-types)
   (send-bind-message out param-values)
   (send-describe-message out)
   (send-execute-message out)
   (send-sync-message out)
   (read-response in)))

(deftest test-extended-query
  (let [port *port*
        user "xtdb"
        database "xtdb"
        insert "INSERT INTO docs (_id, name) VALUES ('aln', $1)"
        param-types [(-> types/pg-types :text :oid)]
        param-values ["alan"]
        query "SELECT * FROM docs"]
    (with-open [^AutoCloseable socket (connect port)]

      (startup socket user database)

      (t/is (= [{:message-type "msg-parse-complete"}
                {:message-type "msg-bind-complete"}
                {:message-type "msg-no-data"}
                {:message-type "msg-command-complete", :command "INSERT 0 0"}
                {:message-type "msg-ready", :status :idle}]
               (extended-query socket insert param-types param-values)))

      (t/is (= [{:message-type "msg-parse-complete"}
                {:message-type "msg-bind-complete"}
                {:message-type "msg-row-description"
                 :columns
                 [{:column-name "_id",
                   :table-oid 0,
                   :column-attribute-number 0,
                   :column-oid 25,
                   :typlen 65535,
                   :type-modifier -1,
                   :result-format :text}
                  {:column-name "name",
                   :table-oid 0,
                   :column-attribute-number 0,
                   :column-oid 25,
                   :typlen 65535,
                   :type-modifier -1,
                   :result-format :text}]}
                {:message-type "msg-data-row" , :vals ["aln" "alan"]}
                {:message-type "msg-command-complete", :command "SELECT 1"}
                {:message-type "msg-ready", :status :idle}]
               (extended-query socket query))))))

(deftest test-wrong-param-encoding-3653
  (let [port *port*
        user "xtdb"
        database "xtdb"
        param-types [(-> types/pg-types :timestamp :oid)]
        param-values ["alan"]
        query "SELECT $1 as v"]
    (with-open [^AutoCloseable socket (connect port)]

      (startup socket user database)

      (t/is (= [{:message-type "msg-parse-complete"}
                {:error-fields
                 {:severity "ERROR",
                  :localized-severity "ERROR",
                  :sql-state "22P02",
                  :message "Can not parse 'alan' as timestamp"},
                 :message-type "msg-error-response"}
                {:status :idle, :message-type "msg-ready"}]
               (extended-query socket query param-types param-values))))))

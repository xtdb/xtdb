(ns xtdb.pgwire.io
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]
            [xtdb.pgwire :as-alias pgw]
            [xtdb.util :as util])
  (:import [clojure.lang MapEntry]
           [java.io BufferedInputStream BufferedOutputStream ByteArrayInputStream ByteArrayOutputStream DataInputStream DataOutputStream EOFException InputStream OutputStream]
           [java.lang AutoCloseable]
           [java.net Socket]
           [java.nio.charset StandardCharsets]
           [javax.net.ssl SSLContext SSLSocket]))

(defprotocol Frontend
  (send-client-msg!
    [frontend msg-def]
    [frontend msg-def data])

  (read-client-msg! [frontend])
  (host-address [frontend])

  (upgrade-to-ssl [frontend ssl-ctx])

  (flush! [frontend]))

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

(defn read-c-string
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

(defn write-c-string
  "Postgres strings are null terminated, writes a null terminated utf8 string to out."
  [^OutputStream out ^String s]
  (.write out (.getBytes s StandardCharsets/UTF_8))
  (.write out 0))

(defn cmd-write-msg
  "Writes out a single message given a definition (msg-def) and optional data record."
  ([{:keys [frontend]} msg-def]
   (send-client-msg! frontend msg-def))

  ([{:keys [frontend]} msg-def data]
   (send-client-msg! frontend msg-def data)))

(def ^:private version-messages
  "Integer codes sent by the client to identify a startup msg"
  {;; this is the normal 'hey I'm a postgres client' if ssl is not requested
   196608 :30
   ;; cancellation messages come in as special startup sequences (pgwire does not handle them yet!)
   80877102 :cancel
   ;; ssl messages are used when the client either requires, prefers, or allows ssl connections.
   80877103 :ssl
   ;; gssapi encoding is not supported by xt, and we tell the client that
   80877104 :gssenc})

(defn- read-untyped-msg [^DataInputStream in]
  (let [size (- (.readInt in) 4)
        barr (byte-array size)
        _ (.readFully in barr)]
    (DataInputStream. (ByteArrayInputStream. barr))))

(defn read-version [^DataInputStream in]
  (let [^DataInputStream msg-in (read-untyped-msg in)
        version (.readInt msg-in)]
    {:msg-in msg-in
     :version (version-messages version)}))

(declare ->socket-frontend)

(def ^:private flush-messages
  #{:msg-error-response :msg-notice-response
    :msg-parameter-status :msg-auth :msg-ready
    :msg-portal-suspended
    :msg-copy-in-response :msg-copy-done})

(defn err-protocol-violation
  ([msg] (err-protocol-violation msg nil))
  ([msg cause]
   (ex-info msg {::pgw/severity :error, ::pgw/error-code "08P01"} cause)))

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
                      (throw (err-protocol-violation (str "Unknown client message " type-char))))
          rdr (:read @msg-var)]
      (try
        (-> (rdr (read-untyped-msg in))
            (assoc :msg-name (:name @msg-var)))
        (catch Exception e
          (log/error e "Error reading client message")
          (throw (err-protocol-violation (str "Error reading client message " (ex-message e))))))))

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

(def ^:private io-uint8
  "An unsigned 32bit integer"
  {:read #(.readByte ^DataInputStream %)
   :write #(.writeByte ^DataOutputStream %1 (int %2))})

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

(def ^:private io-format-code8
  "Postgres format codes are integers, whose value is either

  0 (:text)
  1 (:binary)

  On read returns a keyword :text or :binary, write of these keywords will be transformed into the correct integer."
  {:read (fn [^DataInputStream in] (case (.readUnsignedByte in) 0 :text, 1 :binary))
   :write (fn [^DataOutputStream out v] (.writeByte out (case v :text 0, :binary 1)))})

(def ^:private io-format-code16
  "Postgres format codes are integers, whose value is either

  0 (:text)
  1 (:binary)

  On read returns a keyword :text or :binary, write of these keywords will be transformed into the correct integer."
  {:read (fn [^DataInputStream in] ({0 :text, 1 :binary} (.readUnsignedShort in)))
   :write (fn [^DataOutputStream out v] (.writeShort out ({:text 0, :binary 1} v)))})

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

(def io-just-bytes
  {:read (fn read-bytes-or-null [^DataInputStream in]
           (let [ba (byte-array (.available in))]
             (.readFully in ba)
             ba))})

(def ^:private error-or-notice-type->char
  {:localized-severity \S
   :severity \V
   :sql-state \C
   :message \M
   :detail \D
   :position \P
   :where \W
   :routine \R})

(def ^:private char->error-or-notice-type (set/map-invert error-or-notice-type->char))

(def ^:private io-error-notice-field
  "An io-data type that writes a (vector/map-entry pair) k and v as an error field."
  {:read (fn read-error-or-notice-field [^DataInputStream in]
           (when-let [field-key (char->error-or-notice-type (char (.readByte in)))]
             ;; TODO this might fail if we don't implement some message type
             (MapEntry/create field-key (read-c-string in))))

   :write (fn write-error-or-notice-field [^DataOutputStream out [k v]]
            (when-let [field-char8 (error-or-notice-type->char k)]
              (.writeByte out (byte field-char8))
              (if (bytes? v)
                (do
                  (.write out ^bytes v)
                  (.writeByte out 0))

                (write-c-string out (str v)))))})

(def ^:private io-portal-or-stmt
  "An io data type that returns a keyword :portal or :prepared-statement given the next char8 in the buffer.
  This is useful for describe/close who name either a portal or statement."
  {:read (comp {\P :portal, \S :prepared-stmt} (:read io-char8)),
   :write no-write})

(def io-cancel-request
  (io-record :process-id io-uint32
             :secret-key io-uint32))

;; client messages

(def-msg msg-bind :client \B
  :portal-name io-string
  :stmt-name io-string
  :arg-format (io-list io-uint16 io-format-code16)
  :args (io-list io-uint16 (io-bytes-or-null io-uint32))
  :result-format (io-list io-uint16 io-format-code16))

(def-msg msg-close :client \C
  :close-type io-portal-or-stmt
  :close-name io-string)

(def-msg msg-copy-data :client \d
  :data io-just-bytes)

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

(def-msg msg-copy-in-response :server \G
  :copy-format io-format-code8
  :column-formats (io-list io-uint16 io-format-code16))

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
  :columns (io-list io-uint16
                    (io-record :column-name io-string
                               :table-oid io-uint32
                               :column-attribute-number io-uint16
                               :column-oid io-uint32
                               :typlen  io-uint16
                               :type-modifier io-uint32
                               :result-format io-format-code16)))

(defn cmd-send-notice
  "Sends an notice message back to the client (e.g (cmd-send-notice conn (warning \"You are doing this wrong!\"))."
  [conn notice]
  (cmd-write-msg conn msg-notice-response {:notice-fields notice}))

(ns ^{:clojure.tools.namespace.repl/load false}
    xtdb.serde
  (:require [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [cognitect.transit :as transit]
            [xtdb.error :as err]
            [xtdb.mirrors.time-literals :as tl]
            [xtdb.table :as table]
            [xtdb.time :as time])
  (:import clojure.lang.Keyword
           [java.io ByteArrayInputStream ByteArrayOutputStream Writer]
           (java.net URI)
           [java.nio ByteBuffer]
           java.nio.charset.StandardCharsets
           (java.nio.file Path Paths)
           (java.time Duration Period)
           [org.apache.arrow.vector PeriodDuration]
           [org.apache.commons.codec.binary Hex]
           (org.postgresql.util PGobject PSQLException)
           (xtdb.api TransactionAborted TransactionCommitted TransactionKey)
           (xtdb.api.query IKeyFn IKeyFn$KeyFn)
           xtdb.TaggedValue
           (xtdb.time Interval)
           (xtdb.types ClojureForm ZonedDateTimeRange)
           xtdb.util.NormalForm))

(defrecord TxKey [tx-id system-time]
  TransactionKey
  (getTxId [_] tx-id)
  (getSystemTime [_] system-time))

(defmethod print-dup TxKey [^TxKey tx-key, ^Writer w]
  (.write w (str "#xt/tx-key " (pr-str (into {} tx-key)))))

(defmethod print-method TxKey [tx-key w]
  (print-dup tx-key w))

(defrecord TxCommitted [tx-id system-time committed?]
  TransactionCommitted
  (getTxId [_] tx-id)
  (getSystemTime [_] system-time))

(defn ->tx-committed [tx-id system-time]
  (->TxCommitted tx-id system-time true))

(defrecord TxAborted [tx-id system-time committed? error]
  TransactionAborted
  (getTxId [_] tx-id)
  (getSystemTime [_] system-time)
  (getError [_] error))

(defn ->tx-aborted [tx-id system-time error]
  (->TxAborted tx-id system-time false error))

(defn period-duration-reader [[p d]]
  (PeriodDuration. (Period/parse p) (Duration/parse d)))

(defmethod print-dup PeriodDuration [^PeriodDuration pd ^Writer w]
  (.write w (format "#xt/period-duration %s" (pr-str [(str (.getPeriod pd)) (str (.getDuration pd))]))))

(defmethod print-method PeriodDuration [c ^Writer w]
  (print-dup c w))

(defn interval-reader [^String i] (time/<-iso-interval-str i))

(defmethod print-dup Interval [i, ^Writer w]
  (.write w (format "#xt/interval %s" (pr-str (str i)))))

(defmethod print-method Interval [i ^Writer w]
  (print-dup i w))

(defn- render-tstz-range [^ZonedDateTimeRange range]
  [(.getFrom range) (.getTo range)])

(defmethod print-dup ZonedDateTimeRange [^ZonedDateTimeRange tstz-range, ^Writer w]
  (.write w (str "#xt/tstz-range " (pr-str (render-tstz-range tstz-range)))))

(defmethod print-method ZonedDateTimeRange [tstz-range w]
  (print-dup tstz-range w))

(defn tstz-range-reader [[from to]]
  (ZonedDateTimeRange. (time/->zdt from) (time/->zdt to)))

(defmethod print-dup URI [^URI uri, ^Writer w]
  (.write w (str "#xt/uri " (pr-str (.toASCIIString uri)))))

(defmethod print-method URI [uri w]
  (print-dup uri w))

(defn uri-reader [uri] (URI. uri))

(defn write-key-fn [^IKeyFn$KeyFn key-fn]
  (-> (.name key-fn)
      (str/lower-case)
      (str/replace #"_" "-")
      keyword))

(def ^:private key-fns
  (->> (IKeyFn$KeyFn/values)
       (into {} (map (juxt write-key-fn identity)))))

(defn read-key-fn ^xtdb.api.query.IKeyFn [k]
  (if (instance? IKeyFn k)
    k
    (or (get key-fns k)
        (throw (err/incorrect :xtdb/invalid-key-fn "Invalid key-fn" {:key k})))))

(defmethod print-dup IKeyFn$KeyFn [key-fn ^Writer w]
  (.write w (str "#xt/key-fn " (write-key-fn key-fn))))

(defmethod print-method IKeyFn$KeyFn [e, ^Writer w]
  (print-dup e w))

(defmethod print-dup TxKey [tx-key ^Writer w]
  (.write w "#xt/tx-key ")
  (print-method (into {} tx-key) w))

(defmethod print-method TxKey [tx-key w]
  (print-dup tx-key w))

(defn iae-reader [[k message data cause]]
  (err/incorrect k message (cond-> (or data {}) cause (assoc ::err/cause cause))))

(defn runex-reader [[k message data cause]]
  (err/incorrect k message (cond-> (or data {}) cause (assoc ::err/cause cause))))

(defmethod print-dup Path [^Path p, ^Writer w]
  (.write w "#xt/path ")
  (print-method (.toString p) w))

(defmethod print-method Path [p, w]
  (print-dup p w))

(defn path-reader [path-ish]
  (let [uri (URI. path-ish)]
    (if (.getScheme uri)
      (Paths/get uri)
      (Paths/get path-ish (make-array String 0)))))

#_{:clojure-lsp/ignore [:clojure-lsp/unused-public-var]} ; reader-macro
(defn read-tagged [[tag v]]
  (TaggedValue. tag v))

(defmethod print-dup TaggedValue [^TaggedValue tv, ^Writer w]
  (.write w (format "#xt/tagged [%s %s]" (.getTag tv) (pr-str (.getValue tv)))))

(defmethod print-method TaggedValue [tv w]
  (print-dup tv w))

(defmethod print-dup (Class/forName "[B") [^bytes ba, ^Writer w]
  (.write w (str "#bytes " (pr-str (Hex/encodeHexString ba)))))

(defmethod print-method (Class/forName "[B") [ba w]
  (print-dup ba w))

(defmethod pp/simple-dispatch (Class/forName "[B") [ba]
  (print-dup ba *out*))

#_{:clojure-lsp/ignore [:clojure-lsp/unused-public-var]} ; reader-macro
(defn read-bytes [^String s]
  (Hex/decodeHex s))

(do
  (def transit-read-handlers
    (merge transit/default-read-handlers
           tl/transit-read-handlers
           err/transit-readers
           table/transit-read-handlers
           {"r" (transit/read-handler uri-reader)

            ;; NOTE: these three have been serialised to disk - do not remove.
            "xtdb/illegal-arg" (transit/read-handler iae-reader)
            "xtdb/runtime-err" (transit/read-handler runex-reader)
            "xtdb/exception-info" (transit/read-handler #(ex-info (first %) (second %)))

            "xtdb/clj-form" (transit/read-handler ClojureForm/new)
            "xtdb/tx-key" (transit/read-handler map->TxKey)
            "xtdb/key-fn" (transit/read-handler read-key-fn)
            "xtdb/period-duration" period-duration-reader
            "xtdb/interval" (transit/read-handler interval-reader)
            "xtdb/tstz-range" (transit/read-handler tstz-range-reader)
            "f64" (transit/read-handler double)
            "f32" (transit/read-handler float)
            "i64" (transit/read-handler long)
            "i32" (transit/read-handler int)
            "i16" (transit/read-handler short)
            "i8" (transit/read-handler byte)
            "xtdb/byte-array" (transit/read-handler #(ByteBuffer/wrap (Hex/decodeHex (subs % 2))))
            "xtdb/path" (transit/read-handler path-reader)}))

  (def transit-read-handler-map
    (transit/read-handler-map transit-read-handlers)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]} ; TransitVector
(defn transit-msgpack-reader [in]
  (.r ^cognitect.transit.Reader (transit/reader in :msgpack {:handlers transit-read-handler-map})))

(defn- bb->ba ^bytes [^ByteBuffer bb]
  (let [ba (byte-array (.remaining bb))]
    (.get bb ba)
    (.flip bb)
    ba))

(def transit-write-handlers
  (merge transit/default-write-handlers
         tl/transit-write-handlers
         err/transit-writers
         table/transit-write-handlers
         {TxKey (transit/write-handler "xtdb/tx-key" #(into {} %))
          clojure.lang.ExceptionInfo (transit/write-handler "xtdb/exception-info" (juxt ex-message ex-data))
          IKeyFn$KeyFn (transit/write-handler "xtdb/key-fn" write-key-fn)

          ClojureForm (transit/write-handler "xtdb/clj-form" #(.form ^ClojureForm %))

          Interval (transit/write-handler "xtdb/interval" str)

          ZonedDateTimeRange (transit/write-handler "xtdb/tstz-range" render-tstz-range)
          ByteBuffer (transit/write-handler "xtdb/byte-array" #(str "0x" (Hex/encodeHexString (bb->ba %))))
          Path (transit/write-handler "xtdb/path" #(str %))}))

(def transit-write-handler-map
  (transit/write-handler-map transit-write-handlers))

(defn read-transit
  ([bytes-or-str] (read-transit bytes-or-str nil))
  ([bytes-or-str fmt]
   (with-open [bais (ByteArrayInputStream. (cond
                                             (bytes? bytes-or-str) bytes-or-str
                                             (string? bytes-or-str) (-> ^String bytes-or-str (.getBytes StandardCharsets/UTF_8))))]
     (transit/read (transit/reader bais (or fmt :msgpack) {:handlers transit-read-handler-map})))))

(defn transit-seq [rdr]
  (lazy-seq
   (try
     (cons (transit/read rdr)
           (transit-seq rdr))
     (catch RuntimeException ex
       (when-not (instance? java.io.EOFException (.getCause ex))
         (throw (ex-cause ex)))))))

(defn write-transit
  (^bytes [v] (write-transit v nil))
  (^bytes [v fmt]
   (with-open [baos (ByteArrayOutputStream.)]
     (-> (transit/writer baos (or fmt :msgpack) {:handlers transit-write-handler-map})
         (transit/write v))
     (.toByteArray baos))))

(defn write-transit-seq ^bytes [vs format]
  (with-open [baos (ByteArrayOutputStream.)]
    (let [wtr (transit/writer baos (or format :msgpack) {:handlers transit-write-handler-map})]
      (doseq [doc vs]
        (transit/write wtr doc)))
     (.toByteArray baos)))

;; here to prevent a cyclic dep on xtdb.error
(extend-protocol err/ToAnomaly
  PSQLException
  (->anomaly [ex data]
    (let [sem (.getServerErrorMessage ex)]
      (or (when-let [detail (some-> sem .getDetail)]
            (when-not (str/blank? detail)
              (try
                (read-transit detail :json)
                (catch Exception _))))

          (let [sql-state (.getSQLState ex)
                msg (or (some-> sem .getMessage) (ex-message ex))
                data (into {::err/cause ex, :psql/state sql-state} data)]
            (case sql-state
              "22023" (err/incorrect :psql/invalid-parameter-value msg data)

              (err/fault :psql/exception msg data)))))))

(defn ->pg-obj
  "This function serialises a Clojure/Java (potentially nested) data structure into a PGobject,
   in order to preserve the data structure's type information when stored in XTDB."
  [v]
  (doto (PGobject.)
    (.setType "transit")
    (.setValue (-> v
                   (->> (walk/postwalk (fn [v]
                                         (cond-> v
                                           (map? v) (update-keys (fn [k]
                                                                   (if (keyword? k)
                                                                     (NormalForm/normalForm ^Keyword k)
                                                                     k)))))))
                   (write-transit :json)
                   (String. StandardCharsets/UTF_8)))))

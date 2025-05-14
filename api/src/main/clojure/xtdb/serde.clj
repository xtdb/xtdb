(ns ^{:clojure.tools.namespace.repl/load false}
    xtdb.serde
  (:require [clojure.string :as str]
            [clojure.walk :as w]
            [cognitect.transit :as transit]
            [xtdb.error :as err]
            [xtdb.mirrors.time-literals :as tl]
            [xtdb.time :as time]
            [xtdb.tx-ops :as tx-ops]
            [xtdb.xtql :as xtql])
  (:import clojure.lang.Keyword
           [java.io ByteArrayInputStream ByteArrayOutputStream Writer]
           (java.net URI)
           [java.nio ByteBuffer]
           java.nio.charset.StandardCharsets
           (java.nio.file Path Paths)
           (java.time Duration Period)
           [java.util List]
           [org.apache.arrow.vector PeriodDuration]
           [org.apache.commons.codec.binary Hex]
           org.postgresql.util.PGobject
           (xtdb.api TransactionAborted TransactionCommitted TransactionKey)
           (xtdb.api.query Binding IKeyFn IKeyFn$KeyFn XtqlQuery)
           (xtdb.api.tx TxOp$Sql TxOps)
           (xtdb.time Interval)
           (xtdb.tx_ops DeleteDocs EraseDocs PutDocs)
           (xtdb.types ClojureForm ZonedDateTimeRange)
           xtdb.util.NormalForm
           (xtdb.xtql Aggregate DocsRelation From Join LeftJoin Limit Offset OrderBy ParamRelation Pipeline Return Unify UnionAll Where With Without)))

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

(defn- render-binding [binding]
  (xtql/unparse-binding identity identity binding))

(defn- render-query [^XtqlQuery query]
  (xtql/unparse-query query))

(defn- xtql-query-reader [q-edn]
  (xtql/parse-query q-edn))

(defn- render-sql-op [^TxOp$Sql op]
  {:sql (.sql op), :arg-rows (.argRows op)})

(defn sql-op-reader [{:keys [sql ^List arg-rows]}]
  (-> (TxOps/sql sql)
      (.argRows arg-rows)))

(defmethod print-dup TxOp$Sql [op ^Writer w]
  (.write w (format "#xt.tx/sql %s" (pr-str (render-sql-op op)))))

(defmethod print-method TxOp$Sql [op ^Writer w]
  (print-dup op w))

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
        (throw (err/illegal-arg :xtdb/invalid-key-fn {:key k})))))

(defmethod print-dup IKeyFn$KeyFn [key-fn ^Writer w]
  (.write w (str "#xt/key-fn " (write-key-fn key-fn))))

(defmethod print-method IKeyFn$KeyFn [e, ^Writer w]
  (print-dup e w))

(defmethod print-dup TxKey [tx-key ^Writer w]
  (.write w "#xt/tx-key ")
  (print-method (into {} tx-key) w))

(defmethod print-method TxKey [tx-key w]
  (print-dup tx-key w))

(defn tx-result-read-fn [{:keys [committed?] :as tx-res}]
  (if committed?
    (map->TxCommitted tx-res)
    (map->TxAborted tx-res)))

(defmethod print-dup TxCommitted [tx-result ^Writer w]
  (.write w "#xt/tx-result ")
  (print-method (into {} tx-result) w))

(defmethod print-method TxCommitted [tx-result ^Writer w]
  (print-dup tx-result w))

(defmethod print-dup TxAborted [tx-result ^Writer w]
  (.write w "#xt/tx-result ")
  (print-method (into {} tx-result) w))

(defmethod print-method TxAborted [tx-result ^Writer w]
  (print-dup tx-result w))

(defn- render-iae [^IllegalArgumentException e]
  [nil (ex-message e) (-> (ex-data e) (dissoc ::err/error-key))])

(defn- render-xt-iae [^xtdb.IllegalArgumentException e]
  [(.getKey e) (ex-message e) (-> (ex-data e) (dissoc ::err/error-key))])

(defmethod print-dup xtdb.IllegalArgumentException [e, ^Writer w]
  (.write w (str "#xt/illegal-arg " (render-xt-iae e))))

(defmethod print-method xtdb.IllegalArgumentException [e, ^Writer w]
  (print-dup e w))

(defn iae-reader [[k message data]]
  (xtdb.IllegalArgumentException. k message (or data {}) nil))

(defn- render-runex [^xtdb.RuntimeException e]
  [(.getKey e) (ex-message e) (-> (ex-data e) (dissoc ::err/error-key))])

(defmethod print-dup xtdb.RuntimeException [e, ^Writer w]
  (.write w (str "#xt/runtime-err " (render-runex e))))

(defmethod print-method xtdb.RuntimeException [e, ^Writer w]
  (print-dup e w))

(defn runex-reader [[k message data cause]]
  (xtdb.RuntimeException. k message data cause))

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

(do
  (def transit-read-handlers
    (merge transit/default-read-handlers
           tl/transit-read-handlers
           {"r" (transit/read-handler uri-reader)

            "xtdb/clj-form" (transit/read-handler ClojureForm/new)
            "xtdb/tx-key" (transit/read-handler map->TxKey)
            "xtdb/tx-result" (transit/read-handler tx-result-read-fn)
            "xtdb/key-fn" (transit/read-handler read-key-fn)
            "xtdb/illegal-arg" (transit/read-handler iae-reader)
            "xtdb/runtime-err" (transit/read-handler runex-reader)
            "xtdb/exception-info" (transit/read-handler #(ex-info (first %) (second %)))
            "xtdb/period-duration" period-duration-reader
            "xtdb/interval" (transit/read-handler interval-reader)
            "xtdb/tstz-range" (transit/read-handler tstz-range-reader)
            "xtdb.query/xtql" (transit/read-handler xtql-query-reader)
            "xtdb.tx/sql" (transit/read-handler sql-op-reader)
            "xtdb.tx/xtql" (transit/read-handler tx-ops/parse-tx-op)
            "xtdb.tx/put-docs" (transit/read-handler tx-ops/map->PutDocs)
            "xtdb.tx/delete-docs" (transit/read-handler tx-ops/map->DeleteDocs)
            "xtdb.tx/erase-docs" (transit/read-handler tx-ops/map->EraseDocs)
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
         {TxKey (transit/write-handler "xtdb/tx-key" #(into {} %))
          TxCommitted (transit/write-handler "xtdb/tx-result" #(into {} %))
          TxAborted (transit/write-handler "xtdb/tx-result" #(into {} %))
          IllegalArgumentException (transit/write-handler "xtdb/illegal-arg" render-iae)
          xtdb.IllegalArgumentException (transit/write-handler "xtdb/illegal-arg" render-xt-iae)
          xtdb.RuntimeException (transit/write-handler "xtdb/runtime-err" render-runex)
          clojure.lang.ExceptionInfo (transit/write-handler "xtdb/exception-info" (juxt ex-message ex-data))
          IKeyFn$KeyFn (transit/write-handler "xtdb/key-fn" write-key-fn)

          ClojureForm (transit/write-handler "xtdb/clj-form" #(.form ^ClojureForm %))

          Interval (transit/write-handler "xtdb/interval" str)

          ZonedDateTimeRange (transit/write-handler "xtdb/tstz-range" render-tstz-range)
          ByteBuffer (transit/write-handler "xtdb/byte-array" #(str "0x" (Hex/encodeHexString (bb->ba %))))
          Path (transit/write-handler "xtdb/path" #(str %))

          Binding (transit/write-handler "xtdb.query/binding" render-binding)
          XtqlQuery (transit/write-handler "xtdb.query/xtql" render-query)
          Pipeline (transit/write-handler "xtdb.query/xtql" render-query)
          From (transit/write-handler "xtdb.query/xtql" render-query)
          Where (transit/write-handler "xtdb.query/xtql" render-query)
          With (transit/write-handler "xtdb.query/xtql" render-query)
          Join (transit/write-handler "xtdb.query/xtql" render-query)
          LeftJoin (transit/write-handler "xtdb.query/xtql" render-query)
          Without (transit/write-handler "xtdb.query/xtql" render-query)
          Return (transit/write-handler "xtdb.query/xtql" render-query)
          UnionAll (transit/write-handler "xtdb.query/xtql" render-query)
          OrderBy (transit/write-handler "xtdb.query/xtql" render-query)
          Limit (transit/write-handler "xtdb.query/xtql" render-query)
          Offset (transit/write-handler "xtdb.query/xtql" render-query)
          DocsRelation (transit/write-handler "xtdb.query/xtql" render-query)
          ParamRelation (transit/write-handler "xtdb.query/xtql" render-query)
          Aggregate (transit/write-handler "xtdb.query/xtql" render-query)
          Unify (transit/write-handler "xtdb.query/xtql" render-query)

          TxOp$Sql (transit/write-handler "xtdb.tx/sql" render-sql-op)

          PutDocs (transit/write-handler "xtdb.tx/put-docs" (partial into {}))
          DeleteDocs (transit/write-handler "xtdb.tx/delete-docs" (partial into {}))
          EraseDocs (transit/write-handler "xtdb.tx/erase-docs" (partial into {}))}))

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
         (throw ex))))))

(defn write-transit
  (^bytes [v] (write-transit v nil))
  (^bytes [v fmt]
   (with-open [baos (ByteArrayOutputStream.)]
     (-> (transit/writer baos (or fmt :msgpack) {:handlers transit-write-handler-map})
         (transit/write v))
     (.toByteArray baos))))

(defn ->pg-obj
  "This function serialises a Clojure/Java (potentially nested) data structure into a PGobject,
   in order to preserve the data structure's type information when stored in XTDB."
  [v]
  (doto (PGobject.)
    (.setType "transit")
    (.setValue (-> v
                   (->> (w/postwalk (fn [v]
                                      (cond-> v
                                        (map? v) (update-keys (fn [k]
                                                                (if (keyword? k)
                                                                  (NormalForm/normalForm ^Keyword k)
                                                                  k)))))))
                   (write-transit :json)
                   (String. StandardCharsets/UTF_8)))))


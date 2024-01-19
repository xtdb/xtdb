(ns ^{:clojure.tools.namespace.repl/load false}
    xtdb.serde
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [cognitect.transit :as transit]
            [time-literals.read-write :as time-literals]
            [xtdb.time :as time]
            [xtdb.xtql.edn :as xtql.edn]
            [xtdb.error :as err])
  (:import java.io.Writer
           (java.time DayOfWeek Duration Instant LocalDate LocalDateTime LocalTime Month MonthDay OffsetDateTime OffsetTime Period Year YearMonth ZoneId ZonedDateTime)
           java.util.List
           [org.apache.arrow.vector PeriodDuration]
           (xtdb.api TransactionKey)
           (xtdb.api.query IKeyFn IKeyFn$KeyFn XtqlQuery)
           (xtdb.api.tx Call Delete Erase Put Sql TxOp TxOptions Xtql XtqlAndArgs)
           (xtdb.types ClojureForm IntervalDayTime IntervalMonthDayNano IntervalYearMonth)))

(when-not (or (some-> (System/getenv "XTDB_NO_JAVA_TIME_LITERALS") Boolean/valueOf)
              (some-> (System/getProperty "xtdb.no-java-time-literals") Boolean/valueOf))
  (time-literals/print-time-literals-clj!))

(defn period-duration-reader [[p d]]
  (PeriodDuration. (Period/parse p) (Duration/parse d)))

(defmethod print-dup PeriodDuration [^PeriodDuration pd ^Writer w]
  (.write w (format "#xt/period-duration %s" (pr-str [(str (.getPeriod pd)) (str (.getDuration pd))]))))

(defmethod print-method PeriodDuration [c ^Writer w]
  (print-dup c w))

(defn interval-ym-reader [p]
  (IntervalYearMonth. (Period/parse p)))

(defmethod print-dup IntervalYearMonth [^IntervalYearMonth i, ^Writer w]
  (.write w (format "#xt/interval-ym %s" (pr-str (str (.period i))))))

(defmethod print-method IntervalYearMonth [i ^Writer w]
  (print-dup i w))

(defn interval-dt-reader [[p d]]
  (IntervalDayTime. (Period/parse p) (Duration/parse d)))

(defmethod print-dup IntervalDayTime [^IntervalDayTime i, ^Writer w]
  (.write w (format "#xt/interval-dt %s" (pr-str [(str (.period i)) (str (.duration i))]))))

(defmethod print-method IntervalDayTime [i ^Writer w]
  (print-dup i w))

(defn interval-mdn-reader [[p d]]
  (IntervalMonthDayNano. (Period/parse p) (Duration/parse d)))

(defmethod print-dup IntervalMonthDayNano [^IntervalMonthDayNano i, ^Writer w]
  (.write w (format "#xt/interval-mdn %s" (pr-str [(str (.period i)) (str (.duration i))]))))

(defmethod print-method IntervalMonthDayNano [i ^Writer w]
  (print-dup i w))

(defn- render-query [^XtqlQuery query]
  (xtql.edn/unparse query))

(defn- xtql-query-reader [q-edn]
  (xtql.edn/parse-query q-edn))

(defn- render-sql-op [^Sql op]
  {:sql (.sql op), :arg-rows (.argRows op)})

(defn sql-op-reader [{:keys [sql ^List arg-rows]}]
  (-> (TxOp/sql sql)
      (.withArgs arg-rows)))

(defmethod print-dup Sql [op ^Writer w]
  (.write w (format "#xt.tx/sql %s" (pr-str (render-sql-op op)))))

(defmethod print-method Sql [op ^Writer w]
  (print-dup op w))

(defn- render-xtql-op [^Xtql op]
  (xtql.edn/unparse op))

(defmethod print-dup Xtql [op ^Writer w]
  (.write w (format "#xt.tx/xtql %s" (pr-str (render-xtql-op op)))))

(defmethod print-method Xtql [op ^Writer w]
  (print-dup op w))

(defn- render-xtql+args [^XtqlAndArgs op]
  (into (xtql.edn/unparse (.op op)) (.args op)))

(defmethod print-dup XtqlAndArgs [op ^Writer w]
  (.write w (format "#xt.tx/xtql %s" (pr-str (render-xtql+args op)))))

(defmethod print-method XtqlAndArgs [op ^Writer w]
  (print-dup op w))

(defn xtql-reader [xtql]
  (xtql.edn/parse-dml xtql))

(defn- render-put-op [^Put op]
  {:table-name (keyword (.tableName op)), :docs (.docs op)
   :valid-from (.validFrom op), :valid-to (.validTo op)})

(defmethod print-dup Put [op ^Writer w]
  (.write w (format "#xt.tx/put %s" (pr-str (render-put-op op)))))

(defmethod print-method Put [op ^Writer w]
  (print-dup op w))

(defn put-op-reader [{:keys [table-name ^List docs valid-from valid-to]}]
  (-> (TxOp/put (str (symbol table-name)) docs)
      (.during (time/->instant valid-from) (time/->instant valid-to))))

(defn- render-delete-op [^Delete op]
  {:table-name (keyword (.tableName op)), :xt/id (.entityId op)
   :valid-from (.validFrom op), :valid-to (.validTo op)})

(defmethod print-dup Delete [op ^Writer w]
  (.write w (format "#xt.tx/delete %s" (pr-str (render-delete-op op)))))

(defmethod print-method Delete [op ^Writer w]
  (print-dup op w))

(defn delete-op-reader [{:keys [table-name xt/id valid-from valid-to]}]
  (-> (TxOp/delete (str (symbol table-name)) id)
      (.during (time/->instant valid-from) (time/->instant valid-to))))

(defn- render-erase-op [^Erase op]
  {:table-name (keyword (.tableName op)), :xt/id (.entityId op)})

(defmethod print-dup Erase [op ^Writer w]
  (.write w (format "#xt.tx/erase %s" (pr-str (render-erase-op op)))))

(defmethod print-method Erase [op ^Writer w]
  (print-dup op w))

(defn erase-op-reader [{:keys [table-name xt/id]}]
  (TxOp/erase (str (symbol table-name)) id))

(defn- render-call-op [^Call op]
  {:fn-id (.fnId op), :args (.args op)})

(defmethod print-dup Call [op ^Writer w]
  (.write w (format "#xt.tx/call %s" (pr-str (render-call-op op)))))

(defmethod print-method Call [op ^Writer w]
  (print-dup op w))

(defn call-op-reader [{:keys [fn-id args]}]
  (TxOp/call fn-id args))

(defn write-key-fn [^IKeyFn$KeyFn key-fn]
  (-> (.name key-fn)
      (str/lower-case)
      (str/replace #"_" "-")
      keyword))

(def ^:private key-fns
  (->> (IKeyFn$KeyFn/values)
       (into {} (map (juxt write-key-fn identity)))))

(defn read-key-fn [k]
  (if (instance? IKeyFn k)
    k
    (or (get key-fns k)
        (throw (err/illegal-arg :xtdb/invalid-key-fn {:key k})))))

(defmethod print-dup IKeyFn$KeyFn [key-fn ^Writer w]
  (.write w (str "#xt/key-fn " (write-key-fn key-fn))))

(defmethod print-method IKeyFn$KeyFn [e, ^Writer w]
  (print-dup e w))

(defmethod print-dup TransactionKey [^TransactionKey tx-key ^Writer w]
  (.write w "#xt/tx-key ")
  (print-method {:tx-id (.getTxId tx-key) :system-time (.getSystemTime tx-key)} w))

(defmethod print-method TransactionKey [tx-key w]
  (print-dup tx-key w))

(defn tx-key-read-fn [{:keys [tx-id system-time]}]
  (TransactionKey. tx-id system-time))

(defn tx-key-write-fn [^TransactionKey tx-key]
  {:tx-id (.getTxId tx-key) :system-time (.getSystemTime tx-key)})

(defn tx-opts-read-fn [{:keys [system-time default-tz default-all-valid-time?]}]
  (TxOptions. system-time default-tz default-all-valid-time?))

(defn tx-opts-write-fn [^TxOptions tx-opts]
  {:system-time (.getSystemTime tx-opts), :default-tz (.getDefaultTz tx-opts), :default-all-valid-time? (.getDefaultAllValidTime tx-opts)})

(defmethod print-dup TxOptions [tx-opts ^Writer w]
  (.write w "#xt/tx-opts ")
  (print-method (tx-opts-write-fn tx-opts) w))

(defn- render-iae [^xtdb.IllegalArgumentException e]
  [(.getKey e) (ex-message e) (-> (ex-data e) (dissoc ::err/error-key))])

(defmethod print-dup xtdb.IllegalArgumentException [e, ^Writer w]
  (.write w (str "#xt/illegal-arg " (render-iae e))))

(defmethod print-method xtdb.IllegalArgumentException [e, ^Writer w]
  (print-dup e w))

(defn iae-reader [[k message data]]
  (xtdb.IllegalArgumentException. k message data nil))

(defn- render-runex [^xtdb.RuntimeException e]
  [(.getKey e) (ex-message e) (-> (ex-data e) (dissoc ::err/error-key))])

(defmethod print-dup xtdb.RuntimeException [e, ^Writer w]
  (.write w (str "#xt/runtime-err " (render-runex e))))

(defmethod print-method xtdb.RuntimeException [e, ^Writer w]
  (print-dup e w))

(defn runex-reader [[k message data]]
  (xtdb.RuntimeException. k message data nil))

(defmethod print-method TxOptions [tx-opts w]
  (print-dup tx-opts w))

(def transit-read-handlers
  (merge transit/default-read-handlers
         (-> time-literals/tags
             (update-keys str)
             (update-vals transit/read-handler))
         {"xtdb/clj-form" (transit/read-handler #(ClojureForm. %))
          "xtdb/tx-key" (transit/read-handler tx-key-read-fn)
          "xtdb/key-fn" (transit/read-handler read-key-fn)
          "xtdb/illegal-arg" (transit/read-handler iae-reader)
          "xtdb/runtime-err" (transit/read-handler runex-reader)
          "xtdb/exception-info" (transit/read-handler #(ex-info (first %) (second %)))
          "xtdb/period-duration" period-duration-reader
          "xtdb.interval/year-month" interval-ym-reader
          "xtdb.interval/day-time" interval-dt-reader
          "xtdb.interval/month-day-nano" interval-mdn-reader
          "xtdb/list" (transit/read-handler edn/read-string)
          "xtdb.query/xtql" (transit/read-handler xtql-query-reader)
          "xtdb.tx/sql" (transit/read-handler sql-op-reader)
          "xtdb.tx/xtql" (transit/read-handler xtql-reader)
          "xtdb.tx/put" (transit/read-handler put-op-reader)
          "xtdb.tx/delete" (transit/read-handler delete-op-reader)
          "xtdb.tx/erase" (transit/read-handler erase-op-reader)
          "xtdb.tx/call" (transit/read-handler call-op-reader)
          "xtdb/tx-opts" (transit/read-handler tx-opts-read-fn)}))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]} ; TransitVector
(defn transit-msgpack-reader [in]
  (.r ^cognitect.transit.Reader (transit/reader in :msgpack {:handlers transit-read-handlers})))

(def transit-write-handlers
  (merge transit/default-write-handlers
         (-> {Period "time/period"
              LocalDate "time/date"
              LocalDateTime "time/date-time"
              ZonedDateTime "time/zoned-date-time"
              OffsetTime "time/offset-time"
              Instant "time/instant"
              OffsetDateTime "time/offset-date-time"
              ZoneId "time/zone"
              DayOfWeek "time/day-of-week"
              LocalTime "time/time"
              Month "time/month"
              Duration "time/duration"
              Year "time/year"
              YearMonth "time/year-month"
              MonthDay "time/month-day"}
             (update-vals #(transit/write-handler % str)))
         {TransactionKey (transit/write-handler "xtdb/tx-key" tx-key-write-fn)
          TxOptions (transit/write-handler "xtdb/tx-opts" tx-opts-write-fn)
          xtdb.IllegalArgumentException (transit/write-handler "xtdb/illegal-arg" render-iae)
          xtdb.RuntimeException (transit/write-handler "xtdb/runtime-err" render-runex)
          clojure.lang.ExceptionInfo (transit/write-handler "xtdb/exception-info" (juxt ex-message ex-data))
          IKeyFn$KeyFn (transit/write-handler "xtdb/key-fn" write-key-fn)

          ClojureForm (transit/write-handler "xtdb/clj-form" #(.form ^ClojureForm %))

          IntervalYearMonth (transit/write-handler "xtdb.interval/year-month" #(str (.period ^IntervalYearMonth %)))

          IntervalDayTime (transit/write-handler "xtdb.interval/day-time"
                                                 #(vector (str (.period ^IntervalDayTime %))
                                                          (str (.duration ^IntervalDayTime %))))

          IntervalMonthDayNano (transit/write-handler "xtdb.interval/month-day-nano"
                                                      #(vector (str (.period ^IntervalMonthDayNano %))
                                                               (str (.duration ^IntervalMonthDayNano %))))
          clojure.lang.PersistentList (transit/write-handler "xtdb/list" #(pr-str %))

          XtqlQuery (transit/write-handler "xtdb.query/xtql" render-query)

          Sql (transit/write-handler "xtdb.tx/sql" render-sql-op)

          XtqlAndArgs (transit/write-handler "xtdb.tx/xtql" render-xtql+args)
          Xtql (transit/write-handler "xtdb.tx/xtql" render-xtql-op)

          Put (transit/write-handler "xtdb.tx/put" render-put-op)
          Delete (transit/write-handler "xtdb.tx/delete" render-delete-op)
          Erase (transit/write-handler "xtdb.tx/erase" render-erase-op)
          Call (transit/write-handler "xtdb.tx/call" render-call-op)}))

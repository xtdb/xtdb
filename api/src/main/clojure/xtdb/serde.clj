(ns ^{:clojure.tools.namespace.repl/load false}
    xtdb.serde
  (:require [clojure.edn :as edn]
            [cognitect.transit :as transit]
            [time-literals.read-write :as time-literals]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            [xtdb.time :as time])
  (:import java.io.Writer
           (java.time DayOfWeek Duration Instant LocalDate LocalDateTime LocalTime Month MonthDay OffsetDateTime OffsetTime Period Year YearMonth ZoneId ZonedDateTime)
           java.util.List
           [org.apache.arrow.vector PeriodDuration]
           (xtdb.api TransactionKey TxOptions)
           (xtdb.tx Ops Call Delete Erase Put Sql Xtql)
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

(defn- render-sql-op [^Sql op]
  {:sql (.sql op), :arg-rows (.argRows op)})

(defn sql-op-reader [{:keys [sql ^List arg-rows]}]
  (-> (Ops/sql sql)
      (.withArgs arg-rows)))

(defmethod print-dup Sql [op ^Writer w]
  (.write w (format "#xt.tx/sql %s" (pr-str (render-sql-op op)))))

(defmethod print-method Sql [op ^Writer w]
  (print-dup op w))

(defn- render-xtql-op [^Xtql op]
  {:xtql (.query op), :args (.args op)})

(defmethod print-dup Xtql [op ^Writer w]
  (.write w (format "#xt.tx/xtql %s" (pr-str (render-xtql-op op)))))

(defmethod print-method Xtql [op ^Writer w]
  (print-dup op w))

(defn xtql-op-reader [{:keys [xtql ^List args]}]
  (-> (Ops/xtql xtql)
      (.withArgs args)))

(defn- render-put-op [^Put op]
  {:table-name (.tableName op), :doc (.doc op)
   :valid-from (.validFrom op), :valid-to (.validTo op)})

(defmethod print-dup Put [op ^Writer w]
  (.write w (format "#xt.tx/put %s" (pr-str (render-put-op op)))))

(defmethod print-method Put [op ^Writer w]
  (print-dup op w))

(defn put-op-reader [{:keys [table-name doc valid-from valid-to]}]
  (-> (Ops/put table-name doc)
      (.during (time/->instant valid-from) (time/->instant valid-to))))

(defn- render-delete-op [^Delete op]
  {:table-name (.tableName op), :xt/id (.entityId op)
   :valid-from (.validFrom op), :valid-to (.validTo op)})

(defmethod print-dup Delete [op ^Writer w]
  (.write w (format "#xt.tx/delete %s" (pr-str (render-delete-op op)))))

(defmethod print-method Delete [op ^Writer w]
  (print-dup op w))

(defn delete-op-reader [{:keys [table-name xt/id valid-from valid-to]}]
  (-> (Ops/delete table-name id)
      (.during (time/->instant valid-from) (time/->instant valid-to))))

(defn- render-erase-op [^Erase op]
  {:table-name (.tableName op), :xt/id (.entityId op)})

(defmethod print-dup Erase [op ^Writer w]
  (.write w (format "#xt.tx/erase %s" (pr-str (render-erase-op op)))))

(defmethod print-method Erase [op ^Writer w]
  (print-dup op w))

(defn erase-op-reader [{:keys [table-name xt/id]}]
  (Ops/erase table-name id))

(defn- render-call-op [^Call op]
  {:fn-id (.fnId op), :args (.args op)})

(defmethod print-dup Call [op ^Writer w]
  (.write w (format "#xt.tx/call %s" (pr-str (render-call-op op)))))

(defmethod print-method Call [op ^Writer w]
  (print-dup op w))

(defn call-op-reader [{:keys [fn-id args]}]
  (Ops/call fn-id args))

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

(defmethod print-method TxOptions [tx-opts w]
  (print-dup tx-opts w))

(def transit-read-handlers
  (merge transit/default-read-handlers
         (-> time-literals/tags
             (update-keys str)
             (update-vals transit/read-handler))
         {"xtdb/clj-form" (transit/read-handler xt/->ClojureForm)
          "xtdb/tx-key" (transit/read-handler tx-key-read-fn)
          "xtdb/illegal-arg" (transit/read-handler err/-iae-reader)
          "xtdb/runtime-err" (transit/read-handler err/-runtime-err-reader)
          "xtdb/exception-info" (transit/read-handler #(ex-info (first %) (second %)))
          "xtdb/period-duration" period-duration-reader
          "xtdb.interval/year-month" interval-ym-reader
          "xtdb.interval/day-time" interval-dt-reader
          "xtdb.interval/month-day-nano" interval-mdn-reader
          "xtdb/list" (transit/read-handler edn/read-string)
          "xtdb.tx/sql" (transit/read-handler sql-op-reader)
          "xtdb.tx/xtql" (transit/read-handler xtql-op-reader)
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
          xtdb.IllegalArgumentException (transit/write-handler "xtdb/illegal-arg" ex-data)
          xtdb.RuntimeException (transit/write-handler "xtdb/runtime-err" ex-data)
          clojure.lang.ExceptionInfo (transit/write-handler "xtdb/exception-info" #(vector (ex-message %) (ex-data %)))

          ClojureForm (transit/write-handler "xtdb/clj-form" #(.form ^ClojureForm %))

          IntervalYearMonth (transit/write-handler "xtdb.interval/year-month" #(str (.period ^IntervalYearMonth %)))

          IntervalDayTime (transit/write-handler "xtdb.interval/day-time"
                                                 #(vector (str (.period ^IntervalDayTime %))
                                                          (str (.duration ^IntervalDayTime %))))

          IntervalMonthDayNano (transit/write-handler "xtdb.interval/month-day-nano"
                                                      #(vector (str (.period ^IntervalMonthDayNano %))
                                                               (str (.duration ^IntervalMonthDayNano %))))
          clojure.lang.PersistentList (transit/write-handler "xtdb/list" #(pr-str %))

          Sql (transit/write-handler "xtdb.tx/sql" render-sql-op)
          Xtql (transit/write-handler "xtdb.tx/xtql" render-xtql-op)
          Put (transit/write-handler "xtdb.tx/put" render-put-op)
          Delete (transit/write-handler "xtdb.tx/delete" render-delete-op)
          Erase (transit/write-handler "xtdb.tx/erase" render-erase-op)
          Call (transit/write-handler "xtdb.tx/call" render-call-op)}))

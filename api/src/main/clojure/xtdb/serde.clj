(ns ^{:clojure.tools.namespace.repl/load false}
    xtdb.serde
  (:require [clojure.edn :as edn]
            [cognitect.transit :as transit]
            [time-literals.read-write :as time-literals]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            [xtdb.time :as time]
            [xtdb.types :as types])
  (:import (com.cognitect.transit TransitFactory)
           java.io.Writer
           (java.time DayOfWeek Duration Instant LocalDate LocalDateTime LocalTime Month MonthDay OffsetDateTime OffsetTime Period Year YearMonth ZoneId ZonedDateTime)
           java.util.List
           [org.apache.arrow.vector PeriodDuration]
           (org.apache.arrow.vector.types.pojo ArrowType Field FieldType)
           xtdb.api.TransactionKey
           (xtdb.tx Ops Ops$Call Ops$Delete Ops$Erase Ops$Put Ops$Sql Ops$Xtql)
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

(defn- render-sql-op [^Ops$Sql op]
  {:sql (.sql op), :arg-rows (.argRows op)})

(defn- sql-op-reader [{:keys [sql ^List arg-rows]}]
  (-> (Ops/sql sql)
      (.withArgs arg-rows)))

(defmethod print-dup Ops$Sql [op ^Writer w]
  (.write w (format "#xt.tx/sql %s" (pr-str (render-sql-op op)))))

(defmethod print-method Ops$Sql [op ^Writer w]
  (print-dup op w))

(defn- render-xtql-op [^Ops$Xtql op]
  {:xtql (.query op), :args (.args op)})

(defmethod print-dup Ops$Xtql [op ^Writer w]
  (.write w (format "#xt.tx/xtql %s" (pr-str (render-xtql-op op)))))

(defmethod print-method Ops$Xtql [op ^Writer w]
  (print-dup op w))

(defn- xtql-op-reader [{:keys [xtql ^List args]}]
  (-> (Ops/xtql xtql)
      (.withArgs args)))

(defn- render-put-op [^Ops$Put op]
  {:table-name (.tableName op), :doc (.doc op)
   :valid-from (.validFrom op), :valid-to (.validTo op)})

(defmethod print-dup Ops$Put [op ^Writer w]
  (.write w (format "#xt.tx/put %s" (pr-str (render-put-op op)))))

(defmethod print-method Ops$Put [op ^Writer w]
  (print-dup op w))

(defn- put-op-reader [{:keys [table-name doc valid-from valid-to]}]
  (-> (Ops/put table-name doc)
      (.during (time/->instant valid-from) (time/->instant valid-to))))

(defn- render-delete-op [^Ops$Delete op]
  {:table-name (.tableName op), :xt/id (.entityId op)
   :valid-from (.validFrom op), :valid-to (.validTo op)})

(defmethod print-dup Ops$Delete [op ^Writer w]
  (.write w (format "#xt.tx/delete %s" (pr-str (render-delete-op op)))))

(defmethod print-method Ops$Delete [op ^Writer w]
  (print-dup op w))

(defn- delete-op-reader [{:keys [table-name xt/id valid-from valid-to]}]
  (-> (Ops/delete table-name id)
      (.during (time/->instant valid-from) (time/->instant valid-to))))

(defn- render-erase-op [^Ops$Erase op]
  {:table-name (.tableName op), :xt/id (.entityId op)})

(defmethod print-dup Ops$Erase [op ^Writer w]
  (.write w (format "#xt.tx/erase %s" (pr-str (render-erase-op op)))))

(defmethod print-method Ops$Erase [op ^Writer w]
  (print-dup op w))

(defn- erase-op-reader [{:keys [table-name xt/id]}]
  (Ops/erase table-name id))

(defn- render-call-op [^Ops$Call op]
  {:fn-id (.fnId op), :args (.args op)})

(defmethod print-dup Ops$Call [op ^Writer w]
  (.write w (format "#xt.tx/call %s" (pr-str (render-call-op op)))))

(defmethod print-method Ops$Call [op ^Writer w]
  (print-dup op w))

(defn- call-op-reader [{:keys [fn-id args]}]
  (Ops/call fn-id args))

(def tj-read-handlers
  (merge (-> time-literals/tags
             (update-keys str)
             (update-vals transit/read-handler))
         {"xtdb/clj-form" (transit/read-handler xt/->ClojureForm)
          "xtdb/tx-key" (transit/read-handler xt/map->TransactionKey)
          "xtdb/illegal-arg" (transit/read-handler err/-iae-reader)
          "xtdb/runtime-err" (transit/read-handler err/-runtime-err-reader)
          "xtdb/exception-info" (transit/read-handler #(ex-info (first %) (second %)))
          "xtdb/period-duration" period-duration-reader
          "xtdb.interval/year-month" interval-ym-reader
          "xtdb.interval/day-time" interval-dt-reader
          "xtdb.interval/month-day-nano" interval-mdn-reader
          "xtdb/list" (transit/read-handler edn/read-string)
          "xtdb/arrow-type" (transit/read-handler types/->arrow-type)
          "xtdb/field-type" (transit/read-handler (fn [[arrow-type nullable?]]
                                                    (if nullable?
                                                      (FieldType/nullable arrow-type)
                                                      (FieldType/notNullable arrow-type))))
          "xtdb/field" (transit/read-handler (fn [[name field-type children]]
                                               (Field. name field-type children)))

          "xtdb.tx/sql" (transit/read-handler sql-op-reader)
          "xtdb.tx/xtql" (transit/read-handler xtql-op-reader)
          "xtdb.tx/put" (transit/read-handler put-op-reader)
          "xtdb.tx/delete" (transit/read-handler delete-op-reader)
          "xtdb.tx/erase" (transit/read-handler erase-op-reader)
          "xtdb.tx/call" (transit/read-handler call-op-reader)}))

(def tj-write-handlers
  (merge (-> {Period "time/period"
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
         {TransactionKey (transit/write-handler "xtdb/tx-key" #(select-keys % [:tx-id :system-time]))
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
          ArrowType (transit/write-handler "xtdb/arrow-type" #(types/<-arrow-type %))
          ;; beware that this currently ignores dictionary encoding and metadata of FieldType's
          FieldType (transit/write-handler "xtdb/field-type"
                                           (fn [^FieldType field-type]
                                             (TransitFactory/taggedValue "array" [(.getType field-type) (.isNullable field-type)])))
          Field (transit/write-handler "xtdb/field"
                                       (fn [^Field field]
                                         (TransitFactory/taggedValue "array" [(.getName field) (.getFieldType field) (.getChildren field)])))

          Ops$Sql (transit/write-handler "xtdb.tx/sql" render-sql-op)
          Ops$Xtql (transit/write-handler "xtdb.tx/xtql" render-xtql-op)
          Ops$Put (transit/write-handler "xtdb.tx/put" render-put-op)
          Ops$Delete (transit/write-handler "xtdb.tx/delete" render-delete-op)
          Ops$Erase (transit/write-handler "xtdb.tx/erase" render-erase-op)
          Ops$Call (transit/write-handler "xtdb.tx/call" render-call-op)}))

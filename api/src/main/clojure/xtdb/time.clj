(ns xtdb.time
  (:require [clojure.spec.alpha :as s]
            [xtdb.protocols :as xtp])
  (:import (java.time Duration Instant LocalDate LocalDateTime LocalTime OffsetDateTime ZoneId ZonedDateTime)
           java.time.temporal.ChronoUnit
           (java.util Date)))

(defn ->duration [d]
  (cond
    (instance? Duration d) d
    (nat-int? d) (Duration/ofMillis d)
    (string? d) (Duration/parse d)
    :else ::s/invalid))

(s/def ::duration
  (s/and (s/conformer ->duration) #(instance? Duration %)))

(s/def ::datetime-value
  (some-fn (partial instance? Date)
           (partial instance? Instant)
           (partial instance? ZonedDateTime)))

(defprotocol TimeConversions
  (^java.time.Instant ->instant [v])
  (^java.time.ZonedDateTime ->zdt [v]))

(def utc (ZoneId/of "UTC"))

(extend-protocol TimeConversions
  nil
  (->instant [_] nil)
  (->zdt [_] nil)

  Instant
  (->instant [i] i)
  (->zdt [i] (-> i (.atZone utc)))

  Date
  (->instant [d] (.toInstant d))
  (->zdt [d] (->zdt (->instant d)))

  ZonedDateTime
  (->instant [zdt] (.toInstant zdt))
  (->zdt [zdt] zdt)

  OffsetDateTime
  (->instant [odt] (.toInstant odt)))

(defn instant->micros ^long [^Instant inst]
  (-> (Math/multiplyExact (.getEpochSecond inst) #=(long 1e6))
      (Math/addExact (quot (.getNano inst) 1000))))

(defn sql-temporal->micros
  "Given some temporal value (such as a Date, LocalDateTime, OffsetDateTime and so on) will return the corresponding Instant.

   To do this for LocalDate and LocalDateTime, the provided SQL session time zone is assumed to be the implied time zone of the date/time."
  ^long [temporal ^ZoneId session-zone]
  (condp instance? temporal
    LocalDate (sql-temporal->micros (.atTime ^LocalDate temporal LocalTime/MIDNIGHT) session-zone)
    LocalDateTime (instant->micros (.toInstant (.atZone ^LocalDateTime temporal session-zone)))
    (instant->micros (->instant temporal))))

(defn instant->nanos ^long [^Instant inst]
  (-> (Math/multiplyExact (.getEpochSecond inst) #=(long 1e9))
      (Math/addExact (long (.getNano inst)))))

(defn micros->instant ^java.time.Instant [^long μs]
  (.plus Instant/EPOCH μs ChronoUnit/MICROS))

(defn nanos->instant ^java.time.Instant [^long ns]
  (.plus Instant/EPOCH ns ChronoUnit/NANOS))

(def ^java.time.Instant end-of-time
  (micros->instant Long/MAX_VALUE))

(defn max-tx [l r]
  (if (or (nil? l)
          (and r (neg? (compare l r))))
    r
    l))

(defn after-latest-submitted-tx [{:keys [basis] :as opts} node]
  (cond-> opts
    (not (or (contains? basis :at-tx) (contains? opts :after-tx)))
    (assoc :after-tx (xtp/latest-submitted-tx node))))

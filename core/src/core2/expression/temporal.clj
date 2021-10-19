(ns core2.expression.temporal
  (:require [clojure.walk :as w]
            [core2.expression :as expr]
            [core2.expression.metadata :as expr.meta]
            [core2.temporal :as temporal]
            [core2.types :as types]
            [core2.util :as util])
  (:import java.time.Instant
           java.util.Date
           [org.apache.arrow.vector.types.pojo ArrowType$Bool ArrowType$Duration ArrowType$Timestamp]))

(set! *unchecked-math* :warn-on-boxed)

;; SQL:2011 Time-related-predicates

(defmethod expr/codegen-call [:overlaps ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp]
  [{[{x-start :code} {x-end :code} {y-start :code} {y-end :code}] :args}]

  {:code `(and (< ~x-start ~y-end) (> ~x-end ~y-start))
   :return-type ArrowType$Bool/INSTANCE})

(defmethod expr/codegen-call [:contains ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp]
  [{[{x-start :code} {x-end :code} {y :code}] :args}]

  {:code `(let [y# ~y]
            (and (<= ~x-start y#) (> ~x-end y#)))
   :return-type ArrowType$Bool/INSTANCE})

(defmethod expr/codegen-call [:contains ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp]
  [{[{x-start :code} {x-end :code} {y-start :code}  {y-end :code} ] :args}]

  {:code `(and (<= ~x-start ~y-start) (>= ~x-end ~y-end))
   :return-type ArrowType$Bool/INSTANCE})

(defmethod expr/codegen-call [:precedes ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp]
  [{[_x-start {x-end :code} {y-start :code} _y-end] :args}]

  {:code `(<= ~x-end ~y-start)
   :return-type ArrowType$Bool/INSTANCE})

(defmethod expr/codegen-call [:succeeds ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp]
  [{[{x-start :code} _x-end _y-start {y-end :code}] :args}]

  {:code `(>= ~x-start ~y-end)
   :return-type ArrowType$Bool/INSTANCE})

(defmethod expr/codegen-call [:immediately-precedes ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp]
  [{[_x-start {x-end :code} {y-start :code} _y-end] :args}]

  {:code `(= ~x-end ~y-start)
   :return-type ArrowType$Bool/INSTANCE})

(defmethod expr/codegen-call [:immediately-succeeds ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp]
  [{[{x-start :code} _x-end _y-start {y-end :code}] :args}]

  {:code `(= ~x-start ~y-end)
   :return-type ArrowType$Bool/INSTANCE})

;; SQL:2011 Operations involving datetimes and intervals

(defmethod expr/codegen-call [:- ArrowType$Timestamp ArrowType$Timestamp] [{:keys [emitted-args]}]
  {:code `(- ~@emitted-args)
   :return-type types/duration-micro-type})

(defmethod expr/codegen-call [:- ArrowType$Timestamp ArrowType$Duration] [{:keys [emitted-args]}]
  {:code `(- ~@emitted-args)
   :return-type types/timestamp-micro-tz-type})

(defmethod expr/codegen-call [:+ ArrowType$Timestamp ArrowType$Duration] [{:keys [emitted-args]}]
  {:code `(+ ~@emitted-args)
   :return-type types/timestamp-micro-tz-type})

(defmethod expr/codegen-call [:- ArrowType$Duration ArrowType$Duration] [{:keys [emitted-args]}]
  {:code `(+ ~@emitted-args)
   :return-type types/duration-micro-type})

(defmethod expr/codegen-call [:+ ArrowType$Duration ArrowType$Timestamp] [{:keys [emitted-args]}]
  {:code `(+ ~@emitted-args)
   :return-type types/timestamp-micro-tz-type})

(defmethod expr/codegen-call [:+ ArrowType$Duration ArrowType$Duration] [{:keys [emitted-args]}]
  {:code `(+ ~@emitted-args)
   :return-type types/duration-micro-type})

(defmethod expr/codegen-call [:* ArrowType$Duration ::types/Number] [{:keys [emitted-args]}]
  {:code `(* ~@emitted-args)
   :return-type types/duration-micro-type})

(defmethod expr/codegen-call [:* ::types/Number ArrowType$Duration] [{:keys [emitted-args]}]
  {:code `(* ~@emitted-args)
   :return-type types/duration-micro-type})

(defmethod expr/codegen-call [:/ ArrowType$Duration ::types/Number] [{:keys [emitted-args]}]
  {:code `(quot ~@emitted-args)
   :return-type types/duration-micro-type})

(defn apply-constraint [^longs min-range ^longs max-range
                        f col-name ^Instant time]
  (let [range-idx (temporal/->temporal-column-idx col-name)
        time-μs (util/instant->micros time)]
    (case f
      < (aset max-range range-idx (min (dec time-μs)
                                       (aget max-range range-idx)))
      <= (aset max-range range-idx (min time-μs
                                        (aget max-range range-idx)))
      > (aset min-range range-idx (max (inc time-μs)
                                       (aget min-range range-idx)))
      >= (aset min-range range-idx (max time-μs
                                        (aget min-range range-idx)))
      nil)))

(defn ->temporal-min-max-range [selects srcs]
  (let [min-range (temporal/->min-range)
        max-range (temporal/->max-range)]
    (doseq [[col-name select-form] selects
            :when (temporal/temporal-column? col-name)
            :let [select-expr (expr/form->expr select-form srcs)
                  {:keys [expr param-types params]} (expr/normalise-params select-expr srcs)
                  meta-expr (@#'expr.meta/meta-expr expr param-types)]]
      (w/prewalk (fn [x]
                   (when-not (and (map? x) (= 'or (:f x)))
                     (when (and (map? x) (= :metadata-vp-call (:op x)))
                       (let [{:keys [f param]} x]
                         (apply-constraint min-range max-range
                                           f col-name (util/->instant (get params param)))))
                     x))
                 meta-expr))
    [min-range max-range]))

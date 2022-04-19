(ns core2.expression.temporal
  (:require [clojure.walk :as w]
            [core2.expression :as expr]
            [core2.expression.macro :as emacro]
            [core2.expression.metadata :as expr.meta]
            [core2.expression.walk :as ewalk]
            [core2.temporal :as temporal]
            [core2.types :as types]
            [core2.util :as util])
  (:import java.time.Instant
           [org.apache.arrow.vector.types.pojo ArrowType$Duration ArrowType$Int ArrowType$Timestamp ArrowType$Interval ArrowType$Date]
           org.apache.arrow.vector.types.TimeUnit
           [java.time LocalDate]))

(set! *unchecked-math* :warn-on-boxed)

;; SQL:2011 Time-related-predicates
;; FIXME: these don't take different granularities of timestamp into account

(defmethod expr/codegen-call [:overlaps ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp] [_]
  {:continue-call (fn [f [x-start x-end y-start y-end]]
                    (f types/bool-type
                       `(and (< ~x-start ~y-end) (> ~x-end ~y-start))))
   :return-type types/bool-type})

(defmethod expr/codegen-call [:contains ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp] [_]
  {:continue-call (fn [f [x-start x-end y]]
                    (f types/bool-type
                       `(let [y# ~y]
                          (and (<= ~x-start y#) (> ~x-end y#)))))
   :return-type types/bool-type})

(defmethod expr/codegen-call [:contains ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp] [_]
  {:continue-call (fn [f [x-start x-end y-start y-end]]
                    (f types/bool-type
                       `(and (<= ~x-start ~y-start) (>= ~x-end ~y-end))))
   :return-type types/bool-type})

(defmethod expr/codegen-call [:precedes ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp] [_]
  {:continue-call (fn [f [_x-start x-end y-start _y-end]]
                    (f types/bool-type
                       `(<= ~x-end ~y-start)))
   :return-type types/bool-type})

(defmethod expr/codegen-call [:succeeds ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp] [_]
  {:continue-call (fn [f [x-start _x-end _y-start y-end]]
                    (f types/bool-type
                       `(>= ~x-start ~y-end)))
   :return-type types/bool-type})

(defmethod expr/codegen-call [:immediately-precedes ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp] [_]
  {:continue-call (fn [f [_x-start x-end y-start _y-end]]
                    (f types/bool-type
                       `(= ~x-end ~y-start)))
   :return-type types/bool-type})

(defmethod expr/codegen-call [:immediately-succeeds ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp] [_]
  {:continue-call (fn [f [x-start _x-end _y-start y-end]]
                    (f types/bool-type
                       `(= ~x-start ~y-end)))
   :return-type types/bool-type})

;; SQL:2011 Operations involving datetimes and intervals

(defn- units-per-second ^long [^TimeUnit time-unit]
  (case (.name time-unit)
    "SECOND" 1
    "MILLISECOND" 1000
    "MICROSECOND" 1000000
    "NANOSECOND" 1000000000))

(defn- smallest-unit [x-unit y-unit]
  (if (> (units-per-second x-unit) (units-per-second y-unit))
    x-unit
    y-unit))

(defn- with-conversion [form from-unit to-unit]
  (if (= from-unit to-unit)
    form
    `(Math/multiplyExact ~form ~(quot (units-per-second to-unit) (units-per-second from-unit)))))

(defmethod expr/codegen-call [:+ ArrowType$Timestamp ArrowType$Duration] [{[^ArrowType$Timestamp x-type, ^ArrowType$Duration y-type] :arg-types}]
  (let [x-unit (.getUnit x-type)
        y-unit (.getUnit y-type)
        res-unit (smallest-unit x-unit y-unit)
        return-type (ArrowType$Timestamp. res-unit (.getTimezone x-type))]
    {:return-types #{return-type}
     :continue-call (fn [f [x-arg y-arg]]
                      (f return-type
                         `(Math/addExact ~(-> x-arg (with-conversion x-unit res-unit))
                                         ~(-> y-arg (with-conversion y-unit res-unit)))))}))

(defmethod expr/codegen-call [:+ ArrowType$Duration ArrowType$Timestamp] [{[^ArrowType$Duration x-type, ^ArrowType$Timestamp y-type] :arg-types}]
  (let [x-unit (.getUnit x-type)
        y-unit (.getUnit y-type)
        res-unit (smallest-unit x-unit y-unit)
        return-type (ArrowType$Timestamp. res-unit (.getTimezone y-type))]
    {:return-types #{return-type}
     :continue-call (fn [f [x-arg y-arg]]
                      (f return-type
                         `(Math/addExact ~(-> x-arg (with-conversion x-unit res-unit))
                                         ~(-> y-arg (with-conversion y-unit res-unit)))))}))

(defmethod expr/codegen-call [:+ ArrowType$Duration ArrowType$Duration] [{[^ArrowType$Duration x-type, ^ArrowType$Duration y-type] :arg-types}]
  (let [x-unit (.getUnit x-type)
        y-unit (.getUnit y-type)
        res-unit (smallest-unit x-unit y-unit)
        return-type (ArrowType$Duration. res-unit)]
    {:return-types #{return-type}
     :continue-call (fn [f [x-arg y-arg]]
                      (f return-type
                         `(Math/addExact ~(-> x-arg (with-conversion x-unit res-unit))
                                         ~(-> y-arg (with-conversion y-unit res-unit)))))}))

(defmethod expr/codegen-call [:+ ArrowType$Date ArrowType$Interval] [{[^ArrowType$Date x-type, ^ArrowType$Interval y-type] :arg-types}]
  (let [return-type x-type]
    {:return-types #{return-type}
     :continue-call (fn [f [x-arg y-arg]]
                      (f return-type `(.toEpochDay (.plus (LocalDate/ofEpochDay ~x-arg) (.getPeriod ~y-arg)))))}))

(defmethod expr/codegen-call [:- ArrowType$Timestamp ArrowType$Timestamp] [{[^ArrowType$Timestamp x-type, ^ArrowType$Timestamp y-type] :arg-types}]
  (let [x-unit (.getUnit x-type)
        y-unit (.getUnit y-type)
        res-unit (smallest-unit x-unit y-unit)
        return-type (ArrowType$Duration. res-unit)]
    {:return-types #{return-type}
     :continue-call (fn [f [x-arg y-arg]]
                      (f return-type
                         `(Math/subtractExact ~(-> x-arg (with-conversion x-unit res-unit))
                                              ~(-> y-arg (with-conversion y-unit res-unit)))))}))

(defmethod expr/codegen-call [:- ArrowType$Timestamp ArrowType$Duration] [{[^ArrowType$Timestamp x-type, ^ArrowType$Duration y-type] :arg-types}]
  (let [x-unit (.getUnit x-type)
        y-unit (.getUnit y-type)
        res-unit (smallest-unit x-unit y-unit)
        return-type (ArrowType$Timestamp. res-unit (.getTimezone x-type))]
    {:return-types #{return-type}
     :continue-call (fn [f [x-arg y-arg]]
                      (f return-type
                         `(Math/subtractExact ~(-> x-arg (with-conversion x-unit res-unit))
                                              ~(-> y-arg (with-conversion y-unit res-unit)))))}))

(defmethod expr/codegen-call [:- ArrowType$Duration ArrowType$Duration] [{[^ArrowType$Duration x-type, ^ArrowType$Duration y-type] :arg-types}]
  (let [x-unit (.getUnit x-type)
        y-unit (.getUnit y-type)
        res-unit (smallest-unit x-unit y-unit)
        return-type (ArrowType$Duration. res-unit)]
    {:return-types #{return-type}
     :continue-call (fn [f [x-arg y-arg]]
                      (f return-type
                         `(Math/subtractExact ~(-> x-arg (with-conversion x-unit res-unit))
                                              ~(-> y-arg (with-conversion y-unit res-unit)))))}))

(defmethod expr/codegen-call [:- ArrowType$Date ArrowType$Interval] [{[^ArrowType$Date x-type, ^ArrowType$Interval y-type] :arg-types}]
  (let [return-type x-type]
    {:return-types #{return-type}
     :continue-call (fn [f [x-arg y-arg]]
                      (f return-type `(.toEpochDay (.minus (LocalDate/ofEpochDay ~x-arg) (.getPeriod ~y-arg)))))}))

(defmethod expr/codegen-call [:* ArrowType$Duration ArrowType$Int] [{[x-type _y-type] :arg-types}]
  {:continue-call (fn [f emitted-args]
                    (f x-type
                       `(Math/multiplyExact ~@emitted-args)))
   :return-types #{x-type}})

(defmethod expr/codegen-call [:* ArrowType$Duration ::types/Number] [{[x-type _y-type] :arg-types}]
  {:continue-call (fn [f emitted-args]
                    (f x-type
                       `(* ~@emitted-args)))
   :return-types #{x-type}})

(defmethod expr/codegen-call [:* ArrowType$Int ArrowType$Duration] [{[_x-type y-type] :arg-types}]
  {:continue-call (fn [f emitted-args]
                    (f y-type
                       `(Math/multiplyExact ~@emitted-args)))
   :return-types #{y-type}})

(defmethod expr/codegen-call [:* ::types/Number ArrowType$Duration] [{[_x-type y-type] :arg-types}]
  {:continue-call (fn [f emitted-args]
                    (f y-type
                       `(long (* ~@emitted-args))))
   :return-types #{y-type}})

(defmethod expr/codegen-call [:/ ArrowType$Duration ::types/Number] [{[x-type] :arg-types}]
  {:continue-call (fn [f emitted-args]
                    (f x-type
                       `(quot ~@emitted-args)))
   :return-types #{x-type}})

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
        max-range (temporal/->max-range)
        col-names (into #{} (map symbol) (keys temporal/temporal-fields))]
    (doseq [[col-name select-form] selects
            :when (temporal/temporal-column? col-name)]
      (->> (expr/form->expr select-form {:params srcs, :col-names col-names})
           (emacro/macroexpand-all)
           (ewalk/postwalk-expr expr/lit->param)
           (expr.meta/meta-expr)
           (ewalk/prewalk-expr
            (fn [{:keys [op] :as expr}]
              (case op
                :call (when (not= 'or (:f expr))
                        expr)

                :metadata-vp-call
                (let [{:keys [f param-expr]} expr]
                  (apply-constraint min-range max-range
                                    f col-name
                                    (util/->instant (some-> (or (find param-expr :literal)
                                                                (find srcs (get param-expr :param)))
                                                            val))))

                expr)))))
    [min-range max-range]))

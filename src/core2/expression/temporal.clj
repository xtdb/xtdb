(ns core2.expression.temporal
  (:require [clojure.walk :as w]
            [core2.expression :as expr]
            [core2.temporal :as temporal]
            [core2.expression.metadata :as expr.meta]
            [core2.types :as types])
  (:import java.util.Date
           java.time.Duration))

;; SQL:2011 Time-related-predicates

(defmethod expr/codegen-call [:overlaps Date Date Date Date] [{[{x-start :code} {x-end :code} {y-start :code}  {y-end :code} ] :args}]
  {:code `(and (< x-start y-end) (> x-end y-start))
   :return-type Boolean})

(defmethod expr/codegen-call [:contains Date Date Date] [{[{x-start :code} {x-end :code} {y-start :code}  {y :code} ] :args}]
  {:code `(and (<= x-start y) (> x-end y))
   :return-type Boolean})

(defmethod expr/codegen-call [:contains Date Date Date Date] [{[{x-start :code} {x-end :code} {y-start :code}  {y-end :code} ] :args}]
  {:code `(and (<= x-start y-start) (>= x-end y-end))
   :return-type Boolean})

(defmethod expr/codegen-call [:precedes Date Date Date Date] [{[{x-start :code} {x-end :code} {y-start :code}  {y-end :code} ] :args}]
  {:code `(<= x-end y-start)
   :return-type Boolean})

(defmethod expr/codegen-call [:succeedes Date Date Date Date] [{[{x-start :code} {x-end :code} {y-start :code}  {y-end :code} ] :args}]
  {:code `(>= x-start y-end)
   :return-type Boolean})

(defmethod expr/codegen-call [:immediately-precedes Date Date Date Date] [{[{x-start :code} {x-end :code} {y-start :code}  {y-end :code}] :args}]
  {:code `(= x-end y-start)
   :return-type Boolean})

(defmethod expr/codegen-call [:immediately-succeedes Date Date Date Date] [{[{x-start :code} {x-end :code} {y-start :code}  {y-end :code} ] :args}]
  {:code `(= x-start y-end)
   :return-type Boolean})

;; SQL:2011 Operations involving datetimes and intervals

;; TODO: no support for duration vectors yet.

;; (defmethod expr/codegen-call [:- Date Date] [{:keys [emitted-args]}]
;;   {:code `(- ~@emitted-args)
;;    :return-type Duration})

;; (defmethod expr/codegen-call [:- Date Duration] [{:keys [emitted-args]}]
;;   {:code `(- ~@emitted-args)
;;    :return-type Date})

;; (defmethod expr/codegen-call [:+ Date Duration] [{:keys [emitted-args]}]
;;   {:code `(+ ~@emitted-args)
;;    :return-type Date})

;; (defmethod expr/codegen-call [:- Duration Duration] [{:keys [emitted-args]}]
;;   {:code `(+ ~@emitted-args)
;;    :return-type Duration})

;; (defmethod expr/codegen-call [:+ Duration Date] [{:keys [emitted-args]}]
;;   {:code `(+ ~@emitted-args)
;;    :return-type Date})

;; (defmethod expr/codegen-call [:+ Duration Duration] [{:keys [emitted-args]}]
;;   {:code `(+ ~@emitted-args)
;;    :return-type Duration})

;; (defmethod expr/codegen-call [:* Duration Number] [{:keys [emitted-args]}]
;;   {:code `(* ~@emitted-args)
;;    :return-type Duration})

;; (defmethod expr/codegen-call [:* Number Duration] [{:keys [emitted-args]}]
;;   {:code `(* ~@emitted-args)
;;    :return-type Duration})

;; (defmethod expr/codegen-call [:/ Duration Number] [{:keys [emitted-args]}]
;;   {:code `(quot ~@emitted-args)
;;    :return-type Duration})

(defn ->temporal-min-max-range [selects srcs]
  (let [min-range (temporal/->min-range)
        max-range (temporal/->max-range)]
    (doseq [[col-name select-expr] selects
            :when (temporal/temporal-column? col-name)
            :let [range-idx (case col-name
                              "_valid-time-start" temporal/valid-time-start-idx
                              "_valid-time-end" temporal/valid-time-end-idx
                              "_tx-time-start" temporal/tx-time-start-idx
                              "_tx-time-end" temporal/tx-time-end-idx)
                  {:keys [expr param-types params]} (expr/normalise-params select-expr srcs)
                  vars (zipmap (map first param-types) params)
                  var->type (into {} param-types)
                  meta-expr (@#'expr.meta/meta-expr expr var->type)]]
      (w/prewalk (fn [x]
                   (when-not (and (map? x) (= 'or (:f x)))
                     (when (and (map? x) (= :metadata-vp-call (:op x)))
                       (let [{:keys [f param]} x
                             time-ms (.getTime ^Date (get vars param))]
                         (case f
                           < (aset min-range range-idx (max (inc time-ms)
                                                            (aget min-range range-idx)))
                           <= (aset min-range range-idx (max time-ms
                                                             (aget min-range range-idx)))
                           > (aset max-range range-idx (min (dec time-ms)
                                                            (aget max-range range-idx)))
                           >= (aset max-range range-idx (min time-ms
                                                             (aget max-range range-idx)))
                           nil)))
                     x))
                 meta-expr))
    [min-range max-range]))

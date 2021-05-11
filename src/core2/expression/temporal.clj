(ns core2.expression.temporal
  (:require [core2.expression :as expr]
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

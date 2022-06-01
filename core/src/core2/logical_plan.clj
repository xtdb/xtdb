(ns core2.logical-plan
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [core2.util :as util]))

;; See also:
;; https://dbis-uibk.github.io/relax/help#relalg-reference
;; https://calcite.apache.org/javadocAggregate/org/apache/calcite/tools/RelBuilder.html
;; https://github.com/apache/arrow/blob/master/rust/datafusion/src/logical_plan/plan.rs

;; See "Formalising openCypher Graph Queries in Relational Algebra",
;; also contains operators for path expansion:
;; https://core.ac.uk/download/pdf/148787624.pdf

(s/def ::relation simple-symbol?)
(s/def ::column simple-symbol?)

(defn source-sym? [sym]
  (str/starts-with? (name sym) "$"))

(s/def ::source
  (s/and simple-symbol? source-sym?))

(s/def ::expression any?)

(s/def ::column-expression (s/map-of ::column ::expression :conform-keys true :count 1))

(defmulti ra-expr
  (fn [expr]
    (cond
      (vector? expr) (first expr)
      (symbol? expr) :relation))
  :default ::default)

(s/def ::ra-expression (s/multi-spec ra-expr :op))

(s/def ::logical-plan ::ra-expression)

#_{:clj-kondo/ignore #{:unused-binding}}
(defmulti emit-expr
  (fn [ra-expr srcs]
    (:op ra-expr)))

(defn unary-expr {:style/indent 2} [relation args f]
  (let [{->inner-cursor :->cursor, inner-col-names :col-names} (emit-expr relation args)
        {:keys [col-names ->cursor]} (f inner-col-names)]
    {:col-names col-names
     :->cursor (fn [opts]
                 (let [inner (->inner-cursor opts)]
                   (try
                     (->cursor opts inner)
                     (catch Throwable e
                       (util/try-close inner)
                       (throw e)))))}))

(defn binary-expr {:style/indent 3} [left right args f]
  (let [{left-col-names :col-names, ->left-cursor :->cursor} (emit-expr left args)
        {right-col-names :col-names, ->right-cursor :->cursor} (emit-expr right args)
        {:keys [col-names ->cursor]} (f left-col-names right-col-names)]

    {:col-names col-names
     :->cursor (fn [opts]
                 (let [left (->left-cursor opts)]
                   (try
                     (let [right (->right-cursor opts)]
                       (try
                         (->cursor opts left right)
                         (catch Throwable e
                           (util/try-close right)
                           (throw e))))
                     (catch Throwable e
                       (util/try-close left)
                       (throw e)))))}))

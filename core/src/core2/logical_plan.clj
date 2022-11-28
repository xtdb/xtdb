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

(s/def ::param
  (s/and simple-symbol? #(str/starts-with? (name %) "?")))

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

(defn- direct-child-exprs [{:keys [op] :as expr}]
  (case op
    :relation #{}

    :assign (let [{:keys [bindings relation]} expr]
              (into #{relation} (map :value) bindings))

    (let [spec (s/describe (ra-expr [op]))]
      (case (first spec)
        cat (->> (rest spec)
                 (partition 2)
                 (mapcat
                   (fn [[k form]]
                     (cond
                       (= form ::ra-expression)
                       [(expr k)]
                       (= form (list 'coll-of :core2.logical-plan/ra-expression))
                       (expr k))))
                 (vec))))))

(defn child-exprs [ra]
  (into #{ra} (mapcat child-exprs) (direct-child-exprs ra)))

#_{:clj-kondo/ignore #{:unused-binding}}
(defmulti emit-expr
  (fn [ra-expr srcs]
    (:op ra-expr)))

(defn unary-expr {:style/indent 2} [relation f]
  (let [{->inner-cursor :->cursor, inner-col-types :col-types} relation
        {:keys [col-types ->cursor]} (f inner-col-types)]
    {:col-types col-types
     :->cursor (fn [opts]
                 (let [inner (->inner-cursor opts)]
                   (try
                     (->cursor opts inner)
                     (catch Throwable e
                       (util/try-close inner)
                       (throw e)))))}))

(defn binary-expr {:style/indent 3} [left right f]
  (let [{left-col-types :col-types, ->left-cursor :->cursor} left
        {right-col-types :col-types, ->right-cursor :->cursor} right
        {:keys [col-types ->cursor]} (f left-col-types right-col-types)]

    {:col-types col-types
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

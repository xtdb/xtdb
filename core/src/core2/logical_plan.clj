(ns core2.logical-plan
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [core2.util :as util]))

;; Partly based on
;; https://dbis-uibk.github.io/relax/help#relalg-reference
;; See also:
;; https://calcite.apache.org/javadocAggregate/org/apache/calcite/tools/RelBuilder.html
;; https://github.com/apache/arrow/blob/master/rust/datafusion/src/logical_plan/plan.rs

;; See "Formalising openCypher Graph Queries in Relational Algebra",
;; also contains operators for path expansion:
;; https://core.ac.uk/download/pdf/148787624.pdf

(s/def ::relation simple-symbol?)
(s/def ::column simple-symbol?)

(s/def ::source
  (s/and simple-symbol?
         (comp #(str/starts-with? % "$") name)))

(defmulti ra-expr
  (fn [expr]
    (cond
      (vector? expr) (first expr)
      (symbol? expr) :relation))
  :default ::default)

(s/def ::expression some?)

(s/def ::column-expression (s/map-of ::column ::expression :conform-keys true :count 1))

;; TODO be good to just specify a single expression here and have the interpreter split it
;; into metadata + col-preds - the former can accept more than just `(and ~@col-preds)
(defmethod ra-expr :scan [_]
  (s/cat :op #{:scan}
         :source (s/? ::source)
         :columns (s/coll-of (s/or :column ::column
                                   :select ::column-expression)
                             :min-count 1)))

(defmethod ra-expr :table [_]
  (s/cat :op #{:table}
         :explicit-col-names (s/? (s/coll-of ::column :kind set?))
         :table (s/or :rows (s/coll-of (s/map-of simple-ident? any?))
                      :source ::source)))

(s/def ::csv-col-type #{:bit :bigint :float8 :varchar :varbinary :timestamp-milli :duration-milli})

(s/def ::csv
  (s/cat :op #{:csv}
         :path ::util/path
         :col-types (s/? (s/map-of ::column ::csv-col-type))))

(s/def ::arrow
  (s/cat :op #{:arrow}
         :path ::util/path))

(defmethod ra-expr :project [_]
  (s/cat :op #{:π :pi :project}
         :projections (s/coll-of (s/or :column ::column
                                       :row-number-column (s/map-of ::column #{'(row-number)}, :conform-keys true, :count 1)
                                       :extend ::column-expression)
                                 :min-count 1)
         :relation ::ra-expression))

(defmethod ra-expr :select [_]
  (s/cat :op #{:σ :sigma :select}
         :predicate ::expression
         :relation ::ra-expression))

(defmethod ra-expr :rename [_]
  (s/cat :op #{:ρ :rho :rename}
         :prefix (s/? ::relation)
         :columns (s/? (s/map-of ::column ::column :conform-keys true))
         :relation ::ra-expression))

(s/def ::order-direction #{:asc :desc})

(defmethod ra-expr :order-by [_]
  (s/cat :op '#{:τ :tau :order-by order-by}
         :order (s/coll-of (s/map-of ::column ::order-direction :conform-keys true :count 1) :min-count 1)
         :relation ::ra-expression))

(s/def ::aggregate-expr
  (s/cat :aggregate-fn simple-symbol?
         :from-column ::column))

(s/def ::aggregate
  (s/map-of ::column ::aggregate-expr :conform-keys true :count 1))

(defmethod ra-expr :group-by [_]
  (s/cat :op #{:γ :gamma :group-by}
         :columns (s/coll-of (s/or :group-by ::column :aggregate ::aggregate) :min-count 1)
         :relation ::ra-expression))

(s/def ::skip nat-int?)
(s/def ::limit nat-int?)

(defmethod ra-expr :top [_]
  (s/cat :op #{:λ :top}
         :top (s/keys :opt-un [::skip ::limit])
         :relation ::ra-expression))

(defmethod ra-expr :distinct [_]
  (s/cat :op #{:δ ::delta :distinct}
         :relation ::ra-expression))

(defmethod ra-expr :intersect [_]
  (s/cat :op #{:∩ :intersect}
         :left ::ra-expression
         :right ::ra-expression))

(defmethod ra-expr :union-all [_]
  (s/cat :op #{:∪ :union-all}
         :left ::ra-expression
         :right ::ra-expression))

(defmethod ra-expr :difference [_]
  (s/cat :op #{:− :except :difference}
         :left ::ra-expression
         :right ::ra-expression))

(defmethod ra-expr :cross-join [_]
  (s/cat :op #{:⨯ :cross-join}
         :left ::ra-expression
         :right ::ra-expression))

(s/def ::equi-join-columns (s/map-of ::column ::column :conform-keys true :count 1))

(defmethod ra-expr :join [_]
  (s/cat :op #{:⋈ :join}
         :columns ::equi-join-columns
         :left ::ra-expression
         :right ::ra-expression))

(defmethod ra-expr :left-outer-join [_]
  (s/cat :op #{:⟕ :left-outer-join}
         :columns ::equi-join-columns
         :left ::ra-expression
         :right ::ra-expression))

(defmethod ra-expr :full-outer-join [_]
  (s/cat :op #{:⟗ :full-outer-join}
         :columns ::equi-join-columns
         :left ::ra-expression
         :right ::ra-expression))

(defmethod ra-expr :semi-join [_]
  (s/cat :op #{:⋉ :semi-join}
         :columns ::equi-join-columns
         :left ::ra-expression
         :right ::ra-expression))

(defmethod ra-expr :anti-join [_]
  (s/cat :op #{:▷ :anti-join}
         :columns ::equi-join-columns
         :left ::ra-expression
         :right ::ra-expression))

(defmethod ra-expr :fixpoint [_]
  (s/cat :op #{:μ :mu :fixpoint}
         :mu-variable ::relation
         :base ::ra-expression
         :recursive ::ra-expression))

(s/def ::ordinality-column ::column)

(defmethod ra-expr :unwind [_]
  (s/cat :op #{:ω :unwind}
         :columns (s/map-of ::column ::column, :conform-keys true, :count 1)
         :opts (s/? (s/keys :opt-un [::ordinality-column]))
         :relation ::ra-expression))

(defmethod ra-expr :assign [_]
  (s/cat :op #{:← :assign}
         :bindings (s/and vector? (s/* (s/cat :variable ::relation :value ::ra-expression)))
         :relation ::ra-expression))

(defmethod ra-expr :apply [_]
  (s/cat :op #{:apply}
         :mode #{:cross-join, :left-outer-join, :semi-join, :anti-join}
         :columns (s/map-of ::column ::column, :conform-keys true)
         :dependent-column-names (s/coll-of ::column, :kind set?)
         :independent-relation ::ra-expression
         :dependent-relation ::ra-expression))

(defmethod ra-expr :relation [_]
  (s/and ::relation
         (s/conformer (fn [rel]
                        {:op :relation :relation rel})
                      :relation)))

(defmethod ra-expr :max-1-row [_]
  (s/cat :op #{:max-1-row}
         :relation ::ra-expression))

(s/def ::ra-expression (s/multi-spec ra-expr :op))

(s/def ::logical-plan ::ra-expression)

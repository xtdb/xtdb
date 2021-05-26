(ns core2.logical-plan
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [core2.system :as sys]
            [core2.types :as types]))

;; Partly based on
;; https://dbis-uibk.github.io/relax/help#relalg-reference
;; See also:
;; https://calcite.apache.org/javadocAggregate/org/apache/calcite/tools/RelBuilder.html
;; https://github.com/apache/arrow/blob/master/rust/datafusion/src/logical_plan/plan.rs

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

(s/def ::column-expression (s/map-of ::column ::expression :count 1 :conform-keys true))

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
         :table (s/or :rows (s/coll-of (s/map-of simple-ident? any?))
                      :source ::source)))

(s/def ::csv-col-type types/primitive-types)

(s/def ::csv
  (s/cat :op #{:csv}
         :path ::sys/path
         :col-types (s/? (s/map-of ::column ::csv-col-type))))

(s/def ::arrow
  (s/cat :op #{:arrow}
         :path ::sys/path))

(defmethod ra-expr :project [_]
  (s/cat :op #{:π :pi :project}
         :projections (s/coll-of (s/or :column ::column :extend ::column-expression) :min-count 1)
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

(s/def ::order
  (s/+ (s/& (s/cat :column ::column
                   :direction (s/? #{:asc :desc}))
            (s/conformer (fn [el]
                           (into {:direction :asc} el))
                         identity))))

(defmethod ra-expr :order-by [_]
  (s/cat :op '#{:τ :tau :order-by order-by}
         :order (s/spec ::order)
         :relation ::ra-expression))

(s/def ::aggregate-expr
  (s/cat :aggregate-fn simple-symbol?
         :from-column ::column))

(s/def ::aggregate
  (s/and (s/or :just-expr ::aggregate-expr
               :named (s/map-of ::column ::aggregate-expr, :min-count 1, :max-count 1))
         (s/conformer (fn [[agg-type agg-arg]]
                        (case agg-type
                          :named (let [[to-column aggregate-expr] (first agg-arg)]
                                   (assoc aggregate-expr :to-column to-column))
                          :just-expr (let [{:keys [aggregate-fn from-column]} agg-arg]
                                       (assoc agg-arg :to-column (symbol (format "%s-%s" aggregate-fn from-column))))))
                      (fn [{:keys [to-column] :as agg}]
                        [:named {to-column (dissoc agg :to-column)}]))))

(defmethod ra-expr :group-by [_]
  (s/cat :op #{:γ :gamma :group-by}
         :columns (s/coll-of (s/or :group-by ::column, :aggregate ::aggregate), :min-count 1)
         :relation ::ra-expression))

(s/def ::offset nat-int?)
(s/def ::limit nat-int?)

(defmethod ra-expr :slice [_]
  (s/cat :op #{:slice}
         :slice (s/keys :opt-un [::offset ::limit])
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

(defmethod ra-expr :assign [_]
  (s/cat :op #{:← :assign}
         :bindings (s/and vector? (s/* (s/cat :variable ::relation :value ::ra-expression)))
         :relation ::ra-expression))

(defmethod ra-expr :relation [_]
  (s/and ::relation
         (s/conformer (fn [rel]
                        {:op :relation, :relation rel})
                      :relation)))

(s/def ::ra-expression (s/multi-spec ra-expr :op))

(s/def ::logical-plan ::ra-expression)

(comment
  (s/conform
   ::logical-plan
   '[:project [cid]
     [:select (> sum 1000)
      [:group-by [cid {sum (sum balance)}]
       [:join {cid cid}
        [:project [cid balance] Account]
        [:project [cid] Customer]]]]])

  (s/explain ::logical-plan '[:assign [X [:table [{:a 1}]]
                                       Y [:table [{:b 1}]]]
                              [:join {a b} X Y]])

  ;; left-outer-join
  (s/conform ::logical-plan
             '[:union-all
               [:join {x y} R S]
               [:cross-join
                [:anti-join {x y} R S]
                [:table [{:y nil}]]]]))

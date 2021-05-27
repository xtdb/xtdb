(ns core2.logical-plan
  (:require [clojure.spec.alpha :as s]
            [core2.expression :as expr]
            [core2.expression.temporal]
            [core2.system :as sys]
            [core2.types :as types]))

;; Partly based on
;; https://dbis-uibk.github.io/relax/help#relalg-reference
;; See also:
;; https://calcite.apache.org/javadocAggregate/org/apache/calcite/tools/RelBuilder.html
;; https://github.com/apache/arrow/blob/master/rust/datafusion/src/logical_plan/plan.rs

(s/def ::relation simple-symbol?)
(s/def ::source simple-ident?)
(s/def ::column simple-symbol?)

(s/conform ::column 'foo)

(s/def ::expression (s/conformer expr/form->expr))

(s/def ::column-expression (s/map-of ::column ::expression :count 1 :conform-keys true))

;; TODO be good to just specify a single expression here and have the interpreter split it
;; into metadata + col-preds - the former can accept more than just `(and ~@col-preds)
(s/def ::scan (s/cat :op #{:scan}
                     :source (s/? ::source)
                     :columns (s/coll-of (s/or :column ::column
                                               :select ::column-expression)
                                         :min-count 1)))

(s/def ::table (s/cat :op #{:table}
                      :table (s/or :rows (s/coll-of (s/map-of simple-ident? any?))
                                   :source ::source)))

(s/def ::csv-col-type types/primitive-types)

(s/def ::csv (s/cat :op #{:csv}
                    :path ::sys/path
                    :col-types (s/? (s/map-of ::column ::csv-col-type))))

(s/def ::arrow (s/cat :op #{:arrow}
                      :path ::sys/path))

(s/def ::project (s/cat :op #{:π :pi :project}
                        :projections (s/coll-of (s/or :column ::column :extend ::column-expression) :min-count 1)
                        :relation ::ra-expression))

(s/def ::select (s/cat :op #{:σ :sigma :select}
                       :predicate ::expression
                       :relation ::ra-expression))

(s/def ::rename (s/cat :op #{:ρ :rho :rename}
                       :prefix (s/? ::relation)
                       :columns (s/? (s/map-of ::column ::column :conform-keys true))
                       :relation ::ra-expression))

(s/def ::order (s/+ (s/& (s/cat :column ::column
                                :direction (s/? #{:asc :desc}))
                         (s/conformer (fn [el]
                                        (into {:direction :asc} el))))))

(s/def ::order-by (s/cat :op '#{:τ :tau :order-by order-by}
                         :order (s/spec ::order)
                         :relation ::ra-expression))

(s/def ::group-by (s/cat :op #{:γ :gamma :group-by}
                         :columns (s/coll-of (s/or :group-by ::column :aggregate ::column-expression) :min-count 1)
                         :relation ::ra-expression))

(s/def ::offset nat-int?)
(s/def ::limit nat-int?)

(s/def ::slice (s/cat :op #{:slice}
                      :slice (s/keys :opt-un [::offset ::limit])
                      :relation ::ra-expression))

(s/def ::distinct (s/cat :op #{:δ ::delta :distinct}
                         :relation ::ra-expression))

(s/def ::intersect (s/cat :op #{:∩ :intersect}
                          :left ::ra-expression
                          :right ::ra-expression))

(s/def ::union-all (s/cat :op #{:∪ :union-all}
                          :left ::ra-expression
                          :right ::ra-expression))

(s/def ::difference (s/cat :op #{:− :except :difference}
                           :left ::ra-expression
                           :right ::ra-expression))

(s/def ::cross-join (s/cat :op #{:⨯ :cross-join}
                           :left ::ra-expression
                           :right ::ra-expression))

(s/def ::equi-join-columns (s/map-of ::column ::column :conform-keys true :count 1))

(s/def ::join (s/cat :op #{:⋈ :join}
                     :columns ::equi-join-columns
                     :left ::ra-expression
                     :right ::ra-expression))

(s/def ::semi-join (s/cat :op #{:⋉ :semi-join}
                          :columns ::equi-join-columns
                          :left ::ra-expression
                          :right ::ra-expression))

(s/def ::anti-join (s/cat :op #{:▷ :anti-join}
                          :columns ::equi-join-columns
                          :left ::ra-expression
                          :right ::ra-expression))

(s/def ::fixpoint (s/cat :op #{:μ :mu :fixpoint}
                         :mu-variable ::relation
                         :base ::ra-expression
                         :recursive ::ra-expression))

(s/def ::assign (s/cat :op #{:← :assign}
                       :bindings (s/and vector? (s/* (s/cat :variable ::relation :value ::ra-expression)))
                       :relation ::ra-expression))

(s/def ::ra-expression (s/or :relation ::relation
                             :scan ::scan
                             :table ::table
                             :csv ::csv
                             :arrow ::arrow
                             :project ::project
                             :select ::select
                             :rename ::rename
                             :order-by ::order-by
                             :group-by ::group-by
                             :slice ::slice
                             :distinct ::distinct
                             :intersect ::intersect
                             :union-all ::union-all
                             :difference ::difference
                             :cross-join ::cross-join
                             :join ::join
                             :semi-join ::semi-join
                             :anti-join ::anti-join
                             :fixpoint ::fixpoint
                             :assign ::assign))

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

  (s/conform ::logical-plan '[:assign [X [:table [{:a 1}]]
                                       Y [:table [{:b 1}]]]
                              [:join {a b} X Y]])

  ;; left-outer-join
  (s/conform ::logical-plan
             '[:union-all
               [:join {x y} R S]
               [:cross-join
                [:anti-join {x y} R S]
                [:table [{:y nil}]]]]))

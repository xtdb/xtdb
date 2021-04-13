(ns core2.logical-plan
  (:require [clojure.spec.alpha :as s]))

;; Partly based on
;; https://dbis-uibk.github.io/relax/help#relalg-reference
;; See also:
;; https://calcite.apache.org/javadocAggregate/org/apache/calcite/tools/RelBuilder.html
;; https://github.com/apache/arrow/blob/master/rust/datafusion/src/logical_plan/plan.rs

(s/def ::named (some-fn keyword? symbol?))
(s/def ::relation ::named)
(s/def ::column ::named)

(s/def ::expression (s/or :column ::column
                          :expression (s/cat :op ::named :args (s/* ::expression))
                          :atom any?))

(s/def ::column-expression (s/map-of ::column ::expression :count 1 :conform-keys true))

(s/def ::scan (s/cat :op #{:scan}
                     :columns (s/coll-of (s/or :column ::column :select ::column-expression) :min-count 1)
                     :as (s/? ::relation)))

(s/def ::project (s/cat :op #{:π :pi :project}
                        :projections (s/coll-of (s/or :column ::column :extend ::column-expression) :min-count 1)
                        :relation ::ra-expression))

(s/def ::select (s/cat :op #{:σ :sigma :select}
                       :predicate ::expression
                       :relation ::ra-expression))

(s/def ::rename (s/cat :op #{:ρ :rho :rename}
                       :as (s/? ::relation)
                       :columns (s/? (s/map-of ::column ::column :conform-keys true))
                       :relation ::ra-expression))

(s/def ::order-by (s/cat :op '#{:τ :tau :order-by order-by}
                         :order (s/coll-of (s/or :column ::column :direction (s/map-of ::column #{:asc :desc} :count 1 :conform-keys true)))
                         :relation ::ra-expression))

(s/def ::group-by (s/cat :op #{:γ :gamma :group-by}
                         :columns (s/coll-of (s/or :group-by ::column :aggregate ::column-expression) :min-count 1)
                         :relation ::ra-expression))

(s/def ::slice (s/cat :op #{:slice}
                      :offset (s/nilable nat-int?)
                      :limit (s/nilable nat-int?)
                      :relation ::ra-expression))

(s/def ::intersect (s/cat :op #{:∩ :intersect}
                          :left ::ra-expression
                          :right ::ra-expression))

(s/def ::union (s/cat :op #{:∪ :union}
                      :left ::ra-expression
                      :right ::ra-expression))

(s/def ::difference (s/cat :op #{:- :except :difference}
                           :left ::ra-expression
                           :right ::ra-expression))

(s/def ::cross-join (s/cat :op #{:⨯ :cross-join}
                           :left ::ra-expression
                           :right ::ra-expression))

(s/def ::join-type (s/? (s/or :equi-join (s/and (s/tuple #{:=} ::column ::column) ::expression)
                              :theta-join ::expression)))

(s/def ::join (s/cat :op #{:⋈ :join}
                     :join-type ::join-type
                     :left ::ra-expression
                     :right ::ra-expression))

(s/def ::semi-join (s/cat :op #{:⋉ :semi-join}
                          :join-type ::join-type
                          :left ::ra-expression
                          :right ::ra-expression))

(s/def ::anti-join (s/cat :op #{:▷ :anti-join}
                          :join-type ::join-type
                          :left ::ra-expression
                          :right ::ra-expression))

(s/def ::fixpoint (s/cat :op #{:μ :mu :fixpoint}
                         :mu-variable ::relation
                         :union-of-expressions ::ra-expression))

(s/def ::ra-expression (s/or :relation ::relation
                             :scan ::scan
                             :project ::project
                             :select ::select
                             :rename ::rename
                             :order-by ::order-by
                             :group-by ::group-by
                             :slice ::slice
                             :intersect ::intersect
                             :union ::union
                             :difference ::difference
                             :cross-join ::cross-join
                             :join ::join
                             :semi-join ::semi-join
                             :anti-join ::anti-join
                             :fixpoint ::fixpoint))

(s/def ::logical-plan ::ra-expression)

(comment
  (s/conform
   ::logical-plan
   [:π [:Account/cid]
    [:σ [:> :Account/sum 1000]
     [:γ [:Account/cid {:Account/sum [:sum :Account/balance]}]
      [:⋈ [:= :Account/cid :Customer/cid]
       [:scan [:Account/cid :Account/balance]]
       [:scan [:Customer/cid]]]]]])

  (s/conform
   ::logical-plan
   '[:π [:Account/cid]
     [:σ (> :Account/sum 1000)
      [:γ [:Account/cid {:Account/sum (sum :Account/balance)}]
       [:⋈ (= :Account/cid :Customer/cid)
        [:scan [:Account/cid :Account/balance]]
        [:scan [:Customer/cid]]]]]])

  (s/conform
   ::logical-plan
   '[:project [cid]
     [:select (> sum 1000)
      [:group-by [cid {sum (sum balance)}]
       [:join
        [:project [cid balance] Account]
        [:project [cid] Customer]]]]]))

(ns core2.logical-plan
  (:require [clojure.spec.alpha :as s]))

;; Partly based on
;; https://dbis-uibk.github.io/relax/help#relalg-reference

(s/def ::relation symbol?)
(s/def ::column symbol?)

(s/def ::expression (s/or :column ::column
                          :expression (s/and vector? (s/cat :op keyword? :args (s/* ::expression)))
                          :atom (complement vector?)))

(s/def ::projection (s/cat :op #{:π :pi :project}
                           :projections (s/coll-of (s/or :column ::column
                                                         :extend (s/cat :expression ::expression :as ::column)))
                           :relation ::ra-expression))

(s/def ::selection (s/cat :op #{:σ :sigma :select}
                          :predicate ::expression
                          :relation ::ra-expression))

(s/def ::rename (s/cat :op #{:ρ :rho :rename}
                       :as (s/? ::relation)
                       :columns (s/? (s/coll-of (s/cat :column ::column :as ::column)))
                       :relation ::ra-expression))

(s/def ::order-by (s/cat :op #{:τ :tau :order-by}
                         :order (s/coll-of (s/cat :column ::column :direction #{:asc :desc}))
                         :relation ::ra-expression))

(s/def ::group-by (s/cat :op #{:γ :gamma :group-by}
                         :group-by (s/? (s/coll-of ::column))
                         :aggregates (s/coll-of (s/cat :expression ::expression :as ::column))
                         :relation ::ra-expression))

(s/def ::slice (s/cat :op #{:slice}
                      :offset (s/nilable nat-int?)
                      :limit (s/nilable nat-int?)
                      :relation ::ra-expression))

(s/def ::intersection (s/cat :op '#{:∩ :intersect}
                             :left ::ra-expression
                             :right ::ra-expression))

(s/def ::union (s/cat :op #{:∪ :union}
                      :left ::ra-expression
                      :right ::ra-expression))

(s/def ::difference (s/cat :op #{:- :except}
                           :left ::ra-expression
                           :right ::ra-expression))

(s/def ::cross-join (s/cat :op #{:⨯ :cross-join}
                           :left ::ra-expression
                           :right ::ra-expression))

(s/def ::join (s/cat :op #{:⋈ :join}
                     :join-type (s/? (s/or :equi-join (s/and (s/tuple #{:=} ::column ::column) ::expression)
                                           :theta-join ::expression))
                     :left ::ra-expression
                     :right ::ra-expression))

(s/def ::ra-expression (s/or :relation ::relation
                             :projection ::projection
                             :selection ::selection
                             :rename ::rename
                             :order-by ::order-by
                             :group-by ::group-by
                             :slice ::slice
                             :intersection ::intersection
                             :union ::union
                             :difference ::difference
                             :cross-join ::cross-join
                             :join ::join))

(s/def ::logical-plan ::ra-expression)

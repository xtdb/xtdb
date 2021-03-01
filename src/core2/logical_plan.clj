(ns core2.logical-plan
  (:require [clojure.spec.alpha :as s]))

;; Partly based on
;; https://dbis-uibk.github.io/relax/help#relalg-reference

(s/def ::relation symbol?)
(s/def ::column symbol?)
(s/def ::expression list?)

(s/def ::projection (s/cat :op #{:π :pi :project}
                           :projections (s/coll-of (s/or :column ::column
                                                         :expression (s/tuple ::expression ::column))
                                                   :kind vector?)
                           :relation ::ra-expression))

(s/def ::selection (s/cat :op #{:σ :sigma :select}
                          :expression ::expression
                          :relation ::ra-expression))

(s/def ::rename (s/cat :op #{:ρ :rho :rename}
                       :as (s/? ::relation)
                       :columns (s/? (s/coll-of (s/tuple ::column ::column) :kind vector?))
                       :relation ::ra-expression))

(s/def ::order-by (s/cat :op #{:τ :tau :order-by}
                         :order (s/coll-of (s/tuple ::column (s/? #{:asc :desc})) :kind vector?)
                         :relation ::ra-expression))

(s/def ::aggregate (s/cat :function symbol? :column ::column))
(s/def ::group-by (s/cat :op #{:γ :gamma :group-by}
                         :group-by (s/? (s/coll-of ::column :kind vector?))
                         :aggregates (s/coll-of (s/tuple ::aggregate ::column) :kind vector?)
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
                     :expression (s/? (s/or :equi-join (s/cat :op '#{=} :left ::column :right ::column)
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

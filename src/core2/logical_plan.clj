(ns core2.logical-plan
  (:require [clojure.spec.alpha :as s]
            [core2.expression :as expr]
            core2.operator
            [core2.operator.group-by :as group-by]
            [core2.operator.order-by :as order-by]
            [core2.operator.project :as project]
            [core2.util :as util])
  (:import clojure.lang.MapEntry
           core2.operator.IOperatorFactory))

;; Partly based on
;; https://dbis-uibk.github.io/relax/help#relalg-reference
;; See also:
;; https://calcite.apache.org/javadocAggregate/org/apache/calcite/tools/RelBuilder.html
;; https://github.com/apache/arrow/blob/master/rust/datafusion/src/logical_plan/plan.rs

(s/def ::named (some-fn keyword? symbol?))
(s/def ::relation ::named)
(s/def ::column ::named)

(s/def ::expression (s/conformer expr/form->expr))

(s/def ::column-expression (s/map-of ::column ::expression :count 1 :conform-keys true))

;; TODO be good to just specify a single expression here and have the interpreter split it
;; into metadata + col-preds - the former can accept more than just `(and ~@col-preds)
(s/def ::scan (s/cat :op #{:scan}
                     :columns (s/coll-of (s/or :column ::column
                                               :select ::column-expression)
                                         :min-count 1)))

(s/def ::project (s/cat :op #{:π :pi :project}
                        :projections (s/coll-of (s/or :column ::column :extend ::column-expression) :min-count 1)
                        :relation ::ra-expression))

(s/def ::select (s/cat :op #{:σ :sigma :select}
                       :predicate ::expression
                       :relation ::ra-expression))

(s/def ::rename (s/cat :op #{:ρ :rho :rename}
                       :columns (s/? (s/map-of ::column ::column :conform-keys true))
                       :relation ::ra-expression))

(s/def ::order-by (s/cat :op '#{:τ :tau :order-by order-by}
                         :order (s/coll-of (s/or :column ::column
                                                 :direction (s/map-of ::column #{:asc :desc}
                                                                      :count 1
                                                                      :conform-keys true)))
                         :relation ::ra-expression))

(s/def ::group-by (s/cat :op #{:γ :gamma :group-by}
                         :columns (s/coll-of (s/or :group-by ::column :aggregate ::column-expression) :min-count 1)
                         :relation ::ra-expression))

(s/def ::offset nat-int?)
(s/def ::limit nat-int?)

(s/def ::slice (s/cat :op #{:slice}
                      :slice (s/keys :opt-un [::offset ::limit])
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

(s/def ::join-type (s/? (s/or :equi-join (s/and ::expression
                                                (fn [{:keys [f args]}]
                                                  (and (contains? '#{:= =} f)
                                                       (every? (comp #{:variable} :op) args))))
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

(defmulti emit-op first)

(defmethod emit-op :scan [[_ {:keys [columns]}]]
  (let [col-names (for [[col-type arg] columns]
                    (str (case col-type
                           :column arg
                           :select (key (first arg)))))
        selects (->> (for [[col-type arg] columns
                           :when (= col-type :select)]
                       (first arg))
                     (into {}))
        col-preds (->> (for [[col-name select-expr] selects]
                         (MapEntry/create (name col-name)
                                          (expr/->expression-vector-selector select-expr)))
                       (into {}))
        metadata-pred (expr/->metadata-selector {:op :call, :f 'and, :args (vec (vals selects))})]

    (fn [^IOperatorFactory op-factory watermark]
      (.scan op-factory watermark col-names metadata-pred col-preds nil nil))))

(defmethod emit-op :select [[_ {:keys [predicate relation]}]]
  (let [inner-f (emit-op relation)
        selector (expr/->expression-root-selector predicate)]
    (fn [^IOperatorFactory op-factory watermark]
      (let [inner (inner-f op-factory watermark)]
        (try
          (.select op-factory inner selector)
          (catch Exception e
            (util/try-close inner)
            (throw e)))))))

(defmethod emit-op :project [[_ {:keys [projections relation]}]]
  (let [inner-f (emit-op relation)
        projection-specs (for [[p-type arg] projections]
                           (case p-type
                             :column (project/->identity-projection-spec (name arg))
                             :extend (let [[col-name expr] (first arg)]
                                       (expr/->expression-projection-spec (name col-name) expr))))]
    (fn [^IOperatorFactory op-factory watermark]
      (let [inner (inner-f op-factory watermark)]
        (try
          (.project op-factory inner projection-specs)
          (catch Exception e
            (util/try-close inner)
            (throw e)))))))

(defmethod emit-op :rename [[_ {:keys [columns relation]}]]
  (let [inner-f (emit-op relation)
        rename-map (->> columns
                        (into {} (map (juxt (comp name key)
                                            (comp name val)))))]
    (fn [^IOperatorFactory op-factory watermark]
      (let [inner (inner-f op-factory watermark)]
        (try
          (.rename op-factory inner rename-map)
          (catch Exception e
            (util/try-close inner)
            (throw e)))))))

(defmethod emit-op :join [[_ {:keys [join-type left right]}]]
  (let [[join-type arg] join-type
        join-f (case join-type
                 :equi-join (let [{:keys [args]} arg
                                  [{left-col :variable} {right-col :variable}] args]
                              (fn [^IOperatorFactory op-factory left right]
                                (.equiJoin op-factory
                                           left (name left-col)
                                           right (name right-col))))
                 ;; TODO theta-join
                 )
        left-f (emit-op left)
        right-f (emit-op right)]
    (fn [^IOperatorFactory op-factory watermark]
      (let [left (left-f op-factory watermark)]
        (try
          (let [right (right-f op-factory watermark)]
            (try
              (join-f op-factory left right)
              (catch Exception e
                (util/try-close right)
                (throw e))))
          (catch Exception e
            (util/try-close left)
            (throw e)))))))

(defmethod emit-op :group-by [[_ {:keys [columns relation]}]]
  (let [inner-f (emit-op relation)
        agg-specs (for [[col-type arg] columns]
                    (case col-type
                      :group-by (group-by/->group-spec (name arg))
                      :aggregate (let [[to-name {:keys [f args]}] (first arg)
                                       from-name (:variable (first args))

                                       ;; TODO we probably want to compile these too?
                                       ->spec (case f
                                                sum-long group-by/->sum-long-spec
                                                sum-double group-by/->sum-double-spec
                                                avg-long group-by/->avg-long-spec
                                                avg-double group-by/->avg-double-spec
                                                count group-by/->count-spec)]
                                   (->spec (name from-name) (name to-name)))
                      [col-type arg]))]
    (fn [^IOperatorFactory op-factory watermark]
      (let [inner (inner-f op-factory watermark)]
        (try
          (.groupBy op-factory inner agg-specs)
          (catch Exception e
            (util/try-close inner)
            (throw e)))))))

(defmethod emit-op :order-by [[_ {:keys [order relation]}]]
  (let [inner-f (emit-op relation)
        order-specs (for [[order-type arg] order]
                      (case order-type
                        :direction (order-by/->order-spec (name (key (first arg)))
                                                          (val (first arg)))
                        :column (order-by/->order-spec (name arg) :asc)))]
    (fn [^IOperatorFactory op-factory watermark]
      (let [inner (inner-f op-factory watermark)]
        (try
          (.orderBy op-factory inner order-specs)
          (catch Exception e
            (util/try-close inner)
            (throw e)))))))

(defmethod emit-op :slice [[_ {:keys [relation], {:keys [offset limit]} :slice}]]
  (let [inner-f (emit-op relation)]
    (fn [^IOperatorFactory op-factory watermark]
      (let [inner (inner-f op-factory watermark)]
        (try
          (.slice op-factory inner offset limit)
          (catch Exception e
            (util/try-close inner)
            (throw e)))))))

(defn open-q ^core2.ICursor [op-factory watermark lp]
  (let [op-f (emit-op (s/conform ::logical-plan lp))]
    (op-f op-factory watermark)))

(ns core2.logical-plan
  (:require [clojure.spec.alpha :as s]
            [core2.expression :as expr]
            core2.operator
            [core2.operator.group-by :as group-by]
            [core2.operator.order-by :as order-by]
            [core2.operator.project :as project]
            [core2.util :as util])
  (:import clojure.lang.MapEntry
           core2.operator.IOperatorFactory
           [core2.operator.set ICursorFactory IFixpointCursorFactory]))

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

(s/def ::table (s/cat :op #{:table}
                      :rows (s/coll-of (s/map-of ::named any?))))

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
                             :table ::table
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

(defmethod emit-op :table [[_ {:keys [rows]}]]
  (fn [^IOperatorFactory op-factory watermark]
    (.table op-factory rows)))

(defn- unary-op [relation f]
  (let [inner-f (emit-op relation)]
    (fn [^IOperatorFactory op-factory watermark]
      (let [inner (inner-f op-factory watermark)]
        (try
          (f op-factory inner)
          (catch Exception e
            (util/try-close inner)
            (throw e)))))))

(defn- binary-op [left right f]
  (let [left-f (emit-op left)
        right-f (emit-op right)]
    (fn [^IOperatorFactory op-factory watermark]
      (let [left (left-f op-factory watermark)]
        (try
          (let [right (right-f op-factory watermark)]
            (try
              (f op-factory left right)
              (catch Exception e
                (util/try-close right)
                (throw e))))
          (catch Exception e
            (util/try-close left)
            (throw e)))))))

(defmethod emit-op :select [[_ {:keys [predicate relation]}]]
  (let [selector (expr/->expression-root-selector predicate)]
    (unary-op relation (fn [^IOperatorFactory op-factory inner]
                         (.select op-factory inner selector)))))

(defmethod emit-op :project [[_ {:keys [projections relation]}]]
  (let [projection-specs (for [[p-type arg] projections]
                           (case p-type
                             :column (project/->identity-projection-spec (name arg))
                             :extend (let [[col-name expr] (first arg)]
                                       (expr/->expression-projection-spec (name col-name) expr))))]
    (unary-op relation (fn [^IOperatorFactory op-factory inner]
                         (.project op-factory inner projection-specs)))))

(defmethod emit-op :rename [[_ {:keys [columns relation]}]]
  (let [rename-map (->> columns
                        (into {} (map (juxt (comp name key)
                                            (comp name val)))))]
    (unary-op relation (fn [^IOperatorFactory op-factory inner]
                         (.rename op-factory inner rename-map)))))

(defmethod emit-op :union [[_ {:keys [left right]}]]
  (binary-op left right (fn [^IOperatorFactory op-factory left right]
                          (.union op-factory left right))))

(defmethod emit-op :intersection [[_ {:keys [left right]}]]
  (binary-op left right (fn [^IOperatorFactory op-factory left right]
                          (.intersection op-factory left right))))

(defmethod emit-op :difference [[_ {:keys [left right]}]]
  (binary-op left right (fn [^IOperatorFactory op-factory left right]
                          (.difference op-factory left right))))

(defmethod emit-op :cross-join [[_ {:keys [left right]}]]
  (binary-op left right (fn [^IOperatorFactory op-factory left right]
                          (.crossJoin op-factory left right))))

(defmethod emit-op :join [[_ {:keys [join-type left right]}]]
  (let [[join-type arg] join-type
        join-f (case join-type
                 :equi-join (let [{:keys [args]} arg
                                  [{left-col :variable} {right-col :variable}] args]
                              (fn [^IOperatorFactory op-factory left right]
                                (.equiJoin op-factory
                                           left (name left-col)
                                           right (name right-col)))))
        right-f (emit-op right)]
    (binary-op left right (fn [^IOperatorFactory op-factory left right]
                            (join-f op-factory left right)))))

(defmethod emit-op :semi-join [[_ {:keys [join-type left right]}]]
  (let [[join-type arg] join-type
        join-f (case join-type
                 :equi-join (let [{:keys [args]} arg
                                  [{left-col :variable} {right-col :variable}] args]
                              (fn [^IOperatorFactory op-factory left right]
                                (.semiEquiJoin op-factory
                                               left (name left-col)
                                               right (name right-col)))))]
    (binary-op left right (fn [^IOperatorFactory op-factory left right]
                            (join-f op-factory left right)))))

(defmethod emit-op :anti-join [[_ {:keys [join-type left right]}]]
  (let [[join-type arg] join-type
        join-f (case join-type
                 :equi-join (let [{:keys [args]} arg
                                  [{left-col :variable} {right-col :variable}] args]
                              (fn [^IOperatorFactory op-factory left right]
                                (.antiEquiJoin op-factory
                                               left (name left-col)
                                               right (name right-col)))))]
    (binary-op left right (fn [^IOperatorFactory op-factory left right]
                            (join-f op-factory left right)))))

(defmethod emit-op :group-by [[_ {:keys [columns relation]}]]
  (let [agg-specs (for [[col-type arg] columns]
                    (case col-type
                      :group-by (group-by/->group-spec (name arg))
                      :aggregate (let [[to-name {:keys [f args]}] (first arg)
                                       from-name (:variable (first args))
                                       ->spec (case f
                                                sum-long group-by/->sum-long-spec
                                                sum-double group-by/->sum-double-spec
                                                min-long group-by/->min-long-spec
                                                min-double group-by/->min-double-spec
                                                min-number group-by/->min-number-spec
                                                max-long group-by/->max-long-spec
                                                max-double group-by/->max-double-spec
                                                max-number group-by/->max-number-spec
                                                avg-long group-by/->avg-long-spec
                                                avg-double group-by/->avg-double-spec
                                                sum group-by/->sum-number-spec
                                                avg group-by/->avg-number-spec
                                                min group-by/->min-spec
                                                max group-by/->max-spec
                                                count group-by/->count-spec)]
                                   (->spec (name from-name) (name to-name)))
                      [col-type arg]))]
    (unary-op relation (fn [^IOperatorFactory op-factory inner]
                         (.groupBy op-factory inner agg-specs)))))

(defmethod emit-op :order-by [[_ {:keys [order relation]}]]
  (let [order-specs (for [[order-type arg] order]
                      (case order-type
                        :direction (order-by/->order-spec (name (key (first arg)))
                                                          (val (first arg)))
                        :column (order-by/->order-spec (name arg) :asc)))]
    (unary-op relation (fn [^IOperatorFactory op-factory inner]
                         (.orderBy op-factory inner order-specs)))))

(defmethod emit-op :slice [[_ {:keys [relation], {:keys [offset limit]} :slice}]]
  (unary-op relation (fn [^IOperatorFactory op-factory inner]
                       (.slice op-factory inner offset limit))))

(def ^:private ^:dynamic *mu-variable->cursor-factory* {})

(defmethod emit-op :relation [[_ relation-name]]
  (fn [^IOperatorFactory op-factory watermark]
    (let [^ICursorFactory cursor-factory (get *mu-variable->cursor-factory* relation-name)]
      (assert cursor-factory)
      (.createCursor cursor-factory))))

(defmethod emit-op :fixpoint [[_ {:keys [mu-variable union-of-expressions]}]]
  (fn [^IOperatorFactory op-factory watermark]
    (.fixpoint op-factory (reify IFixpointCursorFactory
                            (createCursor [_ cursor-factory]
                              (binding [*mu-variable->cursor-factory* (assoc *mu-variable->cursor-factory* mu-variable cursor-factory)]
                                (let [inner-f (emit-op union-of-expressions)]
                                  (inner-f op-factory watermark))))) false)))

(defn open-q ^core2.ICursor [op-factory watermark lp]
  (let [op-f (emit-op (s/conform ::logical-plan lp))]
    (op-f op-factory watermark)))

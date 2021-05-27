(ns core2.operator
  (:require [clojure.spec.alpha :as s]
            core2.data-source
            [core2.error :as err]
            [core2.expression :as expr]
            [core2.expression.metadata :as expr.meta]
            [core2.expression.temporal :as expr.temp]
            [core2.logical-plan :as lp]
            [core2.operator.arrow :as arrow]
            [core2.operator.csv :as csv]
            [core2.operator.group-by :as group-by]
            [core2.operator.join :as join]
            [core2.operator.order-by :as order-by]
            [core2.operator.project :as project]
            [core2.operator.rename :as rename]
            [core2.operator.select :as select]
            [core2.operator.set :as set-op]
            [core2.operator.slice :as slice]
            [core2.operator.table :as table]
            [core2.util :as util])
  (:import clojure.lang.MapEntry
           core2.data_source.IQueryDataSource
           [core2.operator.set ICursorFactory IFixpointCursorFactory]))

(defmulti emit-op
  (fn [op srcs]
    (first op)))

(alter-meta! #'emit-op assoc :private true)

(defmulti ->aggregate-spec
  (fn [name from-name to-name & args]
    name))

(alter-meta! #'->aggregate-spec assoc :private true)

(defmethod ->aggregate-spec :avg [_ from-name to-name]
  (group-by/->avg-number-spec from-name to-name))

(defmethod ->aggregate-spec :sum [_ from-name to-name]
  (group-by/->sum-number-spec from-name to-name))

(defmethod ->aggregate-spec :min [_ from-name to-name]
  (group-by/->min-spec from-name to-name))

(defmethod ->aggregate-spec :max [_ from-name to-name]
  (group-by/->max-spec from-name to-name))

(defmethod ->aggregate-spec :count [_ from-name to-name]
  (group-by/->count-spec from-name to-name))

(defmethod ->aggregate-spec :count-not-null [_ from-name to-name]
  (group-by/->count-not-null-spec from-name to-name))

(defmethod emit-op :scan [[_ {:keys [source columns]}] srcs]
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
                                          (expr/->expression-column-selector select-expr srcs)))
                       (into {}))
        args (vec (concat (for [col-name col-names
                                :when (not (contains? selects col-name))]
                            {:op :variable :variable (symbol col-name)})
                          (vals selects)))
        metadata-pred (expr.meta/->metadata-selector {:op :call, :f 'and, :args args} srcs)

        ^IQueryDataSource db (or (get srcs (or source '$))
                                 (throw (err/illegal-arg :unknown-db
                                                         {::err/message "Query refers to unknown db"
                                                          :db source
                                                          :srcs (keys srcs)})))]
    (fn [_allocator]
      (let [[^longs temporal-min-range, ^longs temporal-max-range] (expr.temp/->temporal-min-max-range selects srcs)]
        (.scan db col-names metadata-pred col-preds temporal-min-range temporal-max-range)))))

(defmethod emit-op :table [[_ {[table-type table-arg] :table}] srcs]
  (let [rows (case table-type
               :rows table-arg
               :source (or (get srcs table-arg)
                           (throw (err/illegal-arg :unknown-table
                                                   {::err/message "Query refers to unknown table"
                                                    :table table-arg
                                                    :srcs (keys srcs)}))))]
    (fn [allocator]
      (table/->table-cursor allocator rows))))

(defmethod emit-op :csv [[_ {:keys [path col-types]}] _srcs]
  (fn [allocator]
    (csv/->csv-cursor allocator path
                      (into {} (map (juxt (comp name key) val)) col-types))))

(defmethod emit-op :arrow [[_ {:keys [path]}] _srcs]
  (fn [allocator]
    (arrow/->arrow-cursor allocator path)))

(defn- unary-op [relation srcs f]
  (let [inner-f (emit-op relation srcs)]
    (fn [allocator]
      (let [inner (inner-f allocator)]
        (try
          (f allocator inner)
          (catch Exception e
            (util/try-close inner)
            (throw e)))))))

(defn- binary-op [left right srcs f]
  (let [left-f (emit-op left srcs)
        right-f (emit-op right srcs)]
    (fn [allocator]
      (let [left (left-f allocator)]
        (try
          (let [right (right-f allocator)]
            (try
              (f allocator left right)
              (catch Exception e
                (util/try-close right)
                (throw e))))
          (catch Exception e
            (util/try-close left)
            (throw e)))))))

(defmethod emit-op :select [[_ {:keys [predicate relation]}] srcs]
  (let [selector (expr/->expression-relation-selector predicate srcs)]
    (unary-op relation srcs
              (fn [_allocator inner]
                (select/->select-cursor inner selector)))))

(defmethod emit-op :project [[_ {:keys [projections relation]}] srcs]
  (let [projection-specs (for [[p-type arg] projections]
                           (case p-type
                             :column (project/->identity-projection-spec (name arg))
                             :extend (let [[col-name expr] (first arg)]
                                       (expr/->expression-projection-spec (name col-name) expr srcs))))]
    (unary-op relation srcs
              (fn [allocator inner]
                (project/->project-cursor allocator inner projection-specs)))))

(defmethod emit-op :rename [[_ {:keys [columns relation prefix]}] srcs]
  (let [rename-map (->> columns
                        (into {} (map (juxt (comp name key)
                                            (comp name val)))))]
    (unary-op relation srcs
              (fn [_allocator inner]
                (rename/->rename-cursor inner rename-map (some-> prefix (name)))))))

(defmethod emit-op :distinct [[_ {:keys [relation]}] srcs]
  (unary-op relation srcs
            (fn [allocator inner]
              (set-op/->distinct-cursor allocator inner))))

(defmethod emit-op :union-all [[_ {:keys [left right]}] srcs]
  (binary-op left right srcs
             (fn [_allocator left right]
               (set-op/->union-all-cursor left right))))

(defmethod emit-op :intersection [[_ {:keys [left right]}] srcs]
  (binary-op left right srcs
             (fn [allocator left right]
               (set-op/->intersection-cursor allocator left right))))

(defmethod emit-op :difference [[_ {:keys [left right]}] srcs]
  (binary-op left right srcs
             (fn [allocator left right]
               (set-op/->difference-cursor allocator left right))))

(defmethod emit-op :cross-join [[_ {:keys [left right]}] srcs]
  (binary-op left right srcs
             (fn [allocator left right]
               (join/->cross-join-cursor allocator left right))))

(defmethod emit-op :join [[_ {:keys [columns left right]}] srcs]
  (let [[left-col] (keys columns)
        [right-col] (vals columns)]
    (binary-op left right srcs
               (fn [allocator left right]
                 (join/->equi-join-cursor allocator
                                          left (name left-col)
                                          right (name right-col))))))

(defmethod emit-op :semi-join [[_ {:keys [columns left right]}] srcs]
  (let [[left-col] (keys columns)
        [right-col] (vals columns)]
    (binary-op left right srcs
               (fn [allocator left right]
                 (join/->semi-equi-join-cursor allocator
                                               left (name left-col)
                                               right (name right-col))))))

(defmethod emit-op :anti-join [[_ {:keys [columns left right]}] srcs]
  (let [[left-col] (keys columns)
        [right-col] (vals columns)]
    (binary-op left right srcs
               (fn [allocator left right]
                 (join/->anti-equi-join-cursor allocator
                                               left (name left-col)
                                               right (name right-col))))))

(defmethod emit-op :group-by [[_ {:keys [columns relation]}] srcs]
  (let [agg-specs (for [[col-type arg] columns]
                    (case col-type
                      :group-by (group-by/->group-spec (name arg))
                      :aggregate (let [[to-name {:keys [f args]}] (first arg)
                                       from-name (:variable (first args))]
                                   (->aggregate-spec (keyword (name f)) (name from-name) (name to-name)))
                      [col-type arg]))]
    (unary-op relation srcs
              (fn [allocator inner]
                (group-by/->group-by-cursor allocator inner agg-specs)))))

(defmethod emit-op :order-by [[_ {:keys [order relation]}] srcs]
  (let [order-specs (for [{:keys [column direction]} order]
                      (order-by/->order-spec (name column) direction))]
    (unary-op relation srcs
              (fn [allocator inner]
                (order-by/->order-by-cursor allocator inner order-specs)))))

(defmethod emit-op :slice [[_ {:keys [relation], {:keys [offset limit]} :slice}] srcs]
  (unary-op relation srcs
            (fn [_allocator inner]
              (slice/->slice-cursor inner offset limit))))

(def ^:dynamic ^:private *relation-variable->cursor-factory* {})

(defmethod emit-op :relation [[_ relation-name] _srcs]
  (fn [_allocator]
    (let [^ICursorFactory cursor-factory (get *relation-variable->cursor-factory* relation-name)]
      (assert cursor-factory)
      (.createCursor cursor-factory))))

(defmethod emit-op :fixpoint [[_ {:keys [mu-variable base recursive]}] srcs]
  (let [base-f (emit-op base srcs)
        recursive-f (emit-op recursive srcs)]
    (fn [allocator]
      (set-op/->fixpoint-cursor allocator
                                (base-f allocator)
                                (reify IFixpointCursorFactory
                                  (createCursor [_ cursor-factory]
                                    (binding [*relation-variable->cursor-factory* (assoc *relation-variable->cursor-factory* mu-variable cursor-factory)]
                                      (recursive-f allocator))))
                 false))))

(defmethod emit-op :assign [[_ {:keys [bindings relation]}] srcs]
  (fn [allocator]
    (let [assignments (reduce
                       (fn [acc {:keys [variable value]}]
                         (assoc acc variable (let [value-f (emit-op value srcs)]
                                               (reify ICursorFactory
                                                 (createCursor [_]
                                                   (binding [*relation-variable->cursor-factory* acc]
                                                     (value-f allocator)))))))
                       *relation-variable->cursor-factory*
                       bindings)]
      (with-bindings {#'*relation-variable->cursor-factory* assignments}
        (let [inner-f (emit-op relation srcs)]
          (inner-f allocator))))))

(defn open-q ^core2.ICursor [allocator srcs lp]
  (when-not (s/valid? ::lp/logical-plan lp)
    (throw (IllegalArgumentException. (s/explain-str ::lp/logical-plan lp))))
  (let [op-f (emit-op (s/conform ::lp/logical-plan lp) srcs)]
    (op-f allocator)))

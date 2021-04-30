(ns core2.operator
  (:require [clojure.spec.alpha :as s]
            core2.data-source
            [core2.expression :as expr]
            [core2.expression.metadata :as expr.meta]
            [core2.logical-plan :as lp]
            [core2.operator.group-by :as group-by]
            [core2.operator.order-by :as order-by]
            [core2.operator.project :as project]
            [core2.operator.table :as table]
            [core2.util :as util]
            [core2.operator.slice :as slice]
            [core2.operator.select :as select]
            [core2.operator.rename :as rename]
            [core2.operator.set :as set-op]
            [core2.operator.join :as join]
            [core2.error :as err])
  (:import clojure.lang.MapEntry
           core2.data_source.IQueryDataSource
           [core2.operator.set ICursorFactory IFixpointCursorFactory]
           org.apache.arrow.memory.BufferAllocator))

(defmulti emit-op first)

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

(defmethod emit-op :scan [[_ {:keys [source columns]}]]
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
        args (vec (concat (for [col-name col-names
                                :when (not (contains? selects col-name))]
                            {:op :variable :variable (symbol col-name)})
                          (vals selects)))
        metadata-pred (expr.meta/->metadata-selector {:op :call, :f 'and, :args args})]

    (fn [^BufferAllocator allocator, srcs]
      (let [^IQueryDataSource db (or (get srcs (or source '$))
                                     (throw (err/illegal-arg :unknown-db
                                                             {::err/message "Query refers to unknown db"
                                                              :db source
                                                              :srcs (keys srcs)})))]
        (.scan db allocator col-names metadata-pred col-preds nil nil)))))

(defmethod emit-op :table [[_ {[table-type table-arg] :table}]]
  (fn [^BufferAllocator allocator, srcs]
    (table/->table-cursor allocator
                          (case table-type
                            :rows table-arg
                            :source (or (get srcs table-arg)
                                        (throw (err/illegal-arg :unknown-table
                                                                {::err/message "Query refers to unknown table"
                                                                 :table table-arg
                                                                 :srcs (keys srcs)})))))))

(defn- unary-op [relation f]
  (let [inner-f (emit-op relation)]
    (fn [^BufferAllocator allocator, srcs]
      (let [inner (inner-f allocator srcs)]
        (try
          (f allocator inner)
          (catch Exception e
            (util/try-close inner)
            (throw e)))))))

(defn- binary-op [left right f]
  (let [left-f (emit-op left)
        right-f (emit-op right)]
    (fn [^BufferAllocator allocator, srcs]
      (let [left (left-f allocator srcs)]
        (try
          (let [right (right-f allocator srcs)]
            (try
              (f allocator left right)
              (catch Exception e
                (util/try-close right)
                (throw e))))
          (catch Exception e
            (util/try-close left)
            (throw e)))))))

(defmethod emit-op :select [[_ {:keys [predicate relation]}]]
  (let [selector (expr/->expression-root-selector predicate)]
    (unary-op relation (fn [allocator inner]
                         (select/->select-cursor allocator inner selector)))))

(defmethod emit-op :project [[_ {:keys [projections relation]}]]
  (let [projection-specs (for [[p-type arg] projections]
                           (case p-type
                             :column (project/->identity-projection-spec (name arg))
                             :extend (let [[col-name expr] (first arg)]
                                       (expr/->expression-projection-spec (name col-name) expr))))]
    (unary-op relation (fn [allocator inner]
                         (project/->project-cursor allocator inner projection-specs)))))

(defmethod emit-op :rename [[_ {:keys [columns relation prefix]}]]
  (let [rename-map (->> columns
                        (into {} (map (juxt (comp name key)
                                            (comp name val)))))]
    (unary-op relation (fn [allocator inner]
                         (rename/->rename-cursor allocator inner rename-map (some-> prefix (name)))))))

(defmethod emit-op :distinct [[_ {:keys [relation]}]]
  (unary-op relation (fn [allocator inner]
                       (set-op/->distinct-cursor allocator inner))))

(defmethod emit-op :union [[_ {:keys [left right]}]]
  (binary-op left right (fn [_allocator left right]
                          (set-op/->union-cursor left right))))

(defmethod emit-op :intersection [[_ {:keys [left right]}]]
  (binary-op left right (fn [allocator left right]
                          (set-op/->intersection-cursor allocator left right))))

(defmethod emit-op :difference [[_ {:keys [left right]}]]
  (binary-op left right (fn [allocator left right]
                          (set-op/->difference-cursor allocator left right))))

(defmethod emit-op :cross-join [[_ {:keys [left right]}]]
  (binary-op left right (fn [allocator left right]
                          (join/->cross-join-cursor allocator left right))))

(defmethod emit-op :join [[_ {:keys [columns left right]}]]
  (let [[left-col] (keys columns)
        [right-col] (vals columns)]
    (binary-op left right (fn [allocator left right]
                            (join/->equi-join-cursor allocator
                                                     left (name left-col)
                                                     right (name right-col))))))

(defmethod emit-op :semi-join [[_ {:keys [columns left right]}]]
  (let [[left-col] (keys columns)
        [right-col] (vals columns)]
    (binary-op left right (fn [allocator left right]
                            (join/->semi-equi-join-cursor allocator
                                                          left (name left-col)
                                                          right (name right-col))))))

(defmethod emit-op :anti-join [[_ {:keys [columns left right]}]]
  (let [[left-col] (keys columns)
        [right-col] (vals columns)]
    (binary-op left right (fn [allocator left right]
                            (join/->anti-equi-join-cursor allocator
                                                          left (name left-col)
                                                          right (name right-col))))))

(defmethod emit-op :group-by [[_ {:keys [columns relation]}]]
  (let [agg-specs (for [[col-type arg] columns]
                    (case col-type
                      :group-by (group-by/->group-spec (name arg))
                      :aggregate (let [[to-name {:keys [f args]}] (first arg)
                                       from-name (:variable (first args))]
                                   (->aggregate-spec (keyword (name f)) (name from-name) (name to-name)))
                      [col-type arg]))]
    (unary-op relation (fn [allocator inner]
                         (group-by/->group-by-cursor allocator inner agg-specs)))))

(defmethod emit-op :order-by [[_ {:keys [order relation]}]]
  (let [order-specs (for [[order-type arg] order]
                      (case order-type
                        :direction (order-by/->order-spec (name (key (first arg)))
                                                          (val (first arg)))
                        :column (order-by/->order-spec (name arg) :asc)))]
    (unary-op relation (fn [allocator inner]
                         (order-by/->order-by-cursor allocator inner order-specs)))))

(defmethod emit-op :slice [[_ {:keys [relation], {:keys [offset limit]} :slice}]]
  (unary-op relation (fn [allocator inner]
                       (slice/->slice-cursor allocator inner offset limit))))

(def ^:dynamic ^:private *relation-variable->cursor-factory* {})

(defmethod emit-op :relation [[_ relation-name]]
  (fn [_allocator _srcs]
    (let [^ICursorFactory cursor-factory (get *relation-variable->cursor-factory* relation-name)]
      (assert cursor-factory)
      (.createCursor cursor-factory))))

(defmethod emit-op :fixpoint [[_ {:keys [mu-variable base recursive]}]]
  (let [base-f (emit-op base)
        recursive-f (emit-op recursive)]
    (fn [^BufferAllocator allocator, srcs]
      (set-op/->fixpoint-cursor allocator
                                (base-f allocator srcs)
                                (reify IFixpointCursorFactory
                                  (createCursor [_ cursor-factory]
                                    (binding [*relation-variable->cursor-factory* (assoc *relation-variable->cursor-factory* mu-variable cursor-factory)]
                                      (recursive-f allocator srcs))))
                 false))))

(defmethod emit-op :assign [[_ {:keys [bindings relation]}]]
  (fn [^BufferAllocator allocator, srcs]
    (let [assignments (reduce
                       (fn [acc {:keys [variable value]}]
                         (assoc acc variable (let [value-f (emit-op value)]
                                               (reify ICursorFactory
                                                 (createCursor [_]
                                                   (binding [*relation-variable->cursor-factory* acc]
                                                     (value-f allocator srcs)))))))
                       *relation-variable->cursor-factory*
                       bindings)]
      (with-bindings {#'*relation-variable->cursor-factory* assignments}
        (let [inner-f (emit-op relation)]
          (inner-f allocator srcs))))))

(defn open-q ^core2.ICursor [allocator srcs lp]
  (when-not (s/valid? ::lp/logical-plan lp)
    (throw (IllegalArgumentException. (s/explain-str ::lp/logical-plan lp))))
  (let [op-f (emit-op (s/conform ::lp/logical-plan lp))]
    (op-f allocator srcs)))

(ns core2.operator
  (:require [clojure.spec.alpha :as s]
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
            [core2.operator.unwind :as unwind]
            [core2.operator.top :as top]
            [core2.operator.table :as table]
            core2.snapshot
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import clojure.lang.MapEntry
           [core2 ICursor IResultSet]
           [core2.operator.set ICursorFactory IFixpointCursorFactory]
           core2.snapshot.ISnapshot
           java.time.Instant
           java.util.function.Consumer
           java.util.Iterator
           [org.apache.arrow.memory BufferAllocator RootAllocator]))

(defmulti emit-op
  (fn [ra-expr srcs]
    (:op ra-expr)))

(alter-meta! #'emit-op assoc :private true)

(defmethod emit-op :scan [{:keys [source columns]} srcs]
  (let [col-names (distinct (for [[col-type arg] columns]
                              (str (case col-type
                                     :column arg
                                     :select (key (first arg))))))
        selects (->> (for [[col-type arg] columns
                           :when (= col-type :select)]
                       (first arg))
                     (into {}))
        col-preds (->> (for [[col-name select-form] selects]
                         (MapEntry/create (name col-name)
                                          (expr/->expression-relation-selector select-form srcs)))
                       (into {}))
        metadata-args (vec (concat (for [col-name col-names
                                         :when (not (contains? col-preds col-name))]
                                     (symbol col-name))
                                   (vals selects)))
        metadata-pred (expr.meta/->metadata-selector (cons 'and metadata-args) srcs)

        ^ISnapshot db (or (get srcs (or source '$))
                          (throw (err/illegal-arg :unknown-db
                                                  {::err/message "Query refers to unknown db"
                                                   :db source
                                                   :srcs (keys srcs)})))]
    (fn [{:keys [allocator default-valid-time]}]
      (let [[^longs temporal-min-range, ^longs temporal-max-range] (expr.temp/->temporal-min-max-range selects srcs)]
        (when-not (or (contains? col-preds "_valid-time-start")
                      (contains? col-preds "_valid-time-end"))
          (expr.temp/apply-constraint temporal-min-range temporal-max-range
                                      '<= "_valid-time-start" default-valid-time)
          (expr.temp/apply-constraint temporal-min-range temporal-max-range
                                      '> "_valid-time-end" default-valid-time))

        (.scan db allocator col-names metadata-pred col-preds temporal-min-range temporal-max-range)))))

(defmethod emit-op :table [{[table-type table-arg] :table} srcs]
  (let [rows (case table-type
               :rows table-arg
               :source (or (get srcs table-arg)
                           (throw (err/illegal-arg :unknown-table
                                                   {::err/message "Query refers to unknown table"
                                                    :table table-arg
                                                    :srcs (keys srcs)}))))]
    (fn [{:keys [allocator]}]
      (table/->table-cursor allocator rows))))

(defmethod emit-op :csv [{:keys [path col-types]} _srcs]
  (fn [{:keys [allocator]}]
    (csv/->csv-cursor allocator path
                      (into {} (map (juxt (comp name key) val)) col-types))))

(defmethod emit-op :arrow [{:keys [path]} _srcs]
  (fn [{:keys [allocator]}]
    (arrow/->arrow-cursor allocator path)))

(defn- unary-op [relation srcs f]
  (let [inner-f (emit-op relation srcs)]
    (fn [opts]
      (let [inner (inner-f opts)]
        (try
          (f opts inner)
          (catch Exception e
            (util/try-close inner)
            (throw e)))))))

(defn- binary-op [left right srcs f]
  (let [left-f (emit-op left srcs)
        right-f (emit-op right srcs)]
    (fn [opts]
      (let [left (left-f opts)]
        (try
          (let [right (right-f opts)]
            (try
              (f opts left right)
              (catch Exception e
                (util/try-close right)
                (throw e))))
          (catch Exception e
            (util/try-close left)
            (throw e)))))))

(defmethod emit-op :select [{:keys [predicate relation]} srcs]
  (let [selector (expr/->expression-relation-selector predicate srcs)]
    (unary-op relation srcs
              (fn [{:keys [allocator]} inner]
                (select/->select-cursor allocator inner selector)))))

(defmethod emit-op :project [{:keys [projections relation]} srcs]
  (let [projection-specs (for [[p-type arg] projections]
                           (case p-type
                             :column (project/->identity-projection-spec (name arg))
                             :extend (let [[col-name form] (first arg)]
                                       (expr/->expression-projection-spec (name col-name) form srcs))))]
    (unary-op relation srcs
              (fn [{:keys [allocator]} inner]
                (project/->project-cursor allocator inner projection-specs)))))

(defmethod emit-op :rename [{:keys [columns relation prefix]} srcs]
  (let [rename-map (->> columns
                        (into {} (map (juxt (comp name key)
                                            (comp name val)))))]
    (unary-op relation srcs
              (fn [_opts inner]
                (rename/->rename-cursor inner rename-map (some-> prefix (name)))))))

(defmethod emit-op :distinct [{:keys [relation]} srcs]
  (unary-op relation srcs
            (fn [{:keys [allocator]} inner]
              (set-op/->distinct-cursor allocator inner))))

(defmethod emit-op :union-all [{:keys [left right]} srcs]
  (binary-op left right srcs
             (fn [_opts left right]
               (set-op/->union-all-cursor left right))))

(defmethod emit-op :intersection [{:keys [left right]} srcs]
  (binary-op left right srcs
             (fn [{:keys [allocator]} left right]
               (set-op/->intersection-cursor allocator left right))))

(defmethod emit-op :difference [{:keys [left right]} srcs]
  (binary-op left right srcs
             (fn [{:keys [allocator]} left right]
               (set-op/->difference-cursor allocator left right))))

(defmethod emit-op :cross-join [{:keys [left right]} srcs]
  (binary-op left right srcs
             (fn [{:keys [allocator]} left right]
               (join/->cross-join-cursor allocator left right))))

(doseq [[join-op-k ->join-cursor] [[:join join/->equi-join-cursor]
                                   [:left-outer-join join/->left-outer-equi-join-cursor]
                                   [:full-outer-join join/->full-outer-equi-join-cursor]
                                   [:semi-join join/->left-semi-equi-join-cursor]
                                   [:anti-join join/->left-anti-semi-equi-join-cursor]]]
  (defmethod emit-op join-op-k [{:keys [columns left right]} srcs]
    (let [[left-col] (keys columns)
          [right-col] (vals columns)]
      (binary-op left right srcs
                 (fn [{:keys [allocator]} left right]
                   (->join-cursor allocator
                                  left (name left-col)
                                  right (name right-col)))))))

(defmethod emit-op :group-by [{:keys [columns relation]} srcs]
  (let [{group-cols :group-by, aggs :aggregate} (group-by first columns)
        group-cols (mapv (comp name second) group-cols)
        agg-factories (for [[_ agg] aggs]
                        (let [[to-column {:keys [aggregate-fn from-column]}] (first agg)]
                          (group-by/->aggregate-factory aggregate-fn
                                                        (name from-column)
                                                        (name to-column))))]
    (unary-op relation srcs
              (fn [{:keys [allocator]} inner]
                (group-by/->group-by-cursor allocator inner group-cols agg-factories)))))

(defmethod emit-op :order-by [{:keys [order relation]} srcs]
  (let [order-specs (for [arg order
                          :let [[column direction] (first arg)]]
                      (order-by/->order-spec (name column) direction))]
    (unary-op relation srcs
              (fn [{:keys [allocator]} inner]
                (order-by/->order-by-cursor allocator inner order-specs)))))

(defmethod emit-op :top [{:keys [relation], {:keys [skip limit]} :top} srcs]
  (unary-op relation srcs
            (fn [_opts inner]
              (top/->top-cursor inner skip limit))))

(defmethod emit-op :unwind [{:keys [column opts relation]} srcs]
  (unary-op relation srcs
            (fn [{:keys [allocator]} inner]
              (unwind/->unwind-cursor allocator inner (name column) opts))))

(def ^:dynamic ^:private *relation-variable->cursor-factory* {})

(defmethod emit-op :relation [{:keys [relation]} _srcs]
  (fn [_opts]
    (let [^ICursorFactory cursor-factory (get *relation-variable->cursor-factory* relation)]
      (assert cursor-factory)
      (.createCursor cursor-factory))))

(defmethod emit-op :fixpoint [{:keys [mu-variable base recursive]} srcs]
  (let [base-f (emit-op base srcs)
        recursive-f (emit-op recursive srcs)]
    (fn [{:keys [allocator] :as opts}]
      (set-op/->fixpoint-cursor allocator
                                (base-f opts)
                                (reify IFixpointCursorFactory
                                  (createCursor [_ cursor-factory]
                                    (binding [*relation-variable->cursor-factory* (assoc *relation-variable->cursor-factory* mu-variable cursor-factory)]
                                      (recursive-f opts))))
                 false))))

(defmethod emit-op :assign [{:keys [bindings relation]} srcs]
  (fn [opts]
    (let [assignments (reduce
                       (fn [acc {:keys [variable value]}]
                         (assoc acc variable (let [value-f (emit-op value srcs)]
                                               (reify ICursorFactory
                                                 (createCursor [_]
                                                   (binding [*relation-variable->cursor-factory* acc]
                                                     (value-f opts)))))))
                       *relation-variable->cursor-factory*
                       bindings)]
      (with-bindings {#'*relation-variable->cursor-factory* assignments}
        (let [inner-f (emit-op relation srcs)]
          (inner-f opts))))))

;; we have to use our own class (rather than `Spliterators.iterator`) because we
;; need to call rel->rows eagerly - the rel may have been reused/closed after
;; the tryAdvance returns.
(deftype CursorResultSet [^BufferAllocator allocator
                          ^ICursor cursor
                          ^:unsynchronized-mutable ^Iterator next-values]
  IResultSet
  (hasNext [res]
    (boolean
     (or (and next-values (.hasNext next-values))
         (and (.tryAdvance cursor
                           (reify Consumer
                             (accept [_ rel]
                               (set! (.-next-values res)
                                     (.iterator (iv/rel->rows rel))))))
              (.hasNext next-values)))))

  (next [_]
    (.next next-values))

  (close [_]
    (util/try-close cursor)
    (util/try-close allocator)))

(defn open-ra ^core2.IResultSet [query src-or-srcs query-opts]
  (let [srcs (cond
               (nil? src-or-srcs) {}
               (map? src-or-srcs) src-or-srcs
               :else {'$ src-or-srcs})]
    (when-not (s/valid? ::lp/logical-plan query)
      (throw (err/illegal-arg :malformed-query
                              {:plan query
                               :srcs srcs
                               :explain (s/explain-data ::lp/logical-plan query)})))

    (let [allocator (RootAllocator.)]
      (try
        (let [op-f (emit-op (s/conform ::lp/logical-plan query) srcs)
              cursor (op-f (-> (merge {:default-valid-time (Instant/now)} query-opts)
                               (assoc :allocator allocator)))]
          (CursorResultSet. allocator cursor nil))
        (catch Throwable t
          (util/try-close allocator)
          (throw t))))))

(defn query-ra
  ([query src-or-srcs] (query-ra query src-or-srcs {}))
  ([query src-or-srcs query-opts]
   (with-open [res (open-ra query src-or-srcs query-opts)]
     (vec (iterator-seq res)))))

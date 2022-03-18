(ns core2.operator
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [core2.error :as err]
            [core2.expression :as expr]
            [core2.expression.metadata :as expr.meta]
            [core2.expression.temporal :as expr.temp]
            [core2.logical-plan :as lp]
            [core2.operator.apply :as apply]
            [core2.operator.arrow :as arrow]
            [core2.operator.csv :as csv]
            [core2.operator.group-by :as group-by]
            [core2.operator.join :as join]
            [core2.operator.max-1-row :as max1]
            [core2.operator.order-by :as order-by]
            [core2.operator.project :as project]
            [core2.operator.rename :as rename]
            [core2.operator.select :as select]
            [core2.operator.set :as set-op]
            [core2.operator.table :as table]
            [core2.operator.top :as top]
            [core2.operator.unwind :as unwind]
            [core2.snapshot]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import (clojure.lang MapEntry)
           (core2 ICursor IResultSet)
           (core2.operator IProjectionSpec)
           (core2.operator.apply IDependentCursorFactory)
           (core2.operator.group_by IAggregateSpecFactory)
           (core2.operator.set ICursorFactory IFixpointCursorFactory)
           (core2.snapshot ISnapshot)
           (java.time Instant)
           (java.util Iterator)
           (java.util.function Consumer)
           (org.apache.arrow.memory BufferAllocator RootAllocator)))

#_{:clj-kondo/ignore #{:unused-binding}}
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
    {:col-names col-names
     :->cursor (fn [{:keys [allocator default-valid-time]}]
                 (let [[^longs temporal-min-range, ^longs temporal-max-range] (expr.temp/->temporal-min-max-range selects srcs)]
                   (when-not (or (contains? col-preds "_valid-time-start")
                                 (contains? col-preds "_valid-time-end"))
                     (expr.temp/apply-constraint temporal-min-range temporal-max-range
                                                 '<= "_valid-time-start" default-valid-time)
                     (expr.temp/apply-constraint temporal-min-range temporal-max-range
                                                 '> "_valid-time-end" default-valid-time))

                   (.scan db allocator col-names metadata-pred col-preds temporal-min-range temporal-max-range)))}))

(defmethod emit-op :table [{[table-type table-arg] :table, :keys [explicit-col-names]} srcs]
  (let [rows (case table-type
               :rows table-arg
               :source (or (get srcs table-arg)
                           (throw (err/illegal-arg :unknown-table
                                                   {::err/message "Query refers to unknown table"
                                                    :table table-arg
                                                    :srcs (keys srcs)}))))
        col-names (or explicit-col-names
                      (into #{} (map symbol) (keys (first rows))))]

    (when-not (every? #(= col-names (into #{} (map symbol) (keys %))) rows)
      (throw (err/illegal-arg :mismatched-keys-in-table
                              {::err/message "Mismatched keys in table"
                               :expected col-names
                               :key-sets (into #{} (map keys) rows)})))

    {:col-names col-names
     :->cursor (fn [{:keys [allocator]}]
                 (table/->table-cursor allocator rows))}))

(defmethod emit-op :csv [{:keys [path col-types]} _srcs]
  (fn [{:keys [allocator]}]
    (csv/->csv-cursor allocator path
                      (into {} (map (juxt (comp name key) val)) col-types))))

(defmethod emit-op :arrow [{:keys [path]} _srcs]
  (fn [{:keys [allocator]}]
    (arrow/->arrow-cursor allocator path)))

(defn- unary-op [relation srcs f]
  (let [{->inner-cursor :->cursor, inner-col-names :col-names} (emit-op relation srcs)
        {:keys [col-names ->cursor]} (f inner-col-names)]
    {:col-names col-names
     :->cursor (fn [opts]
                 (let [inner (->inner-cursor opts)]
                   (try
                     (->cursor opts inner)
                     (catch Throwable e
                       (util/try-close inner)
                       (throw e)))))}))

(defn- binary-op [left right srcs f]
  (let [{left-col-names :col-names, ->left-cursor :->cursor} (emit-op left srcs)
        {right-col-names :col-names, ->right-cursor :->cursor} (emit-op right srcs)
        {:keys [col-names ->cursor]} (f left-col-names right-col-names)]

    {:col-names col-names
     :->cursor (fn [opts]
                 (let [left (->left-cursor opts)]
                   (try
                     (let [right (->right-cursor opts)]
                       (try
                         (->cursor opts left right)
                         (catch Throwable e
                           (util/try-close right)
                           (throw e))))
                     (catch Throwable e
                       (util/try-close left)
                       (throw e)))))}))

(defmethod emit-op :select [{:keys [predicate relation]} srcs]
  (let [selector (expr/->expression-relation-selector predicate srcs)]
    (unary-op relation srcs
              (fn [inner-col-names]
                {:col-names inner-col-names
                 :->cursor (fn [{:keys [allocator]} inner]
                             (select/->select-cursor allocator inner selector))}))))

(defmethod emit-op :project [{:keys [projections relation], {:keys [append-columns?]} :opts} srcs]
  (unary-op relation srcs
            (fn [inner-col-names]
              (let [projection-specs (concat (when append-columns?
                                               (for [col-name inner-col-names]
                                                 (project/->identity-projection-spec (name col-name))))
                                             (for [[p-type arg] projections]
                                               (case p-type
                                                 :column (project/->identity-projection-spec (name arg))
                                                 :row-number-column (let [[col-name _form] (first arg)]
                                                                      (project/->row-number-projection-spec (name col-name)))
                                                 :extend (let [[col-name form] (first arg)]
                                                           (expr/->expression-projection-spec (name col-name) form srcs)))))]
                {:col-names (->> projection-specs
                                 (into #{} (map #(symbol (.getColumnName ^IProjectionSpec %)))))
                 :->cursor (fn [{:keys [allocator]} inner]
                             (project/->project-cursor allocator inner projection-specs))}))))

(defmethod emit-op :map [op srcs]
  (emit-op (assoc op :op :project :opts {:append-columns? true}) srcs))

(defmethod emit-op :rename [{:keys [columns relation prefix]} srcs]
  (let [rename-map (->> columns
                        (into {} (map (juxt (comp name key)
                                            (comp name val)))))]
    (unary-op relation srcs
              (fn [col-names]
                {:col-names (->> col-names
                                 (into #{}
                                       (map (fn [old-name]
                                              (cond->> (get rename-map old-name old-name)
                                                prefix (str prefix rename/relation-prefix-delimiter))))))
                 :->cursor (fn [_opts inner]
                             (rename/->rename-cursor inner rename-map (some-> prefix (name))))}))))

(defmethod emit-op :distinct [{:keys [relation]} srcs]
  (unary-op relation srcs
            (fn [inner-col-names]
              {:col-names inner-col-names
               :->cursor (fn [{:keys [allocator]} inner]
                           (set-op/->distinct-cursor allocator inner-col-names inner))})))

(defn ensuring-same-col-names [left-col-names right-col-names]
  (when-not (= left-col-names right-col-names)
    (throw (IllegalArgumentException. (format "union incompatible cols: %s vs %s" (pr-str left-col-names) (pr-str right-col-names)))))
  left-col-names)

(defmethod emit-op :union-all [{:keys [left right]} srcs]
  (binary-op left right srcs
             (fn [left-col-names right-col-names]
               {:col-names (ensuring-same-col-names left-col-names right-col-names)
                :->cursor (fn [_opts left right]
                            (set-op/->union-all-cursor left right))})))

(defmethod emit-op :intersection [{:keys [left right]} srcs]
  (binary-op left right srcs
             (fn [left-col-names right-col-names]
               (let [col-names (ensuring-same-col-names left-col-names right-col-names)]
                 {:col-names col-names
                  :->cursor (fn [{:keys [allocator]} left right]
                              (set-op/->intersection-cursor allocator col-names left right))}))))

(defmethod emit-op :difference [{:keys [left right]} srcs]
  (binary-op left right srcs
             (fn [left-col-names right-col-names]
               (let [col-names (ensuring-same-col-names left-col-names right-col-names)]
                 {:col-names col-names
                  :->cursor (fn [{:keys [allocator]} left right]
                              (set-op/->difference-cursor allocator col-names left right))}))))

(defmethod emit-op :cross-join [{:keys [left right]} srcs]
  (binary-op left right srcs
             (fn [left-col-names right-col-names]
               {:col-names (set/union left-col-names right-col-names)
                :->cursor (fn [{:keys [allocator]} left right]
                            (join/->cross-join-cursor allocator left right))})))

(doseq [[join-op-k ->join-cursor ->col-names]
        [[:join join/->equi-join-cursor set/union]
         [:left-outer-join join/->left-outer-equi-join-cursor set/union]
         [:full-outer-join join/->full-outer-equi-join-cursor set/union]
         [:semi-join join/->left-semi-equi-join-cursor (fn [l _r] l)]
         [:anti-join join/->left-anti-semi-equi-join-cursor (fn [l _r] l)]]]

  (defmethod emit-op join-op-k [{:keys [columns left right]} srcs]
    (let [[left-col] (keys columns)
          [right-col] (vals columns)]
      (binary-op left right srcs
                 (fn [left-col-names right-col-names]
                   {:col-names (->col-names left-col-names right-col-names)
                    :->cursor (fn [{:keys [allocator]} left right]
                                (->join-cursor allocator
                                               left (name left-col) left-col-names
                                               right (name right-col) right-col-names))})))))

(defmethod emit-op :group-by [{:keys [columns relation]} srcs]
  (let [{group-cols :group-by, aggs :aggregate} (group-by first columns)
        group-cols (mapv (comp name second) group-cols)
        agg-factories (for [[_ agg] aggs]
                        (let [[to-column {:keys [aggregate-fn from-column]}] (first agg)]
                          (group-by/->aggregate-factory aggregate-fn
                                                        (name from-column)
                                                        (name to-column))))]
    (unary-op relation srcs
              (fn [_inner-col-names]
                {:col-names (set/union (set group-cols)
                                       (->> agg-factories
                                            (into #{} (map #(.getColumnName ^IAggregateSpecFactory %)))))
                 :->cursor (fn [{:keys [allocator]} inner]
                             (group-by/->group-by-cursor allocator inner group-cols agg-factories))}))))

(defmethod emit-op :order-by [{:keys [order relation]} srcs]
  (let [order-specs (for [arg order
                          :let [[column direction] (first arg)]]
                      (order-by/->order-spec (name column) direction))]
    (unary-op relation srcs
              (fn [col-names]
                {:col-names col-names
                 :->cursor (fn [{:keys [allocator]} inner]
                             (order-by/->order-by-cursor allocator inner order-specs))}))))

(defmethod emit-op :top [{:keys [relation], {:keys [skip limit]} :top} srcs]
  (unary-op relation srcs
            (fn [col-names]
              {:col-names col-names
               :->cursor (fn [_opts inner]
                           (top/->top-cursor inner skip limit))})))

(defmethod emit-op :unwind [{:keys [columns opts relation]} srcs]
  (let [[to-col from-col] (first columns)]
    (unary-op relation srcs
              (fn [col-names]
                {:col-names (conj col-names to-col)
                 :->cursor (fn [{:keys [allocator]} inner]
                             (unwind/->unwind-cursor allocator inner (name from-col) (name to-col)
                                                     (update opts :ordinality-column #(some-> % name))))}))))

(def ^:dynamic ^:private *relation-variable->col-names* {})
(def ^:dynamic ^:private *relation-variable->cursor-factory* {})

(defmethod emit-op :relation [{:keys [relation]} _srcs]
  (let [col-names (*relation-variable->col-names* relation)]
    {:col-names col-names
     :->cursor (fn [_opts]
                 (let [^ICursorFactory cursor-factory (get *relation-variable->cursor-factory* relation)]
                   (assert cursor-factory (str "can't find " relation, (pr-str *relation-variable->cursor-factory*)))
                   (.createCursor cursor-factory)))}))

(defmethod emit-op :fixpoint [{:keys [mu-variable base recursive]} srcs]
  (let [{base-col-names :col-names, ->base-cursor :->cursor} (emit-op base srcs)
        {recursive-col-names :col-names, ->recursive-cursor :->cursor} (binding [*relation-variable->col-names* (-> *relation-variable->col-names*
                                                                                                                    (assoc mu-variable base-col-names))]
                                                                         (emit-op recursive srcs))]
    {:col-names (ensuring-same-col-names base-col-names recursive-col-names)
     :->cursor
     (fn [{:keys [allocator] :as opts}]
       (set-op/->fixpoint-cursor allocator
                                 (->base-cursor opts)
                                 (reify IFixpointCursorFactory
                                   (createCursor [_ cursor-factory]
                                     (binding [*relation-variable->cursor-factory* (-> *relation-variable->cursor-factory*
                                                                                       (assoc mu-variable cursor-factory))]
                                       (->recursive-cursor opts))))
                                 false))}))

(defmethod emit-op :assign [{:keys [bindings relation]} srcs]
  (let [{:keys [rel-var->col-names relations]} (->> bindings
                                                     (reduce (fn [{:keys [rel-var->col-names relations]} {:keys [variable value]}]
                                                               (binding [*relation-variable->col-names* rel-var->col-names]
                                                                 (let [{:keys [col-names ->cursor]} (emit-op value srcs)]
                                                                   {:rel-var->col-names (-> rel-var->col-names
                                                                                            (assoc variable col-names))

                                                                    :relations (conj relations {:variable variable, :->cursor ->cursor})})))
                                                             {:rel-var->col-names *relation-variable->col-names*
                                                              :relations []}))
        {:keys [col-names ->cursor]} (binding [*relation-variable->col-names* rel-var->col-names]
                                       (emit-op relation srcs))]
    {:col-names col-names
     :->cursor (fn [opts]
                 (let [rel-var->cursor-factory (->> relations
                                                    (reduce (fn [acc {:keys [variable ->cursor]}]
                                                              (-> acc
                                                                  (assoc variable (reify ICursorFactory
                                                                                    (createCursor [_]
                                                                                      (binding [*relation-variable->cursor-factory* acc]
                                                                                        (->cursor opts)))))))
                                                            *relation-variable->cursor-factory*))]
                   (binding [*relation-variable->cursor-factory* rel-var->cursor-factory]
                     (->cursor opts))))}))

(defmethod emit-op :max-1-row [{:keys [relation]} srcs]
  (unary-op relation srcs
            (fn [col-names]
              {:col-names col-names
               :->cursor (fn [{:keys [allocator]} inner]
                           (max1/->max-1-row-cursor allocator col-names inner))})))

(defmethod emit-op :apply [{:keys [mode columns dependent-column-names
                                   independent-relation dependent-relation]}
                           srcs]
  ;; TODO: decodes/re-encodes row values - can we pass these directly to the sub-query?
  ;; TODO: shouldn't re-emit the op each time - required though because emit-op still takes srcs,
  ;;       and not just the keys to those srcs

  (unary-op independent-relation srcs
            (fn [independent-col-names]
              {:col-names (case mode
                            (:cross-join :left-outer-join) (set/union independent-col-names dependent-column-names)
                            (:semi-join :anti-join) independent-col-names)
               :->cursor (fn [{:keys [allocator] :as query-opts} independent-cursor]
                           (let [dependent-cursor-factory
                                 (reify IDependentCursorFactory
                                   (openDependentCursor [_ in-rel idx]
                                     (let [srcs (into srcs
                                                      (for [[ik dk] columns]
                                                        (let [iv (.vectorForName in-rel (name ik))]
                                                          (MapEntry/create dk (types/get-object (.getVector iv) (.getIndex iv idx))))))
                                           {:keys [->cursor]} (emit-op dependent-relation srcs)]
                                       (->cursor query-opts))))]

                             (apply/->apply-operator allocator mode independent-cursor dependent-column-names dependent-cursor-factory)))})))

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
              next-values
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
        (let [{:keys [->cursor]} (emit-op (s/conform ::lp/logical-plan query) srcs)
              cursor (->cursor (-> (merge {:default-valid-time (Instant/now)} query-opts)
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

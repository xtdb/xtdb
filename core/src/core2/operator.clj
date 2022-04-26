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
           java.time.Clock
           (java.util Iterator)
           (java.util.function Consumer)
           (org.apache.arrow.memory BufferAllocator RootAllocator)))

#_{:clj-kondo/ignore #{:unused-binding}}
(defmulti emit-op
  (fn [ra-expr srcs]
    (:op ra-expr)))

(alter-meta! #'emit-op assoc :private true)

(defmethod emit-op :scan [{:keys [source columns]} {:keys [src-keys params]}]
  (let [ordered-col-names (->> columns
                               (map (fn [[col-type arg]]
                                      (str (case col-type
                                             :column arg
                                             :select (key (first arg))))))
                               (distinct))
        selects (->> (for [[col-type arg] columns
                           :when (= col-type :select)]
                       (first arg))
                     (into {}))
        col-preds (->> (for [[col-name select-form] selects]
                         (MapEntry/create (name col-name)
                                          (expr/->expression-relation-selector select-form #{col-name} params)))
                       (into {}))
        metadata-args (vec (concat (for [col-name ordered-col-names
                                         :when (not (contains? col-preds col-name))]
                                     (symbol col-name))
                                   (vals selects)))
        metadata-pred (expr.meta/->metadata-selector (cons 'and metadata-args) (into #{} (map symbol) ordered-col-names) params)

        src-key (or source '$)]

    (when-not (contains? src-keys src-key)
      (throw (err/illegal-arg :unknown-src
                              {::err/message "Query refers to unknown source"
                               :db source
                               :src-keys src-keys})))
    {:col-names (set ordered-col-names)
     :->cursor (fn [{:keys [allocator srcs params default-valid-time]}]
                 (let [^ISnapshot db (get srcs src-key)
                       [^longs temporal-min-range, ^longs temporal-max-range] (expr.temp/->temporal-min-max-range selects params)]
                   (when-not (or (contains? col-preds "_valid-time-start")
                                 (contains? col-preds "_valid-time-end"))
                     (expr.temp/apply-constraint temporal-min-range temporal-max-range
                                                 '<= "_valid-time-start" default-valid-time)
                     (expr.temp/apply-constraint temporal-min-range temporal-max-range
                                                 '> "_valid-time-end" default-valid-time))

                   (.scan db allocator ordered-col-names metadata-pred col-preds temporal-min-range temporal-max-range)))}))

(defn- table->keys [rows]
  (letfn [(row-keys [row]
            (into #{} (map name) (keys row)))]
    (let [col-names (row-keys (first rows))]
      (when-not (every? #(= col-names (row-keys %)) rows)
        (throw (err/illegal-arg :mismatched-keys-in-table
                                {::err/message "Mismatched keys in table"
                                 :expected col-names
                                 :key-sets (into #{} (map row-keys) rows)})))
      col-names)))

(defmethod emit-op :table [{[table-type table-arg] :table, :keys [explicit-col-names]} {:keys [src-keys table-keys params]}]
  (when (and (= table-type :source) (not (contains? src-keys table-arg)))
    (throw (err/illegal-arg :unknown-table
                            {::err/message "Query refers to unknown table"
                             :table table-arg
                             :src-keys src-keys})))
  (let [col-names (or explicit-col-names
                      (case table-type
                        :rows (table->keys table-arg)
                        :source (get table-keys table-arg)))]
    {:col-names col-names
     :->cursor (fn [{:keys [allocator srcs]}]
                 (let [rows (case table-type
                              :rows table-arg
                              :source (get srcs table-arg))]
                   (table/->table-cursor allocator col-names rows params)))}))

(defmethod emit-op :csv [{:keys [path col-types]} _args]
  (fn [{:keys [allocator]}]
    (csv/->csv-cursor allocator path
                      (into {} (map (juxt (comp name key) val)) col-types))))

(defmethod emit-op :arrow [{:keys [path]} _args]
  (fn [{:keys [allocator]}]
    (arrow/->arrow-cursor allocator path)))

(defn- unary-op [relation args f]
  (let [{->inner-cursor :->cursor, inner-col-names :col-names} (emit-op relation args)
        {:keys [col-names ->cursor]} (f inner-col-names)]
    {:col-names col-names
     :->cursor (fn [opts]
                 (let [inner (->inner-cursor opts)]
                   (try
                     (->cursor opts inner)
                     (catch Throwable e
                       (util/try-close inner)
                       (throw e)))))}))

(defn- binary-op [left right args f]
  (let [{left-col-names :col-names, ->left-cursor :->cursor} (emit-op left args)
        {right-col-names :col-names, ->right-cursor :->cursor} (emit-op right args)
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

(defmethod emit-op :select [{:keys [predicate relation]} {:keys [params] :as args}]
  (unary-op relation args
            (fn [inner-col-names]
              (let [selector (expr/->expression-relation-selector predicate (into #{} (map symbol) inner-col-names) params)]
                {:col-names inner-col-names
                 :->cursor (fn [{:keys [allocator]} inner]
                             (select/->select-cursor allocator inner selector))}))))

(defmethod emit-op :project [{:keys [projections relation], {:keys [append-columns?]} :opts} {:keys [params] :as args}]
  (unary-op relation args
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
                                                           (expr/->expression-projection-spec (name col-name) form (into #{} (map symbol) inner-col-names) params)))))]
                {:col-names (->> projection-specs
                                 (into #{} (map #(.getColumnName ^IProjectionSpec %))))
                 :->cursor (fn [{:keys [allocator]} inner]
                             (project/->project-cursor allocator inner projection-specs))}))))

(defmethod emit-op :map [op args]
  (emit-op (assoc op :op :project :opts {:append-columns? true}) args))

(defmethod emit-op :rename [{:keys [columns relation prefix]} args]
  (let [rename-map (->> columns
                        (into {} (map (juxt (comp name key)
                                            (comp name val)))))]
    (unary-op relation args
              (fn [col-names]
                {:col-names (->> col-names
                                 (into #{}
                                       (map (fn [old-name]
                                              (cond->> (get rename-map old-name old-name)
                                                prefix (str prefix rename/relation-prefix-delimiter))))))
                 :->cursor (fn [_opts inner]
                             (rename/->rename-cursor inner rename-map (some-> prefix (name))))}))))

(defmethod emit-op :distinct [{:keys [relation]} args]
  (unary-op relation args
            (fn [inner-col-names]
              {:col-names inner-col-names
               :->cursor (fn [{:keys [allocator]} inner]
                           (set-op/->distinct-cursor allocator inner-col-names inner))})))

(defn ensuring-same-col-names [left-col-names right-col-names]
  (when-not (= left-col-names right-col-names)
    (throw (IllegalArgumentException. (format "union incompatible cols: %s vs %s" (pr-str left-col-names) (pr-str right-col-names)))))
  left-col-names)

(defmethod emit-op :union-all [{:keys [left right]} args]
  (binary-op left right args
             (fn [left-col-names right-col-names]
               {:col-names (ensuring-same-col-names left-col-names right-col-names)
                :->cursor (fn [_opts left right]
                            (set-op/->union-all-cursor left right))})))

(defmethod emit-op :intersect [{:keys [left right]} args]
  (binary-op left right args
             (fn [left-col-names right-col-names]
               (let [col-names (ensuring-same-col-names left-col-names right-col-names)]
                 {:col-names col-names
                  :->cursor (fn [{:keys [allocator]} left right]
                              (set-op/->intersection-cursor allocator col-names left right))}))))

(defmethod emit-op :difference [{:keys [left right]} args]
  (binary-op left right args
             (fn [left-col-names right-col-names]
               (let [col-names (ensuring-same-col-names left-col-names right-col-names)]
                 {:col-names col-names
                  :->cursor (fn [{:keys [allocator]} left right]
                              (set-op/->difference-cursor allocator col-names left right))}))))

(defmethod emit-op :cross-join [{:keys [left right]} args]
  (binary-op left right args
             (fn [left-col-names right-col-names]
               {:col-names (set/union left-col-names right-col-names)
                :->cursor (fn [{:keys [allocator]} left right]
                            (join/->cross-join-cursor allocator left right))})))

(doseq [[join-op-k ->join-cursor ->col-names]
        [[:join #'join/->equi-join-cursor set/union]
         [:left-outer-join #'join/->left-outer-equi-join-cursor set/union]
         [:full-outer-join #'join/->full-outer-equi-join-cursor set/union]
         [:semi-join #'join/->left-semi-equi-join-cursor (fn [l _r] l)]
         [:anti-join #'join/->left-anti-semi-equi-join-cursor (fn [l _r] l)]]]

  (defmethod emit-op join-op-k [{:keys [condition left right]} args]
    (let [equi-pairs (keep (fn [[tag val]]
                             (when (= :equi-condition tag)
                               (first val)))
                           condition)
          left-key-cols (map (comp name first) equi-pairs)
          right-key-cols (map (comp name second) equi-pairs)
          predicates (keep (fn [[tag val]]
                             (when (= :pred-expr tag)
                               val))
                           condition)]
      (binary-op left right args
                 (fn [left-col-names right-col-names]
                   (let [theta-selector (when (seq predicates)
                                          (expr/->expression-relation-selector (list* 'and predicates)
                                                                               (into #{} (map symbol) (concat left-col-names right-col-names))
                                                                               {}))]
                     {:col-names (->col-names left-col-names right-col-names)
                      :->cursor (fn [{:keys [allocator]} left right]
                                  (->join-cursor allocator
                                                 left left-key-cols left-col-names
                                                 right right-key-cols right-col-names
                                                 theta-selector))}))))))

(def memo-agg-factory (memoize group-by/->aggregate-factory))

(defmethod emit-op :group-by [{:keys [columns relation]} args]
  (let [{group-cols :group-by, aggs :aggregate} (group-by first columns)
        group-cols (mapv (comp name second) group-cols)
        agg-factories (for [[_ agg] aggs]
                        (let [[to-column {:keys [aggregate-fn from-column]}] (first agg)]
                          (memo-agg-factory aggregate-fn
                                            (name from-column)
                                            (name to-column))))]
    (unary-op relation args
              (fn [_inner-col-names]
                {:col-names (set/union (set group-cols)
                                       (->> agg-factories
                                            (into #{} (map #(.getToColumnName ^IAggregateSpecFactory %)))))
                 :->cursor (fn [{:keys [allocator]} inner]
                             (group-by/->group-by-cursor allocator inner group-cols agg-factories))}))))

(defmethod emit-op :order-by [{:keys [order relation]} args]
  (let [order-specs (for [{:keys [column direction null-ordering], :or {direction :asc, null-ordering :nulls-last}} order]
                      {:col-name (name column), :direction direction, :null-ordering null-ordering})]
    (unary-op relation args
              (fn [col-names]
                {:col-names col-names
                 :->cursor (fn [{:keys [allocator]} inner]
                             (order-by/->order-by-cursor allocator inner order-specs))}))))

(defmethod emit-op :top [{:keys [relation], {:keys [skip limit]} :top} args]
  (unary-op relation args
            (fn [col-names]
              {:col-names col-names
               :->cursor (fn [_opts inner]
                           (top/->top-cursor inner skip limit))})))

(defmethod emit-op :unwind [{:keys [columns opts relation]} op-args]
  (let [[to-col from-col] (first columns)]
    (unary-op relation op-args
              (fn [col-names]
                {:col-names (conj col-names to-col)
                 :->cursor (fn [{:keys [allocator]} inner]
                             (unwind/->unwind-cursor allocator inner (name from-col) (name to-col)
                                                     (update opts :ordinality-column #(some-> % name))))}))))

(def ^:dynamic ^:private *relation-variable->col-names* {})
(def ^:dynamic ^:private *relation-variable->cursor-factory* {})

(defmethod emit-op :relation [{:keys [relation]} _opts]
  (let [col-names (*relation-variable->col-names* relation)]
    {:col-names col-names
     :->cursor (fn [_opts]
                 (let [^ICursorFactory cursor-factory (get *relation-variable->cursor-factory* relation)]
                   (assert cursor-factory (str "can't find " relation, (pr-str *relation-variable->cursor-factory*)))
                   (.createCursor cursor-factory)))}))

(defmethod emit-op :fixpoint [{:keys [mu-variable base recursive]} args]
  (let [{base-col-names :col-names, ->base-cursor :->cursor} (emit-op base args)
        {recursive-col-names :col-names, ->recursive-cursor :->cursor} (binding [*relation-variable->col-names* (-> *relation-variable->col-names*
                                                                                                                    (assoc mu-variable base-col-names))]
                                                                         (emit-op recursive args))]
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

(defmethod emit-op :assign [{:keys [bindings relation]} args]
  (let [{:keys [rel-var->col-names relations]} (->> bindings
                                                     (reduce (fn [{:keys [rel-var->col-names relations]} {:keys [variable value]}]
                                                               (binding [*relation-variable->col-names* rel-var->col-names]
                                                                 (let [{:keys [col-names ->cursor]} (emit-op value args)]
                                                                   {:rel-var->col-names (-> rel-var->col-names
                                                                                            (assoc variable col-names))

                                                                    :relations (conj relations {:variable variable, :->cursor ->cursor})})))
                                                             {:rel-var->col-names *relation-variable->col-names*
                                                              :relations []}))
        {:keys [col-names ->cursor]} (binding [*relation-variable->col-names* rel-var->col-names]
                                       (emit-op relation args))]
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

(defmethod emit-op :max-1-row [{:keys [relation]} args]
  (unary-op relation args
            (fn [col-names]
              {:col-names col-names
               :->cursor (fn [{:keys [allocator]} inner]
                           (max1/->max-1-row-cursor allocator col-names inner))})))

(defmethod emit-op :apply [{:keys [mode columns dependent-column-names
                                   independent-relation dependent-relation]}
                           args]
  ;; TODO: decodes/re-encodes row values - can we pass these directly to the sub-query?
  ;; TODO: shouldn't re-emit the op each time - required though because emit-op still takes params,
  ;;       and not just the keys to those params

  (unary-op independent-relation args
            (fn [independent-col-names]
              {:col-names (case mode
                            (:cross-join :left-outer-join) (set/union independent-col-names (set (map name dependent-column-names)))
                            (:semi-join :anti-join) independent-col-names)
               :->cursor (fn [{:keys [allocator] :as query-opts} independent-cursor]
                           (let [dependent-cursor-factory
                                 (reify IDependentCursorFactory
                                   (openDependentCursor [_ in-rel idx]
                                     (let [args (update args :params
                                                        (fnil into {})
                                                        (for [[ik dk] columns]
                                                          (let [iv (.vectorForName in-rel (name ik))]
                                                            (MapEntry/create dk (types/get-object (.getVector iv) (.getIndex iv idx))))))
                                           {:keys [->cursor]} (emit-op dependent-relation args)]
                                       (->cursor query-opts))))]

                             (apply/->apply-operator allocator mode independent-cursor dependent-column-names dependent-cursor-factory)))})))

(deftype CursorResultSet [^BufferAllocator allocator
                          ^ICursor cursor
                          ^Clock clock
                          ^:unsynchronized-mutable ^Iterator next-values]
  IResultSet
  (hasNext [res]
    (boolean
     (or (and next-values (.hasNext next-values))
         (and (binding [expr/*clock* clock]
                ;; need to call rel->rows eagerly - the rel may have been reused/closed after
                ;; the tryAdvance returns.
                (.tryAdvance cursor
                             (reify Consumer
                               (accept [_ rel]
                                 (set! (.-next-values res)
                                       (.iterator (iv/rel->rows rel)))))))
              next-values
              (.hasNext next-values)))))

  (next [_]
    (.next next-values))

  (close [_]
    (util/try-close cursor)
    (util/try-close allocator)))

(defn- args->srcs+params [args]
  (if-not (map? args)
    (recur {'$ args})
    (-> (group-by #(if (lp/source-sym? (key %)) :srcs :params) args)
        (update-vals #(into {} %)))))

(defn open-ra ^core2.IResultSet [query args {:keys [default-valid-time default-tz] :as query-opts}]
  (when-not (s/valid? ::lp/logical-plan query)
    (throw (err/illegal-arg :malformed-query
                            {:plan query
                             :args args
                             :explain (s/explain-data ::lp/logical-plan query)})))

  (let [allocator (RootAllocator.)]
    (try
      (let [default-valid-time (or default-valid-time (.instant expr/*clock*))
            ;; will later be provided as part of the 'SQL session' (see §6.32)
            default-tz (or default-tz (.getZone expr/*clock*))
            clock (Clock/fixed default-valid-time default-tz)]

        (binding [expr/*clock* clock]
          (let [{:keys [srcs params]} (args->srcs+params args)
                {:keys [->cursor]} (emit-op (s/conform ::lp/logical-plan query)
                                            {:src-keys (set (keys srcs)),
                                             :table-keys (->> (for [[src-k src-v] srcs
                                                                    :when (sequential? src-v)]
                                                                [src-k (table->keys src-v)])
                                                              (into {}))
                                             :params params})
                cursor (->cursor (into query-opts
                                       {:srcs srcs, :params params
                                        :default-valid-time default-valid-time
                                        :allocator allocator}))]
            (CursorResultSet. allocator cursor clock nil))))
      (catch Throwable t
        (util/try-close allocator)
        (throw t)))))

(defn query-ra
  ([query inputs] (query-ra query inputs {}))
  ([query inputs query-opts]
   (with-open [res (open-ra query inputs query-opts)]
     (vec (iterator-seq res)))))

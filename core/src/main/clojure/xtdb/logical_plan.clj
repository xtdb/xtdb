(ns xtdb.logical-plan
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.walk :as w]
            [xtdb.error :as err]
            [xtdb.rewrite :as r]
            [xtdb.util :as util])
  (:import (clojure.lang Var)
           java.time.temporal.Temporal
           java.util.Date))

;; See also:
;; https://dbis-uibk.github.io/relax/help#relalg-reference
;; https://calcite.apache.org/javadocAggregate/org/apache/calcite/tools/RelBuilder.html
;; https://github.com/apache/arrow/blob/master/rust/datafusion/src/logical_plan/plan.rs

;; See "Formalising openCypher Graph Queries in Relational Algebra",
;; also contains operators for path expansion:
;; https://core.ac.uk/download/pdf/148787624.pdf

(def util-date? (partial instance? Date))
(def temporal? (partial instance? Temporal))

(s/def ::relation simple-symbol?)
(s/def ::column symbol?)

;; TODO flesh out
(s/def ::value (some-fn nil? string? number? inst? keyword? util-date? temporal? uuid?))

(s/def ::param
  (s/and simple-symbol? #(str/starts-with? (name %) "?")))

(s/def ::expression any?)

(s/def ::column-expression (s/map-of ::column ::expression :conform-keys true :count 1))

(defmulti temporal-filter-spec
  (fn [v]
    (cond-> v (coll? v) first))
  :default ::default)

(defmethod temporal-filter-spec :all-time [_]
  (s/and #{:all-time}
         (s/conformer (constantly [:all-time]) (constantly :all-time))))

(s/def ::temporal-filter-value
  (s/or :now #{:now '(current-timestamp)}
        :literal (some-fn util-date? temporal?)
        :param simple-symbol?))

(defmethod temporal-filter-spec :at [_]
  (s/tuple #{:at} ::temporal-filter-value))

(defmethod temporal-filter-spec :in [_]
  (s/tuple #{:in} (s/nilable ::temporal-filter-value) (s/nilable ::temporal-filter-value)))

(defmethod temporal-filter-spec :from [_]
  (s/and (s/tuple #{:from} (s/nilable ::temporal-filter-value))
         (s/conformer (fn [x] [:in (second x) nil]) identity)))

(defmethod temporal-filter-spec :to [_]
  (s/and (s/tuple #{:to} (s/nilable ::temporal-filter-value))
         (s/conformer (fn [x] [:in nil (second x)]) identity)))

(defmethod temporal-filter-spec :between [_]
  (s/tuple #{:between} (s/nilable ::temporal-filter-value) (s/nilable ::temporal-filter-value)))

(s/def ::temporal-filter
  (s/multi-spec temporal-filter-spec (fn retag [_] (throw (UnsupportedOperationException.)))))

(s/def ::for-valid-time (s/nilable ::temporal-filter))
(s/def ::for-system-time (s/nilable ::temporal-filter))
(s/def ::default-all-valid-time? (s/nilable boolean?))

(defmulti ra-expr
  (fn [expr]
    (cond
      (vector? expr) (first expr)
      (symbol? expr) :relation))
  :default ::default)

(s/def ::ra-expression (s/multi-spec ra-expr :op))

(s/def ::logical-plan ::ra-expression)

(defn- direct-child-exprs [{:keys [op] :as expr}]
  (case op
    :relation #{}

    :assign (let [{:keys [bindings relation]} expr]
              (into #{relation} (map :value) bindings))

    (let [spec (s/describe (ra-expr [op]))]
      (case (first spec)
        cat (->> (rest spec)
                 (partition 2)
                 (mapcat
                   (fn [[k form]]
                     (cond
                       (= form ::ra-expression)
                       [(expr k)]
                       (= form (list 'coll-of :xtdb.logical-plan/ra-expression))
                       (expr k))))
                 (vec))))))

(defn child-exprs [ra]
  (into #{ra} (mapcat child-exprs) (direct-child-exprs ra)))

#_{:clj-kondo/ignore #{:unused-binding}}
(defmulti emit-expr
  (fn [ra-expr opts]
    (:op ra-expr)))

(defn unary-expr {:style/indent 1} [relation f]
  (let [{->inner-cursor :->cursor, inner-fields :fields} relation
        {:keys [fields ->cursor stats]} (f inner-fields)]
    {:fields fields
     :->cursor (fn [opts]
                 (util/with-close-on-catch [inner (->inner-cursor opts)]
                   (->cursor opts inner)))
     :stats stats}))

(defn binary-expr {:style/indent 2} [left right f]
  (let [{left-fields :fields, ->left-cursor :->cursor} left
        {right-fields :fields, ->right-cursor :->cursor} right
        {:keys [fields ->cursor stats]} (f left-fields right-fields)]

    {:fields fields
     :->cursor (fn [opts]
                 (util/with-close-on-catch [left (->left-cursor opts)
                                            right (->right-cursor opts)]
                   (->cursor opts left right)))
     :stats stats}))

;;;; Rewriting of logical plan.

;; Attempt to clean up tree, removing names internally and only add
;; them back at the top. Scan still needs explicit names to access the
;; columns, so these are directly renamed.

(def ^:const ^String relation-id-delimiter "__")
(def ^:const ^String relation-prefix-delimiter "_")

(defn extend-projection? [column-or-expr]
  (map? column-or-expr))

(defn ->projected-column [column-or-expr]
  (if (extend-projection? column-or-expr)
    (key (first column-or-expr))
    column-or-expr))

(defn and-predicate? [predicate]
  (and (sequential? predicate)
       (= 'and (first predicate))))

(defn or-predicate? [predicate]
  (and (sequential? predicate)
       (= 'or (first predicate))))

(defn flatten-expr [pred expr]
  (if (pred expr)
    (mapcat (partial flatten-expr pred) (rest expr))
    [expr]))

;; NOTE: might be better to try do this via projected-columns and meta
;; data when building the initial plan? Though that requires rewrites
;; consistently updating this if they change anything. Some operators
;; don't know their columns, like csv and arrow, though they might
;; have some form of AS clause that does at the SQL-level. This
;; function will mainly be used for decorrelation, so not being able
;; to deduct this, fail, and keep Apply is also an option, say by
;; returning nil instead of throwing an exception like now.

(defn relation-columns [relation-in]
  (r/zmatch relation-in
    [:table explicit-column-names _]
    (vec explicit-column-names)

    [:table table]
    (mapv symbol (keys (first table)))

    [:scan _scan-opts columns]
    (mapv ->projected-column columns)

    [:join _ lhs rhs]
    (vec (mapcat relation-columns [lhs rhs]))

    [:mega-join _ rels]
    (vec (mapcat relation-columns rels))

    [:cross-join lhs rhs]
    (vec (mapcat relation-columns [lhs rhs]))

    [:left-outer-join _ lhs rhs]
    (vec (mapcat relation-columns [lhs rhs]))

    [:semi-join _ lhs _]
    (relation-columns lhs)

    [:anti-join _ lhs _]
    (relation-columns lhs)

    [:mark-join projection lhs _]
    (conj
      (relation-columns lhs)
      (->projected-column projection))

    [:single-join _ lhs rhs]
    (vec (mapcat relation-columns [lhs rhs]))

    [:rename prefix-or-columns relation]
    (if (symbol? prefix-or-columns)
      (vec (for [c (relation-columns relation)]
             (symbol (str prefix-or-columns relation-prefix-delimiter  c))))
      (replace prefix-or-columns (relation-columns relation)))

    [:project projection _]
    (mapv ->projected-column projection)

    [:map projection relation]
    (into (relation-columns relation) (map ->projected-column projection))

    [:group-by columns _]
    (mapv ->projected-column columns)

    [:select _ relation]
    (relation-columns relation)

    [:order-by _ relation]
    (relation-columns relation)

    [:top _ relation]
    (relation-columns relation)

    [:distinct relation]
    (relation-columns relation)

    [:intersect lhs _]
    (relation-columns lhs)

    [:difference lhs _]
    (relation-columns lhs)

    [:union-all lhs _]
    (relation-columns lhs)

    [:fixpoint _ base _]
    (relation-columns base)

    [:unnest columns relation]
    (conj (relation-columns relation) (key (first columns)))

    [:unnest columns opts relation]
    (cond-> (conj (relation-columns relation) (val (first columns)))
      (:ordinality-column opts) (conj (:ordinality-column opts)))

    [:assign _ relation]
    (relation-columns relation)

    [:apply mode _ independent-relation dependent-relation]
    (-> (relation-columns independent-relation)
        (concat
          (when-let [mark-join-projection (:mark-join mode)]
            [(->projected-column mark-join-projection)])
          (case mode
            (:cross-join :left-outer-join :single-join) (relation-columns dependent-relation)
            []))
        (vec))

    [:arrow _path]
    []

    (throw (err/illegal-arg ::cannot-calculate-relation-cols
                            {::err/message (str "cannot calculate columns for: " (pr-str relation-in))
                             :relation relation-in}))))

(def ^:private ^:dynamic *name-counter* (atom 0))

(defn- next-name []
  (with-meta (symbol (str 'x (swap! *name-counter* inc))) {:column? true}))

(defn rename-walk
  "Some rename steps require a walk of a relation to replace names. This is usually fine, but it is possible
  user column names can collide with our 'x1', 'x2' symbols that are generated as part of the remove names step.

  This function is a version of postwalk-replace that does not rename the elements of a `:scan` operator if encountered
  during the walk."
  [smap form]
  ;; consider in the future just ensuring that generated names cannot collide with columns and we
  ;; can go back to clojure.walk
  (letfn [(conditional-walk [pred inner outer form]
            (cond
              (not (pred form)) form
              (list? form) (outer (apply list (map inner form)))
              (instance? clojure.lang.IMapEntry form)
              (outer (clojure.lang.MapEntry/create (inner (key form)) (inner (val form))))
              (seq? form) (outer (doall (map inner form)))
              (instance? clojure.lang.IRecord form)
              (outer (reduce (fn [r x] (conj r (inner x))) form form))
              (coll? form) (outer (into (empty form) (map inner form)))
              :else (outer form)))
          (conditional-postwalk [pred f form] (conditional-walk pred (partial conditional-postwalk pred f) f form))
          (not-scan? [form] (if (vector? form)
                              (not= :scan (nth form 0 nil))
                              true))
          (replace [form] (if (contains? smap form) (smap form) form))]
    (conditional-postwalk not-scan? replace form)))

(defn- remove-names-step [relation-in]
  (letfn [(with-smap [relation smap]
            (vary-meta relation assoc :smap smap))

          (->smap [relation]
            (:smap (meta relation)))

          (remove-projection-names [op projection relation]
            (let [smap (->smap relation)
                  new-smap (reduce
                             (fn [acc p]
                               (if (extend-projection? p)
                                 (let [[k v] (first p)]
                                   (if-let [existing-name (smap v)]
                                     (assoc acc k existing-name)
                                     (assoc acc k (next-name))))
                                 (assoc acc p (smap p))))
                             (if (= op :map)
                               smap
                               (->> smap
                                    (filter #(str/starts-with? (name (key %)) "?"))
                                    (into {})))
                             projection)

                  projection (vec (for [p projection]
                                    (if (extend-projection? p)
                                      (let [[k v] (first p)]
                                        {k (w/postwalk-replace smap v)})
                                      (w/postwalk-replace smap p))))

                  projection (vec (for [p (w/postwalk-replace new-smap projection)]
                                    (if (extend-projection? p)
                                      (let [[k v] (first p)]
                                        (if (= k v)
                                          k
                                          p))
                                      p)))
                  projection (if (= :map op)
                               (filterv map? projection)
                               projection)
                  relation (if (and (every? symbol? projection)
                                    (or (= :map op)
                                        (= (set projection)
                                           (set (relation-columns relation)))))
                             relation
                             [op
                              projection
                              relation])]
              (with-smap relation new-smap)))]
    (r/zmatch relation-in
      [:table explicit-column-names table]
      (let [smap (zipmap explicit-column-names
                         (repeatedly next-name))]
        (with-smap (w/postwalk-replace smap [:table explicit-column-names table]) smap))

      [:table table]
      (let [smap (zipmap (map symbol (keys (first table)))
                         (repeatedly next-name))]
        (with-smap [:table (w/postwalk-replace smap table)]
          smap))

      [:scan scan-opts columns]
      (let [smap (zipmap (map ->projected-column columns)
                         (repeatedly next-name))]
        (with-smap [:rename smap [:scan scan-opts columns]] smap))

      [:join join-map lhs rhs]
      (let [smap (merge (->smap lhs) (->smap rhs))]
        (with-smap [:join (w/postwalk-replace smap join-map) lhs rhs] smap))

      [:cross-join lhs rhs]
      (let [smap (merge (->smap lhs) (->smap rhs))]
        (with-smap [:cross-join lhs rhs] smap))

      [:left-outer-join join-map lhs rhs]
      (let [smap (merge (->smap lhs) (->smap rhs))]
        (with-smap [:left-outer-join (w/postwalk-replace smap join-map) lhs rhs] smap))

      [:semi-join join-map lhs rhs]
      (let [smap (merge (->smap lhs) (->smap rhs))]
        (with-smap [:semi-join (w/postwalk-replace smap join-map) lhs rhs]
          (->smap lhs)))

      [:anti-join join-map lhs rhs]
      (let [smap (merge (->smap lhs) (->smap rhs))]
        (with-smap [:anti-join (w/postwalk-replace smap join-map) lhs rhs]
          (->smap lhs)))

      [:mark-join projection lhs rhs]
      (let [mark-join-projection-smap (let [[column _expr] projection]
                                        {column (next-name)})
            smap (merge (->smap lhs) (->smap rhs))]
        (with-smap [:mark-join (w/postwalk-replace smap projection) lhs rhs]
          (merge (->smap lhs) mark-join-projection-smap)))

      [:rename prefix-or-columns relation]
      (let [smap (->smap relation)]
        (with-smap relation
          (if (symbol? prefix-or-columns)
            (update-keys smap #(if (str/starts-with? (name %) "?")
                                 %
                                 (symbol (str prefix-or-columns relation-prefix-delimiter %))))
            (set/rename-keys smap prefix-or-columns))))

      [:project columns [:arrow path]]
      (let [smap (zipmap (map ->projected-column columns)
                         (repeatedly next-name))]
        (with-smap [:rename smap [:arrow path]] smap))

      [:project projection relation]
      (remove-projection-names :project projection relation)

      [:map projection relation]
      (remove-projection-names :map projection relation)

      [:group-by columns relation]
      (let [smap (->smap relation)
            columns (w/postwalk-replace smap columns)
            smap (merge smap (zipmap (map ->projected-column (filter map? columns))
                                     (repeatedly next-name)))]
        (with-smap [:group-by (w/postwalk-replace smap columns) relation] smap))

      [:select predicate relation]
      (let [smap (->smap relation)]
        (with-smap [:select (w/postwalk-replace smap predicate) relation] smap))

      [:order-by opts relation]
      (let [smap (->smap relation)]
        (with-smap [:order-by (w/postwalk-replace smap opts) relation] smap))

      [:top opts relation]
      (with-smap [:top opts relation] (->smap relation))

      [:distinct relation]
      (with-smap [:distinct relation] (->smap relation))

      [:intersect lhs rhs]
      (with-smap [:intersect lhs (rename-walk (zipmap (relation-columns rhs)
                                                      (relation-columns lhs))
                                              rhs)]
        (->smap lhs))

      [:difference lhs rhs]
      (with-smap [:difference lhs (rename-walk (zipmap (relation-columns rhs)
                                                       (relation-columns lhs))
                                               rhs)]
        (->smap lhs))

      [:union-all lhs rhs]
      (with-smap [:union-all lhs (rename-walk (zipmap (relation-columns rhs)
                                                      (relation-columns lhs))
                                              rhs)]
        (->smap lhs))

      [:apply mode columns independent-relation dependent-relation]
      (let [smap (merge (->smap independent-relation) (->smap dependent-relation))
            params (->> columns
                        (vals)
                        (filter #(str/starts-with? (name %) "?"))
                        (map (fn [param]
                               {param (get
                                        smap
                                        param
                                        (with-meta
                                          (symbol (str "?" (next-name)))
                                          {:correlated-column? true}))}))
                        (into {}))
            mark-join-mode-projection-smap (when-let [[column _expr] (first (:mark-join mode))]
                                             {column (next-name)})
            new-smap (merge smap params mark-join-mode-projection-smap)]
        (-> [:apply
             (w/postwalk-replace new-smap mode)
             (w/postwalk-replace new-smap columns)
             independent-relation
             (rename-walk new-smap dependent-relation)]
            (with-smap (if mark-join-mode-projection-smap
                         (merge (->smap independent-relation) mark-join-mode-projection-smap params)
                         (case mode
                           (:cross-join :left-outer-join :single-join) new-smap
                           (:semi-join :anti-join) (merge (->smap independent-relation) params))))))

      [:unnest columns opts relation]
      (let [smap (->smap relation)
            [to from] (first columns)
            from (get smap from)
            smap (assoc smap to (next-name))
            columns {(get smap to) from}
            [smap opts] (if-let [ordinality-column (:ordinality-column opts)]
                          (let [smap (assoc smap ordinality-column (next-name))]
                            [smap {:ordinality-column (get smap ordinality-column)}])
                          [smap {}])]
        (with-smap [:unnest columns opts relation] smap))

      [:arrow path]
      (with-smap [:arrow path] {})

      (when (and (vector? (r/node relation-in))
                 (keyword? (r/ctor relation-in)))
        (throw (err/illegal-arg ::cannot-remove-names
                                {::err/message (str "cannot remove names for: " (pr-str (r/node relation-in)))
                                 :node (r/node relation-in)}))))))

(defn remove-names [relation {:keys [project-anonymous-columns?]}]
  (let [named-projection (relation-columns relation)
        relation (binding [*name-counter* (atom 0)]
                   (r/node (r/bottomup (r/adhoc-tp r/id-tp remove-names-step) (r/vector-zip relation))))
        smap (:smap (meta relation))
        rename-map (select-keys smap named-projection)
        projection (replace smap named-projection)
        add-projection-fn (fn [relation]
                            (let [relation (if (= projection (relation-columns relation))
                                             relation
                                             [:project projection
                                              relation])
                                  smap-inv (set/map-invert rename-map)
                                  relation (if project-anonymous-columns?
                                             relation
                                             (or (r/zmatch relation
                                                   [:rename rename-map-2 relation-2]
                                                   ;;=>
                                                   (when (= smap-inv (set/map-invert rename-map-2))
                                                     relation-2))
                                                 [:rename smap-inv relation]))]
                              (with-meta relation {:column->name smap
                                                   :named-projection named-projection})))]
    (with-meta relation {:column->name smap
                         :named-projection named-projection
                         :add-projection-fn add-projection-fn})))

(defn expr-symbols [expr]
  (set (for [x (flatten (if (coll? expr)
                          (seq expr)
                          [expr]))
             :when (and (symbol? x)
                        (:column? (meta x)))]
         x)))

(defn expr-correlated-symbols [expr]
  (set (for [x (flatten (if (coll? expr)
                          (seq expr)
                          [expr]))
             :when (and (symbol? x)
                        (:correlated-column? (meta x)))]
         x)))

(defn column? [x]
  (and (symbol? x)
       (:column? (meta x))))

(defn equals-predicate? [predicate]
  (and (sequential? predicate)
       (= 3 (count predicate))
       (= '= (first predicate))))

(defn all-columns-in-relation?
  "Returns true if all columns referenced by the expression are present in the given relation.

  Useful for figuring out whether an expr can be applied as an equi-condition in join."
  [expr relation]
  (when-let [expr-cols (not-empty (expr-symbols expr))]
    (every? (set (relation-columns relation)) expr-cols)))

(defn- all-columns-across-both-relations-with-one-in-each?
  "Returns true if all columns are present across both relations, with at least one column in each"
  [expr left-relation right-relation]
 (let [columns (expr-symbols expr)]
   (-> columns
       (set/difference (set (relation-columns left-relation)))
       (not-empty)
       (set/difference (set (relation-columns right-relation)))
       (empty?))))

(defn- columns-in-both-relations? [predicate lhs rhs]
  (let [predicate-columns (expr-symbols predicate)]
    (and
      (some predicate-columns
            (relation-columns lhs))
      (some predicate-columns
            (relation-columns rhs)))))

(defn- conjunction-clauses [predicate]
  (if (and-predicate? predicate)
    (rest predicate)
    [predicate]))

(defn- merge-conjunctions [predicate-1 predicate-2]
  (let [predicates (->> (concat (conjunction-clauses predicate-1)
                                (conjunction-clauses predicate-2))
                        (distinct)
                        (sort-by str))]
    (if (= 1 (count predicates))
      (first predicates)
      (apply list 'and predicates))))

;; Rewrite rules.

(defn- promote-selection-cross-join-to-join [z]
  (r/zmatch z
    [:select predicate
     [:cross-join lhs rhs]]
    ;;=>
    (when (columns-in-both-relations? predicate lhs rhs)
      [:join [predicate] lhs rhs])))

(defn- merge-joins-to-mega-join [z]
  (r/zmatch z
    [:join jc r1 r2]
    ;;=>
    [:mega-join jc [r1 r2]]

    [:cross-join r1 r2]
    ;;=>
    [:mega-join [] [r1 r2]]

    [:mega-join jc ^:z rels]
    ;;=>
    (when-let [mega-join (r/find-first (partial r/ctor? :mega-join) rels)]
      (let [[_ inner-jc inner-rels :as inner-mega-join] (r/znode mega-join)
            outer-rels (r/znode rels)]
        [:mega-join
         (vec (concat jc inner-jc))
         (vec
           (concat
             (remove #(= inner-mega-join %) outer-rels)
             inner-rels))]))))

(defn- promote-selection-to-join [z]
  (r/zmatch z
    [:select predicate
     [:join join-condition lhs rhs]]
    ;;=>
    (when (columns-in-both-relations? predicate lhs rhs)
      [:join (conj join-condition predicate) lhs rhs])

    [:select predicate
     [:anti-join join-condition lhs rhs]]
    ;;=>
    (when (columns-in-both-relations? predicate lhs rhs)
      [:anti-join (conj join-condition predicate) lhs rhs])

    [:select predicate
     [:semi-join join-condition lhs rhs]]
    ;;=>
    (when (columns-in-both-relations? predicate lhs rhs)
      [:semi-join (conj join-condition predicate) lhs rhs])))

(defn columns-in-predicate-present-in-relation? [relation predicate]
  (set/superset? (set (relation-columns relation)) (expr-symbols predicate)))

(defn no-correlated-columns? [predicate]
  (empty? (expr-correlated-symbols predicate)))

(defn- push-selection-down-past-apply [z]
  (r/zmatch z
    [:select predicate
     [:apply mode columns independent-relation dependent-relation]]
    ;;=>
    (when (no-correlated-columns? predicate)
      (cond
        (columns-in-predicate-present-in-relation? independent-relation predicate)
        [:apply
         mode
         columns
         [:select predicate independent-relation]
         dependent-relation]

        (and (= :join mode)
             (columns-in-predicate-present-in-relation? dependent-relation predicate))
        [:apply
         mode
         columns
         independent-relation
         [:select predicate dependent-relation]]))))

(defn- push-selection-down-past-rename [push-correlated? z]
  (r/zmatch z
    [:select predicate
     [:rename prefix-or-columns
      relation]]
    ;;=>
    (when (or push-correlated? (no-correlated-columns? predicate))
      (when-let [columns (cond
                           (map? prefix-or-columns) (set/map-invert prefix-or-columns)
                           (symbol? prefix-or-columns) (let [prefix (str prefix-or-columns)]
                                                         (->> (for [c (relation-columns relation)]
                                                                [(symbol prefix (name c)) c])
                                                              (into {}))))]
        [:rename prefix-or-columns
         [:select (w/postwalk-replace columns predicate)
          relation]]))))

(defn rename-map-for-projection-spec [projection-spec]
  (into
    {}
    (filter
      #(and (map? %) (column? (val (first %))))
      projection-spec)))

(defn- predicate-depends-on-calculated-column? [predicate projection-spec]
  (not-empty (set/intersection (set (expr-symbols predicate))
                               (set (keep #(when (and (map? %) (not (column? (val (first %)))))
                                             (key (first %)))
                                          projection-spec)))))

(defn- push-selection-down-past-project [push-correlated? z]
  (r/zmatch z
    [:select predicate
     [:project projection
      relation]]
    ;;=>
    (when (and (or push-correlated? (no-correlated-columns? predicate))
               (not (predicate-depends-on-calculated-column? predicate projection)))
      [:project projection
       [:select (w/postwalk-replace (rename-map-for-projection-spec projection) predicate)
        relation]])

    [:select predicate
     [:map projection
      relation]]
    ;;=>
    (when (and (or push-correlated? (no-correlated-columns? predicate))
               (not (predicate-depends-on-calculated-column? predicate projection)))
      [:map projection
       [:select (w/postwalk-replace (rename-map-for-projection-spec projection) predicate)
        relation]])))

(defn- push-selection-down-past-group-by [push-correlated? z]
  (r/zmatch z
    [:select predicate
     [:group-by group-by-columns
      relation]]
    ;;=>
    (when (and (or push-correlated? (no-correlated-columns? predicate))
               (not (predicate-depends-on-calculated-column? predicate group-by-columns)))
      [:group-by group-by-columns
       [:select predicate
        relation]])))

(defn- push-selection-down-past-join [push-correlated? z]
  (r/zmatch z
    [:select predicate
     [join-op join-map lhs rhs]]
    ;;=>
    (when (and
            (contains?
              #{:join :semi-join :anti-join :left-outer-join :single-join :mark-join}
              join-op)
            (or push-correlated? (no-correlated-columns? predicate)))
      (cond
        (columns-in-predicate-present-in-relation? lhs predicate)
        [join-op join-map [:select predicate lhs] rhs]
        (and (= :join join-op)
             (columns-in-predicate-present-in-relation? rhs predicate))
        [join-op join-map lhs [:select predicate rhs]]))

    [:select predicate
     [:cross-join lhs rhs]]
    ;;=>
    (when (or push-correlated? (no-correlated-columns? predicate))
      (cond
        (columns-in-predicate-present-in-relation? lhs predicate)
        [:cross-join [:select predicate lhs] rhs]
        (columns-in-predicate-present-in-relation? rhs predicate)
        [:cross-join lhs [:select predicate rhs]]))))

(defn- push-selections-with-fewer-variables-down [push-correlated? z]
  (r/zmatch z
    [:select predicate-1
     [:select predicate-2
      relation]]
    ;;=>
    (when (and (or push-correlated? (no-correlated-columns? predicate-1))
               (< (count (expr-symbols predicate-1))
                  (count (expr-symbols predicate-2))))
      [:select predicate-2
       [:select predicate-1
        relation]])))

(defn- remove-superseded-projects [z]
  (r/zmatch z
    [:project projections-1
     [:project projections-2
      relation]]
    ;;=>
    (cond
      (and (every? symbol? projections-1)
           (every? symbol? projections-2)
           (<= (count projections-1) (count projections-2)))
      [:project projections-1 relation]

      (and (every? symbol? projections-1)
           (not (every? symbol? projections-2))
           (= projections-1 (mapv ->projected-column projections-2)))
      [:project projections-2 relation])

    [:project projections
     relation]
    ;;=>
    (when (and (every? symbol? projections)
               (= (set projections) (set (relation-columns relation))))
      relation)))

(defn- merge-selections-around-scan [z]
  (r/zmatch z
    [:select predicate-1
     [:select predicate-2
      [:scan table relation]]]
    ;;=>
    [:select (merge-conjunctions predicate-1 predicate-2) [:scan table relation]]))

(defn- add-selection-to-scan-predicate [z]
  (r/zmatch z
    [:select predicate
     [:scan table columns]]
    ;;=>
    (let [underlying-scan-columns (set (map ->projected-column columns))
          {:keys [scan-columns new-select-predicate]}
          (reduce
            (fn [{:keys [scan-columns new-select-predicate] :as acc} predicate]
              (let [expr-columns-also-in-scan-columns
                    (set/intersection underlying-scan-columns
                                      (set (flatten (if (coll? predicate)
                                                      (seq predicate)
                                                      [predicate]))))]
                (if-let [single-column (when (= 1 (count expr-columns-also-in-scan-columns))
                                         (first expr-columns-also-in-scan-columns))]
                  {:scan-columns
                   (vec (for [column-or-select-expr scan-columns
                              :let [column (->projected-column column-or-select-expr)]]
                          (if (= single-column column)
                            (if (extend-projection? column-or-select-expr)
                              (update column-or-select-expr column (partial merge-conjunctions predicate))
                              {column predicate})
                            column-or-select-expr)))
                   :new-select-predicate new-select-predicate}
                  (update
                    acc
                    :new-select-predicate
                    #(if %
                       (merge-conjunctions % predicate)
                       predicate)))))
            {:scan-columns columns :new-select-predicate nil}
            (conjunction-clauses predicate))]
      (when-not (= columns scan-columns)
        (if new-select-predicate
          [:select new-select-predicate
           [:scan table scan-columns]]
          [:scan table scan-columns])))))

;; Decorrelation rules.

;; http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.563.8492&rep=rep1&type=pdf "Orthogonal Optimization of Subqueries and Aggregation"
;; http://www.cse.iitb.ac.in/infolab/Data/Courses/CS632/2010/Papers/subquery-proc-elhemali-sigmod07.pdf "Execution Strategies for SQL Subqueries"
;; https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2000-31.pdf "Parameterized Queries and Nesting Equivalences"

;; TODO: We'll skip any rule that duplicates the independent relation
;; for now (5-7). The remaining rules are implemented using the
;; current rewrite framework, and attempts to take renames, parameters
;; and moving the right parts of the tree into place to ensure they
;; fire. Things missed will still work, it will just stay as Apply
;; operators, so one can chip away at this making it detect more cases
;; over time.

;; This will require us adding support for ROW_NUMBER() OVER() so we
;; can generate unique rows when translating group by (7-9). OVER is a
;; valid (empty) window specification. This value is 1 based.

;; Rules from 2001 paper:

;; 1.
;; R A⊗ E = R ⊗true E,
;; if no parameters in E resolved from R

;; 2.
;; R A⊗(σp E) = R ⊗p E,
;; if no parameters in E resolved from R

;; 3.
;; R A× (σp E) = σp (R A× E)

;; 4.
;; R A× (πv E) = πv ∪ columns(R) (R A× E)

;; 5.
;; R A× (E1 ∪ E2) = (R A× E1) ∪ (R A× E2)

;; 6.
;; R A× (E1 − E2) = (R A× E1) − (R A× E2)

;; 7.
;; R A× (E1 × E2) = (R A× E1) ⋈R.key (R A× E2)

;; 8.
;; R A× (G A,F E) = G A ∪ columns(R),F (R A× E)

;; 9.
;; R A× (G F1 E) = G columns(R),F' (R A⟕ E)

;; Identities 7 through 9 require that R contain a key R.key.

(defn- pull-correlated-selection-up-towards-apply [z]
  (r/zmatch z
    [:select predicate-1
     [:select predicate-2
      relation]]
    ;;=>
    (when (and (not-empty (expr-correlated-symbols predicate-2))
               (or (empty? (expr-correlated-symbols predicate-1))
                   (and (equals-predicate? predicate-2) ;; TODO remove equals preference
                        (not (equals-predicate? predicate-1)))))
      [:select predicate-2
       [:select predicate-1
        relation]])

    [:project projection
     [:select predicate
      relation]]
    ;;=>
    (when (and (not-empty (expr-correlated-symbols predicate))
               (set/subset?
                (expr-symbols predicate)
                (set (relation-columns [:project projection nil]))))
      [:select predicate
       [:project projection
        relation]])

    [:map projection
     [:select predicate
      relation]]
    ;;=>
    (when (not-empty (expr-correlated-symbols predicate))
      [:select predicate
       [:map projection
        relation]])

    [:cross-join [:select predicate lhs] rhs]
    ;;=>
    (when (not-empty (expr-correlated-symbols predicate))
      [:select predicate [:cross-join lhs rhs]])

    [:cross-join lhs [:select predicate rhs]]
    ;;=>
    (when (not-empty (expr-correlated-symbols predicate))
      [:select predicate [:cross-join lhs rhs]])

    [join-op join-map [:select predicate lhs] rhs]
    ;;=>
    (when (not-empty (expr-correlated-symbols predicate))
      [:select predicate [join-op join-map lhs rhs]])

    [:join join-map lhs [:select predicate rhs]]
    ;;=>
    (when (not-empty (expr-correlated-symbols predicate))
      [:select predicate [:join join-map lhs rhs]])

    [:left-outer-join join-map lhs [:select predicate rhs]]
    ;;=>
    (when (not-empty (expr-correlated-symbols predicate))
      [:select predicate [:left-outer-join join-map lhs rhs]])

    [:group-by group-by-columns
     [:select predicate
      relation]]
    ;;=>
    (when (and (not-empty (expr-correlated-symbols predicate))
               (set/subset?
                (expr-symbols predicate)
                (set (relation-columns [:group-by group-by-columns nil]))))
      [:select predicate
       [:group-by group-by-columns
        relation]])))

(defn- squash-correlated-selects [z]
  (r/zmatch z
    [:select predicate-1
     [:select predicate-2
      relation]]
    ;;=>
    (when (and (seq (expr-correlated-symbols predicate-1))
               (seq (expr-correlated-symbols predicate-2)))
      [:select
       (merge-conjunctions predicate-1 predicate-2)
       relation])))

(defn parameters-referenced-in-relation? [dependent-relation parameters]
  (let [apply-symbols (set parameters)
        found? (atom false)]
    (w/prewalk
      #(if (contains? apply-symbols %)
         (reset! found? true)
         %)
      dependent-relation)
    @found?))

(defn- remove-unused-correlated-columns [columns dependent-relation]
  (->> columns
       (filter (comp (partial parameters-referenced-in-relation? dependent-relation) hash-set val))
       (into {})))

(def ^:dynamic *gensym* gensym)

(defn gen-row-number []
  (symbol (str "xt$" (*gensym* "row_number"))))

(defn- decorrelate-group-by-apply [post-group-by-projection group-by-columns
                                   apply-mode columns independent-relation dependent-relation]
  (let [independent-projection (relation-columns independent-relation)
        smap (set/map-invert columns)
        row-number-sym (gen-row-number)
        columns (remove-unused-correlated-columns columns dependent-relation)
        post-group-by-projection (remove symbol? post-group-by-projection)]
    (cond->> [:group-by (vec (concat independent-projection
                                     [row-number-sym]
                                     (w/postwalk-replace smap group-by-columns)))
              [:apply apply-mode columns
               [:map [{row-number-sym '(row-number)}]
                independent-relation]
               dependent-relation]]
      (not-empty post-group-by-projection)
      (conj [:map (vec (w/postwalk-replace
                        smap
                        post-group-by-projection))]))))

(defn- decorrelate-apply-rule-1
  "R A⊗ E = R ⊗true E
  if no parameters in E resolved from R"
  [z]
  (r/zmatch
    z
    [:apply :cross-join columns independent-relation dependent-relation]
    ;;=>
    (when-not (parameters-referenced-in-relation? dependent-relation (vals columns))
      [:cross-join independent-relation dependent-relation])

    [:apply mode columns independent-relation dependent-relation]
    ;;=>
    (when-not (or (:mark-join mode)
                  (parameters-referenced-in-relation? dependent-relation (vals columns)))
      [mode [] independent-relation dependent-relation])))

(defn- apply-mark-join->mark-join
  "If the only references to apply parameters are within the mark-join expression it should be
   valid to convert the apply mark-join to a mark-join"
  [z]
  (r/zmatch
    z
    [:apply mode columns independent-relation dependent-relation]
    ;;=>
    (when (and (:mark-join mode)
               (not (parameters-referenced-in-relation? dependent-relation (vals columns))))
      [:mark-join
       (w/postwalk-replace (set/map-invert columns) (update-vals (:mark-join mode) vector))
       independent-relation
       dependent-relation])))


(defn- decorrelate-apply-rule-2
  "R A⊗(σp E) = R ⊗p E
  if no parameters in E resolved from R"
  [z]
  (r/zmatch
    z
    [:apply mode columns independent-relation
     [:select predicate dependent-relation]]
    ;;=>
    (when-not (:mark-join mode)
      (when (seq (expr-correlated-symbols predicate))
        (when-not (parameters-referenced-in-relation?
                    dependent-relation
                    (vals columns))
          [(if (= :cross-join mode)
             :join
             mode)
           [(w/postwalk-replace (set/map-invert columns) predicate)]
           independent-relation dependent-relation])))))

(defn- decorrelate-apply-rule-3
  "R A× (σp E) = σp (R A× E)"
  [z]
  (r/zmatch z
    [:apply :cross-join columns independent-relation [:select predicate dependent-relation]]
    ;;=>
    (when (seq (expr-correlated-symbols predicate)) ;; select predicate is correlated
      [:select (w/postwalk-replace (set/map-invert columns) predicate)
       (let [columns (remove-unused-correlated-columns columns dependent-relation)]
         [:apply :cross-join columns independent-relation dependent-relation])])))

(defn- decorrelate-apply-rule-4
  "R A× (πv E) = πv ∪ columns(R) (R A× E)"
  [z]
  (r/zmatch z
    [:apply :cross-join columns independent-relation [:project projection dependent-relation]]
    ;;=>
    [:project (vec (concat (relation-columns independent-relation)
                           (w/postwalk-replace (set/map-invert columns) projection)))
     (let [columns (remove-unused-correlated-columns columns dependent-relation)]
       [:apply :cross-join columns independent-relation dependent-relation])]))

(defn- decorrelate-apply-rule-8
  "R A× (G A,F E) = G A ∪ columns(R),F (R A× E)"
  [z]
  (r/zmatch z
    [:apply :cross-join columns independent-relation
     [:project post-group-by-projection
      [:group-by group-by-columns
       dependent-relation]]]
    ;;=>
    (decorrelate-group-by-apply post-group-by-projection group-by-columns
                                :cross-join columns independent-relation dependent-relation)

    [:apply :cross-join columns independent-relation
     [:group-by group-by-columns
      dependent-relation]]
    ;;=>
    (decorrelate-group-by-apply nil group-by-columns
                                :cross-join columns independent-relation dependent-relation)))

(defn- contains-invalid-rule-9-agg-fns?
  "Rule 9 requires that agg-fn(null) = agg-fn(empty-rel) this isn't true for these fns"
  [group-by-specs]
  (let [found? (volatile! false)]
    (w/prewalk
     #(if (and (= 'array-agg %)
               (not (column? %)))
        (vreset! found? true)
        %)
     group-by-specs)
    @found?))

(defn- decorrelate-apply-rule-9
  "R A× (G F1 E) = G columns(R),F' (R A⟕ E)"
  [z]
  (r/zmatch z
    [:apply :single-join columns
     independent-relation
     [:project post-group-by-projection
      [:group-by group-by-columns
       dependent-relation]]]
    ;;=>
    (when-not (contains-invalid-rule-9-agg-fns? group-by-columns)
      (decorrelate-group-by-apply post-group-by-projection group-by-columns
                                  :left-outer-join columns independent-relation dependent-relation))

    [:apply :single-join columns
     independent-relation
     [:group-by group-by-columns
      dependent-relation]]
    ;;=>
    (when-not (contains-invalid-rule-9-agg-fns? group-by-columns)
      (decorrelate-group-by-apply nil group-by-columns
                                  :left-outer-join columns independent-relation dependent-relation))))

(defn- as-equi-condition [expr lhs rhs]
  (when (equals-predicate? expr)
    (let [[_ expr1 expr2] expr
          expr-side
          #(cond (all-columns-in-relation? % lhs) :lhs
                 (all-columns-in-relation? % rhs) :rhs)
          expr1-side (expr-side expr1)
          expr2-side (expr-side expr2)]
      (when (and expr1-side
                 expr2-side
                 (not= expr1-side expr2-side))
        (case expr1-side
          :lhs {expr1 expr2}
          :rhs {expr2 expr1})))))

(defn- optimize-join-expression [join-expressions lhs rhs]
  (if (some map? join-expressions)
    join-expressions
    (->> join-expressions
         (mapcat #(flatten-expr and-predicate? %))
         (mapv (fn [join-clause] (or (as-equi-condition join-clause lhs rhs) join-clause))))))

(defn- rewrite-equals-predicates-in-join-as-equi-join-map [z]
  (r/zcase z
    (:join :semi-join :anti-join :left-outer-join :single-join :mark-join)
    (r/zmatch
      z
      [:mark-join projection lhs rhs]
      (let [projected-col (first (keys projection))
            join-expressions (first (vals projection))
            new-join-expressions (optimize-join-expression join-expressions lhs rhs)]
        (when (not= new-join-expressions join-expressions)
          [:mark-join {projected-col new-join-expressions} lhs rhs]))

      [join-type join-expressions lhs rhs]
      (let [new-join-expressions (optimize-join-expression join-expressions lhs rhs)]
        (when (not= new-join-expressions join-expressions)
          [join-type new-join-expressions lhs rhs])))

    nil))

(defn remove-redundant-projects [z]
  ;; assumes you wont ever have a project like [] whos job is to return an empty rel
  (r/zmatch z
    [:apply :semi-join c i
     [:project projection dependent-relation]]
    ;;=>
    (when (every? symbol? projection)
      [:apply :semi-join c i dependent-relation])

    [:apply :anti-join c i
     [:project projection dependent-relation]]
    ;;=>
    (when (every? symbol? projection)
      [:apply :anti-join c i dependent-relation])

    [:semi-join jc i
     [:project projection dependent-relation]]
    ;;=>
    (when (every? symbol? projection)
      [:semi-join jc i dependent-relation])

    [:anti-join jc i
     [:project projection dependent-relation]]
     ;;=>
     (when (every? symbol? projection)
       [:anti-join jc i dependent-relation])

    [:group-by c
     [:project projection dependent-relation]]
     ;;=>
     (when (every? symbol? projection)
       [:group-by c dependent-relation])))

(defn push-semi-and-anti-joins-down [z]
  (r/zmatch
    z
    [:semi-join join-condition
     [:cross-join inner-lhs inner-rhs]
     rhs]
    ;;=>
    (cond (all-columns-across-both-relations-with-one-in-each? join-condition inner-lhs rhs)
          [:cross-join
           [:semi-join join-condition inner-lhs rhs]
           inner-rhs]

          (all-columns-across-both-relations-with-one-in-each? join-condition inner-rhs rhs)
          [:cross-join
           inner-lhs
           [:semi-join join-condition inner-rhs rhs]])

    [:anti-join join-condition
     [:cross-join inner-lhs inner-rhs]
     rhs]
    ;;=>
    (cond (all-columns-across-both-relations-with-one-in-each? join-condition inner-lhs rhs)
          [:cross-join
           [:anti-join join-condition inner-lhs rhs]
           inner-rhs]

          (all-columns-across-both-relations-with-one-in-each? join-condition inner-rhs rhs)
          [:cross-join
           inner-lhs
           [:anti-join join-condition inner-rhs rhs]])

    [:semi-join join-condition
     [:join inner-join-condition inner-lhs inner-rhs]
     rhs]
    ;;=>
    (cond (all-columns-across-both-relations-with-one-in-each? join-condition inner-lhs rhs)
          [:join inner-join-condition
           [:semi-join join-condition inner-lhs rhs]
           inner-rhs]

          (all-columns-across-both-relations-with-one-in-each? join-condition inner-rhs rhs)
          [:join inner-join-condition
           inner-lhs
           [:semi-join join-condition inner-rhs rhs]])

    [:anti-join join-condition
     [:join inner-join-condition inner-lhs inner-rhs]
     rhs]
    ;;=>
    (cond (all-columns-across-both-relations-with-one-in-each? join-condition inner-lhs rhs)
          [:join inner-join-condition
           [:anti-join join-condition inner-lhs rhs]
           inner-rhs]

          (all-columns-across-both-relations-with-one-in-each? join-condition inner-rhs rhs)
          [:join inner-join-condition
           inner-lhs
           [:anti-join join-condition inner-rhs rhs]])))

(defn- promote-selection-to-mega-join [z]
  (r/zmatch
    z
    [:select predicate
     [:mega-join join-condition rels]]
    ;;=>
    (when (columns-in-predicate-present-in-relation? [:mega-join join-condition rels] predicate)
      [:mega-join (conj join-condition predicate) rels])))

(defn- split-conjunctions-in-mega-join [z]
  (r/zmatch
    z
    [:mega-join join-condition rels]
    ;;=>
    (when (some and-predicate? join-condition)
      [:mega-join (vec (mapcat #(flatten-expr and-predicate? %) join-condition)) rels])))

(defn- push-predicates-from-mega-join-to-child-relations [z]
  (r/zmatch
    z
    [:mega-join join-condition rels]
    ;;=>
    (let [indexed-rels (map-indexed (fn [idx rel] {:idx idx
                                                   :rel rel
                                                   :preds []}) rels)
          indexed-preds (map-indexed (fn [idx pred] {:idx idx
                                                     :pred pred}) join-condition)]
      (when-let [moveable-preds
                 (seq
                   (for [{:keys [pred] :as predicate} indexed-preds
                         {:keys [rel] :as relation} indexed-rels
                         :when (columns-in-predicate-present-in-relation? rel pred)]
                     (update relation :preds conj predicate)))]
        (let [updated-rels (-> (group-by :idx moveable-preds)
                               (update-vals (fn [updates-for-rel]
                                              (reduce
                                                (fn [rel pred]
                                                  [:select (:pred pred)
                                                   rel])
                                                (:rel (first updates-for-rel))
                                                (mapcat :preds updates-for-rel)))))
              pushed-down-preds (set (keys (group-by :idx (mapcat :preds moveable-preds))))

              output-join-conditions (mapv :pred (remove #(pushed-down-preds (:idx %)) indexed-preds))
              output-rels (mapv #(if-let [updated-rel (get updated-rels (:idx %))]
                                   updated-rel
                                   (:rel %)) indexed-rels)]
          [:mega-join output-join-conditions output-rels])))))

(def ^:private push-correlated-selection-down-past-join (partial push-selection-down-past-join true))
(def ^:private push-correlated-selection-down-past-rename (partial push-selection-down-past-rename true))
(def ^:private push-correlated-selection-down-past-project (partial push-selection-down-past-project true))
(def ^:private push-correlated-selection-down-past-group-by (partial push-selection-down-past-group-by true))
(def ^:private push-correlated-selections-with-fewer-variables-down (partial push-selections-with-fewer-variables-down true))

(def ^:private push-decorrelated-selection-down-past-join (partial push-selection-down-past-join false))
(def ^:private push-decorrelated-selection-down-past-rename (partial push-selection-down-past-rename false))
(def ^:private push-decorrelated-selection-down-past-project (partial push-selection-down-past-project false))
(def ^:private push-decorrelated-selection-down-past-group-by (partial push-selection-down-past-group-by false))
(def ^:private push-decorrelated-selections-with-fewer-variables-down (partial push-selections-with-fewer-variables-down false))

;; Logical plan API

(def ^:private optimise-plan-rules
  [#'promote-selection-cross-join-to-join
   #'promote-selection-to-join
   #'promote-selection-to-mega-join
   #'split-conjunctions-in-mega-join
   #'push-predicates-from-mega-join-to-child-relations
   #'push-selection-down-past-apply
   #'push-correlated-selection-down-past-join
   #'push-correlated-selection-down-past-rename
   #'push-correlated-selection-down-past-project
   #'push-correlated-selection-down-past-group-by
   #'push-correlated-selections-with-fewer-variables-down
   #'remove-superseded-projects
   #'merge-selections-around-scan
   #'push-semi-and-anti-joins-down
   #'add-selection-to-scan-predicate])

(def ^:private decorrelate-plan-rules
  [#'pull-correlated-selection-up-towards-apply
   #'remove-redundant-projects
   #'push-selection-down-past-apply
   #'push-decorrelated-selection-down-past-join
   #'push-decorrelated-selection-down-past-rename
   #'push-decorrelated-selection-down-past-project
   #'push-decorrelated-selection-down-past-group-by
   #'push-decorrelated-selections-with-fewer-variables-down
   #'squash-correlated-selects
   #'decorrelate-apply-rule-1
   #'apply-mark-join->mark-join
   #'decorrelate-apply-rule-2
   #'decorrelate-apply-rule-3
   #'decorrelate-apply-rule-4
   #'decorrelate-apply-rule-8
   #'decorrelate-apply-rule-9])

(defn rewrite-plan
  ([plan] (rewrite-plan plan {}))

  ([plan {:keys [decorrelate? instrument-rules?], :or {decorrelate? true, instrument-rules? false}, :as opts}]
   (let [!fired-rules (atom [])]
     (binding [*gensym* (util/seeded-gensym "_" 0)]
       (letfn [(instrument-rule [f]
                 (fn [z]
                   (when-let [successful-rewrite (f z)]
                     (swap! !fired-rules conj
                            [(name (.toSymbol ^Var f))
                             (r/znode z)
                             successful-rewrite
                             "=================="])
                     successful-rewrite)))
               (instrument-rules [rules]
                 (->> rules
                      (mapv (if instrument-rules? instrument-rule deref))
                      (apply some-fn)))]
         (-> (->> plan
                  (r/vector-zip)
                  (#(if decorrelate?
                      (r/innermost (r/mono-tp (instrument-rules decorrelate-plan-rules)) %)
                      %))
                  (r/innermost (r/mono-tp (instrument-rules optimise-plan-rules)))
                  (r/topdown (r/adhoc-tp r/id-tp (instrument-rules [#'rewrite-equals-predicates-in-join-as-equi-join-map])))
                  (r/innermost (r/mono-tp (instrument-rules [#'merge-joins-to-mega-join])))
                  (r/node))))))))

(defn validate-plan [plan]
  (when-not (s/valid? ::logical-plan plan)
    (throw (err/illegal-arg ::invalid-plan
                            {::err/message (s/explain-str ::logical-plan plan)
                             :plan plan
                             :explain-data (s/explain-data ::logical-plan plan)}))))


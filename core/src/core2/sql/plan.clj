(ns core2.sql.plan
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as w]
            [clojure.zip :as z]
            [clojure.spec.alpha :as s]
            [core2.logical-plan :as lp]
            [core2.rewrite :as r]
            [core2.sql.analyze :as sem]
            [instaparse.core :as insta])
  (:import clojure.lang.IObj))

;; Attribute grammar for transformation into logical plan.

;; See https://cs.ulb.ac.be/public/_media/teaching/infoh417/sql2alg_eng.pdf

(def ^:private ^:const ^String relation-id-delimiter "__")
(def ^:private ^:const ^String relation-prefix-delimiter "_")

(declare expr)

(defn- maybe-add-ref [z x]
  (if (instance? IObj x)
    (vary-meta x assoc :ref z)
    x))

(defn- id-symbol [table table-id column]
  (with-meta
    (symbol (str table relation-id-delimiter table-id relation-prefix-delimiter column))
    {:column-reference {:table-id table-id
                        :correlation-name table
                        :column column}}))

(defn- unqualified-id-symbol
  ([column] (unqualified-id-symbol nil column))
  ([table-id column]
   (with-meta column
     {:column-reference (cond-> {:column column}
                          table-id (assoc :table-id table-id))})))

(defn unqualified-projection-symbol [{:keys [identifier ^long index] :as projection}]
  (let [{:keys [table]} (meta projection)
        column (symbol (or identifier (str "$column_" (inc index) "$")))]
    (unqualified-id-symbol (:id table) column)))

(defn qualified-projection-symbol [{:keys [qualified-column original-index] :as projection}]
  (let [{derived-column :ref table :table} (meta projection)]
    (if derived-column
      (expr (r/$ derived-column 1))
      (id-symbol (first qualified-column)
                 (:id table)
                 (unqualified-projection-symbol
                  (cond-> projection
                    original-index (assoc :index original-index)))))))

(defn- column-reference-symbol [{:keys [table-id identifiers] :as column-reference}]
  (let [[table column] identifiers]
    (id-symbol table table-id column)))

(defn- table-reference-symbol [correlation-name id]
  (with-meta
    (symbol (str correlation-name relation-id-delimiter id))
    {:table-reference {:table-id id
                       :correlation-name correlation-name}}))

(defn- aggregate-symbol [prefix z]
  (let [query-id (sem/id (sem/scope-element z))]
    (unqualified-id-symbol query-id
     (symbol (str "$" prefix relation-id-delimiter query-id relation-prefix-delimiter (sem/id z) "$")))))

(defn- exists-symbol [qe]
  (vary-meta (id-symbol "subquery" (sem/id qe) "$exists$") assoc :exists? true))

(defn- subquery-reference-symbol [qe]
  (table-reference-symbol "subquery" (sem/id qe)))

(defn- subquery-projection-symbols [qe]
  (let [subquery-id (sem/id qe)]
    (vec (for [projection (first (sem/projected-columns qe))]
           (id-symbol "subquery" subquery-id (unqualified-projection-symbol projection))))))

;; Expressions.

(defn expr [z]
  (maybe-add-ref
   z
   (r/zmatch z
     [:column_reference _]
     (column-reference-symbol (sem/column-reference z))

     [:boolean_value_expression ^:z bve _ ^:z bt]
     ;;=>
     (list 'or (expr bve) (expr bt))

     [:boolean_term ^:z bt _ ^:z bf]
     ;;=>
     (list 'and (expr bt) (expr bf))

     [:boolean_factor _ ^:z bt]
     ;;=>
     (list 'not (expr bt))

     [:boolean_test ^:z bp]
     (expr bp)

     [:comparison_predicate ^:z rvp-1 [:comparison_predicate_part_2 [_ co] ^:z rvp-2]]
     ;;=>
     (list (symbol co) (expr rvp-1) (expr rvp-2))

     [:numeric_value_expression ^:z nve [_ op] ^:z t]
     (list (symbol op) (expr nve) (expr t))

     [:term ^:z t [_ op] ^:z f]
     (list (symbol op) (expr t) (expr f))

     [:signed_numeric_literal ^:z unl]
     (expr unl)

     [:exact_numeric_literal ^:z ui]
     (expr ui)

     [:unsigned_integer lexeme]
     ;;=>
     (Long/parseLong lexeme)

     [:character_string_literal lexeme]
     (subs lexeme 1 (dec (count lexeme)))

     [:named_columns_join _ _]
     (reduce
      (fn [acc expr]
        (list 'and acc expr))
      (let [{:keys [join-columns] :as env} (sem/named-columns-join-env z)]
        (for [column join-columns]
          (->> (for [side [:lhs :rhs]]
                 (qualified-projection-symbol (first (get-in env [side column]))))
               (apply list '=)))))

     [:aggregate_function _]
     (aggregate-symbol "agg_out" z)

     [:aggregate_function "COUNT" [:asterisk "*"]]
     (aggregate-symbol "agg_out" z)

     [:subquery ^:z qe]
     (let [subquery-type (sem/subquery-type z)]
       (case (:type subquery-type)
         :scalar_subquery (first (subquery-projection-symbols qe))))

     [:exists_predicate _
      [:subquery ^:z qe]]
     (exists-symbol qe)

     [:in_predicate _ [:in_predicate_part_2 _ [:in_predicate_value [:subquery ^:z qe]]]]
     (exists-symbol qe)

     [:in_predicate _ [:in_predicate_part_2 "NOT" _ [:in_predicate_value [:subquery ^:z qe]]]]
     (list 'not (exists-symbol qe))

     [:quantified_comparison_predicate _ [:quantified_comparison_predicate_part_2 _ [:some _] [:subquery ^:z qe]]]
     (exists-symbol qe)

     [:quantified_comparison_predicate _ [:quantified_comparison_predicate_part_2 _ [:all _] [:subquery ^:z qe]]]
     (list 'not (exists-symbol qe))

     (throw (IllegalArgumentException. (str "Cannot build expression for: "  (pr-str (z/node z))))))))

;; Logical plan.

(declare plan)

(defn- correlated-column->param [qe scope-id]
  (->> (for [{:keys [^long table-scope-id] :as column-reference} (sem/all-column-references qe)
             :when (<= table-scope-id scope-id)
             :let [column-reference-symbol (column-reference-symbol column-reference)
                   param-symbol (with-meta
                                  (symbol (str "?" column-reference-symbol))
                                  {:correlated-column-reference
                                   (:column-reference (meta column-reference-symbol))})]]
         [(if (= table-scope-id scope-id)
            column-reference-symbol
            param-symbol)
          param-symbol])
       (into {})))

(defn- build-apply [column->param projected-dependent-columns independent-relation dependent-relation]
  (if (empty? column->param)
    [:cross-join independent-relation dependent-relation]
    [:apply
     :cross-join
     column->param
     projected-dependent-columns
     independent-relation
     (w/postwalk-replace column->param dependent-relation)]))

(defn- wrap-with-exists [exists-column relation]
  [:top {:limit 1}
   [:union-all
    [:project [{exists-column true}]
     relation]
    [:table [{(keyword exists-column) false}]]]])

(defn- flip-comparison [co]
  (case co
    < '>=
    <= '>
    > '<=
    >= '<
    = '<>
    <> '=))

;; TODO: deal with row subqueries.
(defn- wrap-with-apply [z relation]
  (let [subqueries (r/collect-stop
                    (fn [z]
                      (r/zcase z
                        (:subquery
                         :exists_predicate
                         :in_predicate
                         :quantified_comparison_predicate) [z]
                        nil))
                    z)
        scope-id (sem/id (sem/scope-element z))]
    (reduce
     (fn [relation sq]
       (r/zmatch sq
         [:subquery ^:z qe]
         (let [subquery-plan [:rename (subquery-reference-symbol qe) (plan qe)]
               column->param (correlated-column->param qe scope-id)
               subquery-type (sem/subquery-type sq)
               projected-columns (set (subquery-projection-symbols qe))]
           (build-apply
            column->param
            projected-columns
            relation
            (case (:type subquery-type)
              (:scalar_subquery
               :row_subquery) [:max-1-row subquery-plan]
              subquery-plan)))
         [:exists_predicate _
          [:subquery ^:z qe]]
         (let [exists-symbol (exists-symbol qe)
               subquery-plan (wrap-with-exists
                              exists-symbol
                              [:rename (subquery-reference-symbol qe) (plan qe)])
               column->param (correlated-column->param qe scope-id)
               projected-columns #{exists-symbol}]
           (build-apply
            column->param
            projected-columns
            relation
            subquery-plan))

         [:in_predicate ^:z rvp ^:z ipp2]
         (let [qe (r/zmatch ipp2
                    [:in_predicate_part_2 _ [:in_predicate_value [:subquery ^:z qe]]]
                    qe

                    [:in_predicate_part_2 "NOT" _ [:in_predicate_value [:subquery ^:z qe]]]
                    qe

                    (throw (IllegalArgumentException. "unknown in type")))
               qe-id (sem/id qe)
               exists-symbol (exists-symbol qe)
               predicate (list '= (expr rvp) (first (subquery-projection-symbols qe)))
               subquery-plan (wrap-with-exists
                              exists-symbol
                              [:select predicate
                               [:rename (subquery-reference-symbol qe) (plan qe)]])
               column->param (merge (correlated-column->param qe scope-id)
                                    (correlated-column->param rvp scope-id))
               projected-columns #{exists-symbol}]
           (build-apply
            column->param
            projected-columns
            relation
            subquery-plan))

         [:quantified_comparison_predicate ^:z rvp [:quantified_comparison_predicate_part_2 [_ co] [quantifier _] [:subquery ^:z qe]]]
         (let [exists-symbol (exists-symbol qe)
               projection-symbol (first (subquery-projection-symbols qe))
               predicate (case quantifier
                           :all `(~'or (~(flip-comparison (symbol co))
                                        ~(expr rvp)
                                        ~projection-symbol)
                                  (~'nil? ~(expr rvp))
                                  (~'nil? ~projection-symbol))
                           :some (list (symbol co) (expr rvp) projection-symbol))
               subquery-plan (wrap-with-exists
                              exists-symbol
                              [:select predicate
                               [:rename (subquery-reference-symbol qe) (plan qe)]])
               column->param (merge (correlated-column->param qe scope-id)
                                    (correlated-column->param rvp scope-id))
               projected-columns #{exists-symbol}]
           (build-apply
            column->param
            projected-columns
            relation
            subquery-plan))

         (throw (IllegalArgumentException. "unknown subquery type"))))
     relation
     subqueries)))

(defn- and-predicate? [predicate]
  (and (sequential? predicate)
       (= 'and (first predicate))))

(defn- wrap-with-select [sc relation]
  (let [sc-expr (expr sc)]
    (reduce
     (fn [acc predicate]
       [:select predicate acc])
     (wrap-with-apply sc relation)
     ((fn step [sc-expr]
        (if (and-predicate? sc-expr)
          (concat (step (nth sc-expr 1))
                  (step (nth sc-expr 2)))
          [sc-expr]))
      sc-expr))))

(defn- needs-group-by? [z]
  (boolean (:grouping-columns (sem/local-env (sem/group-env z)))))

(defn- wrap-with-group-by [te relation]
  (let [projection (first (sem/projected-columns te))
        {:keys [grouping-columns]} (sem/local-env (sem/group-env te))
        grouping-columns (set grouping-columns)
        grouping-columns (vec (for [{:keys [qualified-column] :as projection} projection
                                    :when (contains? grouping-columns qualified-column)
                                    :let [derived-column (:ref (meta projection))]]
                                (expr (r/$ derived-column 1))))
        aggregates (r/collect-stop
                    (fn [z]
                      (r/zcase z
                        :aggregate_function [z]
                        :subquery []
                        nil))
                    (sem/scope-element te))]
    [:group-by (->> (for [aggregate aggregates]
                      (r/zmatch aggregate
                        [:aggregate_function [:general_set_function [:computational_operation sf] _]]
                        {(aggregate-symbol "agg_out" aggregate)
                         (list (symbol (str/lower-case sf)) (aggregate-symbol "agg_in" aggregate))}

                        [:aggregate_function "COUNT" [:asterisk "*"]]
                        {(aggregate-symbol "agg_out" aggregate)
                         (list 'count (aggregate-symbol "agg_in" aggregate))}))
                    (into grouping-columns))
     [:project (->> (for [aggregate aggregates]
                      (r/zmatch aggregate
                        [:aggregate_function [:general_set_function _ ^:z ve]]
                        {(aggregate-symbol "agg_in" aggregate) (expr ve)}

                        [:aggregate_function "COUNT" [:asterisk "*"]]
                        {(aggregate-symbol "agg_in" aggregate) 1}))
                    (into grouping-columns))
      relation]]))

(declare expr-symbols)

(defn- wrap-with-order-by [ssl relation]
  (let [projection (first (sem/projected-columns ssl))
        query-id (sem/id (sem/scope-element ssl))
        order-by-specs (r/collect-stop
                        (fn [z]
                          (r/zcase z
                            :sort_specification
                            (let [direction (case (sem/ordering-specification z)
                                              "ASC" :asc
                                              "DESC" :desc
                                              :asc)]
                              [(if-let [idx (sem/order-by-index z)]
                                 {:spec {(unqualified-projection-symbol (nth projection idx)) direction}}
                                 (let [column (unqualified-id-symbol query-id (symbol (str "$order_by__" query-id  "_" (r/child-idx z) "$")))]
                                   {:spec {column direction}
                                    :projection {column (expr (r/$ z 1))}}))])

                            :subquery
                            []

                            nil))
                        ssl)
        order-by-projection (keep :projection order-by-specs)
        extra-projection (distinct (mapcat (comp expr-symbols vals) order-by-projection))
        base-projection (mapv unqualified-projection-symbol projection)
        relation (if (not-empty extra-projection)
                   (->> (z/vector-zip relation)
                        (r/once-td-tp
                         (r/mono-tp
                          (fn [z]
                            (r/zmatch z
                              [:project projection relation]
                              ;;=>
                              [:project (vec (concat projection extra-projection)) relation]))))
                        (z/node))
                   relation)
        order-by [:order-by (mapv :spec order-by-specs)
                  (if (not-empty order-by-projection)
                    [:project (vec (concat base-projection order-by-projection)) relation]
                    relation)]]
    (if (not-empty order-by-projection)
      [:project base-projection order-by]
      order-by)))

(defn- build-query-specification [sl te]
  (let [projection (first (sem/projected-columns sl))
        unqualified-rename-map (->> (for [{:keys [qualified-column] :as projection} projection
                                          :when qualified-column
                                          :let [column (qualified-projection-symbol projection)]]
                                      [column (unqualified-projection-symbol projection)])
                                    (into {}))
        qualified-projection (vec (for [{:keys [qualified-column] :as projection} projection
                                        :let [derived-column (:ref (meta projection))]]
                                    (if qualified-column
                                      (qualified-projection-symbol projection)
                                      {(unqualified-projection-symbol projection)
                                       (expr (r/$ derived-column 1))})))
        relation (wrap-with-apply sl (plan te))
        qualified-project [:project qualified-projection relation]]
    (if (not-empty unqualified-rename-map)
      [:rename unqualified-rename-map qualified-project]
      qualified-project)))

(defn- build-set-op [set-op lhs rhs]
  (let [lhs-unqualified-project (mapv unqualified-projection-symbol (first (sem/projected-columns lhs)))
        rhs-unqualified-project (mapv unqualified-projection-symbol (first (sem/projected-columns rhs)))]
    [set-op (plan lhs)
     (if (= lhs-unqualified-project rhs-unqualified-project)
       (plan rhs)
       [:rename (zipmap rhs-unqualified-project lhs-unqualified-project)
        (plan rhs)])]))

;; TODO: Probably better if the operator explicitly takes the name of the
;; source column, the destination column and an optional ordinal
;; column instead of this workaround.
(defn- build-collection-derived-table [tp with-ordinality?]
  (let [{:keys [id correlation-name] :as table} (sem/table tp)
        [unwind-column ordinality-column] (map qualified-projection-symbol (first (sem/projected-columns tp)))
        cdt (r/$ tp 1)
        qualified-projection (vec (for [table (vals (sem/local-env-singleton-values (sem/env cdt)))
                                        :let [{:keys [ref]} (meta table)]
                                        projection (first (sem/projected-columns ref))
                                        :let [column (qualified-projection-symbol projection)]
                                        :when (not= ordinality-column column)]
                                    (if (= unwind-column column)
                                      {unwind-column (expr (r/$ cdt 2))}
                                      column)))
        unwind-relation [:unwind
                         (with-meta
                           unwind-column
                           {:table-reference {:table-id id
                                              :correlation-name correlation-name}})
                         {:with-ordinality? with-ordinality?}
                         [:project qualified-projection nil]]]
    (if ordinality-column
      [:rename {'_ordinal ordinality-column} unwind-relation]
      unwind-relation)))

(defn- build-table-primary [tp]
  (let [{:keys [id correlation-name] :as table} (sem/table tp)
        projection (first (sem/projected-columns tp))]
    [:rename (table-reference-symbol correlation-name id)
     (if-let [subquery-ref (:subquery-ref (meta table))]
       (if-let [derived-columns (sem/derived-columns tp)]
         [:rename (zipmap (map unqualified-projection-symbol (first (sem/projected-columns subquery-ref)))
                          (map symbol derived-columns))
          (plan subquery-ref)]
         (plan subquery-ref))
       [:scan (vec (for [{:keys [identifier]} projection]
                     (symbol identifier)))])]))

(defn- build-lateral-derived-table [tp qe]
  (let [scope-id (sem/id (sem/scope-element tp))
        column->param (correlated-column->param qe scope-id)
        projected-columns (set (map qualified-projection-symbol (first (sem/projected-columns tp))))
        relation (build-table-primary tp)]
    (if (every? true? (map = (keys column->param) (vals column->param)))
      relation
      (build-apply column->param projected-columns nil relation))))

;; TODO: both UNNEST and LATERAL are only dealt with on top-level in
;; FROM. UNNEST also needs to take potential subqueries in cve into
;; account.
(defn- build-table-reference-list [trl]
  (reduce
   (fn [acc table]
     (r/zmatch table
       [:unwind cve {:with-ordinality? false} [:project projection nil]]
       ;;=>
       [:unwind cve {:with-ordinality? false} [:project projection acc]]

       [:rename columns [:unwind cve {:with-ordinality? true} [:project projection nil]]]
       ;;=>
       [:rename columns [:unwind cve {:with-ordinality? true} [:project projection acc]]]


       [:apply :cross-join columns dependent-column-names nil dependent-relation]
       ;;=>
       [:apply :cross-join columns dependent-column-names acc dependent-relation]

       [:cross-join acc table]))
   (r/collect-stop
    (fn [z]
      (r/zcase z
        (:table_primary
         :qualified_join) [(plan z)]
        :subquery []
        nil))
    trl)))

(defn- plan [z]
  (maybe-add-ref
   z
   (r/zmatch z
     [:directly_executable_statement ^:z dsds]
     (plan dsds)

     [:query_expression ^:z qeb]
     (plan qeb)

     [:query_expression ^:z qeb [:order_by_clause _ _ ^:z ssl]]
     (wrap-with-order-by ssl (plan qeb))

     [:query_expression ^:z qeb [:result_offset_clause _ rorc _]]
     [:top {:skip (expr rorc)} (plan qeb)]

     [:query_expression ^:z qeb [:order_by_clause _ _ ^:z ssl] [:result_offset_clause _ rorc _]]
     [:top {:skip (expr rorc)} (wrap-with-order-by ssl (plan qeb))]

     [:query_expression ^:z qeb [:fetch_first_clause _ _ ffrc _ _]]
     [:top {:limit (expr ffrc)} (plan qeb)]

     [:query_expression ^:z qeb [:order_by_clause _ _ ^:z ssl] [:fetch_first_clause _ _ ffrc _ _]]
     [:top {:limit (expr ffrc)} (wrap-with-order-by ssl (plan qeb))]

     [:query_expression ^:z qeb [:result_offset_clause _ rorc _] [:fetch_first_clause _ _ ffrc _ _]]
     [:top {:skip (expr rorc) :limit (expr ffrc)} (plan qeb)]

     [:query_expression ^:z qeb [:order_by_clause _ _ ^:z ssl] [:result_offset_clause _ rorc _] [:fetch_first_clause _ _ ffrc _ _]]
     [:top {:skip (expr rorc) :limit (expr ffrc)} (wrap-with-order-by ssl (plan qeb))]

     [:query_specification _ ^:z sl ^:z te]
     ;;=>
     (build-query-specification sl te)

     [:query_specification _ [:set_quantifier "ALL"] ^:z sl ^:z te]
     ;;=>
     (build-query-specification sl te)

     [:query_specification _ [:set_quantifier "DISTINCT"] ^:z sl ^:z te]
     ;;=>
     [:distinct (build-query-specification sl te)]

     [:query_expression_body ^:z qeb "UNION" ^:z qt]
     [:distinct (build-set-op :union-all qeb qt)]

     [:query_expression_body ^:z qeb "UNION" "ALL" ^:z qt]
     (build-set-op :union-all qeb qt)

     [:query_expression_body ^:z qeb "EXCEPT" ^:z qt]
     (build-set-op :difference qeb qt)

     [:query_term ^:z qt "INTERSECT" ^:z qp]
     (build-set-op :intersect qt qp)

     [:table_expression ^:z fc]
     ;;=>
     (cond->> (plan fc)
       (needs-group-by? z) (wrap-with-group-by z))

     [:table_expression ^:z fc [:group_by_clause _ _ _]]
     ;;=>
     (wrap-with-group-by z (plan fc))

     [:table_expression ^:z fc [:where_clause _ ^:z sc]]
     ;;=>
     (cond->> (wrap-with-select sc (plan fc))
       (needs-group-by? z) (wrap-with-group-by z))

     [:table_expression ^:z fc [:where_clause _ ^:z sc] [:group_by_clause _ _ _]]
     ;;=>
     (->> (wrap-with-select sc (plan fc))
          (wrap-with-group-by z))

     [:table_expression ^:z fc [:where_clause _ ^:z sc] [:group_by_clause _ _ _] [:having_clause _ ^:z hsc]]
     ;;=>
     (->> (wrap-with-select sc (plan fc))
          (wrap-with-group-by z)
          (wrap-with-select hsc))

     [:table_expression ^:z fc [:where_clause _ ^:z sc] [:having_clause _ ^:z hsc]]
     ;;=>
     (->> (wrap-with-select sc (plan fc))
          (wrap-with-group-by z)
          (wrap-with-select hsc))

     [:table_expression ^:z fc [:group_by_clause _ _ _] [:having_clause _ ^:z hsc]]
     ;;=>
     (->> (wrap-with-group-by z (plan fc))
          (wrap-with-select hsc))

     [:table_expression ^:z fc [:having_clause _ ^:z hsc]]
     ;;=>
     (->> (wrap-with-group-by z (plan fc))
          (wrap-with-select hsc))

     [:table_primary [:collection_derived_table _ _] _ _]
     ;;=>
     (build-collection-derived-table z false)

     [:table_primary [:collection_derived_table _ _] _ _ _]
     (build-collection-derived-table z false)

     [:table_primary [:collection_derived_table _ _ "WITH" "ORDINALITY"] _ _]
     ;;=>
     (build-collection-derived-table z true)

     [:table_primary [:collection_derived_table _ _ "WITH" "ORDINALITY"] _ _ _]
     (build-collection-derived-table z true)

     [:table_primary [:lateral_derived_table _ [:subquery ^:z qe]] _ _]
     (build-lateral-derived-table z qe)

     [:table_primary _]
     ;;=>
     (build-table-primary z)

     [:table_primary _ _]
     ;;=>
     (build-table-primary z)

     [:table_primary _ _ _]
     ;;=>
     (build-table-primary z)

     [:table_primary _ _ _ _]
     ;;=>
     (build-table-primary z)

     [:qualified_join ^:z lhs _ ^:z rhs [:join_condition _ ^:z sc]]
     ;;=>
     (wrap-with-select sc [:join {} (plan lhs) (plan rhs)])

     [:qualified_join ^:z lhs ^:z jt _ ^:z rhs [:join_condition _ ^:z sc]]
     ;;=>
     (wrap-with-select sc (case (sem/join-type jt)
                            "LEFT" [:left-outer-join {} (plan lhs) (plan rhs)]
                            "RIGHT" [:left-outer-join {} (plan rhs) (plan lhs)]
                            "INNER" [:join {} (plan lhs) (plan rhs)]))

     [:qualified_join ^:z lhs _ ^:z rhs ^:z ncj]
     ;;=>
     (wrap-with-select ncj [:join {} (plan lhs) (plan rhs)])

     [:qualified_join ^:z lhs ^:z jt _ ^:z rhs ^:z ncj]
     ;;=>
     (wrap-with-select ncj (case (sem/join-type jt)
                             "LEFT" [:left-outer-join {} (plan lhs) (plan rhs)]
                             "RIGHT" [:left-outer-join {} (plan rhs) (plan lhs)]
                             "INNER" [:join {} (plan lhs) (plan rhs)]))

     [:from_clause _ ^:z trl]
     ;;=>
     (build-table-reference-list trl)

     (throw (IllegalArgumentException. (str "Cannot build plan for: "  (pr-str (z/node z))))))))

;; Rewriting of logical plan.

;; NOTE: might be better to try do this via projected-columns and meta
;; data when building the initial plan? Though that requires rewrites
;; consistently updating this if they change anything. Some operators
;; don't know their columns, like csv and arrow, though they might
;; have some form of AS clause that does at the SQL-level. This
;; function will mainly be used for decorrelation, so not being able
;; to deduct this, fail, and keep Apply is also an option, say by
;; returning nil instead of throwing an exception like now.
(defn- relation-columns [relation]
  (letfn [(->column [column-or-expr]
            (if (map? column-or-expr)
              (first (keys column-or-expr))
              column-or-expr))]
    (r/zmatch relation
      [:table explicit-column-names _]
      (vec explicit-column-names)

      [:table table]
      (vec (keys (first table)))

      [:scan columns]
      (mapv ->column columns)

      [:join _ lhs rhs]
      (vec (mapcat relation-columns [lhs rhs]))

      [:cross-join lhs rhs]
      (vec (mapcat relation-columns [lhs rhs]))

      [:left-outer-join _ lhs rhs]
      (vec (mapcat relation-columns [lhs rhs]))

      [:semi-join _ lhs _]
      (relation-columns lhs)

      [:anti-join _ lhs _]
      (relation-columns lhs)

      [:rename prefix-or-columns relation]
      (if (symbol? prefix-or-columns)
        (vec (for [c (relation-columns relation)]
               (symbol (str prefix-or-columns "_"  c))))
        (replace prefix-or-columns (relation-columns relation)))

      [:project projection _]
      (mapv ->column projection)

      [:group-by columns _]
      (mapv ->column columns)

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

      [:unwind _ opts relation]
      (cond-> (relation-columns relation)
        (:with-ordinality? opts) (conj '_ordinal))

      [:assign _ relation]
      (relation-columns relation)

      [:apply mode _ dependent-column-names independent-relation _]
      (-> (relation-columns independent-relation)
          (concat (case mode
                    (:cross-join :left-outer-join) dependent-column-names
                    []))
          (vec))

      [:max-1-row relation]
      (relation-columns relation)

      (throw (IllegalArgumentException. (str "cannot calculate columns for: " (pr-str relation)))))))

(defn- table-references-in-subtree [op]
  (set (r/collect-stop
        (fn [z]
          (r/zmatch z
            [:rename prefix _]
            (when-let [table-reference (:table-reference (meta prefix))]
              [table-reference])
            [:unwind cve _]
            (when-let [table-reference (:table-reference (meta cve))]
              [table-reference])))
        (z/vector-zip op))))

(defn- table-ids-in-subtree [op]
  (->> (table-references-in-subtree op)
       (map :table-id)
       (set)))

(defn expr-symbols [expr]
  (set (for [x (flatten (if (coll? expr)
                          (seq expr)
                          [expr]))
             :when (:column-reference (meta x))]
         x)))

(defn expr-correlated-symbols [expr]
  (set (for [x (flatten (if (coll? expr)
                          (seq expr)
                          [expr]))
             :when (:correlated-column-reference (meta x))]
         x)))

(defn- expr-column-references [expr]
  (->> (expr-symbols expr)
       (map (comp :column-reference meta))
       (set)))

(defn- expr-table-ids [expr]
  (->> (expr-column-references expr)
       (keep :table-id)
       (set)))

(defn- equals-predicate? [predicate]
  (r/zmatch predicate
    [:= x y]
    true

    false))

(defn- all-predicate? [predicate]
  (r/zmatch predicate
    [:or [:= x y] [:nil? x] [:nil? y]]
    true

    false))

(defn- not-predicate? [predicate]
  (r/zmatch predicate
    [:not _]
    true

    false))

(defn- exists-predicate? [predicate]
  (if (symbol? predicate)
    (:exists? (meta predicate))
    (and (not-predicate? predicate)
         (exists-predicate? (second predicate)))))

(defn- build-join-map [predicate lhs rhs]
  (when (equals-predicate? predicate)
    (let [[_ x y] predicate
          {x-table-id :table-id} (:column-reference (meta x))
          {y-table-id :table-id} (:column-reference (meta y))
          [lhs-v rhs-v] (for [side [lhs rhs]
                              :let [table-ids (table-ids-in-subtree side)]]
                          (cond
                            (contains? table-ids x-table-id)
                            x
                            (contains? table-ids y-table-id)
                            y))]
      (when (and lhs-v rhs-v)
        {lhs-v rhs-v}))))

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
    (when-let [join-map (build-join-map predicate lhs rhs)]
      [:join join-map lhs rhs])))

(defn- promote-selection-to-join [z]
  (r/zmatch z
     [:select predicate
      [join-op {} lhs rhs]]
     ;;=>
     (when-let [join-map (build-join-map predicate lhs rhs)]
       [join-op join-map lhs rhs])))

(defn- push-selection-down-past-apply [z]
  (r/zmatch z
    [:select predicate
     [:apply mode columns dependent-column-names independent-relation dependent-relation]]
    ;;=>
    (when (and (empty? (set/intersection (expr-table-ids predicate)
                                         (expr-table-ids dependent-column-names)))
               (empty? (set/intersection (expr-symbols predicate)
                                         (relation-columns dependent-relation))))
      [:apply
       mode
       columns
       dependent-column-names
       [:select predicate independent-relation]
       dependent-relation])))

(defn- push-selection-down-past-rename [z]
  (r/zmatch z
    [:select predicate
     [:rename columns
      relation]]
    ;;=>
    (cond
      (and (symbol? columns)
           (set/subset? (expr-table-ids predicate)
                        (table-ids-in-subtree [:rename columns relation])))
      (let [column->unqualified-column (->> (for [c (expr-symbols predicate)]
                                              [c (with-meta (symbol (get-in (meta c) [:column-reference :column]))
                                                   (meta c))])
                                            (into {}))]
        [:rename columns
         [:select (w/postwalk-replace column->unqualified-column predicate)
          relation]])

      (map? columns)
      [:rename columns
       [:select (w/postwalk-replace (set/map-invert columns) predicate)
        relation]])))

(defn- predicate-depends-on-calculated-expression? [predicate projection]
  (not (set/subset? (set (expr-symbols predicate))
                    (set (filter symbol? projection)))))

(defn- push-selection-down-past-project [z]
  (r/zmatch z
    [:select predicate
     [:project projection
      relation]]
    ;;=>
    (when-not (predicate-depends-on-calculated-expression? predicate projection)
      [:project projection
       [:select predicate
        relation]])))

(defn- push-selection-down-past-group-by [z]
  (r/zmatch z
    [:select predicate
     [:group-by group-by-columns
      relation]]
    ;;=>
    (when-not (predicate-depends-on-calculated-expression? predicate group-by-columns)
      [:group-by group-by-columns
       [:select predicate
        relation]])))


(defn- push-selection-down-past-join [z]
  (letfn [(push-selection-down [predicate lhs rhs]
            (let [expr-table-ids (expr-table-ids predicate)
                  lhs-table-ids (table-ids-in-subtree lhs)
                  rhs-table-ids (table-ids-in-subtree rhs)
                  on-lhs? (set/subset? expr-table-ids lhs-table-ids)
                  on-rhs? (set/subset? expr-table-ids rhs-table-ids)]
              (cond
                (and on-rhs? (not on-lhs?))
                [lhs [:select predicate rhs]]

                (and on-lhs? (not on-rhs?))
                [[:select predicate lhs] rhs])))]
    (r/zmatch z
      [:select predicate
       [join-op join-map lhs rhs]]
      ;;=>
      (when-let [[lhs rhs] (push-selection-down predicate lhs rhs)]
        [join-op join-map lhs rhs])

      [:select predicate
       [:cross-join lhs rhs]]
      ;;=>
      (when-let [[lhs rhs] (push-selection-down predicate lhs rhs)]
        [:cross-join lhs rhs]))))

(defn- push-selections-with-fewer-variables-down [z]
  (r/zmatch z
    [:select predicate-1
     [:select predicate-2
      relation]]
    ;;=>
    (when (and (not (equals-predicate? predicate-2))
               (< (count (expr-table-ids predicate-1))
                  (count (expr-table-ids predicate-2))))
      [:select predicate-2
       [:select predicate-1
        relation]])))

(defn- push-selections-with-equals-down [z]
  (r/zmatch z
    [:select predicate-1
     [:select predicate-2
      relation]]
    ;;=>
    (when (and (equals-predicate? predicate-1)
               (not (equals-predicate? predicate-2)))
      [:select predicate-2
       [:select predicate-1
        relation]])))

(defn- merge-selections-with-same-variables [z]
  (r/zmatch z
    [:select predicate-1
     [:select predicate-2
      relation]]
    ;;=>
    (when (= (expr-table-ids predicate-1) (expr-table-ids predicate-2))
      [:select (merge-conjunctions predicate-1 predicate-2) relation])))

(defn- merge-renames [z]
  (r/zmatch z
    [:rename columns-1
     [:rename columns-2
      relation]]
    ;;=>
    (when (and (map? columns-1) (map? columns-2))
      (let [rename-map (reduce-kv
                        (fn [acc k v]
                          (assoc acc k (get columns-1 v v)))
                        (apply dissoc columns-1 (vals columns-2))
                        columns-2)]
        [:rename (with-meta rename-map (meta columns-2)) relation]))))

(defn- remove-superseded-projects [z]
  (r/zmatch z
    [:project projections-1
     [:rename prefix-or-columns
      [:project projections-2
       relation]]]
    ;;=>
    (when (every? symbol? projections-2)
      [:project projections-1
       [:rename prefix-or-columns
        relation]])

    [:project projections
     [:rename prefix
      [:scan columns]]]
    ;;=>
    (when (and (every? symbol? projections)
               (symbol? prefix)
               (= (count projections) (count columns)))
      [:rename prefix
       [:scan columns]])))

(defn- add-selection-to-scan-predicate [z]
  (r/zmatch z
    [:select predicate
     [:scan columns]]
    ;;=>
    (let [new-columns (reduce
                       (fn [acc predicate]
                         (let [expr-symbols (expr-symbols predicate)]
                           (if-let [single-symbol (when (= 1 (count expr-symbols))
                                                    (first expr-symbols))]
                             (vec (for [column-or-select acc
                                        :let [column (if (map? column-or-select)
                                                       (key (first column-or-select))
                                                       column-or-select)]]
                                    (if (= single-symbol column)
                                      (if (map? column-or-select)
                                        (update column-or-select column (partial merge-conjunctions predicate))
                                        {column predicate})
                                      column-or-select)))
                             acc)))
                       columns
                       (conjunction-clauses predicate))]
      (when-not (= columns new-columns)
        [:select predicate
         [:scan new-columns]]))))

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
                   (and (equals-predicate? predicate-2)
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
        relation]])

    [:rename columns
     [:select predicate
      relation]]
    ;;=>
    (when (not-empty (expr-correlated-symbols predicate))
      (cond
        (map? columns)
        [:select (w/postwalk-replace columns predicate)
         [:rename columns
          relation]]

        (symbol? columns)
        (let [{:keys [table-id correlation-name]} (:table-reference (meta columns))
              column->qualified-column (->> (for [c (expr-symbols predicate)]
                                              [c (id-symbol correlation-name table-id c)])
                                            (into {}))]
          [:select (w/postwalk-replace column->qualified-column predicate)
           [:rename columns
            relation]])))))

(defn- remove-unused-correlated-columns [columns dependent-relation]
  (->> columns
       (filter (comp (expr-correlated-symbols dependent-relation) val))
       (into {})))

(defn- group-by-after-apply-rename-map [rename-columns post-group-by-projection]
  (if (map? rename-columns)
    rename-columns
    (let [{:keys [table-id correlation-name]} (:table-reference (meta rename-columns))]
      (->> (for [c (relation-columns [:project post-group-by-projection nil])]
             [c (id-symbol correlation-name table-id c)])
           (into {})))))

;; Rule 3, 4, 8 and 9.
(defn- decorrelate-apply [z]
  (r/zmatch z
    ;; Rule 3.
    [:apply mode columns dependent-column-names independent-relation [:select predicate dependent-relation]]
    ;;=>
    (when (or (= :cross-join mode)
              (equals-predicate? predicate)
              (all-predicate? predicate))
      [:select (w/postwalk-replace (set/map-invert columns) (if (all-predicate? predicate)
                                                              (second predicate)
                                                              predicate))
       (let [columns (remove-unused-correlated-columns columns dependent-relation)]
         [:apply mode columns dependent-column-names independent-relation dependent-relation])])

    ;; Rule 4.
    [:apply mode columns dependent-column-names independent-relation [:project projection dependent-relation]]
    ;;=>
    [:project (vec (concat (relation-columns independent-relation)
                           (w/postwalk-replace (set/map-invert columns) projection)))
     (let [columns (remove-unused-correlated-columns columns dependent-relation)]
       [:apply mode columns dependent-column-names independent-relation dependent-relation])]

    ;; Rule 8.
    [:apply :cross-join columns dependent-column-names independent-relation
     [:rename rename-columns
      [:project post-group-by-projection
       [:group-by group-by-columns
        [:project pre-group-by-projection
         dependent-relation]]]]]
    ;;=>
    (let [independent-projection (relation-columns independent-relation)
          smap (set/map-invert columns)
          row-number-sym (unqualified-id-symbol '$row_number$)]
      [:rename (group-by-after-apply-rename-map rename-columns post-group-by-projection)
       [:project (vec (concat independent-projection
                              (w/postwalk-replace smap post-group-by-projection)))
        [:group-by (vec (concat independent-projection
                                [row-number-sym]
                                (w/postwalk-replace smap group-by-columns)))
         [:project (vec (concat independent-projection
                                [row-number-sym]
                                (w/postwalk-replace smap pre-group-by-projection)))
          (let [columns (remove-unused-correlated-columns columns dependent-relation)]
            [:apply :cross-join columns dependent-column-names
             [:project (vec (concat independent-projection [{row-number-sym '(row_number)}]))
              independent-relation]
             dependent-relation])]]]])

    ;; Rule 9.
    [:apply :cross-join columns dependent-column-names independent-relation
     [:max-1-row
      [:rename rename-columns
       [:project post-group-by-projection
        [:group-by group-by-columns
         [:project pre-group-by-projection
          dependent-relation]]]]]]
    ;;=>
    (let [independent-projection (relation-columns independent-relation)
          smap (set/map-invert columns)
          row-number-sym (unqualified-id-symbol '$row_number$)]
      [:rename (group-by-after-apply-rename-map rename-columns post-group-by-projection)
       [:project (vec (concat independent-projection
                              (w/postwalk-replace smap post-group-by-projection)))
        [:group-by (vec (concat independent-projection
                                [row-number-sym]
                                (w/postwalk-replace smap group-by-columns)))
         [:project (vec (concat independent-projection
                                [row-number-sym]
                                (w/postwalk-replace smap pre-group-by-projection)))
          (let [columns (remove-unused-correlated-columns columns dependent-relation)]
            [:apply :left-outer-join columns dependent-column-names
             [:project (vec (concat independent-projection [{row-number-sym '(row_number)}]))
              independent-relation]
             dependent-relation])]]]])))

(defn- promote-apply-mode [z]
  (r/zmatch z
    [:select predicate-1
     [:select predicate-2
      relation]]
    ;;=>
    (when (exists-predicate? predicate-1)
      [:select predicate-2
       [:select predicate-1
        relation]])

    [:select predicate
     [:apply :cross-join columns dependent-column-names independent-relation
      [:top {:limit 1}
       [:union-all
        [:project [_]
         dependent-relation]
        [:table [_]]]]]]
    ;;=>
    (cond
      (and (symbol? predicate)
           (= #{predicate} dependent-column-names)
           (exists-predicate? predicate))
      [:apply :semi-join columns #{} independent-relation dependent-relation]

      (and (not-predicate? predicate)
           (= #{(second predicate)} dependent-column-names)
           (exists-predicate? predicate))
      [:apply :anti-join columns #{} independent-relation dependent-relation])))

;; Rule 1 and 2.
(defn- remove-uncorrelated-apply [z]
  (r/zmatch z
    [:apply :cross-join {} _ independent-relation dependent-relation]
    ;;=>
    [:cross-join independent-relation dependent-relation]

    [:select predicate
     [:apply :semi-join {} _ independent-relation dependent-relation]]
    ;;=>
    (when-let [join-map (build-join-map predicate independent-relation dependent-relation)]
      [:semi-join join-map independent-relation dependent-relation])

    [:select predicate
     [:apply :anti-join {} _ independent-relation dependent-relation]]
    ;;=>
    (when-let [join-map (build-join-map predicate independent-relation dependent-relation)]
      [:anti-join join-map independent-relation dependent-relation])

    [:select predicate
     [:apply :left-outer-join {} _ independent-relation dependent-relation]]
    ;;=>
    (when-let [join-map (build-join-map predicate independent-relation dependent-relation)]
      [:left-outer-join join-map independent-relation dependent-relation])))

(def decorrelate-plan
  (some-fn pull-correlated-selection-up-towards-apply
           decorrelate-apply
           promote-apply-mode
           remove-uncorrelated-apply))

(def optimize-plan
  (some-fn promote-selection-cross-join-to-join
           promote-selection-to-join
           push-selection-down-past-join
           push-selection-down-past-apply
           push-selection-down-past-rename
           push-selection-down-past-project
           push-selection-down-past-group-by
           push-selections-with-fewer-variables-down
           push-selections-with-equals-down
           merge-selections-with-same-variables
           merge-renames
           remove-superseded-projects
           add-selection-to-scan-predicate))

;; Logical plan API

(defn plan-query [query]
  (if-let [parse-failure (insta/get-failure query)]
    {:errs [(prn-str parse-failure)]}
    (r/with-memoized-attributes [sem/id
                                 sem/ctei
                                 sem/cteo
                                 sem/cte-env
                                 sem/dcli
                                 sem/dclo
                                 sem/env
                                 sem/group-env
                                 sem/projected-columns
                                 sem/column-reference]
      (let [ag (z/vector-zip query)]
        (if-let [errs (not-empty (sem/errs ag))]
          {:errs errs}
          {:plan (let [plan (->> (z/vector-zip (plan ag))
                                 (r/innermost (r/mono-tp decorrelate-plan))
                                 (r/innermost (r/mono-tp optimize-plan))
                                 (z/node))]
                   (if (s/invalid? (s/conform ::lp/logical-plan plan))
                     (throw (IllegalArgumentException. (s/explain-str ::lp/logical-plan plan)))
                     plan))})))))

;; Building plans using the Apply operator:

;; Aims to digest information from the following in terms of our
;; logical plan:

;; http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.563.8492&rep=rep1&type=pdf "Orthogonal Optimization of Subqueries and Aggregation"
;; http://www.cse.iitb.ac.in/infolab/Data/Courses/CS632/2010/Papers/subquery-proc-elhemali-sigmod07.pdf "Execution Strategies for SQL Subqueries"
;; https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2000-31.pdf "Parameterized Queries and Nesting Equivalences"

;; Introduction:

;; Each subquery has two parts, an expression replacing it in its
;; parent expression, and a relational algebra tree used in the
;; wrapping Apply operator. The replacement expression is usually a
;; fresh variable/column, but could be a constant, like true (when the
;; mode is anti/semi-join).

;; The Apply operator ends up around the existing tree and below the
;; operator introducing the subquery, so if the subquery sits in a
;; select: [:select (... replaced expression ...) [:apply .... input
;; relation subquery] Apply will map the out columns of the subquery
;; to fresh column/variable(s) and these will be used in the replaced
;; expression. The default Apply mode is left-outer-join, but
;; max-1-row may replace this need, and use cross-join?

;; Usage of Apply for expressions can happen in two relational
;; contexts, in a top-level conjunction select or inside a normal
;; select/project. During top-level select, the subquery in the
;; expression may get replaced with the constant true (which could be
;; optimised away) and the Apply operator mode configured to semi/anti
;; etc. Otherwise, one needs to calculate the actual value of the
;; subquery and bind it to a fresh variable/column so it can be used
;; in the original expression, which may be arbitrary complex.

;; The top-level conjunction context could/should be detected and
;; dealt with as a rewrite instead of being generated directly. The
;; main reason to use different modes is to simplify further rewrites
;; to get rid of the Apply operator, which are out of scope for this
;; note.

;; A few special cases:

;; A table subquery may also occur in the FROM clause (and not in an
;; expression as above), in which case it's dealt with separately. If
;; the subquery isn't a LATERAL derived table, it will simply be
;; translated verbatim without the Apply operator. It may still have
;; correlated variables from an outer query, but that would been dealt
;; with higher up. A LATERAL subquery is executed as an Apply cross
;; join where used columns from tables defined to the left may be
;; among its parameters. If there are no columns to the left used, it
;; doesn't need to use the Apply operator.

;; A row subquery may only appear in a few places, like inside a
;; VALUES or IN expression, in which case it will bind N number of
;; original columns to fresh variables/columns, and use
;; max-1-row. (Alternatively, this could be translated into returning
;; a single struct.) It would expand into a literal row where the
;; elements are these columns. [:table [{:a <fv-1>, :b <fv-2>}]
;; [:apply ... [:max-1-row [:project [{<fv-1> <row-col-1} {fv-2
;; <row-col-2}] ...]]]] This is just an example to illustrate the
;; translation, the keys would normally be determined based on the
;; projected columns of the surrounding query. Often the columns would
;; be anonymous and ordinal only. This is a general problem of how to
;; generate plans for VALUES and not specifically to Apply, so not in
;; scope for this note.

;; Conditional evaluation, like CASE and short-circuiting AND/OR (if
;; the spec enforces this) pose additional challenges, but are not
;; currently dealt with in this note.

;; Scalar subqueries:

;; This is the basic case, they introduce a fresh variable/column in
;; its place in the expression, and then an Apply operator where the
;; parameterised relation is wrapped with max-1-row (why would
;; left-outer-join also be the mode?), and the original output column
;; is renamed to its fresh variable/column. This works the same
;; regardless context. [:select (... <fv> ...) [:apply ... [:project
;; [{<fv> <scalar-column>}] ...]] There are situations where one can
;; avoid wrapping via the max-1-row, like if the parameterised
;; relation is a scalar aggregate.

;; Table subqueries:

;; There are three places these can occur inside expressions: (NOT)
;; IN, ALL/ANY and (NOT) EXISTS. (NOT) IN and ALL/ANY are normalised
;; into (NOT) EXISTS. Exactly how this is translated differs between
;; (top-level) select and usage inside other expressions.

;; ALL is translated into NOT EXISTS, where the left hand side is
;; moved into parameterised relation using the inverse comparison
;; operator, where the right hand side is a reference to the scalar
;; column itself. That is, this can be done via wrapping the
;; parameterised relation with [:select (or (<inv-comp-op> <lhs>
;; <scalar-column>) (nil? <lhs>) (nil? <scalar-column>)) ...]. Moving
;; the left hand side into the parameterised expression will introduce
;; correlation on any column references it contains. Left hand side
;; may be arbitrary complex, and may contain further Apply operators.

;; ANY is translated into EXISTS where the left hand side is moved
;; inside to wrap the parameterised relation. [:select (<comp-op>
;; <lhs> <scalar-column>) ...] Correlation maybe introduced as
;; mentioned above.

;; A potential different translation is to pass in lhs as well as a
;; parameter, avoiding introducing correlation. As above, this
;; parameter might need to be calculated via Apply itself if it's
;; complex. This translation diverges more from the literature, and
;; may require adapted rewrite rules to decorrelate.


;; IN is translated into = ANY and then as above.
;; NOT IN is translated into <> ALL and then as above.
;; IN value lists can be seen as an IN with a VALUES subquery.

;; Inside top-level select, the original expression is replaced with
;; true (or dropped) in all the above cases, and the Apply mode is set
;; to semi-join for EXISTS and anti-join for NOT EXISTS.

;; Otherwise, the expression is a fresh variable/column which boolean
;; value is calculated with an parameterised relation as [:project
;; [{<fv-out> (= <fv-in> <N>)}] [:group-by [{<fv-in> (count ...)}]
;; [:top {:limit 1} ....]  where N is 1 for EXISTS and 0 for NOT
;; EXISTS. The Apply mode here is cross join.

;; Note that for ALL/ANY/IN, the subquery may return more than one
;; column, which can be deal with by returning a struct.

;; Correlated variables:

;; In all cases, the dependent variables/columns necessary to give as
;; a parameters to the Apply operator can be calculated via the
;; attributes, in ways similar to how the scope attribute currently
;; does it. Note that variables may need to be made available and
;; passed down several levels to be used in other Apply operators. In
;; these cases the variable itself will already be in the current
;; parameter scope having been passed down, and simply needs to be
;; passed on. Column references to outer columns will be generated as
;; parameters in the plan, that is, they will be prefixed with a
;; question mark.

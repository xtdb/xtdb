(ns core2.sql.plan
  (:require [clojure.set :as set]
            [clojure.main]
            [clojure.string :as str]
            [clojure.walk :as w]
            [clojure.zip :as z]
            [clojure.spec.alpha :as s]
            [core2.logical-plan :as lp]
            [core2.rewrite :as r]
            [core2.sql.analyze :as sem]
            [instaparse.core :as insta])
  (:import clojure.lang.IObj
           [java.time LocalDate Period]))

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
  (symbol (str table relation-id-delimiter table-id relation-prefix-delimiter column)))

(defn unqualified-projection-symbol [{:keys [identifier ^long index] :as projection}]
  (symbol (or identifier (str "$column_" (inc index) "$"))))

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
  (symbol (str correlation-name relation-id-delimiter id)))

(defn- aggregate-symbol [prefix z]
  (let [query-id (sem/id (sem/scope-element z))]
    (symbol (str "$" prefix relation-id-delimiter query-id relation-prefix-delimiter (sem/id z) "$"))))

(defn- exists-symbol [qe]
  (id-symbol "subquery" (sem/id qe) "$exists$"))

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
     ;;=>
     (column-reference-symbol (sem/column-reference z))

     [:boolean_value_expression ^:z bve _ ^:z bt]
     ;;=>
     (list 'or (expr bve) (expr bt))

     [:boolean_term ^:z bt _ ^:z bf]
     ;;=>
     (list 'and (expr bt) (expr bf))

     [:boolean_factor "NOT" ^:z bt]
     ;;=>
     (list 'not (expr bt))

     [:boolean_test ^:z bp]
     ;;=>
     (expr bp)

     [:boolean_literal bl]
     (case bl
       "TRUE" true
       "FALSE" false
       "UNKNOWN" nil)

     [:null_specification _]
     nil

     [:comparison_predicate ^:z rvp-1 [:comparison_predicate_part_2 [_ co] ^:z rvp-2]]
     ;;=>
     (list (symbol co) (expr rvp-1) (expr rvp-2))

     [:numeric_value_expression ^:z nve [_ op] ^:z t]
     ;;=>
     (list (symbol op) (expr nve) (expr t))

     [:term ^:z t [_ op] ^:z f]
     ;;=>
     (list (symbol op) (expr t) (expr f))

     [:signed_numeric_literal ^:z unl]
     ;;=>
     (expr unl)

     [:exact_numeric_literal lexeme]
     ;;=>
     (if (str/includes? lexeme ".")
       (Double/parseDouble lexeme)
       (Long/parseLong lexeme))

     [:unsigned_integer lexeme]
     ;;=>
     (Long/parseLong lexeme)

     [:factor [_ sign] ^:z np]
     (if (= "-" sign)
       (- (expr np))
       (expr np))

     [:character_string_literal lexeme]
     ;;=>
     (subs lexeme 1 (dec (count lexeme)))

     [:date_literal _
      [:date_string [:date_value [:unsigned_integer year] _ [:unsigned_integer month] _ [:unsigned_integer day]]]]
     ;;=>
     (LocalDate/of (Long/parseLong year) (Long/parseLong month) (Long/parseLong day))

     [:interval_literal _
      [:interval_string [:unquoted_interval_string
                         [_ [:unsigned_integer year-month-literal]]]]
      [:interval_qualifier
       [:single_datetime_field [:non_second_primary_datetime_field datetime-field]]]]
     ;;=>
     (case datetime-field
       "DAY" (Period/ofDays (Long/parseLong year-month-literal))
       "MONTH" (Period/ofMonths (Long/parseLong year-month-literal))
       "YEAR" (Period/ofYears (Long/parseLong year-month-literal)))

     [:character_like_predicate ^:z rvp [:character_like_predicate_part_2 "LIKE" ^:z cp]]
     ;;=>
     (list 'like (expr rvp) (expr cp))

     [:character_like_predicate ^:z rvp [:character_like_predicate_part_2 "NOT" "LIKE" ^:z cp]]
     ;;=>
     (list 'not (list 'like (expr rvp) (expr cp)))

     ;; TODO: this is called substr in expression engine.
     [:character_substring_function "SUBSTRING" ^:z cve "FROM" ^:z sp "FOR" ^:z sl]
     ;;=>
     (list 'substring (expr cve) (expr sp) (expr sl))

     [:between_predicate ^:z rvp-1 [:between_predicate_part_2 "BETWEEN" ^:z rvp-2 "AND" ^:z rvp-3]]
     ;;=>
     (list 'between (expr rvp-1) (expr rvp-2) (expr rvp-3))

     [:extract_expression "EXTRACT"
      [:primary_datetime_field [:non_second_primary_datetime_field extract-field]] "FROM" ^:z es]
     ;;=>
     (list 'extract extract-field (expr es))

     [:searched_case "CASE"
      [:searched_when_clause "WHEN" ^:z wol "THEN" [:result ^:z then]]
      [:else_clause "ELSE" [:result ^:z else]] "END"]
     ;;=>
     (list 'if (expr wol) (expr then) (expr else))

     [:named_columns_join _ _]
     ;;=>
     (reduce
      (fn [acc expr]
        (list 'and acc expr))
      (let [{:keys [join-columns] :as env} (sem/named-columns-join-env z)]
        (for [column join-columns]
          (->> (for [side [:lhs :rhs]]
                 (qualified-projection-symbol (first (get-in env [side column]))))
               (apply list '=)))))

     [:aggregate_function _]
     ;;=>
     (aggregate-symbol "agg_out" z)

     [:aggregate_function "COUNT" [:asterisk "*"]]
     ;;=>
     (aggregate-symbol "agg_out" z)

     [:aggregate_function _ _]
     ;;=>
     (aggregate-symbol "agg_out" z)

     [:subquery ^:z qe]
     ;;=>
     (let [subquery-type (sem/subquery-type z)]
       (case (:type subquery-type)
         :scalar_subquery (first (subquery-projection-symbols qe))))

     [:exists_predicate _
      [:subquery ^:z qe]]
     ;;=>
     (exists-symbol qe)

     [:in_predicate _ [:in_predicate_part_2 _ [:in_predicate_value [:subquery ^:z qe]]]]
     ;;=>
     (exists-symbol qe)

     [:in_predicate _ [:in_predicate_part_2 _ [:in_predicate_value ^:z ivl]]]
     ;;=>
     (exists-symbol ivl)

     [:in_predicate _ [:in_predicate_part_2 "NOT" _ [:in_predicate_value [:subquery ^:z qe]]]]
     ;;=>
     (list 'not (exists-symbol qe))

     [:in_predicate _ [:in_predicate_part_2 "NOT" _ [:in_predicate_value ^:z ivl]]]
     ;;=>
     (list 'not (exists-symbol ivl))

     [:quantified_comparison_predicate _ [:quantified_comparison_predicate_part_2 _ [:some _] [:subquery ^:z qe]]]
     ;;=>
     (exists-symbol qe)

     [:quantified_comparison_predicate _ [:quantified_comparison_predicate_part_2 _ [:all _] [:subquery ^:z qe]]]
     ;;=>
     (list 'not (exists-symbol qe))

     (throw (IllegalArgumentException. (str "Cannot build expression for: "  (pr-str (z/node z))))))))

;; Logical plan.

(declare plan)

(defn- correlated-column->param [qe scope-id]
  (->> (for [{:keys [^long table-scope-id] :as column-reference} (sem/all-column-references qe)
             :when (<= table-scope-id scope-id)
             :let [column-reference-symbol (column-reference-symbol column-reference)
                   param-symbol (symbol (str "?" column-reference-symbol))]]
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
    [:table [{exists-column false}]]]])

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

                    [:in_predicate_part_2 _ [:in_predicate_value ^:z ivl]]
                    ivl

                    [:in_predicate_part_2 "NOT" _ [:in_predicate_value ^:z ivl]]
                    ivl

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
                    (sem/scope-element te))
        group-by (for [aggregate aggregates]
                   (r/zmatch aggregate
                     [:aggregate_function [:general_set_function [:computational_operation sf] ^:z ve]]
                     {:in {(aggregate-symbol "agg_in" aggregate) (expr ve)}
                      :out {(aggregate-symbol "agg_out" aggregate)
                            (list (symbol (str/lower-case sf)) (aggregate-symbol "agg_in" aggregate))}}

                     ;; TODO: ensure we can generate right aggregation function in backend.
                     [:aggregate_function [:general_set_function [:computational_operation sf] [:set_quantifier sq] ^:z ve]]
                     {:in {(aggregate-symbol "agg_in" aggregate) (expr ve)}
                      :out {(aggregate-symbol "agg_out" aggregate)
                            (list (symbol (str/lower-case sf)) (aggregate-symbol "agg_in" aggregate))}}

                     [:aggregate_function "COUNT" [:asterisk "*"]]
                     {:in {(aggregate-symbol "agg_in" aggregate) 1}
                      :out {(aggregate-symbol "agg_out" aggregate)
                            (list 'count (aggregate-symbol "agg_in" aggregate))}}

                     (throw (IllegalArgumentException. "unknown aggregation function"))))]
    [:group-by (->> (map :out group-by)
                    (into grouping-columns))
     [:map (mapv :in group-by) relation]]))

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
                                 (let [column (symbol (str "$order_by" relation-id-delimiter query-id relation-prefix-delimiter (r/child-idx z) "$"))]
                                   {:spec {column direction}
                                    :projection {column (expr (r/$ z 1))}}))])

                            :subquery
                            []

                            nil))
                        ssl)
        order-by-projection (vec (keep :projection order-by-specs))
        base-projection (mapv unqualified-projection-symbol projection)
        relation (if (not-empty order-by-projection)
                   (->> (z/vector-zip relation)
                        (r/once-td-tp
                         (r/mono-tp
                          (fn [z]
                            (r/zmatch z
                              [:project projection relation]
                              ;;=>
                              [:project (vec (concat projection (mapcat keys order-by-projection)))
                               [:map order-by-projection relation]]))))
                        (z/node))
                   relation)
        order-by [:order-by (mapv :spec order-by-specs) relation]]
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

(defn- build-collection-derived-table [tp]
  (let [{:keys [id correlation-name]} (sem/table tp)
        [unwind-column ordinality-column] (map qualified-projection-symbol (first (sem/projected-columns tp)))
        cdt (r/$ tp 1)
        cve (r/$ cdt 2)
        unwind-symbol (symbol (str "$unwind" relation-id-delimiter id "$"))]
    [:unwind {unwind-column unwind-symbol}
     (cond-> {}
       ordinality-column (assoc :ordinality-column ordinality-column))
     [:map [{unwind-symbol (expr cve)}] nil]]))

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
       [:unwind cve unwind-opts [:map projection nil]]
       ;;=>
       [:unwind cve unwind-opts [:map projection acc]]

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

(defn- build-values-list [z]
  (let [ks (mapv unqualified-projection-symbol (first (sem/projected-columns z)))]
    [:table (r/collect-stop
             (fn [z]
               (r/zcase z
                 (:row_value_expression_list
                  :contextually_typed_row_value_expression_list
                  :in_value_list)
                 nil

                 (:explicit_row_value_constructor
                  :contextually_typed_row_value_constructor)
                 (let [vs (r/collect-stop
                           (fn [z]
                             (r/zcase z
                               (:row_value_constructor_element
                                :contextually_typed_row_value_constructor_element)
                               [(expr (r/$ z 1))]

                               :subquery
                               [(expr z)]

                               nil))
                           z)]
                   [(zipmap ks vs)])

                 (when (r/ctor z)
                   [{(first ks) (expr z)}])))
             z)]))

(defn plan [z]
  (maybe-add-ref
   z
   (r/zmatch z
     [:directly_executable_statement ^:z dsds]
     (plan dsds)

     [:query_expression ^:z qeb]
     (plan qeb)

     [:query_expression ^:z qeb [:order_by_clause _ _ ^:z ssl]]
     (wrap-with-order-by ssl (plan qeb))

     ;; TODO: Deal properly with WITH?
     [:query_expression [:with_clause "WITH" _] ^:z qeb [:order_by_clause _ _ ^:z ssl]]
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
     (build-collection-derived-table z)

     [:table_primary [:collection_derived_table _ _] _ _ _]
     (build-collection-derived-table z)

     [:table_primary [:collection_derived_table _ _ _ _] _ _]
     ;;=>
     (build-collection-derived-table z)

     [:table_primary [:collection_derived_table _ _ _ _] _ _ _]
     (build-collection-derived-table z)

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

     [:table_value_constructor _ ^:z rvel]
     (build-values-list rvel)

     [:contextually_typed_table_value_constructor _ ^:z cttvl]
     (build-values-list cttvl)

     (r/zcase z
       :in_value_list (build-values-list z)
       (throw (IllegalArgumentException. (str "Cannot build plan for: "  (pr-str (z/node z)))))))))

(defn- extend-projection? [column-or-expr]
  (map? column-or-expr))

(defn- ->projected-column [column-or-expr]
  (if (extend-projection? column-or-expr)
    (key (first column-or-expr))
    column-or-expr))

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
  (r/zmatch relation
    [:table explicit-column-names _]
    (vec explicit-column-names)

    [:table table]
    (mapv symbol (keys (first table)))

    [:scan columns]
    (mapv ->projected-column columns)

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

    [:unwind columns opts relation]
    (cond-> (conj (relation-columns relation) (val (first columns)))
      (:ordinality-column opts) (conj (:ordinality-column opts)))

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

    (throw (IllegalArgumentException. (str "cannot calculate columns for: " (pr-str relation))))))

;; Attempt to clean up tree, removing names internally and only add
;; them back at the top. Scan still needs explicit names to access the
;; columns, so these are directly renamed.

(def ^:private ^:dynamic *name-counter* (atom 0))

(defn- next-name []
  (with-meta (symbol (str 'x (swap! *name-counter* inc))) {:column? true}))

(defn- remove-names-step [relation]
  (letfn [(with-smap [relation smap]
            (vary-meta relation assoc :smap smap))
          (->smap [relation]
            (:smap (meta relation)))
          (remove-projection-names [op projection relation]
            (let [smap (->smap relation)
                  projection (w/postwalk-replace smap projection)
                  smap (reduce
                        (fn [acc p]
                          (if (extend-projection? p)
                            (let [[k v] (first p)]
                              (if (symbol? v)
                                (assoc acc k v)
                                (assoc acc k (next-name))))
                            acc))
                        smap
                        projection)
                  projection (vec (for [p (w/postwalk-replace smap projection)]
                                    (if (extend-projection? p)
                                      (let [[k v] (first p)]
                                        (if (= k v)
                                          k
                                          p))
                                      p)))
                  projection (if (= :map op)
                               (filterv map? projection)
                               projection)
                  relation (if (every? symbol? projection)
                             relation
                             [op
                              projection
                              relation])]
              (with-smap relation smap)))]
    (r/zmatch relation
      [:table explicit-column-names table]
      (let [smap (zipmap explicit-column-names
                         (repeatedly next-name))]
        (with-smap (w/postwalk-replace smap [:table explicit-column-names table]) smap))

      [:table table]
      (let [smap (zipmap (map symbol (keys (first table)))
                         (repeatedly next-name))]
        (with-smap [:table (w/postwalk-replace smap table)]
          smap))

      [:scan columns]
      (let [smap (zipmap (map ->projected-column columns)
                         (repeatedly next-name))]
        (with-smap [:rename smap [:scan columns]] smap))

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

      [:rename prefix-or-columns relation]
      (let [smap (->smap relation)
            columns (if (symbol? prefix-or-columns)
                      (->> (for [c (keys smap)]
                             [c (symbol (str prefix-or-columns relation-prefix-delimiter c))])
                           (into {}))
                      prefix-or-columns)]
        (with-smap relation (->> (for [[k v] columns]
                                   [v (get smap k)])
                                 (into smap))))

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
      (with-smap [:intersect lhs (w/postwalk-replace (zipmap (relation-columns rhs)
                                                             (relation-columns lhs))
                                                     rhs)]
        (->smap lhs))

      [:difference lhs rhs]
      (with-smap [:difference lhs (w/postwalk-replace (zipmap (relation-columns rhs)
                                                              (relation-columns lhs))
                                                      rhs)]
        (->smap lhs))

      [:union-all lhs rhs]
      (with-smap [:union-all lhs (w/postwalk-replace (zipmap (relation-columns rhs)
                                                             (relation-columns lhs))
                                                     rhs)]
        (->smap lhs))

      [:apply mode columns dependent-column-names independent-relation dependent-relation]
      (let [params (zipmap (filter #(str/starts-with? (name %) "?") (vals columns))
                           (map #(with-meta (symbol (str "?" %)) {:correlated-column? true}) (repeatedly next-name)))]
        (with-smap [:apply mode
                    (w/postwalk-replace (merge params (->smap independent-relation)) columns)
                    (w/postwalk-replace (->smap dependent-relation) dependent-column-names)
                    independent-relation
                    (w/postwalk-replace params dependent-relation)]
          (merge (->smap independent-relation) (->smap dependent-relation))))

      [:unwind columns opts relation]
      (let [smap (->smap relation)
            [to from] (first columns)
            from (get smap from)
            smap (assoc smap to (next-name))
            columns {(get smap to) from}
            [smap opts] (if-let [ordinality-column (:ordinality-column opts)]
                          (let [smap (assoc smap ordinality-column (next-name))]
                            [smap {:ordinality-column (get smap ordinality-column)}])
                          [smap {}])]
        (with-smap [:unwind columns opts relation] smap))

      [:max-1-row relation]
      (with-smap [:max-1-row relation] (->smap relation))

      (when (and (vector? (z/node relation))
                 (keyword? (r/ctor relation)))
        (throw (IllegalArgumentException. (str "cannot remove names for: " (pr-str (z/node relation)))))))))

(defn- remove-names [relation]
  (let [projection (relation-columns relation)
        relation (binding [*name-counter* (atom 0)]
                   (z/node (r/bottomup (r/adhoc-tp r/id-tp remove-names-step) (r/->zipper relation))))
        smap (:smap (meta relation))
        rename-map (select-keys smap projection)
        projection (replace smap projection)
        add-projection-fn (fn [relation]
                            (let [relation (if (= projection (relation-columns relation))
                                             relation
                                             [:project projection
                                              relation])
                                  smap-inv (set/map-invert rename-map)
                                  relation (or (r/zmatch relation
                                                 [:rename rename-map-2 relation]
                                                 ;;=>
                                                 (when (= smap-inv (set/map-invert rename-map-2))
                                                   relation))
                                               [:rename smap-inv relation])]
                              (with-meta relation {:column->name smap})))]
    (with-meta relation {:column->name smap
                         :add-projection-fn add-projection-fn})))

(defn reconstruct-names [relation]
  (let [smap (:column->name (meta relation))
        relation (w/postwalk-replace (set/map-invert smap) relation)]
    (or (r/zmatch relation
          [:rename rename-map relation]
          ;;=>
          (when-let [rename-map (and (map? rename-map)
                                     (->> (for [[k v] rename-map
                                                :when (not= k v)]
                                            [k v])
                                          (into {})))]
            (if (empty? rename-map)
              relation
              [:rename rename-map relation])))

        relation)))

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

(defn- build-join-map [predicate lhs rhs]
  (when (equals-predicate? predicate)
    (let [[_ x y] predicate
          [lhs-v rhs-v] (for [side [lhs rhs]
                              :let [projection (set (relation-columns side))]]
                          (cond
                            (contains? projection x)
                            x
                            (contains? projection y)
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
    (when (empty? (set/intersection (expr-symbols predicate) dependent-column-names))
      (cond
        (set/superset? (set (relation-columns independent-relation)) (expr-symbols predicate))
        [:apply
         mode
         columns
         dependent-column-names
         [:select predicate independent-relation]
         dependent-relation]

        (set/superset? (set (relation-columns dependent-relation)) (expr-symbols predicate))
        [:apply
         mode
         columns
         dependent-column-names
         independent-relation
         [:select predicate dependent-relation]]))))

(defn- push-selection-down-past-rename [z]
  (r/zmatch z
    [:select predicate
     [:rename columns
      relation]]
    ;;=>
    (when (map? columns)
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
        relation]])

    [:select predicate
     [:map projection
      relation]]
    ;;=>
    (when-not (predicate-depends-on-calculated-expression? predicate (relation-columns relation))
      [:map projection
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
            (let [syms (expr-symbols predicate)
                  lhs-projection (set (relation-columns lhs))
                  rhs-projection (set (relation-columns rhs))
                  on-lhs? (set/subset? syms lhs-projection)
                  on-rhs? (set/subset? syms rhs-projection)]
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
               (< (count (expr-symbols predicate-1))
                  (count (expr-symbols predicate-2))))
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

(defn- remove-superseded-projects [z]
  (r/zmatch z
    [:project projections-1
     [:project projections-2
      relation]]
    ;;=>
    (cond
      (and (every? symbol? projections-2)
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
      [:scan relation]]]
    ;;=>
    [:select (merge-conjunctions predicate-1 predicate-2) [:scan relation]]))

(defn- add-selection-to-scan-predicate [z]
  (r/zmatch z
    [:select predicate
     [:scan columns]]
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
           [:scan scan-columns]]
          [:scan scan-columns])))))

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
        relation]])))

(defn- remove-unused-correlated-columns [columns dependent-relation]
  (->> columns
       (filter (comp (expr-correlated-symbols dependent-relation) val))
       (into {})))

(defn- decorrelate-group-by-apply [post-group-by-projection group-by-columns pre-group-by-projection
                                   apply-mode columns independent-relation dependent-relation]
  (let [independent-projection (relation-columns independent-relation)
        smap (set/map-invert columns)
        row-number-sym '$row_number$
        columns (remove-unused-correlated-columns columns dependent-relation)
        post-group-by-projection (remove symbol? post-group-by-projection)]
    (cond->> [:apply apply-mode columns (set (relation-columns dependent-relation))
              [:map [{row-number-sym '(row-number)}]
               independent-relation]
              dependent-relation]
      (not-empty pre-group-by-projection)
      (conj [:map (w/postwalk-replace smap pre-group-by-projection)])

      true
      (conj [:group-by (vec (concat independent-projection
                                    [row-number-sym]
                                    (w/postwalk-replace smap group-by-columns)))])

      (not-empty post-group-by-projection)
      (conj [:map (vec (w/postwalk-replace
                        smap
                        post-group-by-projection))]))))

;; Rule 3, 4, 8 and 9.
(defn- decorrelate-apply [z]
  (r/zmatch z
    ;; Rule 3.
    [:apply mode columns dependent-column-names independent-relation [:select predicate dependent-relation]]
    ;;=>
    (when (and (or (= :cross-join mode)
                   (equals-predicate? predicate)
                   (all-predicate? predicate))
               (seq (expr-correlated-symbols predicate))) ;; select predicate is correlated
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
    [:apply :cross-join columns _ independent-relation
     [:project post-group-by-projection
      [:group-by group-by-columns
       [:map pre-group-by-projection
        dependent-relation]]]]
    ;;=>
    (decorrelate-group-by-apply post-group-by-projection group-by-columns pre-group-by-projection
                                :cross-join columns independent-relation dependent-relation)

    [:apply :cross-join columns _ independent-relation
     [:group-by group-by-columns
      [:map pre-group-by-projection
       dependent-relation]]]
    ;;=>
    (decorrelate-group-by-apply nil group-by-columns pre-group-by-projection
                                :cross-join columns independent-relation dependent-relation)

    [:apply :cross-join columns _ independent-relation
     [:project post-group-by-projection
      [:group-by group-by-columns
       dependent-relation]]]
    ;;=>
    (decorrelate-group-by-apply post-group-by-projection group-by-columns nil
                                :cross-join columns independent-relation dependent-relation)

    [:apply :cross-join columns _ independent-relation
     [:group-by group-by-columns
      dependent-relation]]
    ;;=>
    (decorrelate-group-by-apply nil group-by-columns nil
                                :cross-join columns independent-relation dependent-relation)

    ;; Rule 9.
    [:apply :cross-join columns _ independent-relation
     [:max-1-row
      [:project post-group-by-projection
       [:group-by group-by-columns
        [:map pre-group-by-projection
         dependent-relation]]]]]
    ;;=>
    (decorrelate-group-by-apply post-group-by-projection group-by-columns pre-group-by-projection
                                :left-outer-join columns independent-relation dependent-relation)

    [:apply :cross-join columns _ independent-relation
     [:max-1-row
      [:group-by group-by-columns
       [:map pre-group-by-projection
        dependent-relation]]]]
    ;;=>
    (decorrelate-group-by-apply nil group-by-columns pre-group-by-projection
                                :left-outer-join columns independent-relation dependent-relation)

    [:apply :cross-join columns _ independent-relation
     [:max-1-row
      [:project post-group-by-projection
       [:group-by group-by-columns
        dependent-relation]]]]
    ;;=>
    (decorrelate-group-by-apply post-group-by-projection group-by-columns nil
                                :left-outer-join columns independent-relation dependent-relation)

    [:apply :cross-join columns _ independent-relation
     [:max-1-row
      [:group-by group-by-columns
       dependent-relation]]]
    ;;=>
    (decorrelate-group-by-apply nil group-by-columns nil
                                :left-outer-join columns independent-relation dependent-relation)))

(defn- potential-exists-predicate? [predicate]
  (or (symbol? predicate)
      (and (not-predicate? predicate)
           (symbol? (second predicate)))))

(defn- promote-apply-mode [z]
  (r/zmatch z
    [:select predicate-1
     [:select predicate-2
      relation]]
    ;;=>
    (when (and (potential-exists-predicate? predicate-1)
               (not (potential-exists-predicate? predicate-2)))
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
           (= #{predicate} dependent-column-names))
      [:apply :semi-join columns #{} independent-relation dependent-relation]

      (and (not-predicate? predicate)
           (symbol? (second predicate))
           (= #{(second predicate)} dependent-column-names))
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
          (let [fired-rules (atom [])
                instrument-rules (fn [rules]
                                   (apply
                                     some-fn
                                     (map (fn [f]
                                            (fn [z]
                                              (when-let [successful-rewrite (f z)]
                                                (swap!
                                                  fired-rules
                                                  #(conj
                                                     %
                                                     (second
                                                       (str/split (clojure.main/demunge (str f)) #"/|@"))))
                                                successful-rewrite)))
                                          rules)))
                optimize-plan [promote-selection-cross-join-to-join
                               promote-selection-to-join
                               push-selection-down-past-join
                               push-selection-down-past-apply
                               push-selection-down-past-rename
                               push-selection-down-past-project
                               push-selection-down-past-group-by
                               push-selections-with-fewer-variables-down
                               push-selections-with-equals-down
                               remove-superseded-projects
                               merge-selections-around-scan
                               add-selection-to-scan-predicate]
                decorrelate-plan [pull-correlated-selection-up-towards-apply
                                  push-selection-down-past-apply
                                  decorrelate-apply
                                  promote-apply-mode
                                  remove-uncorrelated-apply]
                plan (remove-names (plan ag))
                add-projection-fn (:add-projection-fn (meta plan))
                plan (->> plan
                          (z/vector-zip)
                          (r/innermost (r/mono-tp (instrument-rules decorrelate-plan)))
                          (r/innermost (r/mono-tp (instrument-rules optimize-plan)))
                          (z/node)
                          (add-projection-fn))]

            (if (s/invalid? (s/conform ::lp/logical-plan plan))
              (throw (IllegalArgumentException. (s/explain-str ::lp/logical-plan plan)))
              {:plan plan
               :fired-rules @fired-rules})))))))

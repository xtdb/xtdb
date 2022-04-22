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
  (:import (clojure.lang IObj Var)
           (java.time LocalDate Period Duration)
           (org.apache.arrow.vector PeriodDuration)))

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

     [:null_predicate ^:z rvp [:null_predicate_part_2 "IS" "NULL"]]
     ;;=>
     (list 'nil? (expr rvp))

     [:null_predicate ^:z rvp [:null_predicate_part_2 "IS" "NOT" "NULL"]]
     ;;=>
     (list 'not (list 'nil? (expr rvp)))

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
       "DAY" (PeriodDuration. (Period/ofDays (Long/parseLong year-month-literal)) Duration/ZERO)
       "MONTH" (PeriodDuration. (Period/ofMonths (Long/parseLong year-month-literal)) Duration/ZERO)
       "YEAR" (PeriodDuration. (Period/ofYears (Long/parseLong year-month-literal)) Duration/ZERO))

     [:current_date_value_function _] '(current-date)
     [:current_time_value_function _] '(current-time)
     [:current_time_value_function _ ^:z tp] (list 'current-time (expr tp))
     [:current_timestamp_value_function _] '(current-timestamp)
     [:current_timestamp_value_function _ ^:z tp] (list 'current-timestamp (expr tp))
     [:current_local_time_value_function _] '(local-time)
     [:current_local_time_value_function _ ^:z tp] (list 'local-time (expr tp))
     [:current_local_timestamp_value_function _] '(local-timestamp)
     [:current_local_timestamp_value_function _ ^:z tp] (list 'local-timestamp (expr tp))

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

     [:between_predicate ^:z rvp-1 [:between_predicate_part_2 "NOT" "BETWEEN" ^:z rvp-2 "AND" ^:z rvp-3]]
     ;;=>
     (list 'not (list 'between (expr rvp-1) (expr rvp-2) (expr rvp-3)))

     [:between_predicate ^:z rvp-1 [:between_predicate_part_2 "BETWEEN" mode ^:z rvp-2 "AND" ^:z rvp-3]]
     ;;=>
     (let [f (case mode
               "SYMMETRIC" 'between-symmetric
               "ASYMMETRIC" 'between)]
       (list f (expr rvp-1) (expr rvp-2) (expr rvp-3))
       (list f (expr rvp-1) (expr rvp-2) (expr rvp-3)))

     [:between_predicate ^:z rvp-1 [:between_predicate_part_2 "NOT" "BETWEEN" mode ^:z rvp-2 "AND" ^:z rvp-3]]
     ;;=>
     (let [f (case mode
               "SYMMETRIC" 'between-symmetric
               "ASYMMETRIC" 'between)]
       (list 'not (list f (expr rvp-1) (expr rvp-2) (expr rvp-3)))
       (list 'not (list f (expr rvp-1) (expr rvp-2) (expr rvp-3))))

     [:extract_expression "EXTRACT"
      [:primary_datetime_field [:non_second_primary_datetime_field extract-field]] "FROM" ^:z es]
     ;;=>
     (list 'extract extract-field (expr es))

     [:modulus_expression _ ^:z nve-1 ^:z nve-2]
     ;;=>
     (list 'mod (expr nve-1) (expr nve-2))

     [:power_function _ ^:z nve-1 ^:z nve-2]
     ;;=>
     (list 'power (expr nve-1) (expr nve-2))

     [:absolute_value_expression _ ^:z nve]
     ;;=>
     (list 'abs (expr nve))

     [:ceiling_function _ ^:z nve]
     ;;=>
     (list 'ceil (expr nve))

     [:floor_function _ ^:z nve]
     ;;=>
     (list 'floor (expr nve))

     [:natural_logarithm _ ^:z nve]
     ;;=>
     (list 'ln (expr nve))

     [:exponential_function _ ^:z nve]
     ;;=>
     (list 'exp (expr nve))

     [:common_logarithm _ ^:z nve]
     ;;=>
     (list 'log10 (expr nve))

     [:general_logarithm_function _ ^:z nve-1 ^:z nve-2]
     ;;=>
     (list 'log (expr nve-1) (expr nve-2))

     [:trigonometric_function [:trigonometric_function_name tfn] ^:z nve]
     ;;=>
     (list (symbol (str/lower-case tfn)) (expr nve))

     [:trim_function _ [:trim_operands [:trim_specification trim-spec] ^:z trim-char _ ^:z nve]]
     (list 'trim (expr nve) trim-spec (expr trim-char))

     [:trim_function _ [:trim_operands [:trim_specification trim-spec] _ ^:z nve]]
     (list 'trim (expr nve) trim-spec " ")

     [:trim_function _ [:trim_operands ^:z nve]]
     (list 'trim (expr nve) "BOTH" " ")

     [:fold mode ^:z nve]
     (case mode
       "LOWER" (list 'lower (expr nve))
       "UPPER" (list 'upper (expr nve)))

     [:concatenation ^:z nve1 _ ^:z nve2]
     (list 'concat (expr nve1) (expr nve2))

     [:char_length_expression _ ^:z nve]
     (list 'character-length (expr nve) "CHARACTERS")

     [:char_length_expression _ ^:z nve _ [:char_length_units unit]]
     (list 'character-length (expr nve) unit)

     [:octet_length_expression _ ^:z nve]
     (list 'octet-length (expr nve))

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

     [:array_element_reference ^:z ave ^:z nve]
     ;;=>
     (list 'nth (expr ave) (expr nve))

     [:dynamic_parameter_specification _]
     ;;=>
     (symbol (str "?_" (sem/dynamic-param-idx z)))

     (r/zcase z
       :case_abbreviation
       (->> (r/collect-stop
             (fn [z]
               (r/zcase z
                 (:case_abbreviation nil) nil
                 [(expr z)]))
             z)
            (cons (case (r/lexeme z 1)
                    "COALESCE" 'coalesce
                    "NULLIF" 'nullif)))

       :searched_case
       (->> (r/collect-stop
             (fn [z]
               (r/zmatch z
                 [:searched_when_clause "WHEN" ^:z sc "THEN" [:result ^:z then]]
                 [(expr sc) (expr then)]

                 [:else_clause "ELSE" [:result ^:z else]]
                 [(expr else)]))
             z)
            (cons 'cond))

       :simple_case
       (->> (r/collect-stop
             (fn [z]
               (r/zmatch z
                 [:case_operand ^:z rvp]
                 [(expr rvp)]

                 [:simple_when_clause "WHEN" ^:z wol "THEN" [:result ^:z then]]
                 (let [then-expr (expr then)
                       wo-exprs (r/collect-stop
                                 (fn [z]
                                   (r/zcase z
                                     (:when_operand_list nil) nil
                                     [(expr z)]))
                                 wol)]
                   (->> (for [wo-expr wo-exprs]
                          [wo-expr then-expr])
                        (reduce into [])))

                 [:else_clause "ELSE" [:result ^:z else]]
                 [(expr else)]))
             z)
            (cons 'case))

       (throw (IllegalArgumentException. (str "Cannot build expression for: "  (pr-str (z/node z)))))))))

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

(defn- build-apply [apply-mode column->param projected-dependent-columns independent-relation dependent-relation]
  [:apply
   apply-mode
   column->param
   projected-dependent-columns
   independent-relation
   (w/postwalk-replace column->param dependent-relation)])

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

(defn- and-predicate? [predicate]
  (and (sequential? predicate)
       (= 'and (first predicate))))

(defn- or-predicate? [predicate]
  (and (sequential? predicate)
       (= 'or (first predicate))))

(defn- flatten-expr [pred expr]
  (if (pred expr)
    (concat (flatten-expr pred (nth expr 1))
            (flatten-expr pred (nth expr 2)))
    [expr]))

;; subquery anti/semi optimisation
;; EXISTS - type subqueries can be optimised in certain cases to semi/anti joins.

;; A decision was taken to optimise when we build the initial plan, rather than as a rewrite rule
;; due to the possibility of inter node dependence on the pre-optimised cross apply:

;; imagine a node [:select $exists [:apply :cross-join ...]] we may chose to optimise this away into [:apply :semi-join ...]
;; a problem arises in that the projected column set of the previous relation contains columns that are not returned by the :semi-join.
;; Namely the $exists column. This can interfere with other rules and introduce ordering dependencies between rules.

;; e.g decorrelation rule-9 caused a problem in this case as it introduced a dependency in the group-by on the $exists column - only to later find
;; that column had been rewritten away by the semi/anti join apply rule.

(defn- interpret-subquery
  "Returns a map of data about the given subquery AST zipper.

  Many sub queries can be treated as different exists checks, for example ANY / ALL/ IN / EXISTS / NOT EXISTS can be
  optimised in a similar way to semi/anti join applies.

  Returns a 'subquery info' map. See its usage in apply-subqery/apply-predicative-subquery."
  [sq]
  (let [scope-id (sem/id (sem/scope-element sq))]
    (r/zmatch sq
      [:subquery ^:z qe]
      (let [subquery-plan [:rename (subquery-reference-symbol qe) (plan qe)]
            column->param (correlated-column->param qe scope-id)
            subquery-type (sem/subquery-type sq)
            projected-columns (set (subquery-projection-symbols qe))]
        {:type :subquery
         :plan (case (:type subquery-type)
                 (:scalar_subquery
                   :row_subquery) [:max-1-row subquery-plan]
                 subquery-plan)
         :projected-columns projected-columns
         :column->param column->param})

      [:exists_predicate _
       [:subquery ^:z qe]]
      (let [exists-symbol (exists-symbol qe)
            subquery-plan [:rename (subquery-reference-symbol qe) (plan qe)]
            column->param (correlated-column->param qe scope-id)
            projected-columns #{exists-symbol}]
        {:type :exists
         :plan subquery-plan
         :projected-columns projected-columns
         :column->param column->param
         :sym exists-symbol})

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
            exists-symbol (exists-symbol qe)
            predicate (list '= (expr rvp) (first (subquery-projection-symbols qe)))
            subquery-plan [:select predicate [:rename (subquery-reference-symbol qe) (plan qe)]]
            column->param (merge (correlated-column->param qe scope-id)
                                 (correlated-column->param rvp scope-id))
            projected-columns #{exists-symbol}]
        {:type :exists
         :plan subquery-plan
         :projected-columns projected-columns
         :column->param column->param
         :sym exists-symbol})

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
            subquery-plan [:select predicate [:rename (subquery-reference-symbol qe) (plan qe)]]
            column->param (merge (correlated-column->param qe scope-id)
                                 (correlated-column->param rvp scope-id))
            projected-columns #{exists-symbol}]
        {:type :exists
         :plan subquery-plan
         :projected-columns projected-columns
         :column->param column->param
         :sym exists-symbol})

      (throw (IllegalArgumentException. "unknown subquery type")))))

(defn- apply-subquery
  "Ensures the subquery projection is available on the outer relation. Used in the general case when we are using
  the subqueries projected column in a predicate, or as part of a projection itself.

   e.g select foo.a from foo where foo.a = (select bar.a from bar where bar.c = a.c)

   See (interpret-subquery) for 'subquery info'."
  [relation subquery-info]
  (let [{:keys [type plan projected-columns column->param sym]} subquery-info]
    (case type
      :exists (build-apply :cross-join column->param projected-columns relation (wrap-with-exists sym plan))
      (build-apply :cross-join column->param projected-columns relation plan))))

(defn- apply-predicative-subquery
  "In certain situations, we are able to optimise subqueries into semi or anti joins, and simplify the resulting plan.

  The predicate-set is a set of all top level conjunctive predicates for the WHERE / ON clause. e.g `where a = b and c = 42` would give the predicate set `#{(= a b), (= c 42)}`.

  In order to apply a subquery with the semi/anti join apply mode the following has to be true:

  - the subquery is an EXISTS / NOT EXISTS check, or can be viewed as one. e.g IN/ANY/ALL also count as EXISTS checks.
  - we are using the subquery in a setting such as in an ON clause, or a WHERE clause where all we are trying to do is filter rows.
  - The top level predicate that tests the query result must be 'simple' e.g $exists / (not $exists) are ok, but (or $exists (= 42 c)) is not. (this is why we need the predicate set).

  Returns a pair of new [relation, predicate-set]. The returned relation will then be wrapped in an [:apply ...] whose apply mode
  is dependent on whether or not a semi/anti optimisation can be applied.

  The returned predicate set contains only those predicates that still need to be tested in some outer [:select] or theta join predicate.

  See also (wrap-with-select)."
  [relation subquery-info predicate-set]
  (let [{:keys [type plan column->param sym]} subquery-info]
    (cond
      (not= :exists type) [(apply-subquery relation subquery-info) predicate-set]
      (predicate-set sym) [(build-apply :semi-join column->param #{} relation plan) (disj predicate-set sym)]
      (predicate-set (list 'not sym)) [(build-apply :anti-join column->param #{} relation plan) (disj predicate-set (list 'not sym))]
      :else [(apply-subquery relation subquery-info) predicate-set])))

(defn- find-sub-queries [z]
  (r/collect-stop
    (fn [z] (r/zcase z (:subquery :exists_predicate :in_predicate :quantified_comparison_predicate) [z] nil))
    z))

;; TODO: deal with row subqueries.
(defn- wrap-with-apply [z relation]
  (reduce (fn [relation sq] (apply-subquery relation (interpret-subquery sq))) relation (find-sub-queries z)))

;; https://www.spoofax.dev/background/stratego/strategic-rewriting/term-rewriting/
;; https://www.spoofax.dev/background/stratego/strategic-rewriting/limitations-of-rewriting/

(defn- prop-eval-rules [z]
  (r/zmatch z
    [:not true]
    ;;=>
    false

    [:not false]
    ;;=>
    true

    [:and true x]
    ;;=>
    x

    [:and x true]
    ;;=>
    x

    [:and false x]
    ;;=>
    false

    [:and x false]
    ;;=>
    false

    [:or true x]
    ;;=>
    true

    [:or x true]
    ;;=>
    true

    [:or false x]
    ;;=>
    x

    [:or x false]
    ;;=>
    x))

(defn- prop-simplify [z]
  (r/zmatch z
    [:not [:not x]]
    ;;=>
    x

    [:not [:and x y]]
    ;;=>
    `(~'or (~'not ~x) (~'not ~y))

    [:not [:or x y]]
    ;;=>
    `(~'and (~'not ~x) (~'not ~y))))

(defn- prop-further-simplify [z]
  (r/zmatch z
    [:and x x]
    ;;=>
    x

    [:or x x]
    ;;=>
    x

    [:and x [:or x y]]
    ;;=>
    x

    [:or x [:and x y]]
    ;;=>
    x

    [:and [:or x y] [:or y x]]
    ;;=>
    `(~'or ~x ~y)))

(defn- prop-dnf [z]
  (r/zmatch z
    [:and [:or x y] z]
    ;;=>
    `(~'or (~'and ~x ~z) (~'and ~y ~z))

    [:and z [:or x y]]
    ;;=>
    `(~'or (~'and ~z ~x) (~'and ~z ~y))))

(defn- prop-cnf [z]
  (r/zmatch z
    [:or [:and x y] z]
    ;;=>
    `(~'and (~'or ~x ~z) (~'or ~y ~z))

    [:or z [:and x y]]
    ;;=>
    `(~'and (~'or ~z ~x) (~'or ~z ~y))))

(defn- dnf [predicate]
  (->> (r/->zipper predicate)
       (r/innermost (r/mono-tp (some-fn prop-eval-rules prop-simplify prop-further-simplify prop-dnf)))
       (z/node)))

(defn- cnf [predicate]
  (->> (r/->zipper predicate)
       (r/innermost (r/mono-tp (some-fn prop-eval-rules prop-simplify prop-further-simplify prop-cnf)))
       (z/node)))

(defn- boolean-constraint-propagation [cnf-clauses]
  (loop [clauses (distinct (for [clause cnf-clauses]
                             (if (or-predicate? clause)
                               (cons 'or (flatten-expr or-predicate? clause))
                               clause)))]
    (let [units (set (remove or-predicate? clauses))
          not-units (set (for [u units]
                           `(~'not ~u)))
          new-clauses (->> (for [clause clauses
                                 :let [clause (if (or-predicate? clause)
                                                (let [literals (remove not-units (rest clause))]
                                                  (when-not (some units literals)
                                                    (cons 'or literals)))
                                                clause)]
                                 :when (some? clause)]
                             clause)
                           (distinct))]
      (if (= clauses new-clauses)
        new-clauses
        (recur new-clauses)))))

(defn- predicate-conjunctive-clauses [predicate]
  (if (or-predicate? predicate)
    (let [disjuncts (->> (flatten-expr or-predicate? predicate)
                         (map predicate-conjunctive-clauses))
          common-disjuncts (->> (map set disjuncts)
                                (reduce set/intersection))
          disjuncts (for [disjunct disjuncts
                          :let [disjunct (remove common-disjuncts disjunct)]
                          :when (seq disjunct)]
                      (if (= 1 (count disjunct))
                        (first disjunct)
                        (cons 'and disjunct)))
          disjuncts (if (> (count disjuncts) 1)
                      (cons 'or disjuncts)
                      (first disjuncts))]
      (cond-> (seq common-disjuncts)
        disjuncts (concat [disjuncts])))
    (flatten-expr and-predicate? predicate)))

(defn- wrap-with-select [sc relation]
  (let [sc-expr (expr sc)
        predicates (predicate-conjunctive-clauses sc-expr)
        predicate-set (set predicates)
        [new-relation predicate-set]
        (reduce
          (fn [[new-relation predicate-set] sq]
            (apply-predicative-subquery new-relation (interpret-subquery sq) predicate-set))
          [relation predicate-set]
          (find-sub-queries sc))
        unused-predicates (filter predicate-set predicates)]
    (reduce
      (fn [acc predicate]
        [:select predicate acc])
      new-relation
      unused-predicates)))

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

                     [:aggregate_function [:general_set_function [:computational_operation sf] [:set_quantifier sq] ^:z ve]]
                     {:in {(aggregate-symbol "agg_in" aggregate) (expr ve)}
                      :out {(aggregate-symbol "agg_out" aggregate)
                            (list (symbol (str (str/lower-case sf) "-" (str/lower-case sq))) (aggregate-symbol "agg_in" aggregate))}}

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
                          (letfn [(->order-by-spec [sk os no]
                                    (let [direction (case os
                                                      "ASC" :asc
                                                      "DESC" :desc
                                                      :asc)
                                          null-ordering (case no
                                                          "FIRST" :nulls-first
                                                          "LAST" :nulls-last
                                                          :nulls-last)]
                                      [(if-let [idx (sem/order-by-index z)]
                                         {:spec [(unqualified-projection-symbol (nth projection idx)) direction null-ordering]}
                                         (let [column (symbol (str "$order_by" relation-id-delimiter query-id relation-prefix-delimiter (r/child-idx z) "$"))]
                                           {:spec [column direction null-ordering]
                                            :projection {column (expr sk)}}))]))]

                            (r/zmatch z
                              [:sort_specification ^:z sk] (->order-by-spec sk nil nil)
                              [:sort_specification ^:z sk [:ordering_specification os]] (->order-by-spec sk os nil)
                              [:sort_specification ^:z sk [:null_ordering _ no]] (->order-by-spec sk nil no)
                              [:sort_specification ^:z sk [:ordering_specification os] [:null_ordering _ no]] (->order-by-spec sk os no)

                              (r/zcase z
                                :subquery []
                                nil))))
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
      (build-apply :cross-join column->param projected-columns nil relation))))

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
     (wrap-with-select sc [:join [] (plan lhs) (plan rhs)])

     [:qualified_join ^:z lhs ^:z jt _ ^:z rhs [:join_condition _ ^:z sc]]
     ;;=>
     (wrap-with-select sc (case (sem/join-type jt)
                            "LEFT" [:left-outer-join [] (plan lhs) (plan rhs)]
                            "RIGHT" [:left-outer-join [] (plan rhs) (plan lhs)]
                            "INNER" [:join [] (plan lhs) (plan rhs)]))

     [:qualified_join ^:z lhs _ ^:z rhs ^:z ncj]
     ;;=>
     (wrap-with-select ncj [:join [] (plan lhs) (plan rhs)])

     [:qualified_join ^:z lhs ^:z jt _ ^:z rhs ^:z ncj]
     ;;=>
     (wrap-with-select ncj (case (sem/join-type jt)
                             "LEFT" [:left-outer-join [] (plan lhs) (plan rhs)]
                             "RIGHT" [:left-outer-join [] (plan rhs) (plan lhs)]
                             "INNER" [:join [] (plan lhs) (plan rhs)]))

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
                  new-smap (reduce
                             (fn [acc p]
                               (if (extend-projection? p)
                                 (let [[k v] (first p)]
                                   (if (symbol? v)
                                     (assoc acc k (smap v))
                                     (assoc acc k (next-name))))
                                 (assoc acc p (smap p))))
                             (if (= op :map)
                               smap
                               (->> smap
                                    (filter #(str/starts-with? (name (key %)) "?"))
                                    (into {})))
                             projection)
                  projection (w/postwalk-replace smap projection)

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
                  relation (if (every? symbol? projection)
                             relation
                             [op
                              projection
                              relation])]
              (with-smap relation new-smap)))]
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
      (let [smap (->smap relation)]
        (with-smap relation
          (if (symbol? prefix-or-columns)
            (update-keys smap #(if (str/starts-with? (name %) "?")
                                 %
                                 (symbol (str prefix-or-columns relation-prefix-delimiter %))))
            (set/rename-keys smap prefix-or-columns))))

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
            new-smap (merge smap params)]
        (with-smap [:apply mode
                    (w/postwalk-replace new-smap columns)
                    (w/postwalk-replace new-smap dependent-column-names)
                    independent-relation
                    (w/postwalk-replace new-smap dependent-relation)]
          (case mode
           (:cross-join :left-outer-join) new-smap
           (:semi-join :anti-join) (merge (->smap independent-relation) params))))

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
                   (z/node (r/bottomup (r/adhoc-tp r/id-tp remove-names-step) (z/vector-zip relation))))
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
    [:join [predicate] lhs rhs]))

(defn- promote-selection-to-join [z]
  (r/zmatch z
    [:select predicate
     [:join join-condition lhs rhs]]
    ;;=>
    (when (columns-in-both-relations? predicate lhs rhs)
      [:join (conj join-condition predicate) lhs rhs])

    [:select predicate
     [:left-outer-join join-condition lhs rhs]]
    ;;=>
    (when (columns-in-both-relations? predicate lhs rhs)
      [:left-outer-join (conj join-condition predicate) lhs rhs])

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
     [:apply mode columns dependent-column-names independent-relation dependent-relation]]
    ;;=>
    (when (and (no-correlated-columns? predicate)
               (empty? (set/intersection (expr-symbols predicate) dependent-column-names)))
      (cond
        (columns-in-predicate-present-in-relation? independent-relation predicate)
        [:apply
         mode
         columns
         dependent-column-names
         [:select predicate independent-relation]
         dependent-relation]

        (columns-in-predicate-present-in-relation? dependent-relation predicate)
        [:apply
         mode
         columns
         dependent-column-names
         independent-relation
         [:select predicate dependent-relation]]))))

(defn- push-selection-down-past-rename [push-correlated? z]
  (r/zmatch z
    [:select predicate
     [:rename columns
      relation]]
    ;;=>
    (when (and (or push-correlated? (no-correlated-columns? predicate))
               (map? columns))
      [:rename columns
       [:select (w/postwalk-replace (set/map-invert columns) predicate)
        relation]])))

(defn- predicate-depends-on-calculated-expression? [predicate projection]
  (not (set/subset? (set (expr-symbols predicate))
                    (set (filter symbol? projection)))))

(defn- push-selection-down-past-project [push-correlated? z]
  (r/zmatch z
    [:select predicate
     [:project projection
      relation]]
    ;;=>
    (when (and (or push-correlated? (no-correlated-columns? predicate))
               (not (predicate-depends-on-calculated-expression? predicate projection)))
      [:project projection
       [:select predicate
        relation]])

    [:select predicate
     [:map projection
      relation]]
    ;;=>
    (when (and (or push-correlated? (no-correlated-columns? predicate))
               (not (predicate-depends-on-calculated-expression? predicate (relation-columns relation))))
      [:map projection
       [:select predicate
        relation]])))

(defn- push-selection-down-past-group-by [push-correlated? z]
  (r/zmatch z
    [:select predicate
     [:group-by group-by-columns
      relation]]
    ;;=>
    (when (and (or push-correlated? (no-correlated-columns? predicate))
               (not (predicate-depends-on-calculated-expression? predicate group-by-columns)))
      [:group-by group-by-columns
       [:select predicate
        relation]])))

(defn- push-selection-down-past-join [push-correlated? z]
  (r/zmatch z
    [:select predicate
     [join-op join-map lhs rhs]]
    ;;=>
    (when (or push-correlated? (no-correlated-columns? predicate))
      (cond
        (columns-in-predicate-present-in-relation? rhs predicate)
        [join-op join-map lhs [:select predicate rhs]]
        (columns-in-predicate-present-in-relation? lhs predicate)
        [join-op join-map [:select predicate lhs] rhs]))

    [:select predicate
     [:cross-join lhs rhs]]
    ;;=>
    (when (or push-correlated? (no-correlated-columns? predicate))
      (cond
        (columns-in-predicate-present-in-relation? rhs predicate)
        [:cross-join lhs [:select predicate rhs]]
        (columns-in-predicate-present-in-relation? lhs predicate)
        [:cross-join [:select predicate lhs] rhs]))))

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


(defn- remove-unused-correlated-columns [columns dependent-relation]
  (->> columns
       (filter (comp (expr-correlated-symbols dependent-relation) val))
       (into {})))

(defn- decorrelate-group-by-apply [post-group-by-projection group-by-columns
                                   apply-mode columns independent-relation dependent-relation]
  (let [independent-projection (relation-columns independent-relation)
        smap (set/map-invert columns)
        row-number-sym '$row_number$
        columns (remove-unused-correlated-columns columns dependent-relation)
        post-group-by-projection (remove symbol? post-group-by-projection)]
    (cond->> [:group-by (vec (concat independent-projection
                                     [row-number-sym]
                                     (w/postwalk-replace smap group-by-columns)))
              [:apply apply-mode columns (set (relation-columns dependent-relation))
               [:map [{row-number-sym '(row-number)}]
                independent-relation]
               dependent-relation]]
      (not-empty post-group-by-projection)
      (conj [:map (vec (w/postwalk-replace
                        smap
                        post-group-by-projection))]))))

(defn parameters-in-e-resolved-from-r? [apply-columns]
  (boolean
    (some
      #(= "x" (subs (str (key %)) 0 1))
      apply-columns)))

(defn- decorrelate-apply-rule-1
  "R A⊗ E = R ⊗true E
  if no parameters in E resolved from R"
  [z]
  (r/zmatch
    z
    [:apply :cross-join columns _ independent-relation dependent-relation]
    ;;=>
    (when-not (parameters-in-e-resolved-from-r? columns)
      [:cross-join independent-relation dependent-relation])

    [:apply mode columns _ independent-relation dependent-relation]
    ;;=>
    (when-not (parameters-in-e-resolved-from-r? columns)
      [mode [] independent-relation dependent-relation])))

(defn- remove-parameters-in-predicate-from-apply-columns [columns predicate]
  (->> columns
       (remove (comp (expr-correlated-symbols predicate) val))
       (into {})))

(defn- decorrelate-apply-rule-2
  "R A⊗(σp E) = R ⊗p E
  if no parameters in E resolved from R"
  [z]
  (r/zmatch
    z
    [:apply mode columns _dependent-column-names independent-relation
     [:select predicate dependent-relation]]
    ;;=>
    (when (seq (expr-correlated-symbols predicate))
      (when-not (parameters-in-e-resolved-from-r?
                  (remove-parameters-in-predicate-from-apply-columns columns predicate))
        [(if (= :cross-join mode)
           :join
           mode)
         [(w/postwalk-replace (set/map-invert columns)
                              (if (all-predicate? predicate)
                                (second predicate)
                                predicate))]
         independent-relation dependent-relation]))))

(defn- decorrelate-apply-rule-3
  "R A× (σp E) = σp (R A× E)"
  [z]
  (r/zmatch z
    [:apply :cross-join columns dependent-column-names independent-relation [:select predicate dependent-relation]]
    ;;=>
    (when (seq (expr-correlated-symbols predicate)) ;; select predicate is correlated
      [:select (w/postwalk-replace (set/map-invert columns) (if (all-predicate? predicate)
                                                              (second predicate)
                                                              predicate))
       (let [columns (remove-unused-correlated-columns columns dependent-relation)]
         [:apply :cross-join columns dependent-column-names independent-relation dependent-relation])])))

(defn- decorrelate-apply-rule-4
  "R A× (πv E) = πv ∪ columns(R) (R A× E)"
  [z]
  (r/zmatch z
    [:apply :cross-join columns dependent-column-names independent-relation [:project projection dependent-relation]]
    ;;=>
    [:project (vec (concat (relation-columns independent-relation)
                           (w/postwalk-replace (set/map-invert columns) projection)))
     (let [columns (remove-unused-correlated-columns columns dependent-relation)]
       [:apply :cross-join columns dependent-column-names independent-relation dependent-relation])]))

(defn- decorrelate-apply-rule-8
  "R A× (G A,F E) = G A ∪ columns(R),F (R A× E)"
  [z]
  (r/zmatch z
    [:apply :cross-join columns _ independent-relation
     [:project post-group-by-projection
      [:group-by group-by-columns
       dependent-relation]]]
    ;;=>
    (decorrelate-group-by-apply post-group-by-projection group-by-columns
                                :cross-join columns independent-relation dependent-relation)

    [:apply :cross-join columns _ independent-relation
     [:group-by group-by-columns
      dependent-relation]]
    ;;=>
    (decorrelate-group-by-apply nil group-by-columns
                                :cross-join columns independent-relation dependent-relation)))

(defn- decorrelate-apply-rule-9
  "R A× (G F1 E) = G columns(R),F' (R A⟕ E)"
  [z]
  (r/zmatch z
    [:apply :cross-join columns _ independent-relation
     [:max-1-row
      [:project post-group-by-projection
       [:group-by group-by-columns
        dependent-relation]]]]
    ;;=>
    (decorrelate-group-by-apply post-group-by-projection group-by-columns
                                :left-outer-join columns independent-relation dependent-relation)

    [:apply :cross-join columns _ independent-relation
     [:max-1-row
      [:group-by group-by-columns
       dependent-relation]]]
    ;;=>
    (decorrelate-group-by-apply nil group-by-columns
                                :left-outer-join columns independent-relation dependent-relation)))

(defn- optimize-join-expression [join-expressions lhs rhs]
  (if (some map? join-expressions)
    join-expressions
    (->> join-expressions
         (mapcat #(flatten-expr and-predicate? %))
         (mapv (fn [join-clause]
                 (if (and (equals-predicate? join-clause)
                          (every? symbol? (rest join-clause)) ;; only contains columns
                          (columns-in-both-relations? join-clause lhs rhs))
                   (let [[_ x y] join-clause]
                     (if (contains? (set (relation-columns lhs)) x)
                       {x y}
                       {y x}))
                   join-clause))))))

(defn- rewrite-equals-predicates-in-join-as-equi-join-map [z]
  (r/zmatch z
    [:join join-expressions lhs rhs]
    ;;=>
    (let [new-join-expressions (optimize-join-expression join-expressions lhs rhs)]
      (when (not= new-join-expressions join-expressions)
        [:join new-join-expressions lhs rhs]))

    [:semi-join join-expressions lhs rhs]
    ;;=>
    (let [new-join-expressions (optimize-join-expression join-expressions lhs rhs)]
      (when (not= new-join-expressions join-expressions)
        [:semi-join new-join-expressions lhs rhs]))


    [:anti-join join-expressions lhs rhs]
    ;;=>
    (let [new-join-expressions (optimize-join-expression join-expressions lhs rhs)]
      (when (not= new-join-expressions join-expressions)
        [:anti-join new-join-expressions lhs rhs]))


    [:left-outer-join join-expressions lhs rhs]
    ;;=>
    (let [new-join-expressions (optimize-join-expression join-expressions lhs rhs)]
      (when (not= new-join-expressions join-expressions)
        [:left-outer-join new-join-expressions lhs rhs]))))

(def push-correlated-selection-down-past-join (partial push-selection-down-past-join true))
(def push-correlated-selection-down-past-rename (partial push-selection-down-past-rename true))
(def push-correlated-selection-down-past-project (partial push-selection-down-past-project true))
(def push-correlated-selection-down-past-group-by (partial push-selection-down-past-group-by true))
(def push-correlated-selections-with-fewer-variables-down (partial push-selections-with-fewer-variables-down true))

(def push-decorrelated-selection-down-past-join (partial push-selection-down-past-join false))
(def push-decorrelated-selection-down-past-rename (partial push-selection-down-past-rename false))
(def push-decorrelated-selection-down-past-project (partial push-selection-down-past-project false))
(def push-decorrelated-selection-down-past-group-by (partial push-selection-down-past-group-by false))
(def push-decorrelated-selections-with-fewer-variables-down (partial push-selections-with-fewer-variables-down false))

;; Logical plan API

(defn plan-query [query]
  (if-let [parse-failure (insta/get-failure query)]
    {:errs [(prn-str parse-failure)]}
    (r/with-memoized-attributes [sem/id
                                 sem/dynamic-param-idx
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
                                                  #(conj ;; could also capture z before the rewrite
                                                         %
                                                         [(name (.toSymbol ^Var f))
                                                          successful-rewrite]))
                                                successful-rewrite)))
                                          rules)))
                optimize-plan [#'promote-selection-cross-join-to-join
                               #'promote-selection-to-join
                               #'push-selection-down-past-apply
                               #'push-correlated-selection-down-past-join
                               #'push-correlated-selection-down-past-rename
                               #'push-correlated-selection-down-past-project
                               #'push-correlated-selection-down-past-group-by
                               #'push-correlated-selections-with-fewer-variables-down
                               #'remove-superseded-projects
                               #'merge-selections-around-scan
                               #'add-selection-to-scan-predicate]
                decorrelate-plan [#'pull-correlated-selection-up-towards-apply
                                  #'push-selection-down-past-apply
                                  #'push-decorrelated-selection-down-past-join
                                  #'push-decorrelated-selection-down-past-rename
                                  #'push-decorrelated-selection-down-past-project
                                  #'push-decorrelated-selection-down-past-group-by
                                  #'push-decorrelated-selections-with-fewer-variables-down
                                  #'squash-correlated-selects
                                  #'decorrelate-apply-rule-1
                                  #'decorrelate-apply-rule-2
                                  #'decorrelate-apply-rule-3
                                  #'decorrelate-apply-rule-4
                                  #'decorrelate-apply-rule-8
                                  #'decorrelate-apply-rule-9]
                plan (remove-names (plan ag))
                add-projection-fn (:add-projection-fn (meta plan))
                plan (->> plan
                          (z/vector-zip)
                          (r/innermost (r/mono-tp (instrument-rules decorrelate-plan)))
                          (r/innermost (r/mono-tp (instrument-rules optimize-plan)))
                          (r/topdown (r/adhoc-tp r/id-tp (instrument-rules [#'rewrite-equals-predicates-in-join-as-equi-join-map])))
                          (z/node)
                          (add-projection-fn))]

            (if (s/invalid? (s/conform ::lp/logical-plan plan))
              (throw (IllegalArgumentException. (s/explain-str ::lp/logical-plan plan)))
              {:plan plan
               :fired-rules @fired-rules})))))))

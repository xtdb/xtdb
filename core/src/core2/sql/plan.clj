(ns core2.sql.plan
  (:require [clojure.set :as set]
            [clojure.main]
            [clojure.string :as str]
            [clojure.walk :as w]
            [clojure.spec.alpha :as s]
            [core2.logical-plan :as lp]
            core2.operator ;; Adds impls logical plan spec
            [core2.rewrite :as r]
            [core2.sql.analyze :as sem]
            [core2.sql.parser :as p]
            [core2.types :as types])
  (:import (clojure.lang IObj Var)
           (java.time LocalDate Period Duration OffsetDateTime ZoneOffset OffsetTime LocalDateTime)
           java.util.HashMap
           (org.apache.arrow.vector PeriodDuration)))

;; Attribute grammar for transformation into logical plan.

;; See https://cs.ulb.ac.be/public/_media/teaching/infoh417/sql2alg_eng.pdf

(def ^:private ^:const ^String relation-id-delimiter "__")
(def ^:private ^:const ^String relation-prefix-delimiter "_")

(def ^:dynamic *include-table-column-in-scan?* false)

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

(defn- subquery-array-symbol
  "When you have a ARRAY ( subquery ) expression, use this to reference the projected column
  containing the array."
  [qe]
  (some-> (subquery-projection-symbols qe) first (str "-array")))

;; Expressions.

(defn- interval-expr
  [e qualifier]
  (r/zmatch qualifier

    [:interval_qualifier
     [:single_datetime_field [:non_second_primary_datetime_field datetime-field]]]
    ;; =>
    (list 'single-field-interval e datetime-field 2 0)

    [:interval_qualifier
     [:single_datetime_field [:non_second_primary_datetime_field datetime-field] [:unsigned_integer leading-precision]]]
    ;; =>
    (list 'single-field-interval e datetime-field (parse-long leading-precision) 0)

    [:interval_qualifier
     [:single_datetime_field "SECOND"]]
    ;; =>
    (list 'single-field-interval e "SECOND" 2 6)

    [:interval_qualifier
     [:single_datetime_field "SECOND" [:unsigned_integer leading-precision]]]
    ;; =>
    (list 'single-field-interval e "SECOND" (parse-long leading-precision) 6)

    [:interval_qualifier
     [:single_datetime_field "SECOND" [:unsigned_integer leading-precision] [:unsigned_integer fractional-precision]]]
    ;; =>
    (list 'single-field-interval e "SECOND" (parse-long leading-precision) (parse-long fractional-precision))

    [:interval_qualifier
     [:start_field [:non_second_primary_datetime_field leading-field]]
     "TO"
     [:end_field [:non_second_primary_datetime_field trailing-field]]]
    ;; =>
    (list 'multi-field-interval e leading-field 2 trailing-field 2)

    [:interval_qualifier
     [:start_field [:non_second_primary_datetime_field leading-field] [:unsigned_integer leading-precision]]
     "TO"
     [:end_field [:non_second_primary_datetime_field trailing-field]]]
    ;; =>
    (list 'multi-field-interval e leading-field (parse-long leading-precision) trailing-field 2)

    [:interval_qualifier
     [:start_field [:non_second_primary_datetime_field leading-field]]
     "TO"
     [:end_field "SECOND"]]
    ;; =>
    (list 'multi-field-interval e leading-field 2 "SECOND" 6)

    [:interval_qualifier
     [:start_field [:non_second_primary_datetime_field leading-field] [:unsigned_integer leading-precision]]
     "TO"
     [:end_field "SECOND"]]
    ;; =>
    (list 'multi-field-interval e leading-field (parse-long leading-precision) "SECOND" 6)

    [:interval_qualifier
     [:start_field [:non_second_primary_datetime_field leading-field] [:unsigned_integer leading-precision]]
     "TO"
     [:end_field "SECOND" [:unsigned_integer fractional-precision]]]
    ;; =>
    (list 'multi-field-interval e leading-field (parse-long leading-precision) "SECOND" (parse-long fractional-precision))

    [:interval_qualifier
     [:start_field [:non_second_primary_datetime_field leading-field]]
     "TO"
     [:end_field "SECOND" [:unsigned_integer fractional-precision]]]
    ;; =>
    (list 'multi-field-interval e leading-field 2 "SECOND" (parse-long fractional-precision))
    (throw (IllegalArgumentException. (str "Cannot build interval for: "  (pr-str qualifier))))))

(defn cast-expr [e cast-spec]
  (r/zmatch cast-spec
    [:exact_numeric_type "INTEGER"]
    (list 'cast e :i32)
    [:exact_numeric_type "INT"]
    (list 'cast e :i32)
    [:exact_numeric_type "BIGINT"]
    (list 'cast e :i64)
    [:exact_numeric_type "SMALLINT"]
    (list 'cast e :i16)

    [:approximate_numeric_type "FLOAT"]
    (list 'cast e :f32)
    [:approximate_numeric_type "REAL"]
    (list 'cast e :f32)
    [:approximate_numeric_type "DOUBLE" "PRECISION"]
    (list 'cast e :f64)

    (throw (IllegalArgumentException. (str "Cannot build cast for: " (pr-str cast-spec))))))

(defn- expr-varargs [z]
  (r/zcase z
    :array_element_list
    (r/collect-stop
     (fn [z]
       (r/zcase z
         (:array_element_list nil) nil
         [(expr z)]))
     z)

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

    (throw (IllegalArgumentException. (str "Cannot build expression for: "  (pr-str (r/node z)))))))

(defn seconds-fraction->nanos [seconds-fraction]
  (* (* (Long/parseLong seconds-fraction)
        (Math/pow 10 (- (count seconds-fraction))))
     (Math/pow 10 9)))

(defn create-offset-date-time
  [year month day hours minutes seconds seconds-fraction offset-hours offset-minutes]
  (OffsetDateTime/of (Long/parseLong year) (Long/parseLong month) (Long/parseLong day)
                     (Long/parseLong hours) (Long/parseLong minutes) (Long/parseLong seconds)
                     (seconds-fraction->nanos seconds-fraction)
                     (ZoneOffset/ofHoursMinutes (Long/parseLong offset-hours) (Long/parseLong offset-minutes))))

(defn create-offset-time
  [hours minutes seconds seconds-fraction offset-hours offset-minutes]
  (OffsetTime/of
    (Long/parseLong hours) (Long/parseLong minutes) (Long/parseLong seconds)
    (seconds-fraction->nanos seconds-fraction)
    (ZoneOffset/ofHoursMinutes (Long/parseLong offset-hours) (Long/parseLong offset-minutes))))

(defn create-local-date-time
  [year month day hours minutes seconds seconds-fraction]
  (LocalDateTime/of (Long/parseLong year) (Long/parseLong month) (Long/parseLong day)
                    (Long/parseLong hours) (Long/parseLong minutes) (Long/parseLong seconds)
                    (int (seconds-fraction->nanos seconds-fraction))))

(defn expr [z]
  (maybe-add-ref
   z
   (r/zmatch z
     [:column_reference _]
     ;;=>
     (column-reference-symbol (sem/column-reference z))

     [:cast_specification _ ^:z e _ cast-spec]
     (cast-expr (expr e) cast-spec)

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

     [:boolean_test ^:z bp "IS" [:truth_value truth-value]]
     ;; =>
     (case truth-value
       "TRUE" (list 'true? (expr bp))
       "FALSE" (list 'false? (expr bp))
       "UNKNOWN" (list 'nil? (expr bp)))

     [:boolean_test ^:z bp "IS" "NOT" [:truth_value truth-value]]
     ;; =>
     (case truth-value
       "TRUE" (list 'not (list 'true? (expr bp)))
       "FALSE" (list 'not (list 'false? (expr bp)))
       "UNKNOWN" (list 'not (list 'nil? (expr bp))))

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

     [:factor [:minus_sign "-"] ^:z np]
     (let [np-expr (expr np)]
       (if (number? np-expr)
         (- np-expr)
         (list '- np-expr)))

     [:factor [:plus_sign "+"] ^:z np]
     ;;=>
     (expr np)

     [:character_string_literal lexeme]
     ;;=>
     (subs lexeme 1 (dec (count lexeme)))

     [:point_in_time ^:z dte]
     ;;=>
     (expr dte)

     [:timestamp_literal _
      [:timestamp_string
       [:unquoted_timestamp_string
        [:date_value
          [:unsigned_integer year]
          [:minus_sign "-"]
          [:unsigned_integer month]
          [:minus_sign "-"]
          [:unsigned_integer day]]
        [:unquoted_time_string
         [:time_value
          [:unsigned_integer hours]
          [:unsigned_integer minutes]
          [:seconds_value
           [:unsigned_integer seconds]]]]]]]
     ;;=>
     (create-local-date-time year month day hours minutes seconds "0")

     [:timestamp_literal _
      [:timestamp_string
       [:unquoted_timestamp_string
        [:date_value
          [:unsigned_integer year]
          [:minus_sign "-"]
          [:unsigned_integer month]
          [:minus_sign "-"]
          [:unsigned_integer day]]
        [:unquoted_time_string
         [:time_value
          [:unsigned_integer hours]
          [:unsigned_integer minutes]
          [:seconds_value
           [:unsigned_integer seconds]
           [:unsigned_integer seconds-fraction]]]]]]]
     ;;=>
     (create-local-date-time year month day hours minutes seconds seconds-fraction)

     [:timestamp_literal _
      [:timestamp_string
       [:unquoted_timestamp_string
        [:date_value
         [:unsigned_integer year]
         [:minus_sign "-"]
         [:unsigned_integer month]
         [:minus_sign "-"]
         [:unsigned_integer day]]
        [:unquoted_time_string
         [:time_value
          [:unsigned_integer hours]
          [:unsigned_integer minutes]
          [:seconds_value
           [:unsigned_integer seconds]]]
         [:time_zone_interval
          [_ sign]
          [:unsigned_integer offset-hours]
          [:unsigned_integer offset-minutes]]]]]]
     ;;=>
     (create-offset-date-time
       year month day hours minutes seconds "0" (str sign offset-hours) (str sign offset-minutes))

     [:timestamp_literal _
      [:timestamp_string
       [:unquoted_timestamp_string
        [:date_value
         [:unsigned_integer year]
         [:minus_sign "-"]
         [:unsigned_integer month]
         [:minus_sign "-"]
         [:unsigned_integer day]]
        [:unquoted_time_string
         [:time_value
          [:unsigned_integer hours]
          [:unsigned_integer minutes]
          [:seconds_value
           [:unsigned_integer seconds]
           [:unsigned_integer seconds-fraction]]]
         [:time_zone_interval
          [_ sign]
          [:unsigned_integer offset-hours]
          [:unsigned_integer offset-minutes]]]]]]
     ;;=>
     (create-offset-date-time
       year month day hours minutes seconds seconds-fraction (str sign offset-hours) (str sign offset-minutes))

     [:date_literal _
      [:date_string [:date_value [:unsigned_integer year] _ [:unsigned_integer month] _ [:unsigned_integer day]]]]
     ;;=>
     (LocalDate/of (Long/parseLong year) (Long/parseLong month) (Long/parseLong day))

     [:time_literal _
      [:time_string
       [:unquoted_time_string
        [:time_value
         [:unsigned_integer hours]
         [:unsigned_integer minutes]
         [:seconds_value
          [:unsigned_integer seconds]]]]]]
     ;;=>
     (create-offset-time hours minutes seconds "0" "0" "0")

     [:time_literal _
      [:time_string
       [:unquoted_time_string
        [:time_value
         [:unsigned_integer hours]
         [:unsigned_integer minutes]
         [:seconds_value
          [:unsigned_integer seconds]
          [:unsigned_integer seconds-fraction]]]]]]
     ;;=>
     (create-offset-time hours minutes seconds seconds-fraction "0" "0")

     [:time_literal _
      [:time_string
       [:unquoted_time_string
        [:time_value
         [:unsigned_integer hours]
         [:unsigned_integer minutes]
         [:seconds_value
          [:unsigned_integer seconds]]]
        [:time_zone_interval
         [_ sign]
         [:unsigned_integer offset-hours]
         [:unsigned_integer offset-minutes]]]]]
     ;;=>
     (create-offset-time
       hours minutes seconds "0" (str sign offset-hours) (str sign offset-minutes))

     [:time_literal _
      [:time_string
       [:unquoted_time_string
        [:time_value
         [:unsigned_integer hours]
         [:unsigned_integer minutes]
         [:seconds_value
          [:unsigned_integer seconds]
          [:unsigned_integer seconds-fraction]]]
        [:time_zone_interval
         [_ sign]
         [:unsigned_integer offset-hours]
         [:unsigned_integer offset-minutes]]]]]
     ;;=>
     (create-offset-time
       hours minutes seconds seconds-fraction (str sign offset-hours) (str sign offset-minutes))

     [:interval_literal _
      [:interval_string [:unquoted_interval_string s]] q]
     ;;=>
     (interval-expr s q)

     [:interval_primary ^:z n q]
     ;; =>
     (interval-expr (expr n) q)

     [:interval_term ^:z i [:asterisk "*"] ^:z n]
     ;; =>
     (list '* (expr i) (expr n))

     [:interval_term ^:z i [:solidus "/"] ^:z n]
     ;; =>
     (list '/ (expr i) (expr n))

     [:interval_factor [:minus_sign "-"] ^:z i]
     ;; =>
     (list '- (expr i))

     [:interval_factor [:plus_sign "+"] ^:z i]
     ;; =>
     (expr i)

     [:interval_value_expression ^:z i1 [:plus_sign "+"] ^:z i2]
     ;; =>
     (list '+ (expr i1) (expr i2))

     [:interval_absolute_value_function "ABS" ^:z i]
     (list 'abs (expr i))

     [:datetime_value_expression ^:z i1 [:plus_sign "+"] ^:z i2]
     ;; =>
     (list '+ (expr i1) (expr i2))

     [:interval_value_expression ^:z i1 [:minus_sign "-"] ^:z i2]
     ;; =>
     (list '- (expr i1) (expr i2))

     [:datetime_value_expression ^:z i1 [:minus_sign "-"] ^:z i2]
     ;; =>
     (list '- (expr i1) (expr i2))

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

     [:regex_like_predicate ^:z rvp [:regex_like_predicate_part_2 "LIKE_REGEX" ^:z cp]]
     (list 'like-regex (expr rvp) (expr cp) "")

     [:regex_like_predicate ^:z rvp [:regex_like_predicate_part_2 "NOT" "LIKE_REGEX" ^:z cp]]
     (list 'not (list 'like-regex (expr rvp) (expr cp) ""))

     [:regex_like_predicate ^:z rvp [:regex_like_predicate_part_2 "LIKE_REGEX" ^:z cp "FLAG" ^:z flag]]
     (list 'like-regex (expr rvp) (expr cp) (expr flag))

     [:regex_like_predicate ^:z rvp [:regex_like_predicate_part_2 "NOT" "LIKE_REGEX" ^:z cp "FLAG" ^:z flag]]
     (list 'not (list 'like-regex (expr rvp) (expr cp) (expr flag)))

     [:character_substring_function "SUBSTRING" ^:z cve "FROM" ^:z sp "FOR" ^:z sl]
     ;;=>
     (list 'substring (expr cve) (expr sp) (expr sl) true)

     [:character_substring_function "SUBSTRING" ^:z cve "FROM" ^:z sp]
     ;;=>
     (list 'substring (expr cve) (expr sp) -1 false)

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

     [:character_position_expression _ ^:z needle _ ^:z haystack]
     (list 'position (expr needle) (expr haystack) "CHARACTERS")

     [:character_position_expression _ ^:z needle _ ^:z haystack _ [:char_length_units unit]]
     (list 'position (expr needle) (expr haystack) unit)

     [:char_length_expression _ ^:z nve]
     (list 'character-length (expr nve) "CHARACTERS")

     [:char_length_expression _ ^:z nve _ [:char_length_units unit]]
     (list 'character-length (expr nve) unit)

     [:octet_length_expression _ ^:z nve]
     (list 'octet-length (expr nve))

     [:character_overlay_function _ ^:z target _ ^:z placing _ ^:z pos _ ^:z len]
     (list 'overlay (expr target) (expr placing) (expr pos) (expr len))

     [:character_overlay_function _ ^:z target _ ^:z placing _ ^:z pos]
     ;; assuming common sub expression & constant folding optimisations should make their way in at some point
     ;; calculating the default length like this should not be a problem.
     (list 'overlay (expr target) (expr placing) (expr pos) (list 'default-overlay-length (expr placing)))

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
     (exists-symbol qe)

     [:in_predicate _ [:in_predicate_part_2 "NOT" _ [:in_predicate_value ^:z ivl]]]
     ;;=>
     (exists-symbol ivl)

     [:quantified_comparison_predicate _ [:quantified_comparison_predicate_part_2 _ [:some _] [:subquery ^:z qe]]]
     ;;=>
     (exists-symbol qe)

     [:quantified_comparison_predicate _ [:quantified_comparison_predicate_part_2 _ [:all _] [:subquery ^:z qe]]]
     ;;=>
     (exists-symbol qe)

     [:array_value_constructor_by_enumeration _ ^:z list]
     ;; =>
     (vec (expr list))

     [:array_value_constructor_by_query _ [:subquery ^:z qe]]
     ;; =>
     (subquery-array-symbol qe)

     [:array_element_reference ^:z ave ^:z nve]
     ;;=>
     (list 'nth (expr ave) (expr nve))

     [:trim_array_function _ ^:z a, ^:z n]
     (list 'trim-array (expr a) (expr n))

     [:dynamic_parameter_specification _]
     ;;=>
     (symbol (str "?_" (sem/dynamic-param-idx z)))

     [:period_contains_predicate ^:z p1_predicand [:period_contains_predicate_part_2 _ ^:z p2_predicand]]
     ;;=>
     (let [p1 (expr p1_predicand)
           p2 (expr p2_predicand)]
       (list 'and (list '<= (:start p1) (or (:start p2) p2)) (list '>= (:end p1) (or (:end p2) p2))))

     [:period_overlaps_predicate ^:z p1_predicand [:period_overlaps_predicate_part_2 _ ^:z p2_predicand]]
     ;;=>
     (let [p1 (expr p1_predicand)
           p2 (expr p2_predicand)]
       (list 'and (list '< (:start p1) (:end p2)) (list '> (:end p1) (:start p2))))

     [:period_equals_predicate ^:z p1_predicand [:period_equals_predicate_part_2 _ ^:z p2_predicand]]
     ;;=>
     (let [p1 (expr p1_predicand)
           p2 (expr p2_predicand)]
       (list 'and (list '= (:start p1) (:start p2)) (list '= (:end p1) (:end p2))))

     [:period_precedes_predicate ^:z p1_predicand [:period_precedes_predicate_part_2 _ ^:z p2_predicand]]
     ;;=>
     (let [p1 (expr p1_predicand)
           p2 (expr p2_predicand)]
       (list '<= (:end p1) (:start p2)))

     [:period_succeeds_predicate ^:z p1_predicand [:period_succeeds_predicate_part_2 _ ^:z p2_predicand]]
     ;;=>
     (let [p1 (expr p1_predicand)
           p2 (expr p2_predicand)]
       (list '>= (:start p1) (:end p2)))

     [:period_immediately_precedes_predicate ^:z p1_predicand [:period_immediately_precedes_predicate_part_2 _ _ ^:z p2_predicand]]
     ;;=>
     (let [p1 (expr p1_predicand)
           p2 (expr p2_predicand)]
       (list '= (:end p1) (:start p2)))

     [:period_immediately_succeeds_predicate ^:z p1_predicand [:period_immediately_succeeds_predicate_part_2 _ _ ^:z p2_predicand]]
     ;;=>
     (let [p1 (expr p1_predicand)
           p2 (expr p2_predicand)]
       (list '= (:start p1) (:end p2)))

     [:period_predicand ^:z col]
     ;;=>
     (let [app-time-symbol (str (expr col))]
       {:start (symbol (str/replace app-time-symbol "APP_TIME" "application_time_start"))
        :end (symbol (str/replace app-time-symbol "APP_TIME" "application_time_end"))})

     [:period_predicand "PERIOD" start end]
     ;;=>
     {:start (expr start) :end (expr end)}

     [:search_condition ^:z bve]
     ;;=>
     (expr bve)

     (expr-varargs z))))

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

(defn- build-apply [apply-mode column->param independent-relation dependent-relation]
  [:apply
   apply-mode
   column->param
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

(defn find-table-operators [tree]
  (let [table-ops (volatile! [])]
    (w/prewalk
      (fn [node]
        (when (and (vector? node)
                   (= :table (first node)))
          (vswap! table-ops conj node))
        node)
      tree)
    @table-ops))

(defn find-aggr-out-column-refs [tree]
  (let [column-refs (volatile! [])]
    (w/prewalk
      (fn [node]
        (when (and (symbol? node)
                   (str/starts-with? (str node) "$agg_out"))
          (vswap! column-refs conj node))
        node
        node)
      tree)
    @column-refs))

(defn build-column->param [columns]
  (zipmap
    columns
    (map
      #(->> % (name) (str "?") (symbol)) columns)))

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
            subquery-type (sem/subquery-type sq)]
        {:type :subquery
         :plan (case (:type subquery-type)
                 (:scalar_subquery
                   :row_subquery) [:max-1-row subquery-plan]
                 subquery-plan)
         :column->param column->param})

      [:exists_predicate _
       [:subquery ^:z qe]]
      (let [exists-symbol (exists-symbol qe)
            subquery-plan [:rename (subquery-reference-symbol qe) (plan qe)]
            column->param (correlated-column->param qe scope-id)]
        {:type :exists
         :plan subquery-plan
         :column->param column->param
         :sym exists-symbol})

      [:in_predicate ^:z rvp ^:z ipp2]
      (let [[qe co] (r/zmatch ipp2
                      [:in_predicate_part_2 _ [:in_predicate_value [:subquery ^:z qe]]]
                      [qe '=]

                      [:in_predicate_part_2 "NOT" _ [:in_predicate_value [:subquery ^:z qe]]]
                      [qe '<>]

                      [:in_predicate_part_2 _ [:in_predicate_value ^:z ivl]]
                      [ivl '=]

                      [:in_predicate_part_2 "NOT" _ [:in_predicate_value ^:z ivl]]
                      [ivl '<>]

                      (throw (IllegalArgumentException. "unknown in type")))
            exists-symbol (exists-symbol qe)
            predicate (list co (expr rvp) (first (subquery-projection-symbols qe)))
            in-value-list-plan (plan qe)
            subquery-plan [:rename (subquery-reference-symbol qe) in-value-list-plan]
            column->param (merge
                            (build-column->param (find-aggr-out-column-refs predicate))
                            (build-column->param
                              (find-aggr-out-column-refs
                                (find-table-operators in-value-list-plan)))
                            (correlated-column->param qe scope-id)
                            (correlated-column->param rvp scope-id))]
        {:type :quantified-comparison
         :quantifier (if (= co '=) :some :all)
         :plan subquery-plan
         :predicate predicate
         :column->param column->param
         :sym exists-symbol})

      [:quantified_comparison_predicate ^:z rvp [:quantified_comparison_predicate_part_2 [_ co] [quantifier _] [:subquery ^:z qe]]]
      (let [exists-symbol (exists-symbol qe)
            projection-symbol (first (subquery-projection-symbols qe))
            predicate (list (symbol co) (expr rvp) projection-symbol)
            subquery-plan [:rename (subquery-reference-symbol qe) (plan qe)]
            column->param (merge (correlated-column->param qe scope-id)
                                 (correlated-column->param rvp scope-id))]
        {:type :quantified-comparison
         :quantifier quantifier
         :plan subquery-plan
         :predicate predicate
         :column->param column->param
         :sym exists-symbol})

      [:array_value_constructor_by_query _ [:subquery ^:z qe]]
      (let [subquery-plan [:rename (subquery-reference-symbol qe) (plan qe)]
            column->param (correlated-column->param qe scope-id)
            projected-columns (set (subquery-projection-symbols qe))]
        (when-not (= 1 (count projected-columns)) (throw (IllegalArgumentException. "ARRAY subquery must return exactly 1 column/")))
        {:type :array
         :plan [:group-by [{(subquery-array-symbol qe) (list 'array-agg (first projected-columns))}] subquery-plan]
         :column->param column->param})

      (throw (IllegalArgumentException. "unknown subquery type")))))

(defn- apply-subquery
  "Ensures the subquery projection is available on the outer relation. Used in the general case when we are using
  the subqueries projected column in a predicate, or as part of a projection itself.

   e.g select foo.a from foo where foo.a = (select bar.a from bar where bar.c = a.c)

   See (interpret-subquery) for 'subquery info'."
  [relation subquery-info]
  (let [{:keys [type plan column->param sym predicate quantifier]} subquery-info]
    (case type
      :quantified-comparison (if (= quantifier :some)
                               (build-apply
                                 (w/postwalk-replace column->param {:mark-join {sym predicate}})
                                 column->param
                                 relation plan)

                               (let [comparator-result-sym (symbol (str sym "_comp_result$"))]
                                 [:map
                                  [{sym (list 'not comparator-result-sym)}]
                                  (build-apply
                                    (w/postwalk-replace
                                      column->param
                                      {:mark-join
                                       (let [[co x y] predicate]
                                         {comparator-result-sym `(~(flip-comparison co) ~x ~y)})})
                                    column->param
                                    relation plan)]))

      :exists (build-apply :cross-join column->param relation (wrap-with-exists sym plan))

      (build-apply :cross-join column->param relation plan))))

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
  (letfn [(all-some->exists [{:keys [quantifier predicate]}]
            (let [[co x y] predicate]
              (case quantifier
                :all {:negated? true
                      :predicate `(~(flip-comparison co) ~x ~y)}
                :some {:negated? false
                       :predicate predicate}
                :exists {:negated? false}
                :cannot-be-exists)))]
    (let [{:keys [type plan column->param sym]} subquery-info
          {:keys [negated? predicate]} (all-some->exists subquery-info)
          [_ x y] predicate]
    (cond
      (= :subquery type) [(apply-subquery relation subquery-info) predicate-set]

      (predicate-set sym)
      [(build-apply
         (if negated?
           :anti-join
           :semi-join)
         column->param
         relation
         (if predicate
           [:select
            (if negated?
              `(~'or ~predicate
                     (~'nil? ~x)
                     (~'nil? ~y))
              predicate)
            plan]
           plan))
       (disj predicate-set sym)]

      (predicate-set (list 'not sym))
      [(build-apply
         (if negated?
           :semi-join
           :anti-join)
         column->param
         relation
         (if predicate
           [:select
            (if negated?
              predicate
              `(~'or ~predicate
                     (~'nil? ~x)
                     (~'nil? ~y)))
            plan]
           plan))
       (disj predicate-set (list 'not sym))]

      :else [(apply-subquery relation subquery-info) predicate-set]))))

(defn- find-sub-queries [z]
  (r/collect-stop
    (fn [z] (r/zcase z (:subquery :exists_predicate :in_predicate :quantified_comparison_predicate :array_value_constructor_by_query) [z] nil))
    z))

;; TODO: deal with row subqueries.
(defn- wrap-with-apply [z relation]
  (reduce (fn [relation sq] (apply-subquery relation (interpret-subquery sq))) relation (find-sub-queries z)))

(defn- predicate-conjunctive-clauses [predicate]
  (if (or-predicate? predicate)
    (let [disjuncts (->> (flatten-expr or-predicate? predicate)
                         (map predicate-conjunctive-clauses))
          common-disjuncts (->> (map set disjuncts)
                                (reduce set/intersection))
          disjuncts (for [disjunct disjuncts
                          :let [filtered-disjunct (remove common-disjuncts disjunct)]]
                      (if (seq filtered-disjunct)
                        (if (= 1 (count filtered-disjunct))
                          (first filtered-disjunct)
                          (cons 'and filtered-disjunct))
                        (if (= 1 (count disjunct))
                          (first disjunct)
                          (cons 'and disjunct))))
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
        unused-predicates (filter #(contains? predicate-set %) predicates)]
    (reduce
      (fn [acc predicate]
        [:select predicate acc])
      new-relation
      unused-predicates)))

(defn- needs-group-by? [z]
  (boolean (:grouping-columns (sem/local-env (sem/group-env z)))))

(defn- wrap-with-group-by [te relation]
  (let [{:keys [grouping-columns]} (sem/local-env (sem/group-env te))
        current-env (sem/env te)
        grouping-columns (vec (for [[table-name column] grouping-columns
                                    :let [table (sem/find-decl current-env table-name)]]
                                (id-symbol table-name (:id table) column)))
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
                                    (let [spec-opts {:direction (case os "ASC" :asc, "DESC" :desc, :asc)
                                                     :null-ordering (case no "FIRST" :nulls-first, "LAST" :nulls-last, :nulls-last)}]
                                      [(if-let [idx (sem/order-by-index z)]
                                         {:spec [(unqualified-projection-symbol (nth projection idx)) spec-opts]}
                                         (let [column (symbol (str "$order_by" relation-id-delimiter query-id relation-prefix-delimiter (r/child-idx z) "$"))]
                                           {:spec [column spec-opts]
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
                   (->> (r/vector-zip relation)
                        (r/once-td-tp
                         (r/mono-tp
                          (fn [z]
                            (r/zmatch z
                              [:project projection-2 relation-2]
                              ;;=>
                              [:project (vec (concat projection-2 (mapcat keys order-by-projection)))
                               [:map order-by-projection relation-2]]))))
                        (r/node))
                   relation)
        order-by [:order-by (mapv :spec order-by-specs) relation]]
    (if (not-empty order-by-projection)
      [:project base-projection order-by]
      order-by)))

(defn generate-unique-column-names [col-names]
  (->> col-names
       (reduce
         (fn try-use-unique-col-name [{:keys [acc ret]} col-name]
           (if-let [col-name-count (get acc col-name)]
             {:acc (assoc acc col-name (inc col-name-count))
              :ret (conj ret (symbol (str (name col-name) "_" (inc col-name-count))))}
             {:acc (assoc acc col-name 0)
              :ret (conj ret col-name)}))
         {:acc {}
          :ret []})
         (:ret)))

(defn- build-query-specification [sl te]
  (let [projection (first (sem/projected-columns sl))
        projection (map
                     (fn [projection-col unique-unqualified-column-name]
                       (assoc projection-col :unique-unqualified-column-name unique-unqualified-column-name))
                     projection
                     (generate-unique-column-names (map #(unqualified-projection-symbol %) projection)))
        qualified-projection (vec (for [{:keys [qualified-column unique-unqualified-column-name] :as column} projection
                                        :let [derived-column (:ref (meta column))]]
                                    (if qualified-column
                                      {unique-unqualified-column-name
                                       (qualified-projection-symbol column)}
                                      {unique-unqualified-column-name
                                       (expr (r/$ derived-column 1))})))
        relation (wrap-with-apply sl (plan te))]
    [:project qualified-projection relation]))

(defn- build-set-op [set-op lhs rhs]
  (let [lhs-unqualified-project (mapv unqualified-projection-symbol (first (sem/projected-columns lhs)))
        rhs-unqualified-project (mapv unqualified-projection-symbol (first (sem/projected-columns rhs)))]
    [set-op (plan lhs)
     (if (= lhs-unqualified-project rhs-unqualified-project)
       (plan rhs)
       [:rename (zipmap rhs-unqualified-project lhs-unqualified-project)
        (plan rhs)])]))

(defn- build-collection-derived-table [tp]
  (let [{:keys [id]} (sem/table tp)
        [unwind-column ordinality-column] (map qualified-projection-symbol (first (sem/projected-columns tp)))
        cdt (r/$ tp 1)
        cve (r/$ cdt 2)
        unwind-symbol (symbol (str "$unwind" relation-id-delimiter id "$"))]
    [:unwind {unwind-column unwind-symbol}
     (cond-> {}
       ordinality-column (assoc :ordinality-column ordinality-column))
     [:map [{unwind-symbol (expr cve)}] nil]]))

(defn coerce-points-in-time [pit1 pit2]
  ;; TODO Needs to support LocalDateTime and take into account session timezone see #280
  (cond
    (and (instance? java.time.LocalDate pit1)
         (instance? java.time.LocalDate pit2))
    [pit1 pit2]
    (and (instance? java.time.LocalDate pit1)
         (instance? java.time.OffsetDateTime pit2))
    [(.atTime ^java.time.LocalDate pit1 ^java.time.OffsetTime (create-offset-time "0" "0" "0" "0" "0" "0"))
     pit2]

    (and (instance? java.time.OffsetDateTime pit1)
         (instance? java.time.LocalDate pit2))
    [pit1
     (.atTime ^java.time.LocalDate pit2 ^java.time.OffsetTime (create-offset-time "0" "0" "0" "0" "0" "0"))]

    (and (instance? java.time.OffsetDateTime pit1)
         (instance? java.time.OffsetDateTime pit2))
    [pit1 pit2]))

(defn pit-less-than [pit1 pit2]
  (let [[pit-1 pit-2] (coerce-points-in-time pit1 pit2)]
    (neg? (compare pit-1 pit-2))))

(defn pit-less-than-or-equal [pit1 pit2]
  (let [[pit-1 pit-2] (coerce-points-in-time pit1 pit2)
        res (compare pit-1 pit-2)]
    (or (neg? res) (= res 0))))

(defn pit-greater-than [pit1 pit2]
  (let [[pit-1 pit-2] (coerce-points-in-time pit1 pit2)]
    (pos? (compare pit-1 pit-2))))

(defn find-system-time-predicates [tp]
  (r/collect-stop
    (fn [tp]
      (r/zmatch tp
        [:query_system_time_period_specification
         "FOR"
         "SYSTEM_TIME"
         "AS"
         "OF"
         point-in-time]
        ;;=>
        [{'system_time_start (list '<= 'system_time_start (expr point-in-time))}
         {'system_time_end (list '> 'system_time_end (expr point-in-time))}]

        [:query_system_time_period_specification
         "FOR"
         "SYSTEM_TIME"
         "FROM"
         point-in-time-1
         "TO"
         point-in-time-2]
        ;;=>
        (let [pit1 (expr point-in-time-1)
              pit2 (expr point-in-time-2)]
          (if (pit-less-than pit1 pit2)
            [{'system_time_start (list '< 'system_time_start pit2)}
             {'system_time_end (list '> 'system_time_end pit1)}]
            [:invalid-points-in-time]))

        [:query_system_time_period_specification
         "FOR"
         "SYSTEM_TIME"
         "BETWEEN"
         point-in-time-1
         "AND"
         point-in-time-2]
        ;;=>
        (let
          [pit1 (expr point-in-time-1)
           pit2 (expr point-in-time-2)]
          (if (pit-less-than-or-equal pit1 pit2)
            [{'system_time_start (list '<= 'system_time_start pit2)}
             {'system_time_end (list '> 'system_time_end pit1)}]
            [:invalid-points-in-time]))

        [:query_system_time_period_specification
         "FOR"
         "SYSTEM_TIME"
         "BETWEEN"
         mode
         point-in-time-1
         "AND"
         point-in-time-2]
        ;;=>
        (let [pit1 (expr point-in-time-1)
              pit2 (expr point-in-time-2)
              [start end] (case mode
                            "SYMMETRIC"
                            (if (pit-greater-than pit1 pit2)
                              [pit2 pit1]
                              [pit1 pit2])
                            "ASYMMETRIC"
                            [pit1 pit2])]
          (if (pit-less-than-or-equal start end)
            [{'system_time_start (list '<= 'system_time_start end)}
             {'system_time_end (list '> 'system_time_end start)}]
            [:invalid-points-in-time]))))
    tp))

(defn- build-table-primary [tp]
  (let [{:keys [id correlation-name table-or-query-name] :as table} (sem/table tp)
        projection (first (sem/projected-columns tp))
        system-time-predicates (find-system-time-predicates tp)]
    [:rename (table-reference-symbol correlation-name id)
     (if-let [subquery-ref (:subquery-ref (meta table))]
       (if-let [derived-columns (sem/derived-columns tp)]
         [:rename (zipmap (map unqualified-projection-symbol (first (sem/projected-columns subquery-ref)))
                          (map symbol derived-columns))
          (plan subquery-ref)]
         (plan subquery-ref))
       (cond->>
         [:scan (let [columns (for [{:keys [identifier]} projection]
                                (symbol identifier))
                      columns-with-system-time-predicates
                      (cond
                        (= (first system-time-predicates) :invalid-points-in-time)
                        columns

                        system-time-predicates
                        (concat
                          system-time-predicates
                          (remove #(contains? (set (mapcat keys system-time-predicates)) %) columns))

                        :else
                        columns)
                      columns-with-app-time-cols
                      (if (contains? (set columns-with-system-time-predicates) 'APP_TIME)
                        (->> columns-with-system-time-predicates
                             (remove #{'application_time_start 'application_time_end 'APP_TIME})
                             (concat ['application_time_start 'application_time_end])
                             vec)
                        columns-with-system-time-predicates)]
                  (if *include-table-column-in-scan?*
                    (conj
                      columns-with-app-time-cols
                      {'_table (list '=  '_table table-or-query-name)})
                    columns-with-app-time-cols))]
         (= (first system-time-predicates) :invalid-points-in-time)
         (vector :select 'false)))]))

(defn- build-lateral-derived-table [tp qe]
  (let [scope-id (sem/id (sem/scope-element tp))
        column->param (correlated-column->param qe scope-id)
        relation (build-table-primary tp)]
    (if (every? true? (map = (keys column->param) (vals column->param)))
      relation
      (build-apply :cross-join column->param nil relation))))

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

       [:apply :cross-join columns nil dependent-relation]
       ;;=>
       [:apply :cross-join columns acc dependent-relation]

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
    [:table ks
     (r/collect-stop
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

(defn- extend-projection? [column-or-expr]
  (map? column-or-expr))

(defn- ->projected-column [column-or-expr]
  (if (extend-projection? column-or-expr)
    (key (first column-or-expr))
    column-or-expr))

;; NOTE: might be better to try do this via projected-columns and meta
;; data when building the initial plan? Though that requires rewrites
;; consistently updating this if they change anything. Some operators
;; don't know their columns, like csv and arrow, though they might
;; have some form of AS clause that does at the SQL-level. This
;; function will mainly be used for decorrelation, so not being able
;; to deduct this, fail, and keep Apply is also an option, say by
;; returning nil instead of throwing an exception like now.

(defn- relation-columns [relation-in]
  (r/zmatch relation-in
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

    [:mark-join projection lhs _]
    (conj
      (relation-columns lhs)
      (->projected-column projection))

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

    [:apply mode _ independent-relation dependent-relation]
    (-> (relation-columns independent-relation)
        (concat
          (when-let [mark-join-projection (:mark-join mode)]
            [(->projected-column mark-join-projection)])
          (case mode
            (:cross-join :left-outer-join) (relation-columns dependent-relation)
            []))
        (vec))

    [:max-1-row relation]
    (relation-columns relation)

    (throw (IllegalArgumentException. (str "cannot calculate columns for: " (pr-str relation-in))))))

(defn plan [z]
  (maybe-add-ref
   z
   (r/zmatch z
     [:directly_executable_statement ^:z dsds]
     (plan dsds)

     [:insert_statement "INSERT" "INTO" [:regular_identifier table] ^:z from-subquery]
     [:insert {:table table}
      (plan from-subquery)]

     [:from_subquery column-list ^:z query-expression]
     (let [columns (mapv (comp symbol second) (rest column-list))
           qe-plan (plan query-expression)
           rename-map (zipmap (relation-columns qe-plan) columns)]
       [:rename rename-map qe-plan])

     [:from_subquery ^:z query-expression]
     (plan query-expression)

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
     [:distinct (build-set-op :difference qeb qt)]

     [:query_expression_body ^:z qeb "EXCEPT" "ALL" ^:z qt]
     (build-set-op :difference qeb qt)

     [:query_term ^:z qt "INTERSECT" ^:z qp]
     [:distinct (build-set-op :intersect qt qp)]

     [:query_term ^:z qt "INTERSECT" "ALL" ^:z qp]
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
       (throw (IllegalArgumentException. (str "Cannot build plan for: "  (pr-str (r/node z)))))))))


;; Rewriting of logical plan.



;; Attempt to clean up tree, removing names internally and only add
;; them back at the top. Scan still needs explicit names to access the
;; columns, so these are directly renamed.

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
        (with-smap [:apply
                    (w/postwalk-replace new-smap mode)
                    (w/postwalk-replace new-smap columns)
                    independent-relation
                    (rename-walk new-smap dependent-relation)]
          (if mark-join-mode-projection-smap
            (merge (->smap independent-relation) mark-join-mode-projection-smap params)
            (case mode
              (:cross-join :left-outer-join) new-smap
              (:semi-join :anti-join) (merge (->smap independent-relation) params)))))

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

      (when (and (vector? (r/node relation-in))
                 (keyword? (r/ctor relation-in)))
        (throw (IllegalArgumentException. (str "cannot remove names for: " (pr-str (r/node relation-in)))))))))

(defn- remove-names [relation {:keys [project-anonymous-columns?]}]
  (let [projection (relation-columns relation)
        relation (binding [*name-counter* (atom 0)]
                   (r/node (r/bottomup (r/adhoc-tp r/id-tp remove-names-step) (r/vector-zip relation))))
        smap (:smap (meta relation))
        rename-map (select-keys smap projection)
        projection (replace smap projection)
        add-projection-fn (fn [relation]
                            (let [relation (if (= projection (relation-columns relation))
                                             relation
                                             [:project projection
                                              relation])
                                  smap-inv (set/map-invert rename-map)
                                  relation (if project-anonymous-columns?
                                             relation
                                             (or (r/zmatch
                                                   relation
                                                   [:rename rename-map-2 relation-2]
                                                   ;;=>
                                                   (when (= smap-inv (set/map-invert rename-map-2))
                                                     relation-2))
                                                 [:rename smap-inv relation]))]
                              (with-meta relation {:column->name smap})))]
    (with-meta relation {:column->name smap
                         :add-projection-fn add-projection-fn})))

(defn reconstruct-names [relation]
  (let [smap (:column->name (meta relation))
        relation (w/postwalk-replace (set/map-invert smap) relation)]
    (or (r/zmatch relation
          [:rename rename-map relation-2]
          ;;=>
          (when-let [rename-map (and (map? rename-map)
                                     (->> (for [[k v] rename-map
                                                :when (not= k v)]
                                            [k v])
                                          (into {})))]
            (if (empty? rename-map)
              relation-2
              [:rename rename-map relation-2])))

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
  (and (sequential? predicate)
       (= 3 (count predicate))
       (= '= (first predicate))))

(defn all-columns-in-relation?
  "Returns true if all columns referenced by the expression are present in the given relation.

  Useful for figuring out whether an expr can be applied as an equi-condition in join."
  [expr relation]
  (every? (set (relation-columns relation)) (expr-symbols expr)))

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

        (columns-in-predicate-present-in-relation? dependent-relation predicate)
        [:apply
         mode
         columns
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
        (columns-in-predicate-present-in-relation? lhs predicate)
        [join-op join-map [:select predicate lhs] rhs]
        (and (contains? #{:join :left-outer-join} join-op)
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

(defn- decorrelate-apply-rule-9
  "R A× (G F1 E) = G columns(R),F' (R A⟕ E)"
  [z]
  (r/zmatch z
    [:apply :cross-join columns independent-relation
     [:max-1-row
      [:project post-group-by-projection
       [:group-by group-by-columns
        dependent-relation]]]]
    ;;=>
    (decorrelate-group-by-apply post-group-by-projection group-by-columns
                                :left-outer-join columns independent-relation dependent-relation)

    [:apply :cross-join columns independent-relation
     [:max-1-row
      [:group-by group-by-columns
       dependent-relation]]]
    ;;=>
    (decorrelate-group-by-apply nil group-by-columns
                                :left-outer-join columns independent-relation dependent-relation)))

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

(defn- rewrite-plan [plan {:keys [decorrelate? instrument-rules?], :or {decorrelate? true, instrument-rules? false}, :as opts}]
  (let [plan (remove-names plan opts)
        {:keys [add-projection-fn]} (meta plan)
        !fired-rules (atom [])]
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
               (r/node)
               (add-projection-fn))

          (vary-meta assoc :fired-rules @!fired-rules)))))

(defn plan-query
  ([query] (plan-query query {}))
  ([query {:keys [validate-plan?], :or {validate-plan? false} :as opts}]
   (if (p/failure? query)
     {:errs [(p/failure->str query)]}
     (binding [r/*memo* (HashMap.)]
       (let [ag (r/vector-zip query)]
         (if-let [errs (not-empty (sem/errs ag))]
           {:errs errs}
           (let [plan (plan ag)
                 {:keys [fired-rules]} (meta plan)
                 validate-plan (fn [plan]
                                 (if (and validate-plan? (not (s/valid? ::lp/logical-plan plan)))
                                   (throw (IllegalArgumentException. (s/explain-str ::lp/logical-plan plan)))
                                   plan))]
             {:plan (if (#{:insert} (first plan))
                      (let [[dml-op dml-op-opts plan] plan]
                        [dml-op dml-op-opts
                         (validate-plan (rewrite-plan plan opts))])
                      (validate-plan (rewrite-plan plan opts)))
              :fired-rules fired-rules})))))))

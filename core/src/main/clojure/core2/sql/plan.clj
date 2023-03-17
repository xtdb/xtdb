(ns core2.sql.plan
  (:require [clojure.main]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.walk :as w]
            [core2.error :as err]
            [core2.logical-plan :as lp]
            core2.operator ;; Adds impls logical plan spec
            [core2.rewrite :as r]
            [core2.sql.analyze :as sem])
  (:import (java.time LocalDate LocalDateTime LocalTime OffsetTime ZoneOffset ZonedDateTime ZoneId)))

;; Attribute grammar for transformation into logical plan.

;; See https://cs.ulb.ac.be/public/_media/teaching/infoh417/sql2alg_eng.pdf

(def ^:dynamic *opts* {})

(declare expr)

(defn- id-symbol [table table-id column]
  (symbol (str table lp/relation-id-delimiter table-id lp/relation-prefix-delimiter column)))

(defn unqualified-projection-symbol [{:keys [identifier ^long index] :as _projection}]
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

(defn- column-reference-symbol [{:keys [table-id identifiers] :as _column-reference}]
  (let [[table column] identifiers]
    (id-symbol table table-id column)))

(defn- table-reference-symbol [correlation-name id]
  (symbol (str correlation-name lp/relation-id-delimiter id)))

(defn- aggregate-symbol [prefix z]
  (let [query-id (sem/id (sem/scope-element z))]
    (symbol (str "$" prefix lp/relation-id-delimiter query-id lp/relation-prefix-delimiter (sem/id z) "$"))))

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
    (throw (err/illegal-arg :core2.sql/parse-error
                            {::err/message (str "Cannot build interval for: "  (pr-str qualifier))
                             :qualifier qualifier}))))

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

    [:datetime_type "TIMESTAMP" [:with_or_without_time_zone "WITH" "TIME" "ZONE"]]
    (list 'cast-tstz e)

    (throw (err/illegal-arg :core2.sql/parse-error
                            {::err/message (str "Cannot build cast for: " (pr-str cast-spec))
                             :cast-spec cast-spec}))))

(defn- expr-varargs [z]
  (r/zcase z
    :array_element_list
    (r/collect-stop
     (fn [z]
       (r/zcase z
         (:array_element_list nil) nil
         [(expr z)]))
     z)

    :object_constructor
    (into {} (r/collect-stop
              (fn [z]
                (r/zmatch z
                  [:object_name_and_value ^:z on ^:z ve]
                  [[(expr on) (expr ve)]]))
              z))

    :case_abbreviation
    (->> (r/collect-stop
           (fn [z]
             (if (or
                   (not (r/ctor z))
                   (and (r/ctor? :case_abbreviation z)
                        (= (r/lexeme z 1) "COALESCE")))
               nil
               [(expr z)]))
           z)
         (cons 'coalesce))

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

    :least_function
    (list* 'least (map expr (r/right-zips (r/$ z 2))))

    :greatest_function
    (list* 'greatest (map expr (r/right-zips (r/$ z 2))))

    (throw (err/illegal-arg :core2.sql/parse-error
                            {::err/message (str "Cannot build expression for: "  (pr-str (r/node z)))
                             :node (r/node z)}))))

(defn seconds-fraction->nanos [seconds-fraction]
  (* (Long/parseLong seconds-fraction)
     (long (Math/pow 10 (- 9 (count seconds-fraction))))))

(defn create-offset-time
  [hours minutes seconds seconds-fraction offset-hours offset-minutes]
  (OffsetTime/of
    (Long/parseLong hours) (Long/parseLong minutes) (Long/parseLong seconds)
    (seconds-fraction->nanos seconds-fraction)
    (ZoneOffset/ofHoursMinutes (Long/parseLong offset-hours) (Long/parseLong offset-minutes))))

(defn create-local-time ^java.time.LocalTime [hours minutes seconds seconds-fraction]
  (LocalTime/of (Long/parseLong hours) (Long/parseLong minutes) (Long/parseLong seconds)
                (seconds-fraction->nanos seconds-fraction)))

(defn- plan-period-predicand [period-predicand]
  (r/zmatch
    period-predicand
    [:period_predicand ^:z col]
    ;;=>
    (let [[start end] (sem/expand-underlying-column-references (sem/column-reference col))]
      {:start (column-reference-symbol start)
       :end (column-reference-symbol end)})

    [:period_predicand "PERIOD" ^:z start ^:z end]
    ;;=>
    {:start (expr start) :end (expr end)}

    ;; handles CONTAINS period_or_point_in_time_predicand
    (expr period-predicand)))

(defn expr [z]
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

    [:factor [:minus_sign "-"] [:exact_numeric_literal "9223372036854775808"]]
    Long/MIN_VALUE

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
       ^:z uts]]]
    ;;=>
    (let [ld (LocalDate/of (Long/parseLong year) (Long/parseLong month) (Long/parseLong day))]
      (letfn [(->lt [tv]
                (r/zmatch tv
                  [:time_value [:unsigned_integer hours] [:unsigned_integer minutes] [:seconds_value [:unsigned_integer seconds]]]
                  (create-local-time hours minutes seconds "0")

                  [:time_value [:unsigned_integer hours] [:unsigned_integer minutes]
                   [:seconds_value [:unsigned_integer seconds] [:unsigned_integer seconds-fraction]]]
                  (create-local-time hours minutes seconds seconds-fraction)))

              (->zo ^java.time.ZoneOffset [sign offset-hours offset-mins]
                (ZoneOffset/of (str sign offset-hours ":" offset-mins)))]

        (r/zmatch uts
          [:unquoted_time_string ^:z tv]
          (LocalDateTime/of ld (->lt tv))

          [:unquoted_time_string ^:z tv "Z"]
          (ZonedDateTime/of ld (->lt tv) (ZoneId/of "Z"))

          [:unquoted_time_string ^:z tv
           [:time_zone_interval [_ sign]
            [:unsigned_integer offset-hours]
            [:unsigned_integer offset-mins]]]
          (ZonedDateTime/of ld (->lt tv) (->zo sign offset-hours offset-mins))

          [:unquoted_time_string ^:z tv
           [:time_zone_interval [_ sign]
            [:unsigned_integer offset-hours]
            [:unsigned_integer offset-mins]]
           [:time_zone_region region]]
          (ZonedDateTime/ofLocal (LocalDateTime/of ld (->lt tv))
                                 (ZoneId/of region)
                                 (->zo sign offset-hours offset-mins)))))

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
    (create-local-time hours minutes seconds "0")

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
    (create-local-time hours minutes seconds seconds-fraction)

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
    (create-offset-time hours minutes seconds "0"
                        (str sign offset-hours) (str sign offset-minutes))

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
    (create-offset-time hours minutes seconds seconds-fraction
                        (str sign offset-hours) (str sign offset-minutes))

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
    [:end_of_time_value_function _] 'core2/end-of-time

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

    [:square_root _ ^:z nve]
    ;;=>
    (list 'sqrt (expr nve))

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

    [:trim_function _ [:trim_operands ^:z trim-char _ ^:z nve]]
    (list 'trim (expr nve) "BOTH" (expr trim-char))

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

    ;; TODO: this will eventually be resolved at runtime.
    [:object_name ^:z n]
    ;; =>
    (keyword (expr n))

    ;; Does not work for column references, see above.
    [:field_reference ^:z vep [:regular_identifier fn]]
    (list '. (expr vep) (keyword fn))

    [:array_value_constructor_by_enumeration]
    ;; =>
    []

    [:array_value_constructor_by_enumeration "ARRAY"]
    ;; =>
    []

    [:empty_specification]
    ;; =>
    []

    [:empty_specification "ARRAY"]
    ;; =>
    []

    [:array_value_constructor_by_enumeration ^:z list]
    ;; =>
    (vec (expr list))

    [:array_value_constructor_by_enumeration _ ^:z list]
    ;; =>
    (vec (expr list))

    [:array_value_constructor_by_query _ [:subquery ^:z qe]]
    ;; =>
    (subquery-array-symbol qe)

    [:array_element_reference ^:z ave ^:z nve]
    ;;=>
    (list 'nth (expr ave) (list '- (expr nve) 1))

    [:cardinality_expression _ ^:z ave]
    ;;=>
    (list 'cardinality (expr ave))

    [:trim_array_function _ ^:z a, ^:z n]
    (list 'trim-array (expr a) (expr n))

    [:dynamic_parameter_specification _]
    ;;=>
    (symbol (str "?_" (sem/dynamic-param-idx z)))

    ;; $1, $2, $3 etc will share symbol with :dynamic_parameter_specification for now
    ;; we will dec the ints as our params are zero-based unlike postgres 1-based. So $1 will become ?_0, $2 ?_1 and so on.
    [:postgres_parameter_specification s]
    ;; =>
    (symbol (str "?_" (dec (parse-long (subs s 1)))))

    [:period_contains_predicate ^:z p1_predicand [:period_contains_predicate_part_2 _ ^:z p2_predicand]]
    ;;=>
    (let [p1 (plan-period-predicand p1_predicand)
          p2 (plan-period-predicand p2_predicand)]
      (list 'and (list '<= (:start p1) (or (:start p2) p2)) (list '>= (:end p1) (or (:end p2) p2))))

    [:period_overlaps_predicate ^:z p1_predicand [:period_overlaps_predicate_part_2 _ ^:z p2_predicand]]
    ;;=>
    (let [p1 (plan-period-predicand p1_predicand)
          p2 (plan-period-predicand p2_predicand)]
      (list 'and (list '< (:start p1) (:end p2)) (list '> (:end p1) (:start p2))))

    [:period_equals_predicate ^:z p1_predicand [:period_equals_predicate_part_2 _ ^:z p2_predicand]]
    ;;=>
    (let [p1 (plan-period-predicand p1_predicand)
          p2 (plan-period-predicand p2_predicand)]
      (list 'and (list '= (:start p1) (:start p2)) (list '= (:end p1) (:end p2))))

    [:period_precedes_predicate ^:z p1_predicand [:period_precedes_predicate_part_2 _ ^:z p2_predicand]]
    ;;=>
    (let [p1 (plan-period-predicand p1_predicand)
          p2 (plan-period-predicand p2_predicand)]
      (list '<= (:end p1) (:start p2)))

    [:period_succeeds_predicate ^:z p1_predicand [:period_succeeds_predicate_part_2 _ ^:z p2_predicand]]
    ;;=>
    (let [p1 (plan-period-predicand p1_predicand)
          p2 (plan-period-predicand p2_predicand)]
      (list '>= (:start p1) (:end p2)))

    [:period_immediately_precedes_predicate ^:z p1_predicand [:period_immediately_precedes_predicate_part_2 _ _ ^:z p2_predicand]]
    ;;=>
    (let [p1 (plan-period-predicand p1_predicand)
          p2 (plan-period-predicand p2_predicand)]
      (list '= (:end p1) (:start p2)))

    [:period_immediately_succeeds_predicate ^:z p1_predicand [:period_immediately_succeeds_predicate_part_2 _ _ ^:z p2_predicand]]
    ;;=>
    (let [p1 (plan-period-predicand p1_predicand)
          p2 (plan-period-predicand p2_predicand)]
      (list '= (:start p1) (:end p2)))

    [:case_abbreviation "NULLIF" ^:z v1 ^:z v2]
    ;;=>
    (list 'nullif (expr v1) (expr v2))

    [:search_condition ^:z bve]
    ;;=>
    (expr bve)

    (expr-varargs z)))

;; Logical plan.

(declare plan)

(defn- correlated-column->param [qe scope-id joined-tables]
  (let [joined-tables-ids (set (map :id joined-tables))]
    (->> (for [{:keys [^long table-scope-id table-id type] :as column-reference} (sem/all-column-references qe)
             :when (and (= table-scope-id scope-id)
                        (if joined-tables
                          (contains? joined-tables-ids table-id)
                          true)
                        (not= type :within-group-varying))
             :let [column-reference-symbol (column-reference-symbol column-reference)
                   param-symbol (symbol (str "?" column-reference-symbol))]]

         [column-reference-symbol param-symbol])
       (into {}))))

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
  [sq joined-tables]
  (let [scope-id (sem/id (sem/scope-element sq))]
    (r/zmatch sq
      [:subquery ^:z qe]
      (let [subquery-plan [:rename (subquery-reference-symbol qe) (plan qe)]
            column->param (correlated-column->param qe scope-id joined-tables)]
        {:type :subquery
         :subquery-type (:type (sem/subquery-type sq))
         :plan subquery-plan
         :column->param column->param})

      [:exists_predicate _
       [:subquery ^:z qe]]
      (let [exists-symbol (exists-symbol qe)
            subquery-plan [:rename (subquery-reference-symbol qe) (plan qe)]
            column->param (correlated-column->param qe scope-id joined-tables)]
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

                      (throw (err/illegal-arg :core2.sql/parse-error
                                              {::err/message "unknown in type"
                                               :node (r/znode ipp2)})))
            exists-symbol (exists-symbol qe)
            predicate (list co (expr rvp) (first (subquery-projection-symbols qe)))
            in-value-list-plan (plan qe)
            subquery-plan [:rename (subquery-reference-symbol qe) in-value-list-plan]
            column->param (merge
                            (build-column->param (find-aggr-out-column-refs predicate))
                            (build-column->param
                              (find-aggr-out-column-refs
                                (find-table-operators in-value-list-plan)))
                            (correlated-column->param qe scope-id joined-tables)
                            (correlated-column->param rvp scope-id joined-tables))]
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
            column->param (merge
                            (build-column->param (find-aggr-out-column-refs predicate))
                            (build-column->param
                              (find-aggr-out-column-refs
                                (find-table-operators subquery-plan)))
                            (correlated-column->param qe scope-id joined-tables)
                            (correlated-column->param rvp scope-id joined-tables))]
        {:type :quantified-comparison
         :quantifier quantifier
         :plan subquery-plan
         :predicate predicate
         :column->param column->param
         :sym exists-symbol})

      [:array_value_constructor_by_query _ [:subquery ^:z qe]]
      (let [subquery-plan [:rename (subquery-reference-symbol qe) (plan qe)]
            column->param (correlated-column->param qe scope-id joined-tables)
            projected-columns (set (subquery-projection-symbols qe))]
        (when-not (= 1 (count projected-columns)) (throw (err/illegal-arg :core2.sql/parse-error
                                                                          {::err/message "ARRAY subquery must return exactly 1 column"
                                                                           :columns projected-columns})))
        {:type :array
         :plan [:group-by [{(subquery-array-symbol qe) (list 'array-agg (first projected-columns))}] subquery-plan]
         :column->param column->param})

      (throw (err/illegal-arg :core2.sql/parse-error
                              {::err/message "unknown subquery type"})))))

(defn- apply-subquery
  "Ensures the subquery projection is available on the outer relation. Used in the general case when we are using
  the subqueries projected column in a predicate, or as part of a projection itself.

   e.g select foo.a from foo where foo.a = (select bar.a from bar where bar.c = a.c)

   See (interpret-subquery) for 'subquery info'."
  [relation subquery-info]
  (let [{:keys [type subquery-type plan column->param sym predicate quantifier]} subquery-info]
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

      :subquery (build-apply (case subquery-type
                               (:scalar_subquery :row_subquery) :single-join
                               :cross-join)
                             column->param relation plan)

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
  (reduce (fn [relation sq] (apply-subquery relation (interpret-subquery sq nil))) relation (find-sub-queries z)))

(defn- predicate-conjunctive-clauses [predicate]
  (if (lp/or-predicate? predicate)
    (let [disjuncts (->> (lp/flatten-expr lp/or-predicate? predicate)
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
    (lp/flatten-expr lp/and-predicate? predicate)))

(defn- plan-subquery-containg-clause [sc relation joined-tables]
  (let [sc-expr (expr sc)
        predicates (predicate-conjunctive-clauses sc-expr)
        predicate-set (set predicates)
        [new-relation predicate-set]
        (reduce
          (fn [[new-relation predicate-set] sq]
            (apply-predicative-subquery new-relation (interpret-subquery sq joined-tables) predicate-set))
          [relation predicate-set]
          (find-sub-queries sc))
        unused-predicates (filter #(contains? predicate-set %) predicates)]
    [new-relation unused-predicates]))

(defn- wrap-with-select [sc relation]
  (apply reduce
         (fn [acc predicate]
           [:select predicate acc])
         (plan-subquery-containg-clause sc relation nil)))

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

                     (throw (err/illegal-arg :core2.sql/parse-error
                                             {::err/message "unknown aggregation function"}))))]
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
                                         (let [column (symbol (str "$order_by" lp/relation-id-delimiter query-id lp/relation-prefix-delimiter (r/child-idx z) "$"))]
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

(defn- build-set-op
  ([set-op lhs rhs] (build-set-op set-op lhs rhs false))

  ([set-op lhs rhs wrap-distinct?]
   (letfn [(wrap-distinct [pln]
             (if wrap-distinct?
               [:distinct pln]
               pln))]
     (let [lhs-unqualified-project (mapv unqualified-projection-symbol (first (sem/projected-columns lhs)))
           rhs-unqualified-project (mapv unqualified-projection-symbol (first (sem/projected-columns rhs)))]
       [set-op (-> (plan lhs) (wrap-distinct))
        (if (= lhs-unqualified-project rhs-unqualified-project)
          (-> (plan rhs) (wrap-distinct))
          [:rename (zipmap rhs-unqualified-project lhs-unqualified-project)
           (-> (plan rhs) (wrap-distinct))])]))))

(defn- build-collection-derived-table [tp]
  (let [{:keys [id]} (sem/table tp)
        [unwind-column ordinality-column] (map qualified-projection-symbol (first (sem/projected-columns tp)))
        cdt (r/$ tp 1)
        cve (r/$ cdt 2)
        unwind-symbol (symbol (str "$unwind" lp/relation-id-delimiter id "$"))]
    [:unwind {unwind-column unwind-symbol}
     (cond-> {}
       ordinality-column (assoc :ordinality-column ordinality-column))
     [:map [{unwind-symbol (expr cve)}] nil]]))

(defn- interpret-system-time-period-spec [table-primary-ast]
  (when-let [z (r/find-first (partial r/ctor? :query_system_time_period_specification) table-primary-ast)]
    (r/zmatch z
      [:query_system_time_period_specification "FOR" "ALL" _]
      :all-time

      [:query_system_time_period_specification "FOR" _ "AS" "OF" ^:z point-in-time]
      ;;=>
      [:at (expr point-in-time)]

      [:query_system_time_period_specification "FOR" _ "FROM" ^:z point-in-time-1 "TO" ^:z point-in-time-2]
      ;;=>
      [:in (expr point-in-time-1) (expr point-in-time-2)]

      [:query_system_time_period_specification "FOR" _ "BETWEEN" ^:z point-in-time-1 "AND" ^:z point-in-time-2]
      ;;=>
      [:between (expr point-in-time-1) (expr point-in-time-2)])))

(defn- interpret-application-time-period-spec [table-primary-ast]
  (when-let [z (r/find-first (partial r/ctor? :query_application_time_period_specification) table-primary-ast)]
    (r/zmatch z
      [:query_application_time_period_specification "FOR" "ALL" _]
      ;;=>
      :all-time

      [:query_application_time_period_specification "FOR" _ "AS" "OF" ^:z point-in-time]
      ;;=>
      [:at (expr point-in-time)]

      [:query_application_time_period_specification "FOR" _ "FROM" ^:z point-in-time-1 "TO" ^:z point-in-time-2]
      ;;=>
      [:in (expr point-in-time-1) (expr point-in-time-2)]

      [:query_application_time_period_specification "FOR" _ "BETWEEN" ^:z point-in-time-1 "AND" ^:z point-in-time-2]
      ;;=>
      [:between (expr point-in-time-1) (expr point-in-time-2)])))

(defn- build-table-primary [tp]
  (let [{:keys [id correlation-name table-or-query-name] :as table} (sem/table tp)
        projection (first (sem/projected-columns tp))]
    [:rename (table-reference-symbol correlation-name id)
     (if-let [subquery-ref (:subquery-ref (meta table))]
       (if-let [derived-columns (sem/derived-columns tp)]
         [:rename (zipmap (map unqualified-projection-symbol (first (sem/projected-columns subquery-ref)))
                          (map symbol derived-columns))
          (plan subquery-ref)]
         (plan subquery-ref))
       [:scan (->> {:table (symbol table-or-query-name)
                    :for-app-time (or (interpret-application-time-period-spec tp)
                                      (when (:app-time-as-of-now? *opts*)
                                        [:at :now]))
                    :for-sys-time (interpret-system-time-period-spec tp)}
                   (into {} (remove (comp nil? val))))
        (vec
         (->> (for [{:keys [identifier]} projection]
                (symbol identifier))
              (distinct)
              (vec)))])]))

(defn- build-target-table [tt]
  (let [{:keys [id correlation-name table-or-query-name]} (sem/table tt)
        projection (first (sem/projected-columns tt))]
    [:rename (table-reference-symbol correlation-name id)
     [:scan {:table (symbol table-or-query-name)}
      (for [{:keys [identifier]} projection
            :let [identifier (symbol identifier)]
            :when (not= '_table identifier)]
        identifier)]]))

(defn- build-lateral-derived-table [tp qe]
  (let [scope-id (sem/id (sem/scope-element tp))
        column->param (correlated-column->param qe scope-id nil)
        relation (build-table-primary tp)]
    (if (every? true? (map = (keys column->param) (vals column->param)))
      relation
      (build-apply :cross-join column->param nil relation))))

(defn- build-arrow-table [tp]
  (let [{:keys [id correlation-name]} (sem/table tp)
        projection (first (sem/projected-columns tp))
        url (r/$ (r/$ tp 1) -1)]
    [:rename (table-reference-symbol correlation-name id)
     [:project (mapv unqualified-projection-symbol projection)
      [:arrow (expr url)]]]))

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
      (r/zcase
        z
        :table_primary [(plan
                          (if-let [qualified-join (r/find-first (partial r/ctor? :qualified_join) z)]
                            qualified-join
                            z))]
        :qualified_join [(plan z)]
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


(defn- plan-query-expr [z]
  (let [qeb (if-not (r/ctor? :with_clause (r/$ z 1))
              (r/$ z 1)
              (r/$ z 2))
        obc (r/find-first (partial r/ctor? :order_by_clause) z)
        roc (r/find-first (partial r/ctor? :result_offset_clause) z)
        ffc (r/find-first (partial r/ctor? :fetch_first_clause) z)]
    (letfn [(wrap-with-top [rel]
              (if (or roc ffc)
                [:top (cond-> {}
                        roc (assoc :skip (expr (r/$ roc 2)))
                        ffc (assoc :limit (r/zmatch ffc
                                            [:fetch_first_clause "LIMIT" ^:z ffrc] (expr ffrc)
                                            [:fetch_first_clause _ _ ^:z ffrc _ _] (expr ffrc))))
                 rel]
                rel))]
      (-> (plan qeb)
          (cond->> obc (wrap-with-order-by (r/$ obc -1)))
          (->> (wrap-with-top))))))

(defn- plan-dml [dml-op z]
  (let [{:keys [app-time-as-of-now?]} *opts*
        tt (r/find-first (partial r/ctor? :target_table) z)
        {:keys [table-or-query-name correlation-name] :as table} (sem/table tt)
        rel (build-target-table tt)
        rel (if-let [sc (r/find-first (partial r/ctor? :search_condition) z)]
              (wrap-with-select sc rel)
              rel)]

    (letfn [(->qps [sym]
              (qualified-projection-symbol
               (-> {:identifier (name sym)
                    :qualified-column [correlation-name (name sym)]}
                   (vary-meta assoc :table table))))]

      (let [{app-from :from, app-to :to, :as app-time-extents} (sem/dml-app-time-extents z)
            app-from-expr (some-> app-from (expr))
            app-to-expr (some-> app-to (expr))
            app-start-sym (->qps 'application_time_start)
            app-end-sym (->qps 'application_time_end)]

        [dml-op {:table table-or-query-name}
         [:project (vec
                    (concat (for [{:keys [identifier] :as col} (first (sem/projected-columns z))
                                  :when (not (#{"application_time_start" "application_time_end"} identifier))]
                              {(symbol identifier)
                               (if-let [derived-expr (:ref (meta col))]
                                 (expr derived-expr)
                                 (qualified-projection-symbol col))})

                            [{'application_time_start `(~'cast-tstz ~(cond
                                                                       (= :all-application-time app-time-extents) app-start-sym
                                                                       app-from-expr `(~'greatest ~app-start-sym ~app-from-expr)
                                                                       app-time-as-of-now? `(~'greatest ~app-start-sym (~'current-timestamp))
                                                                       :else app-start-sym))}
                             {'application_time_end `(~'cast-tstz ~(cond
                                                                     (= :all-application-time app-time-extents) app-end-sym
                                                                     app-to-expr `(~'least ~app-end-sym ~app-to-expr)
                                                                     app-time-as-of-now? `(~'least ~app-end-sym ~'core2/end-of-time)
                                                                     :else app-end-sym))}]))
          (if (and app-to app-from)
            [:select `(~'and
                       (~'<= ~app-start-sym ~app-to-expr)
                       (~'>= ~app-end-sym ~app-from-expr))
             rel]
            rel)]]))))

(defn- plan-erase [z]
  (let [tt (r/find-first (partial r/ctor? :target_table) z)
        {:keys [table-or-query-name correlation-name] :as table} (sem/table tt)]
    [:erase {:table table-or-query-name}
     [:project (vec
                (for [{:keys [identifier] :as col} (first (sem/projected-columns z))]
                  {(symbol identifier)
                   (if-let [derived-expr (:ref (meta col))]
                     (expr derived-expr)
                     (qualified-projection-symbol col))}))
      [:select `(~'<=
                 ~(qualified-projection-symbol
                   (-> {:identifier "system_time_end"
                        :qualified-column [correlation-name "system_time_end"]}
                       (vary-meta assoc :table table)))
                 ~'core2/end-of-time)
       (as-> (build-target-table tt) rel
         (if-let [sc (r/find-first (partial r/ctor? :search_condition) z)]
           (wrap-with-select sc rel)
           rel))]]]))

(def app-time-col? (comp #{"application_time_start" "application_time_end"} :identifier))

(defn plan-qualified-join [join-type lhs rhs sc]
  (let [planned-rhs (apply reduce
                           (fn [acc predicate]
                             [:select predicate acc])
                           (plan-subquery-containg-clause sc (plan rhs) (sem/local-tables rhs)))
         apply-params (correlated-column->param sc (sem/id (sem/scope-element sc)) (sem/local-tables lhs))]
    (if (= join-type "INNER")
      [:apply :cross-join apply-params (plan lhs) (w/postwalk-replace apply-params planned-rhs)]
      [:apply :left-outer-join apply-params (plan lhs) (w/postwalk-replace apply-params planned-rhs)])))

(defn plan-named-column-join [join-type lhs rhs named-columns]
  (let [[planned-rhs named-column-expr] (plan-subquery-containg-clause named-columns (plan rhs) nil)
        named-column-join-clause (vec named-column-expr)]
    (if (= join-type "INNER")
      [:join named-column-join-clause (plan lhs) planned-rhs]
      [:left-outer-join named-column-join-clause (plan lhs) planned-rhs])))

(defn plan [z]
  (r/zmatch z
    [:directly_executable_statement ^:z dsds]
    (plan dsds)

    [:insert_statement "INSERT" "INTO" ^:z table ^:z from-subquery]
    [:insert {:table (sem/identifier table)}
     (let [inner-plan (plan from-subquery)
           projection (first (sem/projected-columns z))]
       (if (some app-time-col? projection)
         [:project (vec (for [col projection]
                          (let [col-sym (unqualified-projection-symbol col)]
                            (if (app-time-col? col)
                              {col-sym `(~'cast-tstz ~col-sym)}
                              col-sym))))
          inner-plan]
         inner-plan))]

    [:from_subquery ^:z column-list ^:z query-expression]
    (let [columns (mapv symbol (sem/identifiers column-list))
          qe-plan (plan query-expression)
          rename-map (zipmap (lp/relation-columns qe-plan) columns)]
      [:rename rename-map qe-plan])

    [:from_subquery ^:z query-expression]
    (plan query-expression)

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

    [:query_expression_body ^:z qeb "UNION" "DISTINCT" ^:z qt]
    [:distinct (build-set-op :union-all qeb qt)]

    [:query_expression_body ^:z qeb "EXCEPT" ^:z qt]
    (build-set-op :difference qeb qt true)

    [:query_expression_body ^:z qeb "EXCEPT" "ALL" ^:z qt]
    (build-set-op :difference qeb qt)

    [:query_expression_body ^:z qeb "EXCEPT" "DISTINCT" ^:z qt]
    (build-set-op :difference qeb qt true)

    [:query_term ^:z qt "INTERSECT" ^:z qp]
    [:distinct (build-set-op :intersect qt qp)]

    [:query_term ^:z qt "INTERSECT" "ALL" ^:z qp]
    (build-set-op :intersect qt qp)

    [:query_term ^:z qt "INTERSECT" "DISTINCT" ^:z qp]
    [:distinct (build-set-op :intersect qt qp)]

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

    [:table_primary [:collection_derived_table _ _] _]
    ;;=>
    (build-collection-derived-table z)

    [:table_primary [:collection_derived_table _ _] _ _]
    ;;=>
    (build-collection-derived-table z)

    [:table_primary [:collection_derived_table _ _] _ _ _]
    (build-collection-derived-table z)

    [:table_primary [:collection_derived_table _ _ _ _] _]
    ;;=>
    (build-collection-derived-table z)

    [:table_primary [:collection_derived_table _ _ _ _] _ _]
    ;;=>
    (build-collection-derived-table z)

    [:table_primary [:collection_derived_table _ _ _ _] _ _ _]
    (build-collection-derived-table z)

    [:table_primary [:lateral_derived_table _ [:subquery ^:z qe]] _]
    (build-lateral-derived-table z qe)

    [:table_primary [:lateral_derived_table _ [:subquery ^:z qe]] _ _]
    (build-lateral-derived-table z qe)

    [:table_primary [:lateral_derived_table _ [:subquery ^:z qe]] _ _ _]
    (build-lateral-derived-table z qe)

    [:table_primary [:arrow_table _ _] _]
    ;;=>
    (build-arrow-table z)

    [:table_primary [:arrow_table _ _] _ _]
    ;;=>
    (build-arrow-table z)

    [:table_primary [:arrow_table _ _] _ _ _]
    (build-arrow-table z)

    [:table_primary [:subquery _]]
    ;;=>
    (build-table-primary z)

    [:table_primary [:subquery _] _]
    ;;=>
    (build-table-primary z)

    [:table_primary [:subquery _] _ _]
    ;;=>
    (build-table-primary z)

    [:table_primary [:subquery _] _ _ _]
    ;;=>
    (build-table-primary z)

    ;; ident as first child indicates real table for now
    ;; if and when query_name becomes a valid child of table_or_query_name
    ;; this needs changing to specifically match on the table case

    ;; table case in above comment refers to the situation in which a real
    ;; table appears as a child, not the case of the characters in the table name

    [:table_primary [:regular_identifier _]]
    ;;=>
    (build-table-primary z)

    [:table_primary [:regular_identifier _] _]
    ;;=>
    (build-table-primary z)

    [:table_primary [:regular_identifier _] _ _]
    ;;=>
    (build-table-primary z)

    [:table_primary [:regular_identifier _] _ _ _]
    ;;=>
    (build-table-primary z)

    [:table_primary [:regular_identifier _] _ _ _ _]
    ;;=>
    (build-table-primary z)

    [:table_primary [:regular_identifier _] _ _ _ _ _]
    ;;=>
    (build-table-primary z)

    ;; duplicates matches for delimited_identifier for the same reason as above

    [:table_primary [:delimited_identifier _]]
    ;;=>
    (build-table-primary z)

    [:table_primary [:delimited_identifier _] _]
    ;;=>
    (build-table-primary z)

    [:table_primary [:delimited_identifier _] _ _]
    ;;=>
    (build-table-primary z)

    [:table_primary [:delimited_identifier _] _ _ _]
    ;;=>
    (build-table-primary z)

    [:table_primary [:delimited_identifier _] _ _ _ _]
    ;;=>
    (build-table-primary z)

    [:table_primary [:delimited_identifier _] _ _ _ _ _]
    ;;=>
    (build-table-primary z)

    [:qualified_join ^:z lhs _ ^:z rhs [:join_condition _ ^:z sc]]
    ;;=>
    (plan-qualified-join "INNER" lhs rhs sc)

    [:qualified_join ^:z lhs ^:z jt _ ^:z rhs [:join_condition _ ^:z sc]]
    ;;=>
    (let [join-type (sem/join-type jt)
          [lhs rhs] (if (= join-type "RIGHT") [rhs lhs] [lhs rhs])]
      (plan-qualified-join join-type lhs rhs sc))

    [:qualified_join ^:z lhs _ ^:z rhs ^:z ncj]
    ;;=>
    (plan-named-column-join "INNER" lhs rhs ncj)

    [:qualified_join ^:z lhs ^:z jt _ ^:z rhs ^:z ncj]
    ;;=>
    (let [join-type (sem/join-type jt)
          [lhs rhs] (if (= join-type "RIGHT") [rhs lhs] [lhs rhs])]
      (plan-named-column-join join-type lhs rhs ncj))

    [:from_clause _ ^:z trl]
    ;;=>
    (build-table-reference-list trl)

    [:table_value_constructor _ ^:z rvel]
    (build-values-list rvel)

    [:contextually_typed_table_value_constructor _ ^:z cttvl]
    (build-values-list cttvl)

    (r/zcase z
      :query_expression (plan-query-expr z)
      :in_value_list (build-values-list z)
      :delete_statement__searched (plan-dml :delete z)
      :update_statement__searched (plan-dml :update z)
      :erase_statement__searched (plan-erase z)

      (throw (err/illegal-arg ::cannot-build-plan
                              {::err/message (str "Cannot build plan for: "  (pr-str (r/node z)))
                               :node (r/node z)})))))

(defn rewrite-plan [plan opts]
  (let [plan (lp/remove-names plan opts)
        {:keys [add-projection-fn]} (meta plan)]
    (-> plan
        (lp/rewrite-plan opts)
        (add-projection-fn))))

(defn plan-query
  ([ag] (plan-query ag {}))

  ([ag {:keys [validate-plan?], :or {validate-plan? false}, :as opts}]
   (letfn [(validate-plan [plan]
             (when validate-plan?
               (lp/validate-plan plan)))]
     (try
       (let [plan (plan ag)]
         (if (#{:insert :delete :update :erase} (first plan))
           (let [[dml-op dml-op-opts plan] plan]
             [dml-op dml-op-opts
              (doto (rewrite-plan plan opts)
                (validate-plan))])
           (doto (rewrite-plan plan opts)
             (validate-plan))))
       (catch Throwable t
         (throw (err/illegal-arg ;;might not be a bad query but IAE returns errors via pg-wire
                  ::plan-error
                  {::err/message (format "Error Planning SQL: %s" (ex-message t))}
                  t)))))))

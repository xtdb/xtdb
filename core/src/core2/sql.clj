(ns core2.sql
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as w]
            [clojure.zip :as z]
            [clojure.spec.alpha :as s]
            [core2.logical-plan :as lp]
            [core2.rewrite :as r]
            [instaparse.core :as insta]
            [instaparse.failure])
  (:import clojure.lang.IObj))

(set! *unchecked-math* :warn-on-boxed)

;; Instaparse SQL:2011 parser generator from the official grammar:

(def parse-sql2011
  (insta/parser
   (io/resource "core2/sql/SQL2011.ebnf")
   :auto-whitespace (insta/parser "whitespace = #'\\s+' | #'\\s*--[^\r\n]*\\s*' | #'\\s*/[*].*?([*]/\\s*|$)'")
   :string-ci true))

(defn- prune-tree [tree]
  (w/postwalk
   (fn [x]
     (if (and (vector? x)
              (= 2 (count x)))
       (let [fst (first x)]
         (if (and (keyword? fst)
                  (not (contains? #{:table_primary :query_expression :table_expression} fst))
                  (re-find #"(^|_)(term|factor|primary|expression|query_expression_body)$" (name fst)))
           (second x)
           x))
       x))
   tree))

(def parse
  (memoize
   (fn self
     ([s]
      (self s :directly_executable_statement))
     ([s start-rule]
      (vary-meta
       (prune-tree (parse-sql2011 (str/trimr s) :start start-rule)) assoc ::sql s)))))

(defn- skip-whitespace ^long [^String s ^long n]
  (if (Character/isWhitespace (.charAt s n))
    (recur s (inc n))
    n))

(defn- ->line-info-str [loc]
  (let [{:core2.sql/keys [sql]} (meta (z/root loc))
        {:instaparse.gll/keys [start-index]} (meta (z/node loc))
        start-index (skip-whitespace sql start-index)
        {:keys [line column]} (instaparse.failure/index->line-column start-index sql)]
    (format "at line %d, column %d" line column)))

(defn- ->src-str [loc]
  (let [{:core2.sql/keys [sql]} (meta (z/root loc))
        {:instaparse.gll/keys [start-index end-index]} (meta (z/node loc))
        start-index (skip-whitespace sql start-index)]
    (subs sql start-index end-index)))

;; Attribute grammar for SQL semantics.

;; TODO:
;; - try replace ids with refs.
;; - align names and language with spec, add references?
;; - grouping column check for asterisks should really expand and then fail.
;; - named columns join should only output single named columns: COALESCE(lhs.x, rhs.x) AS x

(defn- enter-env-scope
  ([env]
   (enter-env-scope env {}))
  ([env new-scope]
   (cons new-scope env)))

(defn- update-env [[s & ss :as env] f]
  (cons (f s) ss))

(defn- extend-env [env k v]
  (update-env env (fn [s]
                    (update s k conj v))))

(defn- local-env [[s]]
  s)

(defn- local-env-singleton-values [[s]]
  (->> (for [[k [v]] s]
         [k v])
       (into {})))

(defn- parent-env [[_ & ss]]
  ss)

(defn- find-local-decl [[s & ss :as env] k]
  (when s
    (first (get s k))))

(defn- find-decl [[s & ss :as env] k]
  (when s
    (or (find-local-decl env k)
        (recur ss k))))

(defn- qualified? [ids]
  (> (count ids) 1))

;; Attributes

(declare cte-env env projected-columns scope-element subquery-element)

;; Ids

(defn- id [ag]
  (r/zcase ag
    :table_primary
    (if (r/ctor? :qualified_join ag)
      (id (z/prev ag))
      (inc ^long (id (z/prev ag))))

    (:query_expression
     :query_specification
     :with_list_element
     :aggregate_function)
    (inc ^long (id (z/prev ag)))

    (or (some-> (z/prev ag) (id)) 0)))

;; Identifiers

(defn- identifiers [ag]
  (r/zcase ag
    (:column_name_list
     :column_reference
     :asterisked_identifier_chain
     :identifier_chain)
    (r/collect
     (fn [ag]
       (when (r/ctor? :regular_identifier ag)
         [(r/lexeme ag 1)]))
     ag)))

(defn- identifier [ag]
  (r/zcase ag
    :derived_column
    (r/zmatch ag
      [:derived_column _ [:as_clause _ [:regular_identifier as]]]
      ;;=>
      as

      [:derived_column
       [:column_reference
        [:identifier_chain _ [:regular_identifier column]]]]
      ;;=>
      column

      [:derived_column
       [:host_parameter_name column]]
      ;;=>
      column)

    :as_clause
    (identifier (r/$ ag -1))

    :regular_identifier
    (r/lexeme ag 1)

    nil))

(defn- table-or-query-name [ag]
  (r/zcase ag
    (:table_primary
     :with_list_element)
    (table-or-query-name (r/$ ag 1))

    :regular_identifier
    (identifier ag)

    nil))

(defn- correlation-name [ag]
  (r/zcase ag
    :table_primary
    (or (correlation-name (r/$ ag -1))
        (correlation-name (r/$ ag -2)))

    :regular_identifier
    (identifier ag)

    nil))

;; With

(defn- cte [ag]
  (r/zcase ag
    :with_list_element
    (let [columns (when (r/ctor? :column_name_list (r/$ ag 2))
                    (identifiers (r/$ ag 2)))]
      (with-meta
        (cond-> {:query-name (table-or-query-name ag)
                 :id (id ag)
                 :scope-id (id (scope-element ag))
                 :subquery-scope-id (id (subquery-element (r/$ ag -1)))}
          columns (assoc :columns columns))
        {:ref ag}))))

;; Inherited
(defn- ctei [ag]
  (r/zcase ag
    :query_expression
    (enter-env-scope (cte-env (r/parent ag)))

    :with_list_element
    (let [cte-env (ctei (r/left-or-parent ag))
          {:keys [query-name] :as cte} (cte ag)]
      (extend-env cte-env query-name cte))

    (r/inherit ag)))

;; Synthesised
(defn- cteo [ag]
  (r/zcase ag
    :query_expression
    (if (r/ctor? :with_clause (r/$ ag 1))
      (cteo (r/$ ag 1))
      (ctei ag))

    :with_clause
    (cteo (r/$ ag -1))

    :with_list
    (ctei (r/$ ag -1))))

(defn- cte-env [ag]
  (r/zcase ag
    :query_expression
    (cteo ag)

    :with_list_element
    (let [with-clause (r/parent (r/parent ag))]
      (if (= "RECURSIVE" (r/lexeme with-clause 2))
        (cteo (r/parent ag))
        (ctei (r/left-or-parent ag))))

    (r/inherit ag)))

;; From

(defn- derived-columns [ag]
  (r/zcase ag
    :table_primary
    (when (r/ctor? :column_name_list (r/$ ag -1))
      (identifiers (r/$ ag -1)))))

(defn- subquery-element [ag]
  (r/zcase ag
    (:table_primary
     :subquery)
    (subquery-element (r/$ ag 1))

    :lateral_derived_table
    (subquery-element (r/$ ag 2))

    (:query_expression
     :collection_derived_table)
    ag

    nil))

(defn- table [ag]
  (r/zcase ag
    :table_primary
    (when-not (r/ctor? :qualified_join (r/$ ag 1))
      (let [table-name (table-or-query-name ag)
            correlation-name (or (correlation-name ag) table-name)
            cte (when table-name
                  (find-decl (cte-env ag) table-name))
            subquery-element (when (nil? cte)
                               (subquery-element ag))
            subquery-scope-id (when (and subquery-element (not= :collection_derived_table (r/ctor subquery-element)))
                                (id subquery-element))
            derived-columns (or (derived-columns ag) (:columns cte))]
        (with-meta
          (cond-> {:correlation-name correlation-name
                   :id (id ag)
                   :scope-id (id (scope-element ag))}
            table-name (assoc :table-or-query-name table-name)
            derived-columns (assoc :derived-columns derived-columns)
            subquery-scope-id (assoc :subquery-scope-id subquery-scope-id)
            cte (assoc :cte-id (:id cte) :cte-scope-id (:scope-id cte)))
          (cond-> {:ref ag}
            subquery-element (assoc :subquery-ref subquery-element)
            cte (assoc :cte cte
                       :subquery-ref (r/$ (:ref (meta cte)) -1))))))))

(defn- local-tables [ag]
  (r/collect-stop
   (fn [ag]
     (r/zcase ag
       :table_primary
       (if (r/ctor? :qualified_join (r/$ ag 1))
         (local-tables (r/$ ag 1))
         [(table ag)])

       :subquery []

       nil))
   ag))

(defn- join-type [ag]
  (if (r/ctor? :qualified_join ag)
    (join-type (r/$ ag 2))
    (r/zmatch ag
      [:join_type [:outer_join_type "LEFT"]] :left
      [:join_type [:outer_join_type "LEFT"] "OUTER"] :left
      [:join_type [:outer_join_type "RIGHT"]] :right
      [:join_type [:outer_join_type "RIGHT"] "OUTER"] :right
      [:join_type "INNER"] :inner
      "JOIN" :inner
      nil)))

(defn- named-columns-join-columns [ag]
  (r/zcase ag
    :qualified_join
    (named-columns-join-columns (r/$ ag -1))

    :named_columns_join
    (identifiers (r/$ ag -1))

    nil))

(defn- named-columns-join-env [ag]
  (r/zcase ag
    :qualified_join
    (named-columns-join-env (r/$ ag -1))

    :join_condition
    nil

    :named_columns_join
    (let [join-columns (identifiers (r/$ ag -1))
          qualified-join (r/parent ag)
          lhs (r/$ qualified-join 1)
          rhs (r/$ qualified-join -2)
          [lhs rhs] (for [side [lhs rhs]]
                      (select-keys
                       (->> (for [{:keys [id] :as table} (local-tables side)
                                  projection (first (projected-columns (:ref (meta table))))]
                              projection)
                            (group-by :identifier))
                       join-columns))]
      {:join-columns join-columns
       :lhs lhs
       :rhs rhs})

    (r/inherit ag)))

;; Inherited
(defn- dcli [ag]
  (r/zcase ag
    :query_specification
    (enter-env-scope (env (r/parent ag)))

    (:table_primary
     :qualified_join)
    (reduce (fn [acc {:keys [correlation-name] :as table}]
              (extend-env acc correlation-name table))
            (dcli (r/left-or-parent ag))
            (local-tables ag))

    (r/inherit ag)))

;; Synthesised
(defn- dclo [ag]
  (r/zcase ag
    :query_specification
    (dclo (r/$ ag -1))

    :table_expression
    (dclo (r/$ ag 1))

    :from_clause
    (dclo (r/$ ag 2))

    :table_reference_list
    (dcli (r/$ ag -1))))

(defn- env [ag]
  (r/zcase ag
    :query_specification
    (dclo ag)

    :from_clause
    (parent-env (dclo ag))

    (:collection_derived_table
     :join_condition
     :lateral_derived_table)
    (dcli (r/parent ag))

    :order_by_clause
    (let [query-expression-body (z/left ag)]
      (r/select
       (fn [ag]
         (when (r/ctor? :query_specification ag)
           (env ag)))
       query-expression-body))

    (r/inherit ag)))

;; Group by

(defn- grouping-column-references [ag]
  (r/collect
   (fn [ag]
     (when (r/ctor? :column_reference ag)
       [(identifiers ag)]))
   ag))

(defn- grouping-columns [ag]
  (->> (r/collect-stop
        (fn [ag]
          (r/zcase ag
            (:aggregate_function
             :having_clause)
            [[]]

            :group_by_clause
            [(grouping-column-references ag)]

            :subquery
            []

            nil))
        ag)
       (sort-by count)
       (last)))

(defn- group-env [ag]
  (r/zcase ag
    :query_specification
    (enter-env-scope (group-env (r/parent ag))
                     {:grouping-columns (grouping-columns ag)
                      :group-column-reference-type :ordinary
                      :column-reference-type :ordinary})
    (:select_list
     :having_clause)
    (let [group-env (group-env (r/parent ag))
          {:keys [grouping-columns]} (local-env group-env)]
      (cond-> group-env
        grouping-columns (update-env (fn [s]
                                       (assoc s
                                              :group-column-reference-type :group-invariant
                                              :column-reference-type :invalid-group-invariant)))))

    :aggregate_function
    (update-env (group-env (r/parent ag))
                (fn [s]
                  (assoc s :column-reference-type :within-group-varying)))

    (r/inherit ag)))

;; Select

(defn- set-operator [ag]
  (r/zcase ag
    :query_expression_body
    (case (r/lexeme ag 2)
      "UNION" "UNION"
      "EXCEPT" "EXCEPT"
      nil)

    :query_term
    (case (r/lexeme ag 2)
      "INTERSECT" "INTERSECT"
      nil)

    nil))

(declare column-reference)

(defn- projected-columns [ag]
  (r/zcase ag
    :table_primary
    (when-not (r/ctor? :qualified_join (r/$ ag 1))
      (let [{:keys [correlation-name
                    derived-columns] :as table
             table-id :id} (table ag)
            projections (if-let [derived-columns (not-empty derived-columns)]
                          (for [identifier derived-columns]
                            {:identifier identifier})
                          (if-let [subquery-ref (:subquery-ref (meta table))]
                            (first (projected-columns subquery-ref))
                            (let [query-specification (scope-element ag)
                                  query-expression (scope-element (r/parent query-specification))
                                  named-join-columns (for [identifier (named-columns-join-columns (r/parent ag))]
                                                       {:identifier identifier})]
                              (->> (r/collect
                                    (fn [ag]
                                      (when (r/ctor? :column_reference ag)
                                        (let [{:keys [identifiers] column-table-id :table-id} (column-reference ag)]
                                          (when (= table-id column-table-id)
                                            [{:identifier (last identifiers)}]))))
                                    query-expression)
                                   (concat named-join-columns)
                                   (distinct)))))]
        [(for [{:keys [identifier]} projections]
           (cond-> (with-meta {} {:table table})
             identifier (assoc :identifier identifier)
             correlation-name (assoc :qualified-column [correlation-name identifier])))]))

    :query_specification
    (letfn [(expand-asterisk [ag]
              (r/zcase ag
                :table_primary
                (let [projections (if (r/ctor? :qualified_join (r/$ ag 1))
                                    (r/collect-stop expand-asterisk (r/$ ag 1))
                                    (first (projected-columns ag)))
                      {:keys [grouping-columns]} (local-env (group-env ag))]
                  (if grouping-columns
                    (let [grouping-columns (set grouping-columns)]
                      (for [{:keys [qualified-column] :as projection} projections
                            :when (contains? grouping-columns qualified-column)]
                        projection))
                    projections))

                :subquery
                []

                nil))

            (calculate-select-list [ag]
              (r/zcase ag
                :asterisk
                (let [table-expression (z/right (r/parent ag))]
                  (r/collect-stop expand-asterisk table-expression))

                :qualified_asterisk
                (let [identifiers (identifiers (r/$ ag 1))
                      table-expression (z/right (r/parent ag))]
                  (for [{:keys [qualified-column] :as projection} (r/collect-stop expand-asterisk table-expression)
                        :when (= identifiers (butlast qualified-column))]
                    projection))

                :derived_column
                [(let [identifier (identifier ag)
                       qualified-column (when (r/ctor? :column_reference (r/$ ag 1))
                                          (identifiers (r/$ ag 1)))]
                   (with-meta
                     (cond-> {:normal-form (z/node (r/$ ag 1))}
                       identifier (assoc :identifier identifier)
                       qualified-column (assoc :qualified-column qualified-column))
                     {:ref ag}))]

                :subquery
                []

                nil))]
      [(reduce
        (fn [acc projection]
          (conj acc (assoc projection :index (count acc))))
        []
        (r/collect-stop calculate-select-list ag))])

    :query_expression
    (if (r/ctor? :with_clause (r/$ ag 1))
      (projected-columns (r/$ ag 2))
      (projected-columns (r/$ ag 1)))

    :collection_derived_table
    (if (= "ORDINALITY" (r/lexeme ag -1))
      [[{:index 0} {:index 1}]]
      [[{:index 0}]])

    :table_value_constructor
    (projected-columns (r/$ ag 2))

    :row_value_expression_list
    (r/collect-stop
     (fn [ag]
       (r/zcase ag
         :row_value_expression_list
         nil

         :explicit_row_value_constructor
         (let [degree (r/collect-stop
                       (fn [ag]
                         (r/zcase ag
                           :row_value_constructor_element
                           1

                           :subquery
                           0

                           nil))
                       +
                       ag)]
           [(vec (for [n (range degree)]
                   {:index n}))])

         :subquery
         (projected-columns (r/$ ag 1))

         (when (r/ctor ag)
           [[{:index 0}]])))
     ag)

    (:query_expression_body
     :query_term)
    (r/collect-stop
     (fn [ag]
       (r/zcase ag
         (:query_specification
          :table_value_constructor)
         (projected-columns ag)

         :subquery
         []

         nil))
     ag)

    :subquery
    (projected-columns (r/$ ag 1))

    (r/inherit ag)))

;; Order by

(defn- order-by-index [ag]
  (r/zcase ag
    :query_expression
    nil

    :sort_specification
    (first (for [{:keys [normal-form index identifier]} (first (projected-columns ag))
                 :when (or (= normal-form (r/lexeme ag 1))
                           (= identifier (->src-str (r/$ ag 1))))]
             index))

    (r/inherit ag)))

(defn- order-by-indexes [ag]
  (r/zcase ag
    :query_expression
    (r/collect-stop
     (fn [ag]
       (r/zcase ag
         :sort_specification
         [(order-by-index ag)]

         :subquery
         []

         nil))
     ag)

    (r/inherit ag)))

(defn- ordering-specification [ag]
  (r/zcase ag
    :ordering_specification
    (case (r/lexeme ag -1)
      "ASC" "ASC"
      "DESC" "DESC"
      "ASC")

    :sort_specification
    (or (ordering-specification (r/$ ag -1))
        (ordering-specification (r/$ ag -2)))

    nil))

;; Column references

(defn- column-reference [ag]
  (r/zcase ag
    :column_reference
    (let [identifiers (identifiers ag)
          env (env ag)
          column-scope-id (id (scope-element ag))
          {table-id :id table-scope-id :scope-id :as table} (when (qualified? identifiers)
                                                              (find-decl env (first identifiers)))
          outer-reference? (and table (< ^long table-scope-id ^long column-scope-id))
          group-env (group-env ag)
          column-reference-type (reduce
                                 (fn [acc {:keys [grouping-columns
                                                  group-column-reference-type]}]
                                   (if (contains? (set grouping-columns) identifiers)
                                     (reduced group-column-reference-type)
                                     acc))
                                 (get (local-env group-env) :column-reference-type :ordinary)
                                 group-env)
          column-reference-type (cond
                                  outer-reference?
                                  (case column-reference-type
                                    :ordinary :outer
                                    :group-invariant :outer-group-invariant
                                    :within-group-varying :invalid-outer-within-group-varying
                                    :invalid-group-invariant :invalid-outer-group-invariant)

                                  (order-by-index ag)
                                  :resolved-in-sort-key

                                  (not (qualified? identifiers))
                                  :unqualified

                                  :else
                                  column-reference-type)]
      (with-meta
        (cond-> {:identifiers identifiers
                 :type column-reference-type
                 :scope-id column-scope-id}
          table (assoc :table-id table-id :table-scope-id table-scope-id))
        (cond-> {:ref ag}
          table (assoc :table table))))))

;; Errors

(defn- local-names [[s :as env] k]
  (->> (mapcat val s)
       (map k)))

(defn- check-duplicates [label ag xs]
  (vec (for [[x ^long freq] (frequencies xs)
             :when (> freq 1)]
         (format (str label " duplicated: %s %s")
                 x (->line-info-str ag)))))

(defn- check-unsigned-integer [label ag]
  (when-not (r/zmatch ag
              [:signed_numeric_literal
               [:exact_numeric_literal
                [:unsigned_integer _]]]
              ;;=>
              true

              [:host_parameter_name _]
              ;;=>
              true

              #"^ROWS?$"
              ;;=>
              true)
    [(format (str label " must be an integer: %s %s")
             (->src-str ag) (->line-info-str ag))]))

(defn- check-aggregate-or-subquery [label ag]
  (r/collect-stop
   (fn [inner-ag]
     (r/zcase inner-ag
       :aggregate_function
       [(format (str label " cannot contain aggregate functions: %s %s")
                (->src-str ag) (->line-info-str ag))]
       :query_expression
       [(format (str label " cannot contain nested queries: %s %s")
                (->src-str ag) (->line-info-str ag))]
       nil))
   ag))

(defn- check-set-operator [ag]
  (when-let [set-op (set-operator ag)]
    (let [candidates (projected-columns ag)
          degrees (mapv count candidates)]
      (when-not (apply = degrees)
        [(format "%s requires tables to have same degree: %s"
                 set-op (->line-info-str ag))]))))

(defn- check-values [ag]
  (let [candidates (projected-columns ag)
        degrees (mapv count candidates)]
    (when-not (apply = degrees)
      [(format "VALUES requires rows to have same degree: %s"
               (->line-info-str ag))])))

(defn- check-derived-columns [ag]
  (let [{:keys [derived-columns] :as table} (table ag)]
    (when derived-columns
      (when-let [candidates (some->> (:subquery-ref (meta table))
                                     (projected-columns))]
        (let [degrees (mapv count candidates)]
          (when-not (apply = (count derived-columns) degrees)
            [(format "Derived columns has to have same degree as table: %s"
                     (->line-info-str ag))]))))))

(defn- check-column-reference [ag]
  (let [{:keys [identifiers table-id type] :as column-reference} (column-reference ag)]
    (case type
      (:ordinary
       :group-invariant
       :within-group-varying
       :outer
       :outer-group-invariant)
      (if-not table-id
        [(format "Table not in scope: %s %s"
                 (first identifiers) (->line-info-str ag))]

        (let [projection (first (projected-columns (:ref (meta (:table (meta column-reference))))))]
          (cond
            (->> (map :qualified-column projection)
                 (not-any? #{identifiers}))
            [(format "Column not in scope: %s %s"
                     (str/join "." identifiers) (->line-info-str ag))]

            (< 1 (->> (map :identifier projection)
                      (filter #{(last identifiers)})
                      (count)))
            [(format "Column name ambiguous: %s %s"
                     (str/join "." identifiers) (->line-info-str ag))]

            :else
            [])))

      :resolved-in-sort-key
      []

      :unqualified
      [(format "XTDB requires fully-qualified columns: %s %s"
               (->src-str ag) (->line-info-str ag))]

      :invalid-group-invariant
      [(format "Column reference is not a grouping column: %s %s"
               (->src-str ag) (->line-info-str ag))]

      :invalid-outer-group-invariant
      [(format "Outer column reference is not an outer grouping column: %s %s"
               (->src-str ag) (->line-info-str ag))]

      :invalid-outer-within-group-varying
      [(format "Within group varying column reference is an outer column: %s %s"
               (->src-str ag) (->line-info-str ag))])))

(defn- check-where-clause [ag]
  (r/collect-stop
   (fn [ag]
     (r/zcase ag
       :aggregate_function
       [(format "WHERE clause cannot contain aggregate functions: %s %s"
                (->src-str ag) (->line-info-str ag))]

       :subquery
       []

       nil))
   ag))

(defn- subquery-type [ag]
  (r/zcase ag
    (:query_expression
     :in_value_list)
    {:type :scalar_subquery :single? true}

    :array_value_constructor_by_query
    {:type :table_subquery :single? true}

    (:with_list_element
     :table_primary
     :in_predicate_value
     :quantified_comparison_predicate_part_2
     :exists_predicate)
    {:type :table_subquery}

    (:table_value_constructor
     :row_value_constructor
     :explicit_row_value_constructor)
    {:type :row_subquery}

    (r/inherit ag)))

(defn- check-subquery [ag]
  (let [{:keys [single?]} (subquery-type (r/parent ag))]
    (when (and single? (not= 1 (count (first (projected-columns ag)))))
      [(format "Subquery does not select single column: %s %s"
               (->src-str ag) (->line-info-str ag))])))

(defn- check-select-list [ag]
  (when (= [[]] (projected-columns ag))
    [(format "Query does not select any columns: %s"
             (->line-info-str ag))]))

(defn- check-asterisked-identifier-chain [ag]
  (let [identifiers (identifiers ag)]
    (when-not (find-local-decl (env ag) (first identifiers))
      [(format "Table not in scope: %s %s"
               (first identifiers) (->line-info-str ag))])))

(defn- check-named-columns-join [ag]
  (let [{:keys [join-columns lhs rhs]} (named-columns-join-env ag)
        join-columns (set join-columns)]
    (->> (for [[label side] [["Left" lhs] ["Right" rhs]]]
           (cond-> []
             (not= join-columns (set (keys side)))
             (conj (format "%s side does not contain all join columns: %s %s"
                           label (->src-str ag) (->line-info-str ag)))

             (not (apply = 1 (map count (vals side))))
             (conj (format "%s side contains ambiguous join columns: %s %s"
                           label (->src-str ag) (->line-info-str ag)))))
         (reduce into))))

(defn- errs [ag]
  (r/collect
   (fn [ag]
     (r/zcase ag
       :table_reference_list
       (check-duplicates "Table variable" ag
                         (local-names (dclo ag) :correlation-name))

       :with_list
       (check-duplicates "CTE query name" ag
                         (local-names (cteo ag) :query-name))

       (:query_expression_body
        :query_term)
       (check-set-operator ag)

       :select_list
       (check-select-list ag)

       :asterisked_identifier_chain
       (check-asterisked-identifier-chain ag)

       :table_value_constructor
       (check-values ag)

       :table_primary
       (check-derived-columns ag)

       :column_name_list
       (check-duplicates "Column name" ag (identifiers ag))

       :column_reference
       (check-column-reference ag)

       (:general_set_function
        :array_aggregate_function)
       (check-aggregate-or-subquery "Aggregate functions" ag)

       :sort_specification
       (check-aggregate-or-subquery "Sort specifications" ag)

       :where_clause
       (check-where-clause ag)

       :fetch_first_clause
       (check-unsigned-integer "Fetch first row count" (r/$ ag 3))

       :result_offset_clause
       (check-unsigned-integer "Offset row count" (r/$ ag 2))

       :subquery
       (check-subquery ag)

       :named_columns_join
       (check-named-columns-join ag)

       []))
   ag))

;; Scopes

(defn- all-column-references [ag]
  (r/collect
   (fn [ag]
     (when (r/ctor? :column_reference ag)
       [(column-reference ag)]))
   ag))

(defn- scope-element [ag]
  (r/zcase ag
    (:query_expression
     :query_specification)
    ag

    (r/inherit ag)))

(defn- scope [ag]
  (r/zcase ag
    (:query_expression
     :query_specification)
    (let [scope-id (id ag)
          parent-id (some-> (scope-element (r/parent ag)) (id))
          all-columns (all-column-references ag)
          table-id->all-columns (->> (group-by :table-id all-columns)
                                     (into (sorted-map)))
          ;; NOTE: assumes that tables declared in
          ;; outer scopes have lower ids than the
          ;; current scope.
          dependent-columns (->> (subseq (dissoc table-id->all-columns nil) < scope-id)
                                 (mapcat val)
                                 (set))
          projected-columns (->> (projected-columns ag)
                                 (first)
                                 (mapv #(dissoc % :normal-form)))
          scope (with-meta
                  (cond-> {:id scope-id
                           :dependent-columns dependent-columns
                           :projected-columns projected-columns}
                    parent-id (assoc :parent-id parent-id))
                  {:ref ag})]
      (r/zcase ag
        :query_expression
        (let [local-ctes (local-env-singleton-values (cteo ag))
              order-by-indexes (order-by-indexes ag)]
          (cond-> (assoc scope
                         :type :query-expression
                         :ctes local-ctes)
            (not-empty order-by-indexes) (assoc :order-by-indexes order-by-indexes)))

        :query_specification
        (let [local-tables (local-tables ag)
              local-columns (set (get (group-by :scope-id all-columns) scope-id))
              local-tables (->> (for [{:keys [id correlation-name] :as table} local-tables
                                      :let [used-columns (->> (get table-id->all-columns id)
                                                              (map :identifiers)
                                                              (set))]]
                                  [correlation-name (assoc table :used-columns used-columns)])
                                (into {}))
              grouping-columns (grouping-columns ag)]
          (cond-> (assoc scope
                         :tables local-tables
                         :columns local-columns
                         :type :query-specification)
            grouping-columns (assoc :grouping-columns grouping-columns)))))

    (r/inherit ag)))

(defn- scopes [ag]
  (r/collect
   (fn [ag]
     (r/zcase ag
       (:query_expression
        :query_specification)
       [(scope ag)]

       []))
   ag))

;; Analysis API, might be deprecated.

(defn analyze-query [query]
  (if-let [parse-failure (insta/get-failure query)]
    {:errs [(prn-str parse-failure)]}
    (r/with-memoized-attributes [#'id
                                 #'ctei
                                 #'cteo
                                 #'cte-env
                                 #'dcli
                                 #'dclo
                                 #'env
                                 #'group-env
                                 #'projected-columns
                                 #'column-reference
                                 #'scope-element
                                 #'scope]
      #(let [ag (z/vector-zip query)]
         (if-let [errs (not-empty (errs ag))]
           {:errs errs}
           {:scopes (scopes ag)
            :errs []})))))

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

(defn- unqualifed-projection-symbol [{:keys [identifier ^long index] :as projection}]
  (symbol (or identifier (str "$column_" (inc index) "$"))))

(defn- qualified-projection-symbol [{:keys [qualified-column] :as projection}]
  (let [{derived-column :ref table :table} (meta projection)]
    (if derived-column
      (expr (r/$ derived-column 1))
      (id-symbol (first qualified-column) (:id table) (second qualified-column)))))

(defn- aggregate-symbol [prefix z]
  (let [query-id (id (scope-element z))]
    (symbol (str "$" prefix "__" query-id "_" (id z) "$"))))

;; Expressions.

(defn- expr [z]
  (maybe-add-ref
   z
   (r/zmatch z
     [:column_reference _]
     (let [{:keys [table-id identifiers]} (column-reference z)
           [table column] identifiers]
       (id-symbol table table-id column))

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
      (let [{:keys [join-columns] :as env} (named-columns-join-env z)]
        (for [column join-columns]
          (->> (for [side [:lhs :rhs]]
                 (qualified-projection-symbol (first (get-in env [side column]))))
               (apply list '=)))))

     [:aggregate_function _]
     (aggregate-symbol "agg_out" z)

     (throw (IllegalArgumentException. (str "Cannot build expression for: "  (pr-str (z/node z))))))))

;; Logical plan.

(defn- wrap-with-select [sc-expr relation]
  (reduce
   (fn [acc predicate]
     [:select predicate acc])
   relation
   ((fn step [sc-expr]
      (if (and (list? sc-expr)
               (= 'and (first sc-expr)))
        (concat (step (nth sc-expr 1))
                (step (nth sc-expr 2)))
        [sc-expr]))
    sc-expr)))

(defn- needs-group-by? [z]
  (boolean (:grouping-columns (local-env (group-env z)))))

(defn- wrap-with-group-by [te relation]
  (let [projection (first (projected-columns te))
        {:keys [grouping-columns]} (local-env (group-env te))
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
                    (scope-element te))]
    [:group-by (->> (for [aggregate aggregates]
                      (r/zmatch aggregate
                        [:aggregate_function [:general_set_function [:computational_operation sf] _]]
                        {(aggregate-symbol "agg_out" aggregate)
                         (list (symbol (str/lower-case sf)) (aggregate-symbol "agg_in" aggregate))}))
                    (into grouping-columns))
     [:project (->> (for [aggregate aggregates]
                      (r/zmatch aggregate
                        [:aggregate_function [:general_set_function _ ^:z ve]]
                        {(aggregate-symbol "agg_in" aggregate) (expr ve)}))
                    (into grouping-columns))
      relation]]))

(declare expr-symbols)

(defn- wrap-with-order-by [ssl relation]
  (let [projection (first (projected-columns ssl))
        query-id (id (scope-element ssl))
        order-by-specs (r/collect-stop
                        (fn [z]
                          (r/zcase z
                            :sort_specification
                            (let [direction (case (ordering-specification z)
                                              "ASC" :asc
                                              "DESC" :desc
                                              :asc)]
                              [(if-let [idx (order-by-index z)]
                                 {:spec {(unqualifed-projection-symbol (nth projection idx)) direction}}
                                 (let [column (symbol (str "$order_by__" query-id  "_" (r/child-idx z) "$"))]
                                   {:spec {column direction}
                                    :projection {column (expr (r/$ z 1))}}))])

                            :subquery
                            []

                            nil))
                        ssl)
        order-by-projection (keep :projection order-by-specs)
        extra-projection (distinct (mapcat (comp expr-symbols vals) order-by-projection))
        base-projection (mapv unqualifed-projection-symbol projection)
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

(declare plan)

(defn- build-query-specification [sl te]
  (let [projection (first (projected-columns sl))
        unqualified-rename-map (->> (for [{:keys [qualified-column] :as projection} projection
                                          :when qualified-column]
                                      [(qualified-projection-symbol projection)
                                       (unqualifed-projection-symbol projection)])
                                    (into {}))
        qualified-projection (vec (for [{:keys [qualified-column] :as projection} projection
                                        :let [derived-column (:ref (meta projection))]]
                                    (if qualified-column
                                      (qualified-projection-symbol projection)
                                      {(unqualifed-projection-symbol projection)
                                       (expr (r/$ derived-column 1))})))
        qualified-project [:project qualified-projection (plan te)]]
    (if (not-empty unqualified-rename-map)
      [:rename unqualified-rename-map qualified-project]
      qualified-project)))

(defn- build-set-op [set-op lhs rhs]
  (let [lhs-unqualified-project (mapv unqualifed-projection-symbol (first (projected-columns lhs)))
        rhs-unqualified-project (mapv unqualifed-projection-symbol (first (projected-columns rhs)))]
    [set-op (plan lhs)
     (if (= lhs-unqualified-project rhs-unqualified-project)
       (plan rhs)
       [:rename (zipmap rhs-unqualified-project lhs-unqualified-project)
        (plan rhs)])]))

(defn- build-table-primary [tp]
  (let [{:keys [id correlation-name] :as table} (table tp)
        projection (first (projected-columns tp))]
    [:rename (with-meta
               (symbol (str correlation-name relation-id-delimiter id))
               {:table-reference {:table-id id
                                  :correlation-name correlation-name}})
     (if-let [subquery-ref (:subquery-ref (meta table))]
       (plan subquery-ref)
       [:scan (vec (for [{:keys [identifier]} projection]
                     (symbol identifier)))])]))

(defn- build-table-reference-list [trl]
  (reduce
   (fn [acc table]
     [:cross-join acc table])
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
     (cond->> (wrap-with-select (expr sc) (plan fc))
       (needs-group-by? z) (wrap-with-group-by z))

     [:table_expression ^:z fc [:where_clause _ ^:z sc] [:group_by_clause _ _ _]]
     ;;=>
     (->> (wrap-with-select (expr sc) (plan fc))
          (wrap-with-group-by z))

     [:table_expression ^:z fc [:where_clause _ ^:z sc] [:group_by_clause _ _ _] [:having_clause _ ^:z hsc]]
     ;;=>
     (->> (wrap-with-select (expr sc) (plan fc))
          (wrap-with-group-by z)
          (wrap-with-select (expr hsc)))

     [:table_expression ^:z fc [:where_clause _ ^:z sc] [:having_clause _ ^:z hsc]]
     ;;=>
     (->> (wrap-with-select (expr sc) (plan fc))
          (wrap-with-group-by z)
          (wrap-with-select (expr hsc)))

     [:table_expression ^:z fc [:group_by_clause _ _ _] [:having_clause _ ^:z hsc]]
     ;;=>
     (->> (wrap-with-group-by z)
          (wrap-with-select (expr hsc)))

     [:table_expression ^:z fc [:having_clause _ ^:z hsc]]
     ;;=>
     (->> (wrap-with-group-by z)
          (wrap-with-select (expr hsc)))

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
     (wrap-with-select (expr sc) [:join {} (plan lhs) (plan rhs)])

     [:qualified_join ^:z lhs ^:z jt _ ^:z rhs [:join_condition _ ^:z sc]]
     ;;=>
     (wrap-with-select (expr sc) (case (join-type jt)
                                   :left [:left-outer-join {} (plan lhs) (plan rhs)]
                                   :right [:left-outer-join {} (plan rhs) (plan lhs)]
                                   :inner [:join {} (plan lhs) (plan rhs)]))

     [:qualified_join ^:z lhs _ ^:z rhs ^:z ncj]
     ;;=>
     (wrap-with-select (expr ncj) [:join {} (plan lhs) (plan rhs)])

     [:qualified_join ^:z lhs ^:z jt _ ^:z rhs ^:z ncj]
     ;;=>
     (wrap-with-select (expr ncj) (case (join-type jt)
                                    :left [:left-outer-join {} (plan lhs) (plan rhs)]
                                    :right [:left-outer-join {} (plan rhs) (plan lhs)]
                                    :inner [:join {} (plan lhs) (plan rhs)]))

     [:from_clause _ ^:z trl]
     ;;=>
     (build-table-reference-list trl)

     (throw (IllegalArgumentException. (str "Cannot build plan for: "  (pr-str (z/node z))))))))

;; Rewriting of logical plan.

(defn- table-references-in-subtree [op]
  (set (r/collect-stop
        (fn [z]
          (r/zmatch z
            [:rename prefix _]
            (when-let [table-reference (:table-reference (meta prefix))]
              [table-reference])))
        (z/vector-zip op))))

(defn- table-ids-in-subtree [op]
  (->> (table-references-in-subtree op)
       (map :table-id)
       (set)))

(defn- expr-symbols [expr]
  (set (for [x (flatten expr)
             :when (:column-reference (meta x))]
         x)))

(defn- expr-column-references [expr]
  (->> (expr-symbols expr)
       (map (comp :column-reference meta))
       (set)))

(defn- expr-table-ids [expr]
  (->> (expr-column-references expr)
       (map :table-id)
       (set)))

(defn- equals-predicate? [predicate]
  (and (= '= (first predicate))
       (= 3 (count predicate))))

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
  (if (= 'and (first predicate))
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
      [join-type {} lhs rhs]]
     ;;=>
     (when-let [join-map (build-join-map predicate lhs rhs)]
       [join-type join-map lhs rhs])))

(defn- push-selection-down-past-join [z]
  (r/zmatch z
    [:select predicate
     [join-type join-map lhs rhs]]
    ;;=>
    (let [expr-table-ids (expr-table-ids predicate)
          lhs-table-ids (table-ids-in-subtree lhs)
          rhs-table-ids (table-ids-in-subtree rhs)
          on-lhs? (set/subset? expr-table-ids lhs-table-ids)
          on-rhs? (set/subset? expr-table-ids rhs-table-ids)]
      (cond
        (and on-rhs? (not on-lhs?))
        [join-type join-map lhs [:select predicate rhs]]

        (and on-lhs? (not on-rhs?))
        [join-type join-map [:select predicate lhs] rhs]))))

(defn- push-selections-with-fewer-variables-down [z]
  (r/zmatch z
    [:select predicate-1
     [:select predicate-2
      relation]]
    ;;=>
    (when (< (count (expr-table-ids predicate-1))
             (count (expr-table-ids predicate-2)))
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

(defn- add-selection-to-scan-predicate [z]
  (r/zmatch z
    [:select predicate
     [:rename prefix [:scan columns]]]
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
                                    (if (= single-symbol (symbol (str prefix relation-prefix-delimiter column)))
                                      (let [predicate (w/postwalk-replace {single-symbol column} predicate)]
                                        (if (map? column-or-select)
                                          (update column-or-select column (partial merge-conjunctions predicate))
                                          {column predicate}))
                                      column-or-select)))
                             acc)))
                       columns
                       (conjunction-clauses predicate))]
      (when-not (= columns new-columns)
        [:select predicate
         [:rename prefix [:scan new-columns]]]))))

(def ^:private optimize-plan
  (some-fn promote-selection-cross-join-to-join
           promote-selection-to-join
           push-selection-down-past-join
           push-selections-with-fewer-variables-down
           push-selections-with-equals-down
           merge-selections-with-same-variables
           add-selection-to-scan-predicate))

;; Logical plan API

(defn plan-query [query]
  (if-let [parse-failure (insta/get-failure query)]
    {:errs [(prn-str parse-failure)]}
    (r/with-memoized-attributes [#'id
                                 #'ctei
                                 #'cteo
                                 #'cte-env
                                 #'dcli
                                 #'dclo
                                 #'env
                                 #'group-env
                                 #'projected-columns
                                 #'column-reference]
      #(let [ag (z/vector-zip query)]
         (if-let [errs (not-empty (errs ag))]
           {:errs errs}
           {:plan (->> (z/vector-zip (plan ag))
                       (r/innermost (r/mono-tp optimize-plan))
                       (z/node)
                       (s/assert ::lp/logical-plan))})))))

;; SQL:2011 official grammar:

;; https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html

;; Repository has both grammars for 1992, 1999, 2003, 2006, 2008, 2011
;; and 2016 and draft specifications for 1992, 1999, 2003, 2006 and
;; 2011: https://github.com/JakeWheat/sql-overview

;; See also Date, SQL and Relational Theory, p. 455-458, A Simplified
;; BNF Grammar

;; High level SQL grammar, from
;; https://calcite.apache.org/docs/reference.html

;; SQLite grammar:
;; https://github.com/bkiers/sqlite-parser/blob/master/src/main/antlr4/nl/bigo/sqliteparser/SQLite.g4
;; https://www.sqlite.org/lang_select.html

;; SQL BNF from the spec:
;; https://ronsavage.github.io/SQL/

;; PostgreSQL Antlr4 grammar:
;; https://github.com/tshprecher/antlr_psql/tree/master/antlr4

;; PartiQL: SQL-compatible access to relational, semi-structured, and nested data.
;; https://partiql.org/assets/PartiQL-Specification.pdf

;; SQL-99 Complete, Really
;; https://crate.io/docs/sql-99/en/latest/index.html

;; Why CockroachDB and PostgreSQL Are Compatible
;; https://www.cockroachlabs.com/blog/why-postgres/

;; GQL Parser
;; https://github.com/OlofMorra/GQL-parser

;; Graph database applications with SQL/PGQ
;; https://download.oracle.com/otndocs/products/spatial/pdf/AnD2020/AD_Develop_Graph_Apps_SQL_PGQ.pdf

;; Dependent join symbols:  ⧑ ⧔ ⯈

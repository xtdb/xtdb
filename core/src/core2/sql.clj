(ns core2.sql
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.walk :as w]
            [clojure.zip :as z]
            [core2.rewrite :as r]
            [instaparse.core :as insta]
            [instaparse.failure]))

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
                  (not (contains? #{:table_primary :query_expression} fst))
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

;; Draft attribute grammar for SQL semantics.

;; TODO:
;; - error on unresolved asterisked qualifier.
;; - error if select expands to 0 degree.
;; - prune duplicates caused by asterisk.
;; - deal with actual duplicated names.
;; - calculate projection for referenced CTE.
;; - add derived columns on base table when expanding asterisk.
;; - sanity check base table used columns logic when expanding asterisk.
;; - add or filter by potential grouping columns when expanding asterisk.

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

(declare cte-env env scope-id subquery-scope-id)

;; Ids

(defn- id [ag]
  (r/zcase ag
    :table_primary
    (if (r/ctor? :qualified_join ag)
      (id (z/prev ag))
      (inc ^long (id (z/prev ag))))

    (:query_expression
     :query_specification
     :with_list_element)
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
    {:query-name (table-or-query-name ag)
     :id (id ag)
     :scope-id (scope-id ag)
     :subquery-scope-id (subquery-scope-id (r/$ ag -1))}))

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

(defn- table [ag]
  (r/zcase ag
    :table_primary
    (when-not (r/ctor? :qualified_join (r/$ ag 1))
      (let [table-name (table-or-query-name ag)
            correlation-name (or (correlation-name ag) table-name)
            {cte-id :id cte-scope-id :scope-id} (find-decl (cte-env ag) table-name)
            subquery-scope-id (when (nil? table-name)
                                (subquery-scope-id ag))
            derived-columns (derived-columns ag)]
        (cond-> {:correlation-name correlation-name
                 :id (id ag)
                 :scope-id (scope-id ag)}
          table-name (assoc :table-or-query-name table-name)
          derived-columns (assoc :derived-columns derived-columns)
          subquery-scope-id (assoc :subquery-scope-id subquery-scope-id)
          cte-id (assoc :cte-id cte-id :cte-scope-id cte-scope-id))))))

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

(defn- corresponding [ag]
  (r/zcase ag
    :query_expression_body
    (or (corresponding (r/$ ag -2))
        (corresponding (r/$ ag 2)))

    :query_term
    (corresponding (r/$ ag -2))

    :corresponding_spec
    (if (r/single-child? ag )
      {}
      {:identifiers (identifiers (r/$ ag -1))})

    nil))

(defn- projected-columns [ag]
  (r/zcase ag
    :query_specification
    (letfn [(expand-asterisk-table [ag]
              (r/zcase ag
                :table_value_constructor
                (first (projected-columns ag))

                :subquery
                (first (projected-columns (r/$ ag 1)))

                nil))

            (expand-asterisk [ag]
              (r/zcase ag
                :table_primary
                (if (r/ctor? :qualified_join (r/$ ag 1))
                  (r/collect-stop expand-asterisk (r/$ ag 1))
                  (let [correlation-name (or (correlation-name ag)
                                             (table-or-query-name ag))]
                    (for [{:keys [identifier] :as projection} (if-let [derived-columns (not-empty (derived-columns ag))]
                                                                (for [identifier derived-columns]
                                                                  {:identifier identifier})
                                                                (r/collect-stop expand-asterisk-table ag))]
                      (cond-> projection
                        correlation-name (assoc :qualified-column [correlation-name identifier])))))

                :column_reference
                (let [identifiers (identifiers ag)]
                  [(cond-> {:identifier (last identifiers)}
                     (qualified? identifiers) (assoc :qualified-column identifiers))])

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
                [(let [identifier (identifier ag)]
                   (cond-> {:normal-form (z/node ag)}
                     identifier (assoc :identifier identifier)))]

                :subquery
                []

                nil))]
      [(vec (for [[idx projection] (->> (r/collect-stop calculate-select-list ag)
                                        (distinct)
                                        (map-indexed vector))]
              (assoc projection :index idx)))])

    :query_expression
    (if (r/ctor? :with_clause (r/$ ag 1))
      (projected-columns (r/$ ag 2))
      (projected-columns (r/$ ag 1)))

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
    (let [candidates (r/collect-stop
                      (fn [ag]
                        (r/zcase ag
                          (:query_specification
                           :table_value_constructor)
                          (projected-columns ag)

                          :subquery
                          []

                          nil))
                      ag)]
      (if (set-operator ag)
        (if-let [{:keys [identifiers] :as corresponding} (corresponding ag)]
          (let [common-identifiers (->> (for [projections candidates]
                                          (set (for [{:keys [identifier]} projections]
                                                 identifier)))
                                        (reduce set/intersection))
                identifiers (if identifiers
                              (if (set/subset? (set identifiers) common-identifiers)
                                (set identifiers)
                                #{})
                              common-identifiers)]
            [(vec (for [{:keys [identifier] :as projection} (first candidates)
                        :when (contains? identifiers identifier)]
                    projection))])
          candidates)
        candidates))

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

;; Order by

(defn- order-by-index [ag]
  (r/zcase ag
    :query_expression
    nil

    :sort_specification
    (first (for [{:keys [normal-form index identifier]} (first (projected-columns ag))
                 :when (or (= normal-form (r/lexeme ag 1))
                           (= identifier (->src-str ag)))]
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

;; Column references

(defn- column-reference [ag]
  (r/zcase ag
    :column_reference
    (let [identifiers (identifiers ag)
          env (env ag)
          column-scope-id (scope-id ag)
          {table-id :id table-scope-id :scope-id} (when qualified?
                                                    (find-decl env (first identifiers)))
          outer-reference? (and table-scope-id (< ^long table-scope-id ^long column-scope-id))
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
      (cond-> {:identifiers identifiers
               :type column-reference-type
               :scope-id column-scope-id}
        table-id (assoc :table-id table-id :table-scope-id table-scope-id)))))

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
      (cond
        (= [0] degrees)
        [(format "%s does not have corresponding columns: %s"
                 set-op (->line-info-str ag))]

        (not (apply = degrees))
        [(format "%s requires tables to have same degree: %s"
                 set-op (->line-info-str ag))]))))

(defn- check-values [ag]
  (let [candidates (projected-columns ag)
        degrees (mapv count candidates)]
    (when-not (apply = degrees)
      [(format "VALUES requires rows to have same degree: %s"
               (->line-info-str ag))])))

(defn- check-derived-columns [ag]
  (when-let [derived-columns (derived-columns ag)]
    (let [candidates (r/collect-stop
                      (fn [ag]
                        (r/zcase ag
                          :table_value_constructor
                          (projected-columns ag)

                          :subquery
                          (projected-columns (r/$ ag 1))

                          nil))
                      ag)
          degrees (mapv count candidates)]
      (when-not (apply = (count derived-columns) degrees)
        [(format "Derived columns has to have same degree as table: %s"
                 (->line-info-str ag))]))))

(defn- check-column-reference [ag]
  (let [{:keys [identifiers table-id type] :as cr} (column-reference ag)]
    (case type
      (:ordinary
       :group-invariant
       :within-group-varying
       :outer
       :outer-group-invariant)
      (if-not table-id
        [(format "Table not in scope: %s %s"
                 (first identifiers) (->line-info-str ag))]
        [])

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

       []))
   ag))

;; Scopes

(defn- all-column-references [ag]
  (r/collect
   (fn [ag]
     (when (r/ctor? :column_reference ag)
       [(column-reference ag)]))
   ag))

(defn- scope-id [ag]
  (r/zcase ag
    (:query_expression
     :query_specification)
    (id ag)

    (r/inherit ag)))

(defn- subquery-scope-id [ag]
  (r/zcase ag
    (:query_expression
     :query_specification)
    (scope-id ag)

    (:table_primary
     :subquery)
    (subquery-scope-id (r/$ ag 1))

    :lateral_derived_table
    (subquery-scope-id (r/$ ag 2))

    nil))

(defn- scope [ag]
  (r/zcase ag
    (:query_expression
     :query_specification)
    (let [id (scope-id ag)
          parent-id (scope-id (r/parent ag))
          all-columns (all-column-references ag)
          table-id->all-columns (->> (group-by :table-id all-columns)
                                     (into (sorted-map)))
          ;; NOTE: assumes that tables declared in
          ;; outer scopes have lower ids than the
          ;; current scope.
          dependent-columns (->> (subseq (dissoc table-id->all-columns nil) < id)
                                 (mapcat val)
                                 (set))
          projected-columns (->> (projected-columns ag)
                                 (first)
                                 (mapv #(dissoc % :normal-form)))
          scope (cond-> {:id id
                         :type type
                         :dependent-columns dependent-columns
                         :projected-columns projected-columns}
                  parent-id (assoc :parent-id parent-id))]
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
              local-columns (set (get (group-by :scope-id all-columns) id))
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

;; API

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
                                 #'scope-id
                                 #'scope]
      #(let [ag (z/vector-zip query)]
         (if-let [errs (not-empty (errs ag))]
           {:errs errs}
           {:scopes (scopes ag)
            :errs []})))))

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

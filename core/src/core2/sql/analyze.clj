(ns core2.sql.analyze
  (:require [clojure.string :as str]
            [clojure.zip :as z]
            [core2.rewrite :as r]
            [instaparse.core :as insta]
            [instaparse.failure]))

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

(defn local-env [[s]]
  s)

(defn local-env-singleton-values [[s]]
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

(defn ^:dynamic prev-subtree-seq [ag]
  (when ag
    (lazy-seq (cons ag (prev-subtree-seq (z/prev ag))))))

(defn ^:dynamic id [ag]
  (dec (count (prev-subtree-seq ag))))

(defn ^:dynamic dynamic-param-idx [ag]
  (->> (prev-subtree-seq ag)
       (filter (partial r/ctor? :dynamic_parameter_specification))
       (count)
       (dec)))

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

      [:derived_column _ [:as_clause [:regular_identifier as]]]
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
(defn ^:dynamic ctei [ag]
  (r/zcase ag
    :query_expression
    (enter-env-scope (cte-env (r/parent ag)))

    :with_list_element
    (let [cte-env (ctei (r/left-or-parent ag))
          {:keys [query-name] :as cte} (cte ag)]
      (extend-env cte-env query-name cte))

    (r/inherit ag)))

;; Synthesised
(defn ^:dynamic cteo [ag]
  (r/zcase ag
    :query_expression
    (if (r/ctor? :with_clause (r/$ ag 1))
      (cteo (r/$ ag 1))
      (ctei ag))

    :with_clause
    (cteo (r/$ ag -1))

    :with_list
    (ctei (r/$ ag -1))))

(defn ^:dynamic cte-env [ag]
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

(defn derived-columns [ag]
  (r/zcase ag
    :table_primary
    (when (r/ctor? :column_name_list (r/$ ag -1))
      (identifiers (r/$ ag -1)))))

(defn subquery-element [ag]
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

(defn table [ag]
  (r/zcase ag
    :table_primary
    (when-not (r/ctor? :qualified_join (r/$ ag 1))
      (let [table-name (table-or-query-name ag)
            correlation-name (or (correlation-name ag) table-name)
            cte (when table-name
                  (find-decl (cte-env ag) table-name))
            sq-element (when (nil? cte)
                         (subquery-element ag))
            sq-scope-id (when (and sq-element (not= :collection_derived_table (r/ctor sq-element)))
                          (id sq-element))
            derived-columns (or (derived-columns ag) (:columns cte))]
        (with-meta
          (cond-> {:correlation-name correlation-name
                   :id (id ag)
                   :scope-id (id (scope-element ag))}
            table-name (assoc :table-or-query-name table-name)
            derived-columns (assoc :derived-columns derived-columns)
            sq-scope-id (assoc :subquery-scope-id sq-scope-id)
            cte (assoc :cte-id (:id cte) :cte-scope-id (:scope-id cte)))
          (cond-> {:ref ag}
            sq-element (assoc :subquery-ref sq-element)
            cte (assoc :cte cte
                       :subquery-ref (subquery-element (r/$ (:ref (meta cte)) -1)))))))))

(defn local-tables [ag]
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

(defn join-type [ag]
  (if (r/ctor? :qualified_join ag)
    (join-type (r/$ ag 2))
    (r/zmatch ag
      [:join_type [:outer_join_type ojt]] ojt
      [:join_type [:outer_join_type ojt] "OUTER"] ojt
      [:join_type "INNER"] "INNER"
      "JOIN" "INNER"
      nil)))

(defn- named-columns-join-columns [ag]
  (r/zcase ag
    :qualified_join
    (named-columns-join-columns (r/$ ag -1))

    :named_columns_join
    (identifiers (r/$ ag -1))

    nil))

(defn named-columns-join-env [ag]
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
                       (->> (for [table (local-tables side)
                                  projection (first (projected-columns (:ref (meta table))))]
                              projection)
                            (group-by :identifier))
                       join-columns))]
      {:join-columns join-columns
       :lhs lhs
       :rhs rhs})

    (r/inherit ag)))

;; Inherited
(defn ^:dynamic dcli [ag]
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
(defn ^:dynamic dclo [ag]
  (r/zcase ag
    :query_specification
    (dclo (r/$ ag -1))

    :table_expression
    (dclo (r/$ ag 1))

    :from_clause
    (dclo (r/$ ag 2))

    :table_reference_list
    (dcli (r/$ ag -1))))

(defn ^:dynamic env [ag]
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
      (env query-expression-body))

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

(defn ^:dynamic group-env [ag]
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

(defn ^:dynamic projected-columns [ag]
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
        [(reduce
          (fn [acc {:keys [identifier index] :as projection}]
            (conj acc (cond-> (with-meta {:index (count acc)} {:table table})
                        identifier (assoc :identifier identifier)
                        (and index (nil? identifier)) (assoc :original-index index)
                        correlation-name (assoc :qualified-column [correlation-name identifier]))))
          []
          projections)]))

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
                (let [identifier (identifier ag)
                      qualified-column (when (r/ctor? :column_reference (r/$ ag 1))
                                         (identifiers (r/$ ag 1)))]
                  [(with-meta
                     (cond-> {:normal-form (z/node (r/$ ag 1))}
                       identifier (assoc :identifier identifier)
                       qualified-column (assoc :qualified-column qualified-column))
                     {:ref ag})])

                nil))]
      (let [sl (r/$ ag -2)]
        [(reduce
          (fn [acc projection]
            (conj acc (assoc projection :index (count acc))))
          []
          (r/collect-stop calculate-select-list sl))]))

    :query_expression
    (let [query-expression-body (if (r/ctor? :with_clause (r/$ ag 1))
                                  (r/$ ag 2)
                                  (r/$ ag 1))
          keys-to-keep (if (r/ctor? :query_specification query-expression-body)
                         [:identifier :index :normal-form]
                         [:identifier :index])]
      (vec (for [projections (projected-columns query-expression-body)]
             (vec (for [projection projections]
                    (select-keys projection keys-to-keep))))))

    :collection_derived_table
    (if (= "ORDINALITY" (r/lexeme ag -1))
      [[{:index 0} {:index 1}]]
      [[{:index 0}]])

    :table_value_constructor
    (projected-columns (r/$ ag 2))

    (:row_value_expression_list :contextually_typed_row_value_expression_list :in_value_list)
    (r/collect-stop
     (fn [ag]
       (r/zcase ag
         (:row_value_expression_list
          :contextually_typed_row_value_expression_list)
         nil

         (:explicit_row_value_constructor
          :contextually_typed_row_value_constructor)
         (let [degree (r/collect-stop
                       (fn [ag]
                         (r/zcase ag
                           (:row_value_constructor_element
                            :contextually_typed_row_value_constructor_element)
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

(defn order-by-index [ag]
  (r/zcase ag
    :query_expression
    nil

    :sort_specification
    (first (for [{:keys [normal-form index identifier]} (first (projected-columns ag))
                 :when (or (= normal-form (r/lexeme ag 1))
                           (and (r/ctor? :column_reference (r/$ ag 1))
                                (= identifier (str/join "." (identifiers (r/$ ag 1))))))]
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

(defn ordering-specification [ag]
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

(defn ^:dynamic column-reference [ag]
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
               [:exact_numeric_literal lexeme]]
              ;;=>
              (not (str/includes? lexeme "."))

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

(defn subquery-type [ag]
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

(defn errs [ag]
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

(defn all-column-references [ag]
  (r/collect
   (fn [ag]
     (when (r/ctor? :column_reference ag)
       [(column-reference ag)]))
   ag))

(defn ^:dynamic scope-element [ag]
  (r/zcase ag
    (:query_expression
     :query_specification)
    ag

    (r/inherit ag)))

(defn- ^:dynamic scope [ag]
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
    (r/with-memoized-attributes [prev-subtree-seq
                                 id
                                 dynamic-param-idx
                                 ctei
                                 cteo
                                 cte-env
                                 dcli
                                 dclo
                                 env
                                 group-env
                                 projected-columns
                                 column-reference
                                 scope-element
                                 scope]
      (let [ag (z/vector-zip query)]
        (if-let [errs (not-empty (errs ag))]
          {:errs errs}
          {:scopes (scopes ag)
           :errs []})))))

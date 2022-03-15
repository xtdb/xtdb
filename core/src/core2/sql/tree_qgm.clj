(ns core2.sql.tree-qgm
  (:require [clojure.spec.alpha :as s]
            [clojure.walk :as w]
            [clojure.zip :as z]
            [core2.logical-plan :as lp]
            [core2.rewrite :as r]
            [core2.sql.analyze :as sem]
            [core2.sql.plan :as plan]
            [instaparse.core :as insta]))

;; Query Graph Model
;; http://projectsweb.cs.washington.edu/research/projects/db/weld/pirahesh-starburst-92.pdf
;; https://www.researchgate.net/publication/221214813_Abstract_Extensible_Query_Processing_in_Starburst

(s/def :qgm/id symbol?)

(s/def :qgm.box/type #{:qgm.box/base-table :qgm.box/select})

(defmulti box-spec first)

(s/def :qgm/box (s/multi-spec box-spec :qgm.box/type))

(s/def :qgm.box.base-table/name symbol?)

(defmethod box-spec :qgm.box/base-table [_]
  (s/cat :qgm.box/type #{:qgm.box/base-table}
         :qgm.box.base-table/name :qgm.box.base-table/name))

(s/def :qgm.box.head/distinct? boolean?)
(s/def :qgm.box.head/columns (s/coll-of symbol? :kind vector? :min-count 1))
(s/def :qgm.box.body/columns (s/coll-of any? :kind vector? :min-count 1))
(s/def :qgm.box.body/distinct #{:qgm.box.body.distinct/enforce
                                :qgm.box.body.distinct/preserve
                                :qgm.box.body.distinct/permit})

(defmethod box-spec :qgm.box/select [_]
  (s/cat :qgm.box/type #{:qgm.box/select}
         :qgm.box/properties (s/keys :req [:qgm.box.head/distinct? :qgm.box.head/columns
                                           :qgm.box.body/columns :qgm.box.body/distinct])
         :qgm.box/quantifiers (s/+ :qgm/quantifier)))

(s/def :qgm.box.outer-join/foreach-quantifier (s/nilable :qgm/id))

(defmethod box-spec :qgm.box/outer-join [_]
  (s/cat :qgm.box/type #{:qgm.box/outer-join}
         :qgm.box/properties (s/keys :req [:qgm.box.head/distinct? :qgm.box.head/columns
                                           :qgm.box.body/columns :qgm.box.body/distinct]
                                     :opt [:qgm.box.outer-join/foreach-quantifier])
         :qgm.box/quantifiers (s/+ :qgm/quantifier)))

(defn- set-box-spec [box-type]
  (s/cat :qgm.box/type #{box-type}
         :qgm.box/properties (s/keys :req [:qgm.box.head/distinct? :qgm.box.head/columns
                                           :qgm.box.body/distinct])
         :qgm.box/left-box :qgm/box
         :qgm.box/right-box :qgm/box))

(defmethod box-spec :qgm.box/intersect [_] (set-box-spec :qgm.box/intersect))
(defmethod box-spec :qgm.box/except [_] (set-box-spec :qgm.box/except))
(defmethod box-spec :qgm.box/union [_] (set-box-spec :qgm.box/union))

(s/def :qgm.quantifier/type
  #{:qgm.quantifier/foreach
    :qgm.quantifier/preserved-foreach
    :qgm.quantifier/existential
    :qgm.quantifier/all})

(s/def :qgm.quantifier/id :qgm/id)
(s/def :qgm.quantifier/columns (s/coll-of symbol? :kind vector? :min-count 1))

(s/def :qgm/quantifier
  (s/cat :qgm.quantifier/type :qgm.quantifier/type
         :qgm.quantifier/properties (s/keys :req [:qgm.quantifier/id :qgm.quantifier/columns])
         :qgm.quantifier/ranges-over :qgm/box))

(s/def :qgm.predicate/expression any?)
(s/def :qgm.predicate/quantifiers (s/coll-of :qgm/id :kind set?))
(s/def :qgm/predicate (s/keys :req [:qgm.predicate/expression :qgm.predicate/quantifiers]))

(s/def :qgm/preds (s/map-of :qgm/id :qgm/predicate))

(s/def :qgm/tree :qgm/box)

(s/def :qgm/qgm (s/keys :req-un [:qgm/preds :qgm/tree]))

(defn- ->preds-by-qid [preds]
  (->> (for [[pid pred] preds
             qid (:qgm.predicate/quantifiers pred)]
         [qid pid])
       (reduce (fn [acc [qid pid]]
                 (update acc qid (fnil conj #{}) pid))
               {})))

(defn qgm-zip [{:keys [tree preds]}]
  (-> (z/vector-zip tree)
      (vary-meta assoc ::preds preds)))

(defn qgm-unzip [z]
  {:tree (z/node z), :preds (::preds (meta z))})

(defn build-query-spec [ag distinct?]
  (let [projection (first (sem/projected-columns ag))]
    {:qgm.box.head/distinct? distinct?
     :qgm.box.head/columns (mapv plan/unqualified-projection-symbol projection)
     :qgm.box.body/columns (mapv plan/qualified-projection-symbol projection)
     :qgm.box.body/distinct (if distinct?
                              :qgm.box.body.distinct/enforce
                              :qgm.box.body.distinct/permit)}))

(defn- table->qid [table]
  (symbol (str (:correlation-name table) "__" (:id table))))

(declare qgm-box)

(defn- table-primary->quantifier [ag]
  (let [projection (first (sem/projected-columns ag))]
    [:qgm.quantifier/foreach {:qgm.quantifier/id (table->qid (sem/table ag))
                              :qgm.quantifier/columns (mapv plan/unqualified-projection-symbol projection)}
     (qgm-box ag)]))

(defn- qgm-quantifiers [ag]
  (->> ag
       (r/collect-stop
        (letfn [(oj-quantifier [ag]
                  (let [projection (first (sem/projected-columns ag))]
                    [:qgm.quantifier/foreach {:qgm.quantifier/id 'hack_oj1 ; HACK id pls
                                              :qgm.quantifier/columns (mapv plan/unqualified-projection-symbol projection)}
                     (qgm-box ag)]))

                (sq-quantifier [q-type subquery]
                  (let [sq-el (sem/subquery-element subquery)
                        sq-el (if (= (r/ctor sq-el) :query_expression)
                                (r/$ sq-el 1)
                                sq-el)]

                    [q-type {:qgm.quantifier/id (symbol (str "q" (sem/id sq-el)))
                             :qgm.quantifier/columns (->> (first (sem/projected-columns subquery))
                                                          (mapv plan/unqualified-projection-symbol))}
                     (qgm-box sq-el)]))]
          (fn [ag]
            (r/zmatch ag
              [:qualified_join _ [:join_type [:outer_join_type _]] _ _ _]
              [(oj-quantifier ag)]

              [:qualified_join _ [:join_type [:outer_join_type _] _] _ _ _]
              [(oj-quantifier ag)]

              [:table_primary _ _]
              [(table-primary->quantifier ag)]

              [:table_primary _ _ _]
              [(table-primary->quantifier ag)]

              [:table_primary _ _ _ _]
              [(table-primary->quantifier ag)]

              [:quantified_comparison_predicate _
               [:quantified_comparison_predicate_part_2 [_ _] [q-type _] ^:z subquery]]
              [(sq-quantifier (keyword (name :qgm.quantifier) (name q-type)) subquery)]

              [:in_predicate _
               [:in_predicate_part_2 _ [:in_predicate_value ^:z subquery]]]
              [(sq-quantifier :qgm.quantifier/existential subquery)]

              [:subquery _] []))))))

(defn- ssl->order-by [ssl]
  (let [projection (first (sem/projected-columns ssl))
        query-id (sem/id (sem/scope-element ssl))]
    (r/collect-stop
     (fn [z]
       (r/zcase z
         :sort_specification
         (let [direction (case (sem/ordering-specification z)
                           "ASC" :asc
                           "DESC" :desc
                           :asc)]
           [(if-let [idx (sem/order-by-index z)]
              {:spec {(plan/unqualified-projection-symbol (nth projection idx)) direction}}
              (let [column (symbol (str "$order_by__" query-id  "_" (r/child-idx z) "$"))]
                {:spec {column direction}
                 :projection {column (plan/expr (r/$ z 1))}}))])

         :subquery
         []

         nil))
     ssl)))

(defn qgm-box [ag]
  (->> ag
       (r/collect-stop
        (letfn [(table-box [ag]
                  (let [table (sem/table ag)]
                    (if (:subquery-scope-id table)
                      (qgm-box (sem/subquery-element ag))
                      [:qgm.box/base-table (symbol (:table-or-query-name table))])))

                (with-order-by [box ssl]
                  (-> box
                      (assoc-in [1 :qgm.box.body/order-by] (ssl->order-by ssl))))

                (with-fetch-first [box ffrc]
                  (-> box
                      (assoc-in [1 :qgm.box.body/fetch-first] (plan/expr ffrc))))

                (with-result-offset [box rorc]
                  (-> box
                      (assoc-in [1 :qgm.box.body/result-offset] (plan/expr rorc))))

                (oj-box [lhs ojt rhs]
                  (let [[[_ {lhs-qid :qgm.quantifier/id} :as lhs-q]] (qgm-quantifiers lhs)
                        [[_ {rhs-qid :qgm.quantifier/id} :as rhs-q]] (qgm-quantifiers rhs)
                        f-qid (case ojt "LEFT" rhs-qid, "RIGHT" lhs-qid, nil)]
                    [:qgm.box/outer-join (-> (build-query-spec ag false)
                                             (assoc :qgm.box.outer-join/foreach-quantifier f-qid))
                     (cond-> lhs-q
                       (not= f-qid lhs-qid) (assoc 0 :qgm.quantifier/preserved-foreach))
                     (cond-> rhs-q
                       (not= f-qid rhs-qid) (assoc 0 :qgm.quantifier/preserved-foreach))]))

                (set-box [box-type opts lhs rhs]
                  (let [lbox (qgm-box lhs)]
                    [box-type
                     (into {:qgm.box.head/columns (:qgm.box.head/columns (nth lbox 1))} opts)
                     lbox (qgm-box rhs)]))]

          (fn [ag]
            (r/zmatch ag
              [:table_primary _ _]
              [(table-box ag)]

              [:table_primary _ _ _]
              [(table-box ag)]

              ;; TODO renaming columns of a base table?
              [:table_primary [:regular_identifier _] _ _ _]
              [(table-box ag)]

              [:table_primary ^:z toqn _ _ _]
              [(-> (qgm-box toqn)
                   (assoc-in [1 :qgm.box.head/columns] (mapv symbol (sem/derived-columns ag))))]

              [:query_expression ^:z qeb [:order_by_clause _ _ ^:z ssl]]
              [(-> (qgm-box qeb) (with-order-by ssl))]

              [:query_expression ^:z qeb [:fetch_first_clause _ _ ffrc _ _]]
              [(-> (qgm-box qeb) (with-fetch-first ffrc))]

              [:query_expression ^:z qeb [:result_offset_clause _ rorc _]]
              [(-> (qgm-box qeb) (with-result-offset rorc))]

              [:query_expression ^:z qeb [:order_by_clause _ _ ^:z ssl] [:result_offset_clause _ rorc _]]
              [(-> (qgm-box qeb) (with-order-by ssl) (with-result-offset rorc))]

              [:query_expression ^:z qeb [:order_by_clause _ _ ^:z ssl] [:fetch_first_clause _ _ ffrc _ _]]
              [(-> (qgm-box qeb) (with-order-by ssl) (with-fetch-first ffrc))]

              [:query_expression ^:z qeb [:result_offset_clause _ rorc _] [:fetch_first_clause _ _ ffrc _ _]]
              [(-> (qgm-box qeb) (with-result-offset rorc) (with-fetch-first ffrc))]

              [:query_expression ^:z qeb [:order_by_clause _ _ ^:z ssl] [:result_offset_clause _ rorc _] [:fetch_first_clause _ _ ffrc _ _]]
              [(-> (qgm-box qeb) (with-order-by ssl) (with-result-offset rorc) (with-fetch-first ffrc))]

              [:query_expression_body ^:z qeb "UNION" ^:z qt]
              [(set-box :qgm.box/union
                        {:qgm.box.head/distinct? true
                         :qgm.box.body/distinct :qgm.box.body.distinct/enforce}
                        qeb qt)]

              [:query_expression_body ^:z qeb "UNION" "ALL" ^:z qt]
              [(set-box :qgm.box/union
                        {:qgm.box.head/distinct? false
                         :qgm.box.body/distinct :qgm.box.body.distinct/preserve}
                        qeb qt)]

              [:query_expression_body ^:z qeb "EXCEPT" ^:z qt]
              [(set-box :qgm.box/except
                        {:qgm.box.head/distinct? true
                         :qgm.box.body/distinct :qgm.box.body.distinct/enforce}
                        qeb qt)]

              [:query_term ^:z qt "INTERSECT" ^:z qp]
              [(set-box :qgm.box/intersect
                        {:qgm.box.head/distinct? true
                         :qgm.box.body/distinct :qgm.box.body.distinct/enforce}
                        qt qp)]

              [:query_specification _ _ _]
              [(into [:qgm.box/select (build-query-spec ag false)]
                     (qgm-quantifiers ag))]

              [:query_specification _ [:set_quantifier d] _ _]
              [(into [:qgm.box/select (build-query-spec ag (= d "DISTINCT"))]
                     (qgm-quantifiers ag))]

              [:qualified_join ^:z lhs [:join_type [:outer_join_type ojt]] _ ^:z rhs _]
              [(oj-box lhs ojt rhs)]

              [:qualified_join ^:z lhs [:join_type [:outer_join_type ojt] _] _ ^:z rhs _]
              [(oj-box lhs ojt rhs)]

              [:subquery _ _]
              []))))
       first))

(defn- expr-quantifiers [ag]
  (r/collect-stop
   (fn [ag]
     (r/zmatch ag
       [:column_reference _]
       (let [{:keys [identifiers table-id]} (sem/column-reference ag)
             q (symbol (str (first identifiers) "__" table-id))]
         [q])

       [:subquery _]
       []))
   ag))

(defn- qgm-preds [ag]
  (letfn [(subquery-pred [sq-ag op lhs]
            ;; HACK: give me a proper id
            (let [pred-id 'hack_qp1
                  sq-el (sem/subquery-element sq-ag)
                  sq-el (if (= (r/ctor sq-el) :query_expression)
                          (r/$ sq-el 1)
                          sq-el)
                  qid (symbol (str "q" (sem/id sq-el)))]

              [pred-id {:qgm.predicate/expression
                        (list (symbol op)
                              (plan/expr lhs)
                              (symbol (str qid "__" (->> (ffirst (sem/projected-columns sq-ag))
                                                         plan/unqualified-projection-symbol))))

                        :qgm.predicate/quantifiers (into #{qid} (expr-quantifiers lhs))}]))]
    (->> ag
         (r/collect-stop
          (fn [ag]
            (r/zmatch ag
              [:comparison_predicate _ _]
              (let [pred-id (symbol (str "p" (sem/id ag)))]
                [[pred-id {:qgm.predicate/expression (plan/expr ag)
                           :qgm.predicate/quantifiers (set (expr-quantifiers ag))}]])

              [:quantified_comparison_predicate ^:z lhs
               [:quantified_comparison_predicate_part_2 [_ op] _ ^:z sq-ag]]
              (conj (qgm-preds sq-ag)
                    (subquery-pred sq-ag op lhs))

              [:in_predicate ^:z lhs
               [:in_predicate_part_2 _
                [:in_predicate_value ^:z sq-ag]]]
              (conj (qgm-preds sq-ag) (subquery-pred sq-ag '= lhs))

              [:named_columns_join _ _]
              (let [pred-id 'hack_ncj_p1]
                [[pred-id
                  {:qgm.predicate/expression (plan/expr ag)
                   :qgm.predicate/quantifiers (->> (sem/local-tables (r/parent ag))
                                                   (into #{} (map table->qid)))}]])

              [:order_by_clause _ _ _]
              []

              [:select_list _]
              [])))

         (into {}))))

(defn ->qgm [ag]
  (->> {:tree (->> ag
                   (r/collect-stop
                    (fn [ag]
                      (r/zcase ag
                        :query_expression [(qgm-box ag)]
                        nil)))
                   first)

        :preds (qgm-preds ag)}
       (s/assert :qgm/qgm)))

(defn plan-qgm [{:keys [tree preds]}]
  (let [preds (->> (for [[pred-id pred] preds]
                     [pred-id (assoc pred
                                     :qgm.predicate/expr-symbols
                                     (plan/expr-symbols (:qgm.predicate/expression pred)))])
                   (into {}))
        qid->pids (->preds-by-qid preds)]

    (letfn [(plan-quantifier [[_q-type {qid :qgm.quantifier/id, cols :qgm.quantifier/columns} [box-type :as box]]]
              [:rename qid
               (if (= :qgm.box/base-table box-type)
                 (let [scan-preds (->> (qid->pids qid)
                                       (map preds)
                                       (filter (comp #(= 1 %) count :qgm.predicate/expr-symbols))
                                       (group-by (comp first :qgm.predicate/expr-symbols)))]
                   [:scan (vec
                           (for [col cols]
                             (let [fq-col (symbol (str qid "_" col))
                                   col-scan-preds (->> (get scan-preds fq-col)
                                                       (map :qgm.predicate/expression)
                                                       (w/postwalk-replace {fq-col col}))]
                               (case (count col-scan-preds)
                                 0 col
                                 1 {col (first col-scan-preds)}
                                 {col (list* 'and col-scan-preds)}))))])
                 (plan-box box))])

            (wrap-select [plan qs]
              (let [qids (into #{} (map (comp :qgm.quantifier/id second)) qs)
                    exprs (->> qids
                               (into []
                                     (comp
                                      (mapcat qid->pids)
                                      (distinct)
                                      (map preds)
                                      (filter (fn [{:qgm.predicate/keys [quantifiers expr-symbols]}]
                                                (and (every? qids quantifiers)
                                                     (> (count expr-symbols) 1))))
                                      (map :qgm.predicate/expression))))]
                (case (count exprs)
                  0 plan
                  1 [:select (first exprs) plan]
                  [:select (list* 'and exprs)
                   plan])))

            (wrap-distinct [plan [_box-type box-opts & _qs]]
              (if (= :qgm.box.body.distinct/enforce (:qgm.box.body/distinct box-opts))
                [:distinct plan]
                plan))

            (wrap-top [plan [_box-type {:qgm.box.body/keys [result-offset fetch-first]} & _qs]]
              (if (or result-offset fetch-first)
                [:top (->> {:skip result-offset, :limit fetch-first}
                           (into {} (filter val)))
                 plan]
                plan))

            (wrap-order-by [plan [_box-type box-opts & _qs]]
              (if-let [order-by-specs (:qgm.box.body/order-by box-opts)]
                (let [base-projection (:qgm.box.head/columns box-opts)
                      order-by-projection (keep :projection order-by-specs)

                      extra-projection (distinct (mapcat (comp plan/expr-symbols vals) order-by-projection))
                      relation (if (not-empty extra-projection)
                                 (->> (z/vector-zip plan)
                                      (r/once-td-tp
                                       (r/mono-tp
                                        (fn [z]
                                          (r/zmatch z
                                            [:project projection relation]
                                            ;;=>
                                            [:project (vec (concat projection extra-projection)) relation]))))
                                      (z/node))
                                 plan)
                      order-by [:order-by (mapv :spec order-by-specs)
                                (if (not-empty order-by-projection)
                                  [:project (vec (concat base-projection order-by-projection)) relation]
                                  relation)]]
                  (if (not-empty order-by-projection)
                    [:project base-projection order-by]
                    order-by))

                plan))

            (plan-select-box [[_ box-opts & qs :as box]]
              (let [body-cols (:qgm.box.body/columns box-opts)]
                (-> [:rename (zipmap body-cols (:qgm.box.head/columns box-opts))
                     [:project body-cols
                      (-> (->> qs
                               (map plan-quantifier)
                               (reduce (fn [acc el]
                                         [:cross-join acc el])))
                          (wrap-select qs))]]
                    (wrap-distinct box))))

            (plan-oj-box [[_ box-opts & qs]]
              (assert (= 2 (count qs)) "can't handle more than 2 yet")

              (let [{[fe] :qgm.quantifier/foreach, pfe :qgm.quantifier/preserved-foreach} (group-by first qs)]
                (-> (if fe
                      [:left-outer-join {}
                       (plan-quantifier (first pfe))
                       (plan-quantifier fe)]

                      (into [:full-outer-join {}]
                            (map plan-quantifier)
                            qs))
                    (wrap-select qs))))

            (plan-box [[box-type :as box]]
              (-> (case box-type
                    :qgm.box/select (plan-select-box box)
                    :qgm.box/outer-join (plan-oj-box box)
                    :qgm.box/except (let [[_ _ lbox rbox] box]
                                      [:difference (plan-box lbox) (plan-box rbox)])

                    :qgm.box/intersect (let [[_ _ lbox rbox] box]
                                         [:intersection (plan-box lbox) (plan-box rbox)])

                    :qgm.box/union (let [[_ box-opts lbox rbox] box
                                         plan [:union-all (plan-box lbox) (plan-box rbox)]]
                                     (if (= :qgm.box.body.distinct/enforce
                                            (:qgm.box.body/distinct box-opts))
                                       [:distinct plan]
                                       plan)))
                  (wrap-order-by box)
                  (wrap-top box)))]

      (plan-box tree))))

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
          (let [qgm (->qgm (z/vector-zip query))]
            {:qgm qgm
             :plan (->> (plan-qgm qgm)
                        (z/vector-zip)
                        (r/innermost (r/mono-tp plan/optimize-plan))
                        (z/node)
                        (s/assert ::lp/logical-plan))}))))))

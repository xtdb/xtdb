(ns core2.sql.tree-qgm
  (:require [clojure.spec.alpha :as s]
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
         :qgm.box/quantifiers (s/map-of :qgm/id :qgm/quantifier)))

(s/def :qgm.quantifier/type #{:qgm.quantifier/foreach
                              :qgm.quantifier/preserved-foreach
                              :qgm.quantifier/existential
                              :qgm.quantifier/all})

(s/def :qgm.quantifier/columns (s/coll-of symbol? :kind vector? :min-count 1))

(s/def :qgm/quantifier
  (s/cat :qgm.quantifier/type :qgm.quantifier/type
         :qgm.quantifier/id :qgm/id
         :qgm.quantifier/columns :qgm.quantifier/columns
         :qgm.quantifier/ranges-over :qgm/box))

(s/def :qgm.predicate/expression any?)
(s/def :qgm.predicate/quantifiers (s/coll-of :qgm/id :kind set?))
(s/def :qgm/predicate (s/keys :req [:qgm.predicate/expression :qgm.predicate/quantifiers]))

(s/def :qgm/preds (s/map-of :qgm/id :qgm/predicate))

(s/def :qgm/tree :qgm/box)

(s/def :qgm/qgm (s/keys :req-un [:qgm/preds :qgm/tree]))

(defn build-query-spec [ag distinct?]
  (let [projection (first (sem/projected-columns ag))]
    {:qgm.box.head/distinct? distinct?
     :qgm.box.head/columns (mapv plan/unqualified-projection-symbol projection)
     :qgm.box.body/columns (mapv plan/qualified-projection-symbol projection)
     :qgm.box.body/distinct (if distinct?
                              :qgm.box.body.distinct/enforce
                              :qgm.box.body.distinct/permit)}))

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

(declare qgm-box)

(defn- qgm-quantifiers [ag]
  (->> ag
       (r/collect-stop
        (fn [ag]
          (r/zmatch ag
            [:table_primary _ _]
            (let [table (sem/table ag)
                  qid (symbol (str (:correlation-name table) "__" (:id table)))
                  projection (first (sem/projected-columns ag))]
              (when-not (:subquery-scope-id table)
                [[qid [:qgm.quantifier/foreach qid (mapv plan/unqualified-projection-symbol projection)
                       [:qgm.box/base-table (symbol (:table-or-query-name table))]]]]))

            [:quantified_comparison_predicate _
             [:quantified_comparison_predicate_part_2 [_ _] [q-type _] ^:z subquery]]
            (let [sq-el (sem/subquery-element subquery)
                  sq-el (if (= (r/ctor sq-el) :query_expression)
                          (r/$ sq-el 1)
                          sq-el)
                  qid (symbol (str "q" (sem/id sq-el)))]

              [[qid [(keyword (name :qgm.quantifier) (name q-type))
                     qid
                     (->> (first (sem/projected-columns subquery))
                          (mapv plan/unqualified-projection-symbol))
                     (qgm-box sq-el)]]])

            [:in_predicate _
             [:in_predicate_part_2 _ [:in_predicate_value ^:z subquery]]]
            (let [sq-el (sem/subquery-element subquery)
                  sq-el (if (= (r/ctor sq-el) :query_expression)
                          (r/$ sq-el 1)
                          sq-el)
                  qid (symbol (str "q" (sem/id sq-el)))]
              [[qid [:qgm.quantifier/existential qid
                     (->> (first (sem/projected-columns subquery))
                          (mapv plan/unqualified-projection-symbol))
                     (qgm-box sq-el)]]])

            [:subquery _] [])))

       (into {})))

(defn qgm-box [ag]
  (->> ag
       (r/collect-stop
        (fn [ag]
          (r/zmatch ag
            [:query_specification _ _ _]
            [[:qgm.box/select (build-query-spec ag false)
              (qgm-quantifiers ag)]]

            [:query_specification _ [:set_quantifier d] _ _]
            [[:qgm.box/select (build-query-spec ag (= d "DISTINCT"))
              (qgm-quantifiers ag)]]

            [:subquery _ _]
            [])))
       first))

(defn- qgm-preds [ag]
  (letfn [(subquery-pred [sq-ag op lhs]
            ;; HACK: give me a proper id
            (let [pred-id 'hack-qp1
                  sq-el (sem/subquery-element sq-ag)
                  sq-el (if (= (r/ctor sq-el) :query_expression)
                          (r/$ sq-el 1)
                          sq-el)
                  qid (symbol (str "q" (sem/id sq-el)))]

              [[pred-id {:qgm.predicate/expression
                         (list (symbol op)
                               (plan/expr lhs)
                               (symbol (str qid "__" (->> (ffirst (sem/projected-columns sq-ag))
                                                          plan/unqualified-projection-symbol))))

                         :qgm.predicate/quantifiers (into #{qid} (expr-quantifiers lhs))}]]))]
    (->> ag
         (r/collect
          (fn [ag]
            (r/zmatch ag
              [:comparison_predicate _ _]
              (let [pred-id (symbol (str "p" (sem/id ag)))]
                [[pred-id {:qgm.predicate/expression (plan/expr ag)
                           :qgm.predicate/quantifiers (set (expr-quantifiers ag))}]])

              [:quantified_comparison_predicate ^:z lhs
               [:quantified_comparison_predicate_part_2 [_ op] _ ^:z sq-ag]]
              (subquery-pred sq-ag op lhs)

              [:in_predicate ^:z lhs
               [:in_predicate_part_2 _
                [:in_predicate_value ^:z sq-ag]]]
              (subquery-pred sq-ag '= lhs))))

         (into {}))))

(defn qgm [ag]
  (->> {:tree (->> ag
                   (r/collect-stop
                     (fn [ag]
                       (r/zcase ag
                                :query_specification [(qgm-box ag)]
                                nil)))
                   first)

        :preds (qgm-preds ag)}
       (s/assert :qgm/qgm)))

(defn qgm-zip [{:keys [tree preds]}]
  (-> (z/zipper (fn [qgm]
                  (and (vector? qgm)
                       (contains? #{:qgm.box/select
                                    :qgm.quantifier/foreach
                                    :qgm.quantifier/all
                                    :qgm.quantifier/existential}
                                  (first qgm))))
                (fn [qgm]
                  (case (first qgm)
                    :qgm.box/select (vals (last qgm))

                    (:qgm.quantifier/all :qgm.quantifier/foreach :qgm.quantifier/existential)
                    [(last qgm)]))

                (fn [node children]
                  (case (first node)
                    :qgm.box/select (conj node (into {} (map (juxt second identity)) children))

                    (:qgm.quantifier/all :qgm.quantifier/foreach :qgm.quantifier/existential)
                    (into node children)))

                tree)

      (vary-meta into {::preds preds
                       ::qid->pids (->> (for [[pid pred] preds
                                              qid (:qgm.predicate/quantifiers pred)]
                                          [qid pid])
                                        (reduce (fn [acc [qid pid]]
                                                  (update acc qid (fnil conj #{}) pid))
                                                {}))})))

(defn plan-qgm [{:keys [tree preds]}]
  (let [qid->pids (->> (for [[pid pred] preds
                             qid (:qgm.predicate/quantifiers pred)]
                         [qid pid])
                       (reduce (fn [acc [qid pid]]
                                 (update acc qid (fnil conj #{}) pid))
                               {}))]
    (letfn [(plan-quantifier [[_q-type qid cols [box-type :as _box]]]
              (case box-type
                :qgm.box/base-table
                [:rename qid
                 [:scan cols]]))

            (wrap-select [plan qids]
              (if-let [exprs (seq
                              (for [pid (->> qids (into #{} (mapcat qid->pids)))
                                    :let [{:qgm.predicate/keys [quantifiers expression]} (get preds pid)]
                                    :when (every? (set qids) quantifiers)]
                                expression))]
                [:select (if (= (count exprs) 1)
                           (first exprs)
                           (list* 'and exprs))
                 plan]
                plan))

            (wrap-distinct [plan [_box-type box-opts _qs]]
              (if (= :qgm.box.body.distinct/enforce (:qgm.box.body/distinct box-opts))
                [:distinct plan]
                plan))

            (plan-select-box [[_ box-opts qs :as box]]
              (-> (let [body-cols (:qgm.box.body/columns box-opts)]
                    [:rename (zipmap body-cols (:qgm.box.head/columns box-opts))
                     [:project body-cols
                      (-> (->> (vals qs)
                               (map plan-quantifier)
                               (reduce (fn [acc el]
                                         [:cross-join acc el])))
                          (wrap-select (keys qs)))]])
                  (wrap-distinct box)))]

      (plan-select-box tree))))

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
          {:plan (->> (plan-qgm (qgm (z/vector-zip query)))
                      (z/vector-zip)
                      (r/innermost (r/mono-tp plan/optimize-plan))
                      (z/node)
                      (s/assert ::lp/logical-plan))})))))

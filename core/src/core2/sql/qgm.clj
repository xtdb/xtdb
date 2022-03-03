(ns core2.sql.qgm
  (:require [clojure.java.shell :as sh]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.zip :as z]
            [core2.rewrite :as r]
            [core2.sql.analyze :as sem]
            [core2.sql.plan :as plan]
            [core2.trip :as trip]
            [instaparse.core :as insta]
            [core2.sql :as sql]
            [core2.logical-plan :as lp])
  (:import (java.net URI URL)))

;; Query Graph Model using internal triple store.
;; http://projectsweb.cs.washington.edu/research/projects/db/weld/pirahesh-starburst-92.pdf
;; https://www.researchgate.net/publication/221214813_Abstract_Extensible_Query_Processing_in_Starburst

;; TODO: try constructing this by adding the triples local for each
;; node during a collect somehow?

(defn- setify [x] (if (coll? x) (set x) #{x}))

(s/def :qgm/id symbol?)

(s/def :qgm.box/type #{:qgm.box.type/base-table :qgm.box.type/select})

(defmulti box-spec :qgm.box/type)

(s/def :qgm/box (s/multi-spec box-spec :qgm.box/type))

(s/def :qgm.box.base-table/name symbol?)

(defmethod box-spec :qgm.box.type/base-table [_]
  (s/keys :req [:db/id :qgm.box/type :qgm.box.base-table/name]))

(s/def :qgm.box.head/distinct? boolean?)
(s/def :qgm.box.head/columns (s/coll-of symbol? :kind vector? :min-count 1))
(s/def :qgm.box.body/columns (s/coll-of any? :kind vector? :min-count 1))
(s/def :qgm.box.body/distinct #{:qgm.box.body.distinct/enforce
                                :qgm.box.body.distinct/preserve
                                :qgm.box.body.distinct/permit})
(s/def :qgm.box.body/quantifiers (s/coll-of :qgm/id :kind set? :min-count 1))

(defmethod box-spec :qgm.box.type/select [_]
  (s/keys :req [:db/id :qgm.box/type
                :qgm.box.head/distinct? :qgm.box.head/columns
                :qgm.box.body/columns :qgm.box.body/distinct :qgm.box.body/quantifiers]))

(s/def :qgm.quantifier/type #{:qgm.quantifier.type/foreach
                              :qgm.quantifier.type/preserved-foreach
                              :qgm.quantifier.type/existential
                              :qgm.quantifier.type/all})
(s/def :qgm.quantifier/columns (s/coll-of symbol? :kind vector? :min-count 1))
(s/def :qgm.quantifier/ranges-over :qgm/id)

(s/def :qgm/quantifier (s/keys :req [:db/id :qgm.quantifier/type :qgm.quantifier/columns :qgm.quantifier/ranges-over]))

(s/def :qgm.predicate/expression any?)
(s/def :qgm.predicate/quantifiers (s/coll-of :qgm/id :kind set?))
(s/def :qgm/predicate (s/keys :req [:db/id :qgm.predicate/expression :qgm.predicate/quantifiers]))

(s/def :qgm/node (s/or :box :qgm/box :quantifier :qgm/quantifier :predicate :qgm/predicate))
(s/def :qgm/graph (s/coll-of :qgm/node :kind set?))

;; GraphViz

(defn- box->dot [box qs]
  (case (:qgm.box/type box)
    :qgm.box.type/base-table
    (format "
  subgraph cluster_%s {
    label=\"BASE (%s)\"
    style=dashed
    rank=min
    %s [shape=none, label=\"%s\"]
  }" (:db/id box) (:db/id box) (:db/id box) (:qgm.box.base-table/name box))
    :qgm.box.type/select
    (format "
  subgraph cluster_%s {
    label=\"SELECT (%s)\"
    style=dashed
    rank=max
    %s [label = \"%s\", shape=record, penwidth=2]
    %s
  }"
            (:db/id box)
            (:db/id box)
            (:db/id box)
            (format "{ <head> HEAD: distinct=%s\\l | { { { %s\\l } | { %s\\l } } } | <body> BODY: distinct=%s\\l }"
                    (str/upper-case (:qgm.box.head/distinct? box))
                    (str/join "\\l|" (:qgm.box.head/columns box))
                    (str/join "\\l|" (:qgm.box.body/columns box))
                    (str/upper-case (name (:qgm.box.body/distinct box))))
            (let [id->q (zipmap (map :db/id qs) qs)]
              (str/join "\n    "
                        (for [q (setify (:qgm.box.body/quantifiers box))]
                          (format "%s [label=\"%s(%s)\", shape=circle, style=filled, margin=0]"
                                  q q (str/upper-case (first (name (get-in id->q [q :qgm.quantifier/type] "F")))))))))))

(defn- quantifier->dot [q]
  (format "%s -> %s:head [label=\"%s\", lhead=cluster_%s]"
          (:db/id q)
          (:qgm.quantifier/ranges-over q)
          (str/join ", " (:qgm.quantifier/columns q))
          (:qgm.quantifier/ranges-over q)))

(defn- predicate->dot [p]
  (let [qs (setify (:qgm.predicate/quantifiers p))]
    (format "%s -> %s [label=\"%s\", dir=none, color=grey]"
            (first qs)
            (or (second qs)
                (first qs))
            (str/replace (str (:qgm.predicate/expression p)) "\"" "\\\""))))

(defn qgm->entities [qgm]
  (->> qgm
       distinct
       (reduce (fn [acc [e a v]]
                 (-> acc
                     (assoc-in [e :db/id] e)
                     (update-in [e a]
                                (fn [x]
                                  (cond
                                    (set? x) (conj x v)
                                    (some? x) (conj #{} x v)
                                    :else v)))))
               {})
       vals))

(defn qgm->dot [label qgm]
  (let [entities (qgm->entities qgm)
        {bs :box, qs :quantifier, ps :predicate} (->> entities
                                                      (group-by (fn [e]
                                                                  (cond
                                                                    (:qgm.box/type e) :box
                                                                    (:qgm.quantifier/type e) :quantifier
                                                                    :else :predicate))))]
    (str/trim
     (format "
digraph {
  compound=true
  fontname=courier
  edge [fontname=courier]
  node [fontname=courier]
  newrank=true
  penwidth=2
  label=\"%s\"
  %s

  %s

  %s
}"
             (-> (str/trim label)
                 (str/replace "\n" "\\l")
                 (str/replace "\"" "\\\"")
                 (str "\\l"))
             (str/join "\n"
                       (for [b (sort-by :db/id bs)]
                         (box->dot b qs)))
             (str/join "\n  " (map quantifier->dot (sort-by :db/id qs)))
             (str/join "\n  " (map predicate->dot (sort-by :db/id ps)))))))

(defn- dot->svg [dot]
  (let [{:keys [exit out err]} (sh/sh "dot" "-Tsvg" :in dot)]
    (if (zero? exit)
      out
      (throw (IllegalArgumentException. (str err))))))

(defn dot->file [dot format file]
  (let [{:keys [exit out err]} (sh/sh "dot" (str "-T" format) "-o" file :in dot)]
    (when-not (zero? exit)
      (throw (IllegalArgumentException. (str err))))))

;; https://dreampuf.github.io/GraphvizOnline/#
;; http://magjac.com/graphviz-visual-editor/?dot=
;; https://edotor.net/?engine=dot#

(defn- dot-preview-url
  ([dot]
   (dot-preview-url "http://magjac.com/graphviz-visual-editor/?dot=" dot))
  ([base-url dot]
   (let [url (URL. (str base-url dot))]
     (.toASCIIString (URI. (.getProtocol url) (.getUserInfo url) (.getHost url) (.getPort url) (.getPath url) (.getQuery url) (.getRef url))))))

(comment

  (declare qgm)

  #_
  '[{:db/id b1
     :qgm.box/type :qgm.box.type/base-table
     :qgm.box.base-table/name inventory}
    {:db/id b2
     :qgm.box/type :qgm.box.type/base-table
     :qgm.box.base-table/name quotations}
    {:db/id b3
     :qgm.box/type :qgm.box.type/select
     :qgm.box.head/distinct? true
     :qgm.box.head/columns [partno descr suppno]
     :qgm.box.body/columns [q1.partno q1.descr q2.suppno]
     :qgm.box.body/distinct :qgm.box.body.distinct/enforce
     :qgm.box.body/quantifiers #{q1 q2 q4}}
    {:db/id b4
     :qgm.box/type :qgm.box.type/select
     :qgm.box.head/distinct? false
     :qgm.box.head/columns [price]
     :qgm.box.body/columns [q3.price]
     :qgm.box.body/distinct :qgm.box.body.distinct/permit
     :qgm.box.body/quantifiers #{q3}}
    {:db/id q1
     :qgm.quantifier/type :qgm.quantifier.type/foreach
     :qgm.quantifier/columns [partno descr]
     :qgm.quantifier/ranges-over b1}
    {:db/id q2
     :qgm.quantifier/type :qgm.quantifier.type/foreach
     :qgm.quantifier/columns [partno price]
     :qgm.quantifier/ranges-over b2}
    {:db/id q3
     :qgm.quantifier/type :qgm.quantifier.type/foreach
     :qgm.quantifier/columns [partno price]
     :qgm.quantifier/ranges-over b2}
    {:db/id q4
     :qgm.quantifier/type :qgm.quantifier.type/all
     :qgm.quantifier/columns [price]
     :qgm.quantifier/ranges-over b4}
    {:db/id p1
     :qgm.predicate/expression (= q1.descr "engine")
     :qgm.predicate/quantifiers #{q1}}
    {:db/id p2
     :qgm.predicate/expression (= q1.partno q2.partno)
     :qgm.predicate/quantifiers #{q1 q2}}
    {:db/id p3
     :qgm.predicate/expression (<= q2.price q4.price)
     :qgm.predicate/quantifiers #{q2 q4}}
    {:db/id p4
     :qgm.predicate/expression (= q2.partno q3.partno)
     :qgm.predicate/quantifiers #{q2 q3}}]

  (let [qs ["SELECT q3.price FROM quotations q3 WHERE q3.partno = 1"

            "SELECT DISTINCT q1.partno, q1.descr, q2.suppno
   FROM inventory q1, quotations q2
   WHERE q1.partno = q2.partno AND q1.descr= 'engine'"

            "SELECT DISTINCT q1.partno, q1.descr, q2.suppno
   FROM inventory q1, quotations q2
   WHERE q1.partno = q2.partno AND q1.descr= 'engine'
     AND q2.price <= ALL
         (SELECT q3.price FROM quotations q3
          WHERE q2.partno=q3.partno)"
            "SELECT q1.partno, q1.price, q1.order_qty
             FROM quotations q1
             WHERE q1.partno IN
               (SELECT q3.partno
                FROM inventory q3
                WHERE q3.onhand_qty < q1.order_qty AND q3.type = 'CPU')"]
        q (nth qs 3)]
    (-> (qgm->dot q (qgm (z/vector-zip (core2.sql/parse q))))
        (dot->file "png" "target/qgm.png"))))


(defn build-query-spec [ag distinct?]
  (let [id (sem/id (sem/scope-element ag))
        eid (symbol (str "b" id))
        projection (first (sem/projected-columns ag))
        root? (r/ctor? :directly_executable_statement (r/parent (sem/scope-element (r/parent ag))))]
    (cond-> [[eid :qgm.box/type :qgm.box.type/select]
             [eid :qgm.box.head/distinct? distinct?]
             [eid :qgm.box.head/columns (mapv plan/unqualified-projection-symbol projection)]
             [eid :qgm.box.body/columns (mapv plan/qualified-projection-symbol projection)]
             [eid :qgm.box.body/distinct (if distinct?
                                           :qgm.box.body.distinct/enforce
                                           :qgm.box.body.distinct/permit)]]
      root? (conj [eid :qgm.box/root? true]))))

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

(defn qgm [ag]
  (r/collect
   (fn [ag]
     (r/zmatch ag
       [:query_specification _ _ _]
       (build-query-spec ag false)

       [:query_specification _ [:set_quantifier d] _ _]
       (build-query-spec ag (= d "DISTINCT"))

       [:table_primary _ _]
       (let [table (sem/table ag)
             scope-id (symbol (str "b" (:scope-id table)))
             eid (symbol (str "bt_" (:table-or-query-name table)))
             qid (symbol (str (:correlation-name table) "__" (:id table)))
             projection (first (sem/projected-columns ag))]
         (when-not (:subquery-scope-id table)
           [[eid :qgm.box/type :qgm.box.type/base-table]
            [eid :qgm.box.base-table/name (symbol (:table-or-query-name table))]
            [qid :qgm.quantifier/ranges-over eid]
            [qid :qgm.quantifier/type :qgm.quantifier.type/foreach]
            [qid :qgm.quantifier/columns (mapv plan/unqualified-projection-symbol projection)]
            [scope-id :qgm.box.body/quantifiers qid]]))

       [:comparison_predicate _ _]
       (let [pred-id (symbol (str "p" (sem/id ag)))]
         (into [[pred-id :qgm.predicate/expression (plan/expr ag)]]
               (for [q (expr-quantifiers ag)]
                 [pred-id :qgm.predicate/quantifiers q])))

       [:quantified_comparison_predicate ^:z lhs
        [:quantified_comparison_predicate_part_2 [_ op] [q-type _] ^:z subquery]]
       ;; HACK: give me a proper id
       (let [pred-id 'hack-qp1
             sq-el (sem/subquery-element subquery)
             sq-el (if (= (r/ctor sq-el) :query_expression)
                     (r/$ sq-el 1)
                     sq-el)
             qid (symbol (str "q" (sem/id sq-el)))
             expr (list (symbol op)
                        (plan/expr lhs)
                        (symbol (str qid "__" (->> (ffirst (sem/projected-columns subquery))
                                                   plan/unqualified-projection-symbol))))
             scope-id (symbol (str "b" (sem/id (sem/scope-element ag))))]
         (into [[pred-id :qgm.predicate/expression expr]
                [pred-id :qgm.predicate/quantifiers qid]

                [qid :qgm.quantifier/type (case q-type
                                            :all :qgm.quantifier.type/all)]
                [qid :qgm.quantifier/ranges-over (symbol (str "b" (sem/id sq-el)))]
                [qid :qgm.quantifier/columns (->> (first (sem/projected-columns subquery))
                                                  (mapv plan/unqualified-projection-symbol))]

                [scope-id :qgm.box.body/quantifiers qid]]

               (for [q (expr-quantifiers lhs)]
                 [pred-id :qgm.predicate/quantifiers q])))

       [:in_predicate ^:z lhs
        [:in_predicate_part_2 _ [:in_predicate_value ^:z subquery]]]
       ;; HACK: give me a proper id
       (let [pred-id 'hack-qp1
             sq-el (sem/subquery-element subquery)
             sq-el (if (= (r/ctor sq-el) :query_expression)
                     (r/$ sq-el 1)
                     sq-el)
             qid (symbol (str "q" (sem/id sq-el)))
             expr (list '=
                        (plan/expr lhs)
                        (symbol (str qid "__" (->> (ffirst (sem/projected-columns subquery))
                                                   plan/unqualified-projection-symbol))))
             scope-id (symbol (str "b" (sem/id (sem/scope-element ag))))]
         (into [[pred-id :qgm.predicate/expression expr]
                [pred-id :qgm.predicate/quantifiers qid]

                [qid :qgm.quantifier/type :qgm.quantifier.type/existential]
                [qid :qgm.quantifier/ranges-over (symbol (str "b" (sem/id sq-el)))]
                [qid :qgm.quantifier/columns (->> (first (sem/projected-columns subquery))
                                                  (mapv plan/unqualified-projection-symbol))]

                [scope-id :qgm.box.body/quantifiers qid]]

               (for [q (expr-quantifiers lhs)]
                 [pred-id :qgm.predicate/quantifiers q])))))
   ag))

(defn plan-qgm [trips]
  (let [db (-> (trip/transact {} (vec (for [[e a v] trips]
                                        [:db/add e a v])))
               :db-after)

        eid->entity (->> (qgm->entities trips)
                         (into {} (map (juxt :db/id identity))))]
    (letfn [(plan-quantifier [q]
              (let [box (eid->entity (:qgm.quantifier/ranges-over q))]
                (case (:qgm.box/type box)
                  :qgm.box.type/base-table
                  [:rename (:db/id q)
                   [:scan (:qgm.quantifier/columns q)]])))

            (wrap-select [plan qids]
              (if-let [exprs (seq
                              (for [qid qids
                                    pid (->> (trip/-datoms db :ave [:qgm.predicate/quantifiers qid])
                                             (into #{} (map :e)))
                                    :let [{:qgm.predicate/keys [quantifiers expression]} (eid->entity pid)]
                                    :when (every? (set qids) (setify quantifiers))]
                                expression))]
                [:select (if (= (count exprs) 1)
                           (first exprs)
                           (list* 'and exprs))
                 plan]
                plan))

            (plan-select-box [box]
              (let [body-cols (:qgm.box.body/columns box)]
                [:rename (zipmap body-cols (:qgm.box.head/columns box))
                 [:project body-cols
                  (let [qids (setify (:qgm.box.body/quantifiers box))]
                    (-> (reduce (fn [acc el]
                                  [:cross-join acc el])
                                (for [qid qids]
                                  (plan-quantifier (eid->entity qid))))
                        (wrap-select qids)))]]))]

      (plan-select-box (-> (trip/-datoms db :ave [:qgm.box/root? true])
                           first :e
                           eid->entity)))))

(comment
  (declare plan-query)

  [(plan-query (sql/parse "
SELECT q3.price FROM quotations q3 WHERE q3.partno = 1"))

   (plan-query (sql/parse "
SELECT DISTINCT q1.partno, q1.descr, q2.suppno
FROM inventory q1, quotations q2
WHERE q1.partno = q2.partno AND q1.descr= 'engine'"))])

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

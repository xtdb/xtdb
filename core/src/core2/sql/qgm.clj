(ns core2.sql.qgm
  (:require [clojure.spec.alpha :as s]))

;; Query Graph Model using internal triple store.
;; http://projectsweb.cs.washington.edu/research/projects/db/weld/pirahesh-starburst-92.pdf

;; TODO: try constructing this by adding the triples local for each
;; node during a collect somehow?

(s/def :qgm/id symbol?)

(s/def :qgm.box/type #{:qgm.box.type/base-table :qgm.box.type/select})

(defmulti box-spec :qgm.box/type)

(s/def :qgm/box (s/multi-spec box-spec :qgm.box/type))

(s/def :qgm.box.base-table/name symbol?)

(defmethod box-spec :qgm.box.type/base-table [_]
  (s/keys :req [:db/id :qgm.box/type :qgm.box.base-table/name]))

(s/def :qgm.box.head/distinct? boolean?)
(s/def :qgm.box.head/columns (s/coll-of symbol? :kind vector :min-count 1))
(s/def :qgm.box.body/columns (s/coll-of any? :kind vector :min-count 1))
(s/def :qgm.box.body/distinct #{:qgm.box.body.distinct/enforce
                                :qgm.box.body.distinct/preserve
                                :qgm.box.body.distinct/permit})
(s/def :qgm.box.body/quantifiers (s/coll-of :qgm/id :kind vector :min-count 1))

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

(comment
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
     :qgm.predicate/expression (<= q2.partno q4.partno)
     :qgm.predicate/quantifiers #{q2 q4}}
    {:db/id p4
     :qgm.predicate/expression (= q2.partno q3.partno)
     :qgm.predicate/quantifiers #{q2 q3}}])

;; GraphViz:
;; https://dreampuf.github.io/GraphvizOnline/


;; digraph G {
;;     compound = true
;;     fontname = courier
;;   subgraph cluster_3 {
;;     label = "SELECT (3)"
;;     style = dashed
;;     {
;;       rank = source
;;       b3 [label = "head: distinct=true\l[partno   , descr   , suppno   ]\l[q1.partno, q1.descr, q2.suppno]\lbody: distinct=enforce\l", shape=box, fontname = courier]
;;     }
;;     {
;;       rank = same
;;       q1 [label="q1(F)", shape=circle, style=filled, margin=0, fontname = courier]
;;       q2 [label="q2(F)", shape=circle, style=filled, margin=0, fontname = courier]
;;       q4 [label="q4(A)", shape=circle, style=filled, margin=0, fontname = courier]
;;     }
;;   }

;;   subgraph cluster_4 {
;;     label = "SELECT (4)"
;;     style = dashed
;;     {
;;       rank = source
;;       b4 [label = "head: distinct=false\l[price   ]\l[q3.price]\lbody: distinct=permit\l", shape=box, fontname = courier]
;;     }
;;     {
;;       rank = same
;;       q3 [label="q3(F)", shape=circle, style=filled, margin=0, fontname = courier]
;;     }
;;   }

;;   subgraph cluster_2 {
;;       label = "BASE (2)"
;;       style=dashed
;;       b2 [shape=none, label="quotations", fontname = courier]
;;   }

;;   subgraph cluster_1 {
;;       label = "BASE (1)"
;;       style=dashed
;;       b1 [shape=none, label="inventory", fontname = courier]
;;   }

;;   q1 -> b1 [label="partno, descr" lhead=cluster_1, fontname = courier]
;;   q2 -> b2 [label="partno, price" lhead=cluster_2, fontname = courier]
;;   q3 -> b2 [label="partno, price" lhead=cluster_2, fontname = courier]
;;   q4 -> b4 [label="price" lhead=cluster_4, fontname = courier]

;;   q1 -> q1 [label="q1.descr = \"engine\"", dir=none, fontname = courier]
;;   q1 -> q2 [label="q1.partno = q2.partno", dir=none, fontname = courier]
;;   q2 -> q4 [label="q2.partno <= q4.partno", dir=none, fontname = courier]
;;   q2 -> q3 [label="q2.partno = q3.partno", dir=none, fontname = courier]
;; }

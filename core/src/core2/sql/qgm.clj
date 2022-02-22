(ns core2.sql.qgm
  (:require [clojure.string :as str]
            [clojure.spec.alpha :as s]))

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

;; GraphViz:
;; https://dreampuf.github.io/GraphvizOnline/

(defn- box->dot [box qs]
  (case (:qgm.box/type box)
    :qgm.box.type/base-table
    (format "
  subgraph cluster_%s {
    label=\"BASE (%s)\"
    style=dashed
    rank=min
    %s [shape=none, label=\"%s\", fontname=courier]
  }" (:db/id box) (:db/id box) (:db/id box) (:qgm.box.base-table/name box))
    :qgm.box.type/select
    (format "
  subgraph cluster_%s {
    label=\"SELECT (%s)\"
    style=dashed
    rank=max
    %s [label = \"%s\", shape=record, fontname=courier, penwidth=2]
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
                        (for [q (sort (:qgm.box.body/quantifiers box))]
                          (format "%s [label=\"%s(%s)\", shape=circle, style=filled, margin=0, fontname=courier]"
                                  q q (str/upper-case (first (name (get-in id->q [q :qgm.quantifier/type] "F")))))))))))

(defn- quantifier->dot [q]
  (format "%s -> %s:head [label=\"%s\", lhead=cluster_%s, fontname=courier]"
          (:db/id q)
          (:qgm.quantifier/ranges-over q)
          (str/join ", " (:qgm.quantifier/columns q))
          (:qgm.quantifier/ranges-over q)))

(defn- predicate->dot [p]
  (format "%s -> %s [label=\"%s\", dir=none, fontname=courier, color=grey]"
          (first (:qgm.predicate/quantifiers p))
          (or (second (:qgm.predicate/quantifiers p))
              (first (:qgm.predicate/quantifiers p)))
          (str/replace (str (:qgm.predicate/expression p)) "\"" "'")))

(defn- qgm->dot [bs qs ps]
  (format "
digraph {
  compound=true
  fontname=courier
  newrank=true
  penwidth=2
  %s

  %s

  %s
}"
          (str/join "\n"
                    (for [b (sort-by :db/id bs)]
                      (box->dot b qs)))
          (str/join "\n  " (map quantifier->dot (sort-by :db/id qs)))
          (str/join "\n  " (map predicate->dot (sort-by :db/id ps)))))

(comment

  (println
   (qgm->dot
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
       :qgm.box.body/quantifiers #{q3}}]
    '[{:db/id q1
       :qgm.quantifier/type :qgm.quantifier.type/foreach
       :qgm.quantifier/columns [partno descr]
       :qgm.quantifier/ranges-over b1}
      {:db/id q2
       :qgm.quantifier/type :qgm.quantifier.type/foreach
       :qgm.quantifier/columns [partno price]
       :qgm.quantifier/ranges-over b2}
      {:db/id q3
       :qgm.quantifaier/type :qgm.quantifier.type/foreach
       :qgm.quantifier/columns [partno price]
       :qgm.quantifier/ranges-over b2}
      {:db/id q4
       :qgm.quantifier/type :qgm.quantifier.type/all
       :qgm.quantifier/columns [price]
       :qgm.quantifier/ranges-over b4}]
    '[{:db/id p1
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
       :qgm.predicate/quantifiers #{q2 q3}}])))

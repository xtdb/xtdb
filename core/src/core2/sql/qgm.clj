(ns core2.sql.qgm
  (:require [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.spec.alpha :as s])
  (:import [java.net URI URL]))

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
                        (for [q (sort (:qgm.box.body/quantifiers box))]
                          (format "%s [label=\"%s(%s)\", shape=circle, style=filled, margin=0]"
                                  q q (str/upper-case (first (name (get-in id->q [q :qgm.quantifier/type] "F")))))))))))

(defn- quantifier->dot [q]
  (format "%s -> %s:head [label=\"%s\", lhead=cluster_%s]"
          (:db/id q)
          (:qgm.quantifier/ranges-over q)
          (str/join ", " (:qgm.quantifier/columns q))
          (:qgm.quantifier/ranges-over q)))

(defn- predicate->dot [p]
  (format "%s -> %s [label=\"%s\", dir=none, color=grey]"
          (first (:qgm.predicate/quantifiers p))
          (or (second (:qgm.predicate/quantifiers p))
              (first (:qgm.predicate/quantifiers p)))
          (str/replace (str (:qgm.predicate/expression p)) "\"" "\\\"")))

(defn- qgm->dot [label bs qs ps]
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
           (str/join "\n  " (map predicate->dot (sort-by :db/id ps))))))

(defn- dot->svg [dot]
  (let [{:keys [exit out err]} (sh/sh "dot" "-Tsvg" :in dot)]
    (if (zero? exit)
      out
      (throw (IllegalArgumentException. (str err))))))

(defn- dot->file [dot format file]
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

  (dot->file
   (qgm->dot
    "
SELECT DISTINCT ql.partno, ql .descr, q2.suppno
FROM inventory ql, quotations q2
WHERE ql .partno = qz.partno AND ql .descr= \"engine\"
  AND q2.price <= ALL
      (SELECT q3.price FROM quotations q3
       WHERE q2.partno=q3.partno)"
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
       :qgm.quantifier/type :qgm.quantifier.type/foreach
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
       :qgm.predicate/expression (<= q2.price q4.price)
       :qgm.predicate/quantifiers #{q2 q4}}
      {:db/id p4
       :qgm.predicate/expression (= q2.partno q3.partno)
       :qgm.predicate/quantifiers #{q2 q3}}])
   "png"
   "target/qgm.png"))

(ns core2.sql.tree-qgm.dot
  (:require [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.zip :as z]
            [core2.sql.tree-qgm :as qgm])
  (:import (java.net URI URL)))

;; GraphViz

#_{:clj-kondo/ignore #{:unused-binding}}
(defmulti box->dot
  (fn [box-id box]
    (first box)))

(alter-meta! #'box->dot assoc :private true)

(defmethod box->dot :qgm.box/base-table [box-id [_ table-name]]
  (let [table-name (str table-name)]
    (format "
  subgraph cluster_%s {
    label=\"BASE\"
    style=dashed
    rank=min
    %s [shape=none, label=\"%s\"]
  }" box-id box-id table-name)))

(defmethod box->dot :qgm.box/select [box-id [_ box-opts qs]]
  (->> (into [(format "
subgraph cluster_%s {
  label=\"SELECT\"
  style=dashed
  rank=max
  %s
}"
                      box-id
                      (->> [(format "%s [label = \"%s\", shape=record, penwidth=2]"
                                    box-id
                                    (format "{ <head> HEAD: distinct=%s\\l | { { { %s\\l } | { %s\\l } } } | <body> BODY: distinct=%s\\l }"
                                            (str/upper-case (:qgm.box.head/distinct? box-opts))
                                            (str/join "\\l|" (:qgm.box.head/columns box-opts))
                                            (str/join "\\l|" (:qgm.box.body/columns box-opts))
                                            (str/upper-case (name (:qgm.box.body/distinct box-opts)))))

                            (->> (for [[qid [q-type _qid cols _ranges-over]] qs]
                                   [(format "%s [label=\"%s(%s)\", shape=circle, style=filled, margin=0]"
                                            qid qid (str/upper-case (first (name q-type))))
                                    (let [box-id (symbol (str "box_" qid))]
                                      (format "%s -> %s:head [label=\"%s\", lhead=cluster_%s]"
                                              qid box-id (str/join ", " cols) box-id))])
                                 (apply concat)
                                 (str/join "\n    "))]
                           (str/join "\n")))]

             (for [[qid [_q-type _qid _cols ranges-over]] qs]
               (box->dot (symbol (str "box_" qid)) ranges-over)))

       (str/join "\n\n")))

(defn- predicate->dot [p]
  (let [qs (:qgm.predicate/quantifiers p)]
    (format "%s -> %s [label=\"%s\", dir=none, color=grey]"
            (first qs)
            (or (second qs)
                (first qs))
            (str/replace (str (:qgm.predicate/expression p)) "\"" "\\\""))))

(defn qgm->dot [label {:keys [tree preds]}]
  (str/trim
   (format "
digraph {
  compound=true
  fontname=courier
  edge [fontname=courier]
  node [fontname=courier]
  newrank=true
  penwidth=2
  %s
}"
           (->> [(format "label=\"%s\""
                         (-> (str/trim label)
                             (str/replace "\n" "\\l")
                             (str/replace "\"" "\\\"")
                             (str "\\l")))
                 (box->dot 'root tree)
                 (->> preds
                      (sort-by key)
                      (map (comp predicate->dot val))
                      (str/join "\n  "))]
                (str/join "\n\n")))))

#_{:clj-kondo/ignore #{:unused-private-var}}
(defn- dot->svg [dot]
  (let [{:keys [exit out err]} (sh/sh "dot" "-Tsvg" :in dot)]
    (if (zero? exit)
      out
      (throw (IllegalArgumentException. (str err))))))

(defn dot->file [dot format file]
  (let [{:keys [exit _out err]} (sh/sh "dot" (str "-T" format) "-o" file :in dot)]
    (when-not (zero? exit)
      (throw (IllegalArgumentException. (str err))))))

;; https://dreampuf.github.io/GraphvizOnline/#
;; http://magjac.com/graphviz-visual-editor/?dot=
;; https://edotor.net/?engine=dot#

#_{:clj-kondo/ignore #{:unused-private-var}}
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
     :qgm.predicate/quantifiers #{q2 q3}}])

(comment
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
        q (nth qs 1)]
    (-> (qgm->dot q (qgm/->qgm (z/vector-zip (core2.sql/parse q))))
        #_(doto println)
        (dot->file "png" "target/qgm.png"))))

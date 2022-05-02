(ns core2.sql.parser
  (:require [clojure.java.io :as io]
            [instaparse.core :as insta]
            [instaparse.cfg :as insta-cfg]
            [core2.sql])
  (:import java.io.File
           [java.util HashMap List Map]
           clojure.lang.MapEntry))

;; Spike to replace Instaparse.

;; TODO:
;; add records with parse interface and generate a "compiled" tree from edn input.
;; detect need for left-recursion memo.
;; explore error handling, needs to be passed upwards, merged, a kind of result.
;; try compiling rule bodies to fns?

;; https://arxiv.org/pdf/1509.02439v1.pdf
;; https://medium.com/@gvanrossum_83706/left-recursive-peg-grammars-65dab3c580e1
;; def memoize_left_rec_wrapper(self, *args):
;;     pos = self.mark()
;;     memo = self.memos.get(pos)
;;     if memo is None:
;;         memo = self.memos[pos] = {}
;;     key = (func, args)
;;     if key in memo:
;;         res, endpos = memo[key]
;;         self.reset(endpos)
;;     else:
;;         # Prime the cache with a failure.
;;         memo[key] = lastres, lastpos = None, pos
;;         # Loop until no longer parse is obtained.
;;         while True:
;;             self.reset(pos)
;;             res = func(self, *args)
;;             endpos = self.mark()
;;             if endpos <= lastpos:
;;                 break
;;             memo[key] = lastres, lastpos = res, endpos
;;         res = lastres
;;         self.reset(lastpos)
;;     return res

(defrecord ParseState [ast ^int idx])

(defn- ebnf-parser [grammar ws-regexp ^String in start-rule]
  (let [memos (HashMap.)]
    (letfn [(parse [^long idx {:keys [tag] :as parser}]
              (when (<= idx (.length in))
                (case tag
                  :nt (let [kw (:keyword parser)
                            memo-key (MapEntry/create idx kw)
                            nt-parser (get grammar kw)]
                        (if-let [^ParseState state (.get memos memo-key)]
                          (when (.ast state)
                            state)
                          (loop [last-state (ParseState. nil idx)]
                            (.put memos memo-key last-state)
                            (when-let [^ParseState state (parse idx nt-parser)]
                              (let [new-state (ParseState. (if (:hide parser)
                                                             []
                                                             (if (= {:reduction-type :raw}
                                                                    (:red nt-parser))
                                                               (.ast state)
                                                               [(with-meta
                                                                  (into [kw] (.ast state))
                                                                  {:start-idx idx
                                                                   :end-idx (.idx state)})]))
                                                           (.idx state))]
                                (if (<= (.idx new-state) (.idx last-state))
                                  (when (.ast last-state)
                                    last-state)
                                  (recur new-state)))))))
                  (:star :plus) (let [parser (:parser parser)]
                                  (loop [ast []
                                         idx idx
                                         n 0]
                                    (if-let [^ParseState state (parse idx parser)]
                                      (recur (into ast (.ast state))
                                             (.idx state)
                                             (inc n))
                                      (when (or (= :star tag)
                                                (pos? n))
                                        (ParseState. ast idx)))))
                  :opt (or (parse idx (:parser parser))
                           (ParseState. [] idx))
                  :neg (when-not (parse idx (:parser parser))
                         (ParseState. [] idx))
                  :cat (let [^List parsers (:parsers parser)]
                         (loop [ast []
                                idx idx
                                n 0]
                           (if (< n (.size parsers))
                             (when-let [^ParseState state (parse idx (.get parsers n))]
                               (recur (into ast (.ast state))
                                      (.idx state)
                                      (inc n)))
                             (ParseState. ast idx))))
                  :alt (reduce
                        (fn [^ParseState state parser]
                          (if-let [^ParseState alt-state (parse idx parser)]
                            (if (and state (> (.idx state) (.idx alt-state)))
                              state
                              alt-state)
                            state))
                        nil
                        (:parsers parser))
                  :ord (let [^ParseState state1 (parse idx (:parser1 parser))
                             ^ParseState state2 (parse idx (:parser2 parser))]
                         (if (and state1 state2)
                           (if (< (.idx state1) (.idx state2))
                             state2
                             state1)
                           (or state1 state2)))
                  (:regexp :string) (let [idx (if ws-regexp
                                                (let [m (re-matcher ws-regexp in)
                                                      m (.region m idx (.length in))]
                                                  (if (.lookingAt m)
                                                    (long (.end m))
                                                    idx))
                                                idx)]
                                      (if (= :regexp tag)
                                        (let [m (re-matcher (:regexp parser) in)
                                              m (.region m idx (.length in))]
                                          (when (.lookingAt m)
                                            (ParseState. (when-not (:hide parser)
                                                           [(.group m)])
                                                         (.end m))))
                                        (let [^String s (:string parser)]
                                          (when (.regionMatches in true idx s 0 (.length s))
                                            (ParseState. (when-not (:hide parser)
                                                           [s])
                                                         (+ idx (.length s))))))))))]
      (when-let [^ParseState state (parse 0 {:tag :nt :keyword start-rule})]
        (first (.ast state))))))

(comment

  (let [grammar "prog:	(expr NEWLINE)* ;
expr:	expr ('*'|'/') expr
    |	expr ('+'|'-') expr
    |	INT
    |	'(' expr ')'
    ;
NEWLINE : #'[\\r\\n]+' ;
INT     : #'[0-9]+' ;"
        expr-parser (insta/parser grammar)
        expr-cfg (insta-cfg/ebnf grammar)
        in "100+2*34
"]
    (time
     (dotimes [_ 10000]
       (ebnf-parser expr-cfg nil in :prog)))

    (time
     (dotimes [_ 10000]
       (expr-parser in))))

  (let [grammar "
spec: (HEADER_COMMENT / definition)* ;
definition: NAME <'::='> <'|'>? syntax? ;
syntax_element: (optional / mandatory / NAME / TOKEN) !'::=' REPEATABLE? ;
syntax: syntax_element+ choice* ;
optional: <'['> syntax+ <']'> ;
mandatory: <'{'> syntax+ <'}'> ;
choice: <'|'> syntax_element+ ;
REPEATABLE: <'...'> ;
NAME: #'<[-_:/a-zA-Z 0-9]+?>' ;
TOKEN: !'::=' #'[^ |\\n\\r\\t.!/]+' ;
HEADER_COMMENT: #'// *\\d.*?\\n' ;
        "
        spec-parser (insta/parser grammar :auto-whitespace (insta/parser "whitespace: #'\\s+'"))
        spec-cfg (insta-cfg/ebnf grammar)
        in "<colon> ::=
  :

<semicolon> ::=
  ;

<less than operator> ::=
  <

<equals operator> ::=
  =

<greater than operator> ::=
  >

<question mark> ::=
  ?

<left bracket or trigraph> ::=
    <left bracket>
  | <left bracket trigraph>

<right bracket or trigraph> ::=
    <right bracket>
  | <right bracket trigraph>
"]
    (time
     (dotimes [_ 1000]
       (ebnf-parser spec-cfg #"\s+" in :spec)))

    (time
     (dotimes [_ 1000]
       (spec-parser in)))))

(def sql-cfg (insta-cfg/ebnf (slurp (io/resource "core2/sql/SQL2011.ebnf"))))

(comment

  (let [in "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 ORDER BY 1"]
    (time
     (dotimes [_ 1000]
       (ebnf-parser sql-cfg #"\s+" in :directly_executable_statement)))

    (time
     (dotimes [_ 1000]
       (core2.sql/parse-sql2011 in :start :directly_executable_statement)))))

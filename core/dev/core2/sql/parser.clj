(ns core2.sql.parser
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [instaparse.core :as insta]
            [instaparse.cfg :as insta-cfg]
            [core2.sql])
  (:import java.io.File
           java.nio.IntBuffer
           [java.util ArrayDeque HashMap HashSet List Map]
           java.util.regex.Pattern))

;; Spike to replace Instaparse.

;; TODO:
;; explore error handling, needs to be passed upwards, merged, a kind of result.
;; try compiling rule bodies to fns?

;; https://arxiv.org/pdf/1509.02439v1.pdf
;; https://medium.com/@gvanrossum_83706/left-recursive-peg-grammars-65dab3c580e1
;; https://bford.info/pub/lang/thesis.pdf
;; https://arxiv.org/pdf/1806.11150.pdf

(set! *unchecked-math* :warn-on-boxed)

(defrecord ParseState [ast errs ^int idx])

(defrecord ParseFailure [^String in errs ^int idx])

(definterface IParser
  (^core2.sql.parser.ParseState parse [^String in ^int idx ^java.util.Map memos]))

(defrecord EpsilonParser [errs]
  IParser
  (parse [_ in idx memos]
    (if (= (.length in) idx)
      (ParseState. [] nil idx)
      (ParseState. nil errs idx))))

(defrecord WhitespaceParser [^java.util.regex.Pattern pattern ^core2.sql.parser.IParser parser]
  IParser
  (parse [_ in idx memos]
    (let [m (.matcher pattern in)
          m (.region m idx (.length in))
          idx (if (.lookingAt m)
                (int (.end m))
                idx)]
      (.parse parser in idx memos))))

(defrecord NonTerminalParser [^int rule-id ^objects rules]
  IParser
  (parse [_ in idx memos]
    (.parse ^IParser (aget rules rule-id) in idx memos)))

(defrecord RuleParser [rule-name ^int rule-id ^boolean raw? ^IParser parser errs]
  IParser
  (parse [_ in idx memos]
    (let [state (.parse parser in idx memos)]
      (ParseState. (when (.ast state)
                     (if raw?
                       (.ast state)
                       [(with-meta
                          (into [rule-name] (.ast state))
                          {:start-idx idx
                           :end-idx (.idx state)})]))
                   (when-let [rule-errs (.errs state)]
                     (if (:regexp? (meta rule-errs))
                       errs
                       rule-errs))
                   (.idx state)))))

(defrecord MemoizeParser [^RuleParser parser]
  IParser
  (parse [_ in idx memos]
    (let [memo-key (IntBuffer/wrap (doto (int-array 2)
                                     (aset 0 idx)
                                     (aset 1 (.rule-id parser))))]
      (if-let [state (.get memos memo-key)]
        state
        (doto (.parse parser in idx memos)
          (->> (.put memos memo-key)))))))

(defrecord MemoizeLeftRecParser [^RuleParser parser]
  IParser
  (parse [_ in idx memos]
    (let [memo-key (IntBuffer/wrap (doto (int-array 2)
                                     (aset 0 idx)
                                     (aset 1 (.rule-id parser))))]
      (if-let [state (.get memos memo-key)]
        state
        (loop [last-state (ParseState. nil nil idx)]
          (.put memos memo-key last-state)
          (let [^ParseState new-state (.parse parser in idx memos)]
            (if (.ast new-state)
              (if (<= (.idx new-state) (.idx last-state))
                last-state
                (recur new-state))
              (do (.remove memos memo-key)
                  (if (and (nil? (.ast last-state))
                           (.errs new-state))
                    new-state
                    last-state)))))))))

(defrecord HideParser [^core2.sql.parser.IParser parser]
  IParser
  (parse [_ in idx memos]
    (let [state (.parse parser in idx memos)]
      (ParseState. (when (.ast state)
                     [])
                   (.errs state)
                   (.idx state)))))

(defrecord OptParser [^core2.sql.parser.IParser parser]
  IParser
  (parse [_ in idx memos]
    (let [state (.parse parser in idx memos)]
      (if (.ast state)
        state
        (ParseState. [] nil (.idx state))))))

(defrecord NegParser [^core2.sql.parser.IParser parser]
  IParser
  (parse [_ in idx memos]
    (let [state (.parse parser in idx memos)]
      (if (.ast state)
        (ParseState. nil #{[:unexpected (subs in idx (.idx state))]} idx)
        (ParseState. [] nil idx)))))

(defrecord RepeatParser [^core2.sql.parser.IParser parser ^boolean star?]
  IParser
  (parse [_ in idx memos]
    (loop [ast []
           idx idx
           n 0]
      (let [state (.parse parser in idx memos)]
        (if (.ast state)
          (recur (into ast (.ast state))
                 (.idx state)
                 (inc n))
          (if (or star? (pos? n))
            (ParseState. ast nil idx)
            state))))))

(defrecord CatParser [^java.util.List parsers]
  IParser
  (parse [_ in idx memos]
    (loop [ast []
           idx idx
           n 0]
      (if (< n (.size parsers))
        (let [state (.parse ^IParser (.get parsers n) in idx memos)]
          (if (.ast state)
            (recur (into ast (.ast state))
                   (.idx state)
                   (inc n))
            (ParseState. nil (.errs state) (.idx state))))
        (ParseState. ast nil idx)))))

(defrecord OrdParser [^IParser parser1 ^IParser parser2]
  IParser
  (parse [_ in idx memos]
    (let [state1 (.parse parser1 in idx memos)
          state2 (.parse parser2 in idx memos)
          cmp (Integer/compare (.idx state1) (.idx state2))]
      (cond
        (and (.ast state1)
             (or (nil? (.ast state2))
                 (not (neg? cmp))))
        state1

        (.ast state2)
        state2

        (pos? cmp)
        state1

        (neg? cmp)
        state2

        :else
        (ParseState. nil (into (.errs state1) (.errs state2)) (.idx state1))))))

(defrecord StringParser [^String string errs]
  IParser
  (parse [_ in idx memos]
    (if (.regionMatches in true idx string 0 (.length string))
      (ParseState. [string] nil (+ idx (.length string)))
      (ParseState. nil errs idx))))

(defrecord RegexpParser [^Pattern pattern errs]
  IParser
  (parse [_ in idx memos]
    (let [m (.matcher pattern in)
          m (.region m idx (.length in))]
      (if (.lookingAt m)
        (ParseState. [(.group m)] nil (.end m))
        (ParseState. nil errs idx)))))

(defn- left-recursive? [grammar rule-name]
  (let [visited (HashSet.)
        stack (ArrayDeque.)]
    (.push stack (get grammar rule-name))
    (loop []
      (if-let [node (.poll stack)]
        (cond
          (= (:keyword node) rule-name)
          true

          (.contains visited node)
          (recur)

          :else
          (do (.add visited node)
              (let [node (if (= :nt (:tag node))
                           (get grammar (:keyword node))
                           node)]
                (case (:tag node)
                  :cat (let [[opts mandatory] (split-with (comp #{:opt :star} :tag) (:parsers node))]
                         (doseq [opt opts]
                           (.push stack opt))
                         (.push stack (first mandatory)))
                  :ord (do (.push stack (:parser1 node))
                           (.push stack (:parser2 node)))
                  :alt (doseq [alt (:parsers node)]
                         (.push stack alt))
                  (:star :plus :opt) (.push stack (:parser node))
                  :nt (.push stack node)
                  nil)
                (recur))))
        false))))

(defn- ->trailing-ws-parser [start-rule]
  {:tag :cat
   :parsers [{:tag :nt :keyword start-rule}
             {:tag :epsilon}]})

(defn- rule-kw->name [kw]
   (str "<" (str/replace (name kw) "_" " ") ">"))

(defn index->line-column [^String in ^long idx]
  (loop [line 1
         col 1
         n 0]
    (cond
      (= idx n) {:line line :column col}
      (= \newline (.charAt in n)) (recur (inc line) 1 (inc n))
      :else (recur line (inc col) (inc n)))))

(defn failure? [x]
  (instance? ParseFailure x))

(defn failure->str ^String [^ParseFailure failure]
  (let [{:keys [^long line ^long column]} (index->line-column (.in failure) (.idx failure))]
    (with-out-str
      (println (str "Parse error at line " line ", column " column ":"))
      (println (get (str/split-lines (.in failure)) (dec line)))
      (dotimes [_ (dec column)]
        (print " "))
      (println "^")
      (doseq [[category errs] (group-by first (.errs failure))]
        (println (str (str/capitalize (name category))
                      (if (= 1 (count errs))
                        ":"
                        " one of:")))
        (doseq [[_ msg] errs]
          (println (name msg)))))))

(defn build-ebnf-parser [grammar ws-pattern]
  (let [rule->id (zipmap (keys grammar) (range))
        rules (object-array (count rule->id))]
    (letfn [(build-parser [{:keys [tag hide] :as parser}]
              (cond-> (case tag
                        :nt (->NonTerminalParser (get rule->id (:keyword parser)) rules)
                        :star (->RepeatParser (build-parser (:parser parser)) true)
                        :plus (->RepeatParser (build-parser (:parser parser)) false)
                        :opt (->OptParser (build-parser (:parser parser)))
                        :cat (->CatParser (mapv build-parser (:parsers parser)))
                        :alt (->> (reverse (:parsers parser))
                                  (mapv build-parser)
                                  (reduce
                                   (fn [parser2 parser1]
                                     (->OrdParser parser1 parser2))))
                        :ord (->OrdParser (build-parser (:parser1 parser))
                                          (build-parser (:parser2 parser)))
                        :neg (->WhitespaceParser ws-pattern (->NegParser (build-parser (:parser parser))))
                        :epsilon (->WhitespaceParser ws-pattern (->EpsilonParser #{[:expected "<EOF>"]} ))
                        :regexp (->WhitespaceParser ws-pattern (->RegexpParser (:regexp parser) (with-meta
                                                                                                  #{[:expected (:regexp parser)]}
                                                                                                  {:regexp? true})))
                        :string (->WhitespaceParser ws-pattern (->StringParser (:string parser) #{[:expected (:string parser)]})))
                hide (->HideParser)))]
      (doseq [[k v] grammar
              :let [rule-id (int (get rule->id k))
                    raw? (= {:reduction-type :raw} (:red v))
                    memo-fn (if (left-recursive? grammar k)
                              ->MemoizeLeftRecParser
                              ->MemoizeParser)
                    parser (->RuleParser k rule-id raw? (build-parser v) #{[:expected (rule-kw->name k)]})]]
        (aset rules rule-id (memo-fn parser)))
      (fn [in start-rule]
        (when-let [state (-> (->trailing-ws-parser start-rule)
                             ^IParser (build-parser)
                             (.parse in 0 (HashMap.)))]
          (if (.errs state)
            (ParseFailure. in (.errs state) (.idx state))
            (first (.ast state))))))))

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
        expr-cfg (build-ebnf-parser (insta-cfg/ebnf grammar) #"")
        in "100+2*34
"]
    (time
     (dotimes [_ 10000]
       (expr-cfg in :prog)))

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
        spec-cfg (build-ebnf-parser (insta-cfg/ebnf grammar) #"(\s+|$)")
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
       (spec-cfg in :spec)))

    (time
     (dotimes [_ 1000]
       (spec-parser in)))))

(def sql-cfg (insta-cfg/ebnf (slurp (io/resource "core2/sql/SQL2011.ebnf"))))

(def sql-parser (build-ebnf-parser sql-cfg #"(\s+|$|\s*--[^\r\n]*\s*|\s*/[*].*?([*]/\s*|$))"))

(comment

  (let [in "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END,
       a+b*2+c*3+d*4,
       abs(b-c),
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       c-d,
       a+b*2,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1 WHERE e+d BETWEEN a+b-10 AND c+130
    OR coalesce(a,b,c,d,e)<>0"]

    (time
     (dotimes [_ 1000]
       (sql-parser in :directly_executable_statement)))

    (time
     (dotimes [_ 1000]
       (core2.sql/parse-sql2011 in :start :directly_executable_statement)))))

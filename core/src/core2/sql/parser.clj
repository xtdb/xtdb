(ns core2.sql.parser
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import java.io.File
           [java.util ArrayDeque Arrays HashSet Map Set]
           [java.util.regex Matcher Pattern]
           java.util.function.Function
           [core2.sql.parser Parser$ParseState Parser$ParseErrors Parser$AParser
            Parser$WhitespaceParser Parser$EpsilonParser Parser$RuleParser Parser$MemoizeParser Parser$MemoizeLeftRecParser Parser$NonTerminalParser
            Parser$HideParser Parser$OptParser Parser$NegParser Parser$RepeatParser Parser$CatParser Parser$AltParser
            Parser$RegexpParser Parser$StringParser Parser]))

;; https://arxiv.org/pdf/1509.02439v1.pdf
;; https://medium.com/@gvanrossum_83706/left-recursive-peg-grammars-65dab3c580e1
;; https://bford.info/pub/lang/thesis.pdf
;; https://arxiv.org/pdf/1806.11150.pdf

(set! *unchecked-math* :warn-on-boxed)

(defrecord ParseFailure [^String in ^Set errs ^int idx])

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
          (println msg))))))

(defn build-ebnf-parser
  ([grammar ws-pattern]
   (build-ebnf-parser grammar ws-pattern (fn [rule-name]
                                           Parser/NEVER_RAW)))
  ([grammar ws-pattern ->raw?]
   (let [rule->id (zipmap (keys grammar) (range))
         ^"[Lcore2.sql.parser.Parser$AParser;" rules (make-array Parser$AParser (count rule->id))]
     (letfn [(build-parser [rule-name {:keys [tag hide] :as parser}]
               (let [parser (case tag
                              :nt (Parser$NonTerminalParser. (get rule->id (:keyword parser)))
                              :star (Parser$RepeatParser. (build-parser rule-name (:parser parser)) true)
                              :plus (Parser$RepeatParser. (build-parser rule-name (:parser parser)) false)
                              :opt (Parser$OptParser. (build-parser rule-name (:parser parser)))
                              :cat (Parser$CatParser. (mapv (partial build-parser rule-name) (:parsers parser)))
                              :alt (Parser$AltParser. (mapv (partial build-parser rule-name) (:parsers parser)))
                              :ord (Parser$AltParser. (->> ((fn step [{:keys [parser1 parser2]}]
                                                              (cons parser1 (if (= :ord (:tag parser2))
                                                                              (step parser2)
                                                                              [parser2])))
                                                            parser)
                                                           (mapv (partial build-parser rule-name))))
                              :neg (Parser$WhitespaceParser. ws-pattern (Parser$NegParser. (build-parser rule-name (:parser parser))))
                              :epsilon (Parser$WhitespaceParser. ws-pattern (Parser$EpsilonParser.))
                              :regexp (Parser$WhitespaceParser. ws-pattern (Parser$RegexpParser. (:regexp parser)
                                                                                                 [:expected (rule-kw->name rule-name)]
                                                                                                 (reify Function
                                                                                                   (apply [_ m]
                                                                                                     [(.group ^Matcher m)]))))
                              :string (Parser$WhitespaceParser. ws-pattern
                                                                (if (re-find #"^\w+$" (:string parser))
                                                                  (Parser$RegexpParser. (Pattern/compile (str (Pattern/quote (:string parser)) "\\b")
                                                                                                         Pattern/CASE_INSENSITIVE)
                                                                                        [:expected (:string parser)]
                                                                                        (let [ast [(:string parser)]]
                                                                                          (reify Function
                                                                                            (apply [_ m]
                                                                                              ast))))
                                                                  (Parser$StringParser. (:string parser)))))]
                 (if hide
                   (Parser$HideParser. parser)
                   parser)))]
       (doseq [[k v] grammar
               :let [rule-id (int (get rule->id k))
                     raw? (if (= {:reduction-type :raw} (:red v))
                            Parser/ALWAYS_RAW
                            (->raw? k))
                     parser (Parser$RuleParser. k raw? (build-parser k v))
                     parser (if (left-recursive? grammar k)
                              (Parser$MemoizeLeftRecParser. parser rule-id)
                              (Parser$MemoizeParser. parser rule-id))]]
         (aset rules rule-id ^Parser$AParser parser))

       (doseq [^Parser$AParser rule-parser rules]
         (.init rule-parser rules))

       (fn [^String in start-rule]
         (let [errors (Parser$ParseErrors.)
               m-size (bit-shift-left (inc (.length in)) 9)
               memos (make-array Parser$ParseState m-size)
               _ (Arrays/fill ^objects memos Parser/NOT_FOUND)
               parser (Parser$CatParser. [(aget rules (get rule->id start-rule))
                                          (Parser$WhitespaceParser. ws-pattern (Parser$EpsilonParser.))])]
           (if-let [state (.parse parser in 0 memos errors)]
             (first (.ast state))
             (ParseFailure. in (.getErrors errors) (.getIndex errors)))))))))

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
        spec-cfg (build-ebnf-parser (insta-cfg/ebnf grammar) #"(?:\s+|\A|\b)")
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

(def sql-cfg (read-string (slurp (io/resource "core2/sql/SQL2011.edn"))))

(def sql-parser (build-ebnf-parser sql-cfg
                                   #"(?:\s+|(?<=\p{Punct}|\s)|\b|\s*--[^\r\n]*\s*|\s*/[*].*?(?:[*]/\s*|$))"
                                   (fn [rule-name]
                                     (if (and (not (contains? #{:table_primary :query_expression :table_expression} rule-name))
                                              (re-find #"(^|_)(term|factor|primary|expression|query_expression_body)$" (name rule-name)))
                                       Parser/SINGLE_CHILD
                                       Parser/NEVER_RAW))))

(def parse (memoize
            (fn self
              ([in]
               (self in :directly_executable_statement))
              ([in start-rule]
               (vary-meta
                (sql-parser in start-rule)
                assoc :sql in)))))

(comment
  (sql-parser "SELECT * FROMfoo" :directly_executable_statement)

  (println (failure->str (sql-parser "* FROMfoo" :term)))

  (sql-parser "  (SELECT avg(c) FROM t1)  " :subquery)

  (sql-parser "a[0]" :value_expression_primary)

  (sql-parser "- - 2" :factor)

  (sql-parser "IN ( col0 * col0 )" :in_predicate_part_2)

  (sql-parser "INTERVAL '3 4' DAY TO HOUR" :interval_literal)

  (doseq [sql ["SELECT u.a[0] AS first_el FROM uo"
               "SELECT u.b[u.a[0]] AS dyn_idx FROM u"
               "SELECT 1 YEAR + 3 MONTH + 4 DAY t FROM foo WHERE foo.a = 42"
               "SELECT 3 * 1 YEAR t FROM foo WHERE foo.a = 42"
               "SELECT foo.a YEAR + 1 MONTH + 2 DAY t FROM foo WHERE foo.a = 42"
               "SELECT foo.a YEAR + 1 MONTH - 2 DAY t FROM foo WHERE foo.a = 42"
               "SELECT foo.a || 'a' || 'b' t FROM foo WHERE foo.a = 42"
               "SELECT ALL 4 AS col1 FROM tab2 AS cor0 WHERE NULL IS NULL"
               "SELECT ALL 74 * - COALESCE ( + CASE - CASE WHEN NOT ( NOT - 79 >= NULL ) THEN 48 END WHEN + + COUNT ( * ) THEN 6 END, MIN ( ALL + - 30 ) * 45 * 77 ) * - 14 FROM (VALUES 0) AS no_from"
               "SELECT cor0.col2 AS col2 FROM tab2 AS cor0 GROUP BY col2 HAVING NOT NULL < NULL"
               "SELECT DISTINCT col2 FROM tab1 WHERE NULL BETWEEN NULL AND - col2"
               ;; spec doesn't seem to allow expressions inside IN.
               "SELECT * FROM tab1 WHERE NULL NOT IN ( col0 * col0 )"
               "SELECT INTERVAL '3 4' DAY TO HOUR t FROM foo WHERE foo.a = 42"
               "SELECT * FROMfoo"
               "SELECT + NULLIF ( + 67, + + NULLIF ( + + 46, 66 - CASE WHEN 51 IN ( + 91 ) THEN + SUM ( CAST ( NULL AS INTEGER ) ) + - 92 ELSE - ( - 47 ) END ) ) FROM (VALUES 0) AS no_from
"]
          :let [tree (sql-parser sql :directly_executable_statement)]
          :when (failure? tree)]
    (println (failure->str tree)))

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
       (sql-parser in :directly_executable_statement)))))

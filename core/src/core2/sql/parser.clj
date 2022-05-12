(ns core2.sql.parser
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import java.io.File
           java.nio.IntBuffer
           [java.util ArrayDeque ArrayList HashMap HashSet List Map Set]
           [java.util.regex Matcher Pattern]))

;; https://arxiv.org/pdf/1509.02439v1.pdf
;; https://medium.com/@gvanrossum_83706/left-recursive-peg-grammars-65dab3c580e1
;; https://bford.info/pub/lang/thesis.pdf
;; https://arxiv.org/pdf/1806.11150.pdf

(set! *unchecked-math* :warn-on-boxed)

(defrecord ParseState [ast ^int idx])

(defrecord ParseFailure [^String in errs ^int idx])

(definterface IParseErrors
  (^void addError [error ^int idx])

  (^int getIndex []))

(def ^:private null-parse-errors
  (reify IParseErrors
    (addError [_ _ _])
    (getIndex [_] 0)))

(deftype ParseErrors [^Set errs ^:unsynchronized-mutable ^int idx]
  IParseErrors
  (addError [this error idx]
    (cond
      (= idx (.idx this))
      (.add errs error)

      (> idx (.idx this))
      (do (.clear errs)
          (.add errs error)
          (set! (.idx this) idx))))

  (getIndex [_] idx))

(definterface IParser
  (^core2.sql.parser.ParseState parse [^String in ^int idx ^java.util.Map memos ^core2.sql.parser.IParseErrors errors]))

(def ^:private epsilon-err [:expected "<EOF>"])

(defrecord EpsilonParser []
  IParser
  (parse [_ in idx memos errors]
    (if (= (.length in) idx)
      (ParseState. [] idx)
      (.addError errors epsilon-err idx))))

(def ^:private ws-err [:expected "<WS>"])

(defrecord WhitespaceParser [^Pattern pattern ^IParser parser]
  IParser
  (parse [_ in idx memos errors]
    (let [m (.matcher pattern in)
          m (.region m idx (.length in))
          m (.useTransparentBounds m true)]
      (cond
        (.lookingAt m)
        (.parse parser in (.end m) memos errors)

        (zero? idx)
        (.parse parser in idx memos errors)

        :else
        (.addError errors ws-err idx)))))

(defrecord NonTerminalParser [^int rule-id ^objects rules]
  IParser
  (parse [_ in idx memos errors]
    (.parse ^IParser (aget rules rule-id) in idx memos errors)))

(defrecord RuleParser [rule-name ast-head ^int rule-id raw? ^IParser parser]
  IParser
  (parse [_ in idx memos errors]
    (when-let [state (.parse parser in idx memos errors)]
      (ParseState. (let [ast (.ast state)]
                     (if (raw? ast)
                       ast
                       [(with-meta
                          (into ast-head ast)
                          {:start-idx idx
                           :end-idx (.idx state)})]))
                   (.idx state)))))

(def ^:private not-found (Object.))

(defrecord MemoizeParser [^RuleParser parser]
  IParser
  (parse [_ in idx memos errors]
    (let [memo-key (IntBuffer/wrap (doto (int-array 2)
                                     (aset 0 idx)
                                     (aset 1 (.rule-id parser))))
          state (.getOrDefault memos memo-key not-found)]
      (if (identical? not-found state)
        (doto (.parse parser in idx memos errors)
          (->> (.put memos memo-key)))
        state))))

(defrecord MemoizeLeftRecParser [^RuleParser parser]
  IParser
  (parse [_ in idx memos errors]
    (let [memo-key (IntBuffer/wrap (doto (int-array 2)
                                     (aset 0 idx)
                                     (aset 1 (.rule-id parser))))
          state (.getOrDefault memos memo-key not-found)]
      (if (identical? not-found state)
        (loop [^ParseState last-state nil]
          (.put memos memo-key last-state)
          (if-let [^ParseState new-state (.parse parser in idx memos errors)]
            (if (and last-state (<= (.idx new-state) (.idx last-state)))
              (do (.remove memos memo-key)
                  last-state)
              (recur new-state))
            (do (.remove memos memo-key)
                last-state)))
        state))))

(defrecord HideParser [^IParser parser]
  IParser
  (parse [_ in idx memos errors]
    (when-let [state (.parse parser in idx memos errors)]
      (ParseState. [] (.idx state)))))

(defrecord OptParser [^IParser parser]
  IParser
  (parse [_ in idx memos errors]
    (if-let [state (.parse parser in idx memos errors)]
      state
      (ParseState. [] idx))))

(defrecord NegParser [^IParser parser]
  IParser
  (parse [_ in idx memos errors]
    (if-let [state (.parse parser in idx memos null-parse-errors)]
      (.addError errors [:unexpected (subs in idx (.idx state))] idx)
      (ParseState. [] idx))))

(defrecord RepeatParser [^IParser parser ^boolean star?]
  IParser
  (parse [_ in idx memos errors]
    (loop [ast (ArrayList.)
           idx idx
           n 0]
      (if-let [state (.parse parser in idx memos errors)]
        (recur (doto ast
                 (.addAll (.ast state)))
               (.idx state)
               (inc n))
        (when (or star? (pos? n))
          (ParseState. (vec ast) idx))))))

(defrecord CatParser [^List parsers]
  IParser
  (parse [_ in idx memos errors]
    (loop [ast (ArrayList.)
           idx idx
           n 0]
      (if (< n (.size parsers))
        (when-let [state (.parse ^IParser (.get parsers n) in idx memos errors)]
          (recur (doto ast
                   (.addAll (.ast state)))
                 (.idx state)
                 (inc n)))
        (ParseState. (vec ast) idx)))))

(defrecord AltParser [^List parsers]
  IParser
  (parse [_ in idx memos errors]
    (loop [^ParseState state1 nil
           n 0]
      (if (= n (.size parsers))
        state1
        (let [state2 (.parse ^IParser (.get parsers n) in idx memos errors)]
          (recur (if state1
                   (if state2
                     (if (<= (.idx state2) (.idx state1))
                       state1
                       state2)
                     state1)
                   state2)
                 (inc n)))))))

(defrecord StringParser [^String string ast errs]
  IParser
  (parse [_ in idx memos errors]
    (if (.regionMatches in true idx string 0 (.length string))
      (ParseState. ast (+ idx (.length string)))
      (.addError errors errs idx))))

(defrecord RegexpParser [^Pattern pattern errs matcher-fn]
  IParser
  (parse [_ in idx memos errors]
    (let [m (.matcher pattern in)
          m (.region m idx (.length in))
          m (.useTransparentBounds m true)]
      (if (.lookingAt m)
        (ParseState. (matcher-fn m) (.end m))
        (.addError errors errs idx)))))

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
          (println msg))))))

(defn build-ebnf-parser
  ([grammar ws-pattern]
   (build-ebnf-parser grammar ws-pattern (fn [rule-name]
                                           (fn [ast]
                                             false))))
  ([grammar ws-pattern ->raw?]
   (let [rule->id (zipmap (keys grammar) (range))
         rules (object-array (count rule->id))]
     (letfn [(build-parser [rule-name {:keys [tag hide] :as parser}]
               (cond-> (case tag
                         :nt (->NonTerminalParser (get rule->id (:keyword parser)) rules)
                         :star (->RepeatParser (build-parser rule-name (:parser parser)) true)
                         :plus (->RepeatParser (build-parser rule-name (:parser parser)) false)
                         :opt (->OptParser (build-parser rule-name (:parser parser)))
                         :cat (->CatParser (mapv (partial build-parser rule-name) (:parsers parser)))
                         :alt (->AltParser (mapv (partial build-parser rule-name) (:parsers parser)))
                         :ord (->AltParser (->> ((fn step [{:keys [parser1 parser2]}]
                                                   (cons parser1 (if (= :ord (:tag parser2))
                                                                   (step parser2)
                                                                   [parser2])))
                                                 parser)
                                                (mapv (partial build-parser rule-name))))
                         :neg (->WhitespaceParser ws-pattern (->NegParser (build-parser rule-name (:parser parser))))
                         :epsilon (->WhitespaceParser ws-pattern (->EpsilonParser))
                         :regexp (->WhitespaceParser ws-pattern (->RegexpParser (:regexp parser)
                                                                                [:expected (rule-kw->name rule-name)]
                                                                                (fn [^Matcher m]
                                                                                  [(.group m)])))
                         :string (->WhitespaceParser ws-pattern
                                                     (if (re-find #"^\w+$" (:string parser))
                                                       (->RegexpParser (Pattern/compile (str (Pattern/quote (:string parser)) "\\b")
                                                                                        Pattern/CASE_INSENSITIVE)
                                                                       [:expected (:string parser)]
                                                                       (constantly [(:string parser)]))
                                                       (->StringParser (:string parser) [(:string parser)] [:expected (:string parser)]))))
                 hide (->HideParser)))]
       (doseq [[k v] grammar
               :let [rule-id (int (get rule->id k))
                     raw? (if (= {:reduction-type :raw} (:red v))
                            (fn [ast]
                              true)
                            (->raw? k))
                     memo-fn (if (left-recursive? grammar k)
                               ->MemoizeLeftRecParser
                               ->MemoizeParser)
                     parser (->RuleParser k [k] rule-id raw? (build-parser k v))]]
         (aset rules rule-id (memo-fn parser)))
       (fn [in start-rule]
         (let [errors (ParseErrors. (HashSet.) 0)]
           (if-let [state (-> ^IParser (build-parser nil (->trailing-ws-parser start-rule))
                              (.parse in 0 (HashMap.) errors))]
             (first (.ast state))
             (ParseFailure. in (.errs errors) (.getIndex errors)))))))))

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
                                       (fn [ast]
                                         (= 1 (count ast)))
                                       (fn [ast]
                                         false)))))

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

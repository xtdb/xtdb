(ns core2.sql.antlr
  (:import java.io.File
           org.antlr.v4.Tool
           org.antlr.v4.tool.Grammar
           [org.antlr.v4.runtime CharStream CharStreams CommonTokenStream ParserRuleContext Vocabulary]
           [org.antlr.v4.runtime.tree ErrorNode ParseTree ParseTreeVisitor RuleNode TerminalNode]))

(set! *unchecked-math* :warn-on-boxed)

(defn ^org.antlr.v4.tool.Grammar parse-grammar-from-string [^String s]
  (let [tool  (Tool.)
        ast (.parseGrammarFromString tool s)
        grammar (.createGrammar tool ast)]
    (.process tool grammar false)
    grammar))

(defn generate-parser [^String s ^String package-name ^String grammar-file]
  (let [grammar-file (File. grammar-file)]
    (.mkdirs (.getParentFile grammar-file))
    (spit grammar-file s)
    (-> (Tool. (into-array ["-package" package-name (str grammar-file)]))
        (.processGrammarsOnCommandLine))))

(defn ->ast [^"[Ljava.lang.String;" rule-names
             ^Vocabulary vocabulary
             ^ParseTree tree]
  (.accept tree (reify ParseTreeVisitor
                  (visit [this ^ParseTree node]
                    (.accept node this))

                  (visitChildren [this ^RuleNode node]
                    (let [child-count (.getChildCount node)]
                      (loop [n 0
                             acc (transient [(keyword (aget rule-names (.getRuleIndex (.getRuleContext node))))])]
                        (if (= n child-count)
                          (persistent! acc)
                          (recur (inc n)
                                 (conj! acc (.accept (.getChild node n) this)))))))

                  (visitErrorNode [_ ^ErrorNode node]
                    (let [token (.getSymbol node)]
                      (throw (ex-info (.getText node)
                                      {:text (.getText node)
                                       :line (.getLine token)
                                       :col (.getCharPositionInLine token)})) ))

                  (visitTerminal [_ ^TerminalNode node]
                    (if-let [symbol (.getSymbolicName vocabulary (.getType (.getSymbol node)))]
                      [(keyword symbol) (.getText node)]
                      (.getText node))))))

(defn- upper-case-char-stream ^org.antlr.v4.runtime.CharStream [^CharStream in]
  (reify CharStream
    (getText [_ interval]
      (.getText in interval))

    (consume [_]
      (.consume in))

    (LA [_ i]
      (let [c (.LA in i)]
        (if (pos? c)
          (Character/toUpperCase c)
          c)))

    (mark [_]
      (.mark in))

    (release [_ marker]
      (.release in marker))

    (index [_]
      (.index in))

    (seek [_ index]
      (.seek in index))

    (size [_]
      (.size in))

    (getSourceName [_]
      (.getSourceName in))))

(defn parse
  (^org.antlr.v4.runtime.ParserRuleContext [^Grammar grammar ^String s]
   (parse grammar s {}))
  (^org.antlr.v4.runtime.ParserRuleContext [^Grammar grammar ^String s {:keys [start string-ci]}]
   (let [lexer (.createLexerInterpreter grammar (cond-> (CharStreams/fromString s)
                                                  string-ci (upper-case-char-stream)))
         parser (.createParserInterpreter grammar (CommonTokenStream. lexer))]
     (.parse parser (if start
                      (.index (.getRule grammar (name start)))
                      0)))))

(comment

  (let [expr-g4 "
grammar Expr;
prog:	(expr NEWLINE)* ;
expr:	expr ('*'|'/') expr
    |	expr ('+'|'-') expr
    |	INT
    |	'(' expr ')'
    ;
NEWLINE : [\\r\\n]+ ;
INT     : [0-9]+ ;"
        expr-grammar (parse-grammar-from-string expr-g4)
        rule-names (.getRuleNames expr-grammar)
        vocabulary (.getVocabulary expr-grammar)
        tree (time (parse expr-grammar "100+2*34\n"))
        ast (time (->ast rule-names vocabulary tree))]

    #_(generate-parser expr-g4
                       "core2.expr"
                       "core/target/codegen/core2/expr/Expr.g4")

    ast)

  (let [parser (core2.expr.ExprParser. (CommonTokenStream. (core2.expr.ExprLexer. (CharStreams/fromString "100+2*34\n"))))
        rule-names (.getRuleNames parser)
        vocabulary (.getVocabulary parser)
        tree (time (.prog parser))
        ast (time (->ast rule-names vocabulary tree))]
    ast)

  (require 'instaparse.core)
  (let [expr-bnf "
prog:	(expr NEWLINE)* ;
expr:	expr ('*'|'/') expr
    |	expr ('+'|'-') expr
    |	INT
    |	'(' expr ')'
    ;
NEWLINE : #\"[\\r\\n]+\" ;
INT     : #\"[0-9]+\" ;"
        parser (instaparse.core/parser expr-bnf)
        ast (time (parser "100+2*34\n"))]
    ast)


  (let [sql-g4 "
grammar SQLSpecGrammar;

spec:	definition* ;
definition: NAME '::=' syntax+ ;
syntax:  (NAME | TOKEN | optional | mandatory | SEE_THE_SYNTAX_RULES) REPEATABLE? ('|' syntax)* ;
optional: '[' syntax+ ']' ;
mandatory: '{' syntax+ '}' ;
REPEATABLE: '...' ;
SEE_THE_SYNTAX_RULES: '!!' .*? '\\n' ;
NAME: '<' [-_:/a-zA-Z 0-9]+ '>' ;
TOKEN: ~[ \\n\\r\\t.!]+ ;
WS: [ \\n\\r\\t]+ -> skip ;
COMMENT: '//' .*? '\\n';
        "
        sql-spec-grammar (parse-grammar-from-string sql-g4)
        rule-names (.getRuleNames sql-spec-grammar)
        vocabulary (.getVocabulary sql-spec-grammar)
        tree (time (parse sql-spec-grammar
                          (slurp (clojure.java.io/resource "core2/sql/SQL2011.txt"))
                          {:start "spec"}))
        ast (time (->ast rule-names vocabulary tree))]

    (binding [*print-length* nil
              *print-level* nil
              *print-namespace-maps* false]
      (spit "target/sql2011-ast.edn" (pr-str ast))))

  (def sql2011-ast (clojure.edn/read-string (slurp "target/sql2011-ast.edn")))

  (def literal-set
    (-> (set (take-while (complement #{"<identifier>"}) (map (comp second second) (rest core2.sql.antlr/sql2011-ast))))
        (conj "<character set specification>"
              "<standard character set name>"
              "<implementation-defined character set name>"
              "<user-defined character set name>"
              "<interval qualifier>"
              "<start field>"
              "<end field>"
              "<single datetime field>"
              "<primary datetime field>"
              "<non-second primary datetime field>"
              "<interval fractional seconds precision>"
              "<interval leading field precision>"
              "<SQL language identifier>"
              "<SQL language identifier start>"
              "<SQL language identifier part>"
              "<schema name>"
              "<unqualified schema name>"
              "<catalog name>"
              "<character set name>"
              "<identifier>"
              "<actual identifier>"
              "<non-escaped character>"
              "<escaped character>")))

  (def syntax-rules-overrides
    {'SPACE "' '"
     'QUOTE "'\\''"
     'PERIOD "'.'"
     'REVERSE_SOLIDUS "'\\\\'"
     'LEFT_BRACKET "'['"
     'RIGHT_BRACKET "']'"
     'VERTICAL_BAR "'|'"
     'LEFT_BRACE "'{'"
     'RIGHT_BRACE "'}'"
     'IDENTIFIER_START "SIMPLE_LATIN_LETTER"
     'IDENTIFIER_EXTEND "SIMPLE_LATIN_LETTER | DIGIT | UNDERSCORE"
     'UNICODE_ESCAPE_CHARACTER "'\\\\'"
     'NONDOUBLEQUOTE_CHARACTER "~'\"'"
     'DOUBLEQUOTE_SYMBOL ""
     'DOUBLE_PERIOD "'..'"
     'WHITE_SPACE "[\\n\\r\\t ]+ -> skip"
     'BRACKETED_COMMENT_CONTENTS "."
     'NEWLINE "[\\r\\n]+"
     'NONQUOTE_CHARACTER "~'\\''"
     'NON_ESCAPED_CHARACTER "."
     'ESCAPED_CHARACTER "'\\\\' ."})

  (def fragment-set #{})

  (def skip-rule-set #{})

  (def rule-overrides {})

  (def extra-rules "application_time_period_name : IDENTIFIER ;

embedded_variable_name : IDENTIFIER ;

transition_table_name : IDENTIFIER ;
")

  (spit "core/src/core2/sql/SQL2011.g"
        (-> (with-out-str
              (println "grammar SQL2011;")
              (println)
              (doseq [[n _ & body]
                      (clojure.walk/postwalk
                       (fn [x]
                         (if (vector? x)
                           (case (first x)
                             :NAME
                             (let [[_ n] x
                                   terminal? (contains? literal-set n)
                                   n (subs n 1 (dec (count n)))
                                   n (clojure.string/replace n #"[ :-]" "_")]
                               (symbol (if terminal?
                                         (clojure.string/upper-case n)
                                         (clojure.string/lower-case n))))

                             :TOKEN
                             (str "'" (clojure.string/replace  (second x) "'" "\\'") "'")

                             :REPEATABLE
                             '+

                             :SEE_THE_SYNTAX_RULES
                             (first x)

                             :COMMENT
                             (first x)

                             :syntax
                             (if (= "|" (last (butlast x)))
                               (concat (rest (butlast (butlast x))) ['|] (last x))
                               (rest x))

                             :spec
                             (rest x)

                             :definition
                             (concat (cons (second x)
                                           (cons (symbol ":") (apply concat (nthrest x 3))))
                                     [(symbol ";")])

                             :optional
                             (let [x (apply concat (rest (rest (butlast x))))]
                               (if (= '+ (last x))
                                 (list (butlast x) '*)
                                 (list x '?)))

                             :mandatory
                             (apply concat (rest (rest (butlast x))))

                             x)))
                       (vec sql2011-ast))
                      :when (not (contains? skip-rule-set? n))]
                (when (contains? fragment-set n)
                  (println "fragment"))
                (println n ":")
                (println "    " (or (get rule-overrides n)
                                    (clojure.string/join " " (clojure.walk/postwalk
                                                              #(cond
                                                                 (string? %)
                                                                 (symbol %)

                                                                 (= :SEE_THE_SYNTAX_RULES %)
                                                                 (symbol (get syntax-rules-overrides n %))

                                                                 (sequential? %)
                                                                 (let [x (vec %)]
                                                                   (cond
                                                                     (= 1 (count x))
                                                                     (first x)

                                                                     (and (= 2 (count x)) (contains? '#{* ? +} (last x)))
                                                                     (symbol (clojure.string/join x))

                                                                     :else
                                                                     (seq x)))

                                                                 :else
                                                                 %)
                                                              body))))
                (println)))
            (clojure.string/replace " +" "+")
            extra-rules))

  (-> (Tool. (into-array ["-package" "core2.sql" "-no-listener" "-no-visitor" "core/src/core2/sql/SQL2011.g"]))
      (.processGrammarsOnCommandLine)))

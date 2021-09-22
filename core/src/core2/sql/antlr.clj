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
    ast))

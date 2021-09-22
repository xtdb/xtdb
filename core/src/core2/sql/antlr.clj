(ns core2.sql.antlr
  (:import java.io.File
           org.antlr.v4.Tool
           org.antlr.v4.tool.Grammar
           [org.antlr.v4.runtime CharStreams CommonTokenStream ParserRuleContext]
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

(defn ->ast [^"[Ljava.lang.String;" rule-names ^ParseTree tree]
  (.accept tree (reify ParseTreeVisitor
                  (visit [this ^ParseTree node]
                    (.accept node this))

                  (visitChildren [this ^RuleNode node]
                    (loop [n 0
                           acc [(keyword (aget rule-names (.getRuleIndex (.getRuleContext node))))]]
                      (if (= n (.getChildCount node))
                        acc
                        (recur (inc n)
                               (conj acc (.accept (.getChild node n) this))))))

                  (visitErrorNode [_ ^ErrorNode node]
                    (let [token (.getSymbol node)]
                      (throw (ex-info (.getText node)
                                      {:text (.getText node)
                                       :line (.getLine token)
                                       :col (.getCharPositionInLine token)})) ))

                  (visitTerminal [_ ^TerminalNode node]
                    (.getText node)))))

(defn parse
  (^org.antlr.v4.runtime.ParserRuleContext [^Grammar grammar ^String s]
   (parse grammar s {}))
  (^org.antlr.v4.runtime.ParserRuleContext [^Grammar grammar ^String s {:keys [start]}]
   (let [lexer (.createLexerInterpreter grammar (CharStreams/fromString s))
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
        tree (time (parse expr-grammar "100+2*34\n"))
        ast (time (->ast rule-names tree))]

    #_(generate-parser expr-g4
                       "core2.expr"
                       "core/target/codegen/core2/expr/Expr.g4")

    ast)

  (let [parser (core2.expr.ExprParser. (CommonTokenStream. (core2.expr.ExprLexer.  (CharStreams/fromString "100+2*34\n"))))
        rule-names (.getRuleNames parser)
        tree (time (.prog parser))
        ast (time (->ast rule-names tree))]
    ast))

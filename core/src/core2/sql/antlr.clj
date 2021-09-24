(ns core2.sql.antlr
  (:import [org.antlr.v4.runtime CharStream CharStreams CommonTokenStream ParserRuleContext Vocabulary]
           [org.antlr.v4.runtime.tree ErrorNode ParseTree ParseTreeVisitor RuleNode TerminalNode]))

(set! *unchecked-math* :warn-on-boxed)

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

(defn upper-case-char-stream ^org.antlr.v4.runtime.CharStream [^CharStream in]
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

(ns xtdb.sql.grammar-ambiguity-test
  (:require [clojure.string :as str]
            [clojure.test :as t])
  (:import (org.antlr.v4.runtime BaseErrorListener CharStreams CommonTokenStream DiagnosticErrorListener)
           (org.antlr.v4.runtime.atn PredictionMode)
           (xtdb.antlr Sql SqlLexer)))

(defn- keyword-tokens []
  (let [vocab (SqlLexer/VOCABULARY)]
    (->> (range 1 (inc (.getMaxTokenType vocab)))
         (keep (fn [tt]
                 (when-let [lit (.getLiteralName vocab tt)]
                   (let [word (subs lit 1 (dec (count lit)))]
                     (when (re-matches #"[A-Za-z_]+" word)
                       word)))))
         (into (sorted-set)))))

(defn- ambiguities
  "the ambiguous decisions parsing `sql` provokes, as `<rule> ambigAlts={…}` keys.

  `LL_EXACT_AMBIG_DETECTION` is what surfaces these at all: ANTLR otherwise resolves an ambiguity
  by alternative order and reports nothing, so a grammar that generates cleanly and a suite that
  passes both say nothing about whether a decision is ambiguous."
  [sql]
  (let [!msgs (atom [])
        lexer (SqlLexer. (CharStreams/fromString sql))
        parser (Sql. (CommonTokenStream. lexer))]
    (.removeErrorListeners lexer)
    (.removeErrorListeners parser)
    (.addErrorListener parser (DiagnosticErrorListener.))
    (.addErrorListener parser
                       (proxy [BaseErrorListener] []
                         (syntaxError [_recognizer _offending _line _pos msg _e]
                           (swap! !msgs conj msg))))
    (.setPredictionMode (.getInterpreter parser) PredictionMode/LL_EXACT_AMBIG_DETECTION)
    (try
      (.directSqlStatement parser)
      (catch Exception _))
    (->> @!msgs
         (keep #(when-let [[_ rule alts] (re-matches #"reportAmbiguity d=\d+ \((\w+)\): (ambigAlts=\{[\d, ]+\}).*" %)]
                  (str rule " " alts)))
         (into (sorted-set)))))

(def ^:private name-positions
  [["column ref, bare"        #(str "SELECT " % " FROM docs")]
   ["column ref, qualified"   #(str "SELECT d." % " FROM docs d")]
   ["column alias"            #(str "SELECT 1 AS " %)]
   ["table name"              #(str "SELECT 1 FROM " %)]
   ["schema-qualified table"  #(str "SELECT 1 FROM sch." %)]
   ["insert target"           #(str "INSERT INTO " % " (_id) VALUES (1)")]
   ["insert column list"      #(str "INSERT INTO docs (_id, " % ") VALUES (1, 2)")]
   ["derived col alias"       #(str "SELECT 1 FROM (VALUES (1)) t(" % ")")]
   ["set target"              #(str "UPDATE docs SET " % " = 1")]
   ["cte name"                #(str "WITH " % " AS (SELECT 1 AS a) SELECT * FROM " %)]
   ["window name"             #(str "SELECT 1 FROM docs WINDOW " % " AS (PARTITION BY a)")]
   ["record key"              #(str "SELECT {" % ": 1} AS r")]
   ["field access"            #(str "SELECT d.x." % " FROM docs d")]
   ["exclude"                 #(str "SELECT * EXCLUDE " % " FROM docs")]
   ["rename"                  #(str "SELECT * RENAME a AS " % " FROM docs")]
   ["correlation name"        #(str "SELECT 1 FROM docs AS " %)]
   ["function name"           #(str "SELECT " % "(1) AS r")]
   ["prepared stmt name"      #(str "PREPARE " % " AS SELECT 1")]])

(def ^:private baseline-word "zzz_plain")

;; `avg(1)` fits the aggregate alternative and the generic function call alike, so these are
;; ambiguous as a function name whatever tier they sit in. ANTLR takes the aggregate.
(def ^:private aggregate-names
  #{"AVG" "BOOL_AND" "BOOL_OR" "COUNT" "EVERY" "MAX" "MIN"
    "STDDEV_POP" "STDDEV_SAMP" "SUM" "VAR_POP" "VAR_SAMP"})

(defn- keywords-adding-ambiguity-over [f exempt]
  (into (sorted-map)
        (let [baseline (ambiguities (f baseline-word))]
          (keep (fn [kw]
                  (let [extra (into (sorted-set) (remove baseline) (ambiguities (f kw)))]
                    (when (and (seq extra) (not (exempt kw)))
                      [kw extra])))))
        (keyword-tokens)))

(t/deftest a-keyword-in-a-name-position-is-no-more-ambiguous-than-a-plain-identifier
  (t/is (= {}
           (into (sorted-map)
                 (for [[label f] name-positions
                       :let [extra (keywords-adding-ambiguity-over
                                    f (if (= label "function name") aggregate-names #{}))]
                       :when (seq extra)]
                   [label extra])))))

(t/deftest the-aggregate-names-are-the-only-keywords-ambiguous-as-a-function-name
  (t/is (= aggregate-names
           (into #{} (filter #(seq (ambiguities (str "SELECT " % "(1) AS r")))) (keyword-tokens)))))

(t/deftest the-grammar-s-own-ambiguities-need-no-keyword-to-provoke-them
  (doseq [[sql rule] [["SELECT 1 FROM docs" "tableReference"]
                      ["INSERT INTO docs (_id, foo) VALUES (1, 2)" "insertColumnsAndSource"]
                      ["SELECT d.x.y FROM docs d" "identifierChain"]]]
    (t/is (some #(str/starts-with? % rule) (ambiguities sql)) sql)))

(ns core2.sql.antlr-generator
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as w]
            [core2.sql.antlr :as antlr])
  (:import java.io.File
           org.antlr.v4.Tool
           org.antlr.v4.tool.Grammar
           [org.antlr.v4.runtime CharStreams CommonTokenStream ParserRuleContext Vocabulary]))

(set! *unchecked-math* :warn-on-boxed)

(defn ^org.antlr.v4.tool.Grammar parse-grammar-from-string [^String s]
  (let [tool  (Tool.)
        ast (.parseGrammarFromString tool s)
        grammar (.createGrammar tool ast)]
    (.process tool grammar false)
    grammar))

(defn parse-interpreted
  (^org.antlr.v4.runtime.ParserRuleContext [^Grammar grammar ^String s]
   (parse-interpreted grammar s {}))
  (^org.antlr.v4.runtime.ParserRuleContext [^Grammar grammar ^String s {:keys [start string-ci]}]
   (let [lexer (.createLexerInterpreter grammar (cond-> (CharStreams/fromString s)
                                                  string-ci (antlr/upper-case-char-stream)))
         parser (.createParserInterpreter grammar (CommonTokenStream. lexer))]
     (.parse parser (if start
                      (.index (.getRule grammar (name start)))
                      0)))))

(def sql-spec-grammar-g4 "
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
        ")

(defn parse-sql-spec [sql-spec-string]
  (let [sql-spec-grammar (parse-grammar-from-string sql-spec-grammar-g4)
        rule-names (.getRuleNames sql-spec-grammar)
        vocabulary (.getVocabulary sql-spec-grammar)
        tree (parse-interpreted sql-spec-grammar
                                sql-spec-string
                                {:start "spec"})]
    (antlr/->ast rule-names vocabulary tree)))

(def literal-set
  #{"<signed integer>" "<seconds value>" "<plus sign>" "<simple Latin lower case letter>" "<time literal>" "<white space>" "<equals operator>" "<double period>" "<escaped character>" "<Unicode escape character>" "<Unicode 4 digit escape value>" "<seconds integer value>" "<separator>" "<interval qualifier>" "<hexit>" "<actual identifier>" "<date string>" "<percent>" "<datetime literal>" "<comment character>" "<interval string>" "<bracketed comment introducer>" "<question mark>" "<SQL terminal character>" "<delimited identifier part>" "<left bracket trigraph>" "<minus sign>" "<timestamp literal>" "<left paren>" "<semicolon>" "<timestamp string>" "<interval fractional seconds precision>" "<exponent>" "<unsigned literal>" "<binary string literal>" "<not equals operator>" "<time interval>" "<SQL language identifier start>" "<left bracket>" "<time value>" "<right paren>" "<time string>" "<years value>" "<single datetime field>" "<comment>" "<minutes value>" "<multiplier>" "<ampersand>" "<simple Latin upper case letter>" "<unsigned integer>" "<right brace>" "<approximate numeric literal>" "<interval literal>" "<nonquote character>" "<unqualified schema name>" "<Unicode escape value>" "<primary datetime field>" "<Unicode delimiter body>" "<delimiter token>" "<hours value>" "<regular identifier>" "<time zone interval>" "<implementation-defined character set name>" "<SQL language identifier>" "<newline>" "<Unicode escape specifier>" "<less than operator>" "<signed numeric literal>" "<date value>" "<seconds fraction>" "<SQL language identifier part>" "<identifier start>" "<days value>" "<day-time literal>" "<delimited identifier>" "<Unicode identifier part>" "<identifier extend>" "<solidus>" "<digit>" "<unquoted date string>" "<reserved word>" "<right bracket trigraph>" "<bracketed comment terminator>" "<user-defined character set name>" "<non-second primary datetime field>" "<boolean literal>" "<period>" "<non-reserved word>" "<nondelimiter token>" "<identifier body>" "<asterisk>" "<unsigned numeric literal>" "<simple Latin letter>" "<simple comment introducer>" "<character string literal>" "<Unicode delimited identifier>" "<underscore>" "<months value>" "<double colon>" "<greater than operator>" "<less than or equals operator>" "<comma>" "<doublequote symbol>" "<delimited identifier body>" "<identifier>" "<literal>" "<quote>" "<mantissa>" "<right arrow>" "<SQL special character>" "<reverse solidus>" "<double quote>" "<character set specification>" "<Unicode representation>" "<Unicode character string literal>" "<quote symbol>" "<year-month literal>" "<date literal>" "<Unicode 6 digit escape value>" "<catalog name>" "<non-escaped character>" "<SQL language character>" "<simple comment>" "<sign>" "<Unicode character escape value>" "<circumflex>" "<colon>" "<concatenation operator>" "<start field>" "<right bracket or trigraph>" "<right bracket>" "<token>" "<unquoted timestamp string>" "<national character string literal>" "<named argument assignment token>" "<space>" "<schema name>" "<greater than or equals operator>" "<interval leading field precision>" "<left bracket or trigraph>" "<end field>" "<vertical bar>" "<bracketed comment contents>" "<bracketed comment>" "<left brace>" "<unquoted interval string>" "<nondoublequote character>" "<key word>" "<introducer>" "<standard character set name>" "<character set name>" "<exact numeric literal>" "<character representation>" "<large object length token>" "<identifier part>" "<day-time interval>" "<unquoted time string>" "<datetime value>" "<general literal>"})

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

(def skip-rule-set '#{collection_type
                      array_type
                      multiset_type
                      nonparenthesized_value_expression_primary
                      field_reference
                      attribute_or_method_reference
                      concatenation
                      binary_concatenation
                      interval_value_expression_1
                      interval_term_2
                      array_concatenation
                      array_value_expression_1
                      parenthesized_joined_table
                      joined_table
                      cross_join
                      qualified_join
                      natural_join
                      mutated_target})

(def rule-overrides
  {'data_type
   "predefined_type | row_type | path_resolved_user_defined_type_name | reference_type | data_type 'ARRAY' (LEFT_BRACKET_OR_TRIGRAPH maximum_cardinality RIGHT_BRACKET_OR_TRIGRAPH)? | data_type 'MULTISET'"
   'value_expression_primary
   "parenthesized_value_expression | unsigned_value_specification | column_reference | set_function_specification | window_function | nested_window_function | scalar_subquery | case_expression | cast_specification | value_expression_primary PERIOD field_name | subtype_treatment | value_expression_primary PERIOD method_name sql_argument_list? | generalized_invocation | static_method_invocation | new_specification | value_expression_primary dereference_operator qualified_identifier sql_argument_list? | reference_resolution | collection_value_constructor | value_expression_primary CONCATENATION_OPERATOR array_primary LEFT_BRACKET_OR_TRIGRAPH numeric_value_expression RIGHT_BRACKET_OR_TRIGRAPH | array_value_function LEFT_BRACKET_OR_TRIGRAPH numeric_value_expression RIGHT_BRACKET_OR_TRIGRAPH | value_expression_primary LEFT_BRACKET_OR_TRIGRAPH numeric_value_expression RIGHT_BRACKET_OR_TRIGRAPH | multiset_element_reference | next_value_expression | routine_invocation"
   'character_value_expression
   "character_value_expression CONCATENATION_OPERATOR character_factor | character_factor"
   'binary_value_expression
   "binary_value_expression CONCATENATION_OPERATOR binary_factor | binary_factor"
   'interval_value_expression
   "interval_term | interval_value_expression PLUS_SIGN interval_term_1 | interval_value_expression MINUS_SIGN interval_term_1 | LEFT_PAREN datetime_value_expression MINUS_SIGN datetime_term RIGHT_PAREN INTERVAL_QUALIFIER"
   'interval_term
   "interval_factor | interval_term ASTERISK factor | interval_term SOLIDUS factor | term ASTERISK interval_factor"
   'boolean_predicand
   "parenthesized_boolean_value_expression | value_expression_primary"
   'array_value_expression
   "array_value_expression CONCATENATION_OPERATOR array_primary | array_primary"
   'row_value_special_case
   "value_expression_primary"
   'table_reference
   "table_factor | table_reference 'CROSS' 'JOIN' table_factor | table_reference join_type? 'JOIN' (table_reference | partitioned_join_table) join_specification | partitioned_join_table join_type? 'JOIN' (table_reference | partitioned_join_table) join_specification | table_reference 'NATURAL' join_type? 'JOIN' (table_factor | partitioned_join_table) | partitioned_join_table 'NATURAL' join_type? 'JOIN' (table_factor | partitioned_join_table) | LEFT_PAREN table_reference RIGHT_PAREN"
   'table_primary
   "table_or_query_name query_system_time_period_specification? ('AS'? correlation_name (LEFT_PAREN derived_column_list RIGHT_PAREN)?)? | derived_table 'AS'? correlation_name (LEFT_PAREN derived_column_list RIGHT_PAREN)? | lateral_derived_table 'AS'? correlation_name (LEFT_PAREN derived_column_list RIGHT_PAREN)? | collection_derived_table 'AS'? correlation_name (LEFT_PAREN derived_column_list RIGHT_PAREN)? | table_function_derived_table 'AS'? correlation_name (LEFT_PAREN derived_column_list RIGHT_PAREN)? | only_spec ('AS'? correlation_name (LEFT_PAREN derived_column_list RIGHT_PAREN)?)? | data_change_delta_table ('AS'? correlation_name (LEFT_PAREN derived_column_list RIGHT_PAREN)?)?"})

(def extra-rules "application_time_period_name : IDENTIFIER ;

embedded_variable_name : IDENTIFIER ;

transition_table_name : IDENTIFIER ;
")

(defn sql-spec-ast->antlr-grammar-string [grammar-name sql-ast]
  (-> (with-out-str
        (println "grammar " grammar-name ";")
        (println)
        (doseq [[n _ & body]
                (w/postwalk
                 (fn [x]
                   (if (vector? x)
                     (case (first x)
                       :NAME
                       (let [[_ n] x
                             terminal? (contains? literal-set n)
                             n (subs n 1 (dec (count n)))
                             n (str/replace n #"[ :-]" "_")]
                         (symbol (if terminal?
                                   (str/upper-case n)
                                   (str/lower-case n))))

                       :TOKEN
                       (str "'" (str/replace  (second x) "'" "\\'") "'")

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
                 (vec sql-ast))
                :when (not (contains? skip-rule-set n))]
          (when (contains? fragment-set n)
            (println "fragment"))
          (println n ":")
          (println "    " (or (get rule-overrides n)
                              (str/join
                               " "
                               (w/postwalk
                                (fn [x]
                                  (cond
                                    (string? x)
                                    (symbol x)

                                    (= :SEE_THE_SYNTAX_RULES x)
                                    (symbol (get syntax-rules-overrides n x))

                                    (sequential? x)
                                    (let [x (vec x)]
                                      (cond
                                        (= 1 (count x))
                                        (first x)

                                        (and (= 2 (count x)) (contains? '#{* ? +} (last x)))
                                        (symbol (str/join x))

                                        :else
                                        (seq x)))

                                    :else
                                    x))
                                body))))
          (println)))
      (str/replace " +" "+")
      extra-rules))

(def sql2011-grammar-file (File. (.toURI (io/resource "core2/sql/SQL2011.g"))))
(def sql2011-spec-file (File. (.toURI (io/resource "core2/sql/SQL2011.txt"))))

(defn generate-parser
  ([]
   (generate-parser sql2011-spec-file sql2011-grammar-file))
  ([sql-spec-file antlr-grammar-file]
   (->> (parse-sql-spec (slurp sql-spec-file))
        (sql-spec-ast->antlr-grammar-string "SQL2011")
        (spit antlr-grammar-file))
   (-> (Tool. (into-array ["-package" "core2.sql" "-no-listener" "-no-visitor" (str antlr-grammar-file)]))
       (.processGrammarsOnCommandLine))))

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
        tree (time (parse-interpreted expr-grammar "100+2*34\n"))
        ast (time (antlr/->ast rule-names vocabulary tree))]

    ast)

  (let [parser (core2.expr.ExprParser. (CommonTokenStream. (core2.expr.ExprLexer. (CharStreams/fromString "100+2*34\n"))))
        rule-names (.getRuleNames parser)
        vocabulary (.getVocabulary parser)
        tree (time (.prog parser))
        ast (time (antlr/->ast rule-names vocabulary tree))]
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

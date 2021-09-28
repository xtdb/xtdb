(ns core2.sql.antlr-generator
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
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

spec: (HEADER_COMMENT | definition)* ;
definition: NAME '::=' syntax+ ;
syntax:  (NAME | TOKEN | optional | mandatory | SEE_THE_SYNTAX_RULES) REPEATABLE? choice* ;
optional: '[' syntax+ ']' ;
mandatory: '{' syntax+ '}' ;
choice: ('|' syntax) ;
REPEATABLE: '...' ;
SEE_THE_SYNTAX_RULES: '!!' .*? '\\n' ;
NAME: '<' [-_:/a-zA-Z 0-9]+ '>' ;
TOKEN: ~[ \\n\\r\\t.!]+ ;
WS: [ \\n\\r\\t]+ -> skip ;
HEADER_COMMENT: '//' [ ]* [0-9] .*? '\\n' ;
COMMENT: '//' .*? '\\n' -> skip ;
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
  #{"<seconds value>" "<plus sign>" "<simple Latin lower case letter>" "<character string literal>" "<Unicode character string literal>" "<time literal>" "<white space>" "<equals operator>" "<double period>" "<escaped character>" "<Unicode escape character>" "<Unicode 4 digit escape value>" "<seconds integer value>" "<separator>" "<interval qualifier>" "<hexit>" "<date string>" "<percent>" "<comment character>" "<interval string>" "<bracketed comment introducer>" "<question mark>" "<delimited identifier part>" "<left bracket trigraph>" "<minus sign>" "<timestamp literal>" "<left paren>" "<semicolon>" "<timestamp string>" "<interval fractional seconds precision>" "<binary string literal>" "<not equals operator>" "<time interval>" "<left bracket>" "<time value>" "<right paren>" "<time string>" "<years value>" "<single datetime field>" "<comment>" "<minutes value>" "<multiplier>" "<ampersand>" "<simple Latin upper case letter>" "<unsigned integer>" "<right brace>" "<interval literal>" "<nonquote character>" "<Unicode escape value>" "<primary datetime field>" "<Unicode delimited identifier>"  "<Unicode delimiter body>" "<hours value>" "<time zone interval>" "<newline>" "<Unicode escape specifier>" "<less than operator>" "<date value>" "<seconds fraction>" "<identifier start>" "<days value>" "<day-time literal>" "<delimited identifier>" "<Unicode identifier part>" "<identifier extend>" "<solidus>" "<digit>" "<unquoted date string>" "<right bracket trigraph>" "<bracketed comment terminator>" "<non-second primary datetime field>" "<boolean literal>" "<period>" "<nondelimiter token>" "<identifier body>" "<asterisk>" "<simple Latin letter>" "<simple comment introducer>" "<underscore>" "<months value>" "<double colon>" "<greater than operator>" "<less than or equals operator>" "<comma>" "<doublequote symbol>" "<delimited identifier body>" "<quote>" "<right arrow>" "<reverse solidus>" "<double quote>" "<Unicode representation>" "<quote symbol>" "<year-month literal>" "<date literal>" "<Unicode 6 digit escape value>" "<non-escaped character>" "<simple comment>" "<sign>" "<Unicode character escape value>" "<circumflex>" "<colon>" "<concatenation operator>" "<start field>" "<right bracket or trigraph>" "<right bracket>" "<token>" "<unquoted timestamp string>" "<named argument assignment token>" "<space>" "<greater than or equals operator>" "<interval leading field precision>" "<left bracket or trigraph>" "<end field>" "<vertical bar>" "<bracketed comment contents>" "<bracketed comment>" "<left brace>" "<unquoted interval string>" "<nondoublequote character>" "<introducer>" "<character representation>" "<large object length token>" "<identifier part>" "<day-time interval>" "<unquoted time string>" "<datetime value>"})

(def fragment-set
  #{"<simple Latin lower case letter>" "<Unicode escape character>" "<Unicode 4 digit escape value>" "<hexit>" "<comment character>" "<bracketed comment introducer>" "<delimited identifier part>" "<nondoublequote character>" "<doublequote symbol>" "<simple Latin upper case letter>" "<character representation>" "<Unicode escape value>" "<Unicode delimiter body>" "<identifier start>" "<Unicode identifier part>" "<identifier extend>" "<digit>" "<bracketed comment terminator>" "<simple Latin letter>" "<simple comment introducer>" "<delimited identifier body>" "<Unicode 6 digit escape value>" "<simple comment>" "<Unicode representation>" "<Unicode escape specifier>" "<Unicode character escape value>" "<bracketed comment contents>" "<bracketed comment>" "<identifier part>" "<nonquote character>" "<quote symbol>" "<date string>" "<time string>" "<timestamp string>" "<time zone interval>" "<date value>" "<time value>" "<interval string>" "<unquoted date string>" "<unquoted time string>""<unquoted timestamp string>" "<unquoted interval string>" "<year-month literal>" "<day-time literal>" "<day-time interval>" "<time interval>" "<years value>" "<months value>" "<days value>" "<hours value>" "<minutes value>" "<seconds value>" "<seconds integer value>" "<seconds fraction>" "<datetime value>" "<space>" "<newline>"})

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
   'IDENTIFIER_EXTEND "SIMPLE_LATIN_LETTER
    | DIGIT
    | UNDERSCORE"
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

;; NOTE: A rule must exist to be overridden and cannot be commented
;; out. This is to ensure the override ends up in the right place in
;; the grammar.
(def rule-overrides
  {'SEPARATOR
   "(COMMENT | WHITE_SPACE)+ -> skip"
   'COMMENT
   "(SIMPLE_COMMENT | BRACKETED_COMMENT) -> skip"
   'CHARACTER_STRING_LITERAL
   "QUOTE CHARACTER_REPRESENTATION* QUOTE (SEPARATOR QUOTE CHARACTER_REPRESENTATION* QUOTE)*"
   'UNICODE_CHARACTER_STRING_LITERAL
   "'U' AMPERSAND QUOTE UNICODE_REPRESENTATION* QUOTE (SEPARATOR QUOTE UNICODE_REPRESENTATION* QUOTE)* UNICODE_ESCAPE_SPECIFIER"
   'DATE_LITERAL
   "'DATE' SPACE* DATE_STRING"
   'TIME_LITERAL
   "'TIME' SPACE* TIME_STRING"
   'TIMESTAMP_LITERAL
   "'TIMESTAMP' SPACE* TIMESTAMP_STRING"
   'INTERVAL_LITERAL
   "'INTERVAL' SPACE* SIGN? SPACE* INTERVAL_STRING SPACE* INTERVAL_QUALIFIER"
   'character_set_name
   "(schema_name PERIOD)? identifier"
   'data_type
   "predefined_type
    | row_type
    | path_resolved_user_defined_type_name
    | reference_type
    | data_type 'ARRAY' (LEFT_BRACKET_OR_TRIGRAPH maximum_cardinality RIGHT_BRACKET_OR_TRIGRAPH)?
    | data_type 'MULTISET'"
   'value_expression_primary
   "parenthesized_value_expression
    | unsigned_value_specification
    | column_reference
    | set_function_specification
    | window_function
    | nested_window_function
    | scalar_subquery
    | case_expression
    | cast_specification
    | value_expression_primary PERIOD field_name
    | subtype_treatment
    | value_expression_primary PERIOD method_name sql_argument_list?
    | generalized_invocation
    | static_method_invocation
    | new_specification
    | value_expression_primary dereference_operator qualified_identifier sql_argument_list?
    | reference_resolution
    | collection_value_constructor
    | value_expression_primary CONCATENATION_OPERATOR array_primary LEFT_BRACKET_OR_TRIGRAPH numeric_value_expression RIGHT_BRACKET_OR_TRIGRAPH
    | array_value_function LEFT_BRACKET_OR_TRIGRAPH numeric_value_expression RIGHT_BRACKET_OR_TRIGRAPH
    | value_expression_primary LEFT_BRACKET_OR_TRIGRAPH numeric_value_expression RIGHT_BRACKET_OR_TRIGRAPH
    | multiset_element_reference
    | next_value_expression
    | routine_invocation"
   'numeric_value_function
   "position_expression
    | regex_occurrences_function
    | regex_position_expression
    | extract_expression
    | length_expression
    | cardinality_expression
    | max_cardinality_expression
    | absolute_value_expression
    | modulus_expression
    | trigonometric_function
    | general_logarithm_function
    | common_logarithm
    | natural_logarithm
    | exponential_function
    | power_function
    | square_root
    | floor_function
    | ceiling_function
    | width_bucket_function"
   'character_value_expression
   "character_value_expression CONCATENATION_OPERATOR character_factor
    | character_factor"
   'binary_value_expression
   "binary_value_expression CONCATENATION_OPERATOR binary_factor
    | binary_factor"
   'interval_value_expression
   "interval_term
    | interval_value_expression PLUS_SIGN interval_term_1
    | interval_value_expression MINUS_SIGN interval_term_1
    | LEFT_PAREN datetime_value_expression MINUS_SIGN datetime_term RIGHT_PAREN INTERVAL_QUALIFIER"
   'interval_term
   "interval_factor
    | interval_term ASTERISK factor
    | interval_term SOLIDUS factor
    | term ASTERISK interval_factor"
   'boolean_predicand
   "parenthesized_boolean_value_expression
    | value_expression_primary"
   'array_value_expression
   "array_value_expression CONCATENATION_OPERATOR array_primary
    | array_primary"
   'row_value_special_case
   "value_expression_primary"
   'table_reference
   "table_factor
    | table_reference 'CROSS' 'JOIN' table_factor
    | table_reference join_type? 'JOIN' (table_reference | partitioned_join_table) join_specification
    | partitioned_join_table join_type? 'JOIN' (table_reference | partitioned_join_table) join_specification
    | table_reference 'NATURAL' join_type? 'JOIN' (table_factor | partitioned_join_table)
    | partitioned_join_table 'NATURAL' join_type? 'JOIN' (table_factor | partitioned_join_table)
    | LEFT_PAREN table_reference RIGHT_PAREN"
   'table_primary
   "table_or_query_name query_system_time_period_specification? ('AS'? correlation_name (LEFT_PAREN derived_column_list RIGHT_PAREN)?)?
    | derived_table 'AS'? correlation_name (LEFT_PAREN derived_column_list RIGHT_PAREN)?
    | lateral_derived_table 'AS'? correlation_name (LEFT_PAREN derived_column_list RIGHT_PAREN)?
    | collection_derived_table 'AS'? correlation_name (LEFT_PAREN derived_column_list RIGHT_PAREN)?
    | table_function_derived_table 'AS'? correlation_name (LEFT_PAREN derived_column_list RIGHT_PAREN)?
    | only_spec ('AS'? correlation_name (LEFT_PAREN derived_column_list RIGHT_PAREN)?)?
    | data_change_delta_table ('AS'? correlation_name (LEFT_PAREN derived_column_list RIGHT_PAREN)?)?"
   'mutated_set_clause
   "object_column PERIOD method_name
    | mutated_set_clause PERIOD method_name"
   'start_transaction_statement
   "'START' 'TRANSACTION' (transaction_mode (COMMA transaction_mode)*)?"})

(def extra-rules "// SQL:2011 needed definitions in parts not generated.

// 11 Schema definition and manipulation

// 11.3 <table definition>

application_time_period_name
    : identifier
    ;

// 21 Embedded SQL

// 21.1 <embedded SQL host program>

embedded_variable_name
    : COLON identifier
    ;

// SQL:2016 6.30 <numeric value function>

trigonometric_function
    : trigonometric_function_name LEFT_PAREN numeric_value_expression RIGHT_PAREN
    ;

trigonometric_function_name
    : 'SIN'
    | 'COS'
    | 'TAN'
    | 'SINH'
    | 'COSH'
    | 'TANH'
    | 'ASIN'
    | 'ACOS'
    | 'ATAN'
    ;

general_logarithm_function
    : 'LOG' LEFT_PAREN general_logarithm_base COMMA general_logarithm_argument RIGHT_PAREN
    ;

general_logarithm_base
    : numeric_value_expression
    ;

general_logarithm_argument
    : numeric_value_expression
    ;

common_logarithm
    : 'LOG10' LEFT_PAREN numeric_value_expression RIGHT_PAREN
    ;")

(def ^:private ^:dynamic *sql-ast-print-nesting* 0)
(def ^:private ^:dynamic *sql-ast-current-name*)
(def ^:private sql-print-indent "    ")

(defmulti print-sql-ast first)

(defn print-sql-ast-list [xs]
  (loop [[x & [next-x :as xs]] xs]
    (when x
      (print-sql-ast x)
      (when (and next-x (not= [:REPEATABLE "..."] next-x))
        (print " "))
      (recur xs))))

(defmethod print-sql-ast :spec [[_ & xs]]
  (doseq [x xs]
    (print-sql-ast x)))

(defmethod print-sql-ast :HEADER_COMMENT [[_ x]]
  (println)
  (println (str/trim x)))

(defmethod print-sql-ast :SEE_THE_SYNTAX_RULES [[_ x]]
  (print (get syntax-rules-overrides *sql-ast-current-name*)))

(defmethod print-sql-ast :TOKEN [[_ x]]
  (print (str "'" x "'")))

(defmethod print-sql-ast :NAME [[_ x]]
  (let [x (if (contains? literal-set x)
            (str/upper-case x)
            (str/lower-case x))
        x (subs x 1 (dec (count x)))
        x (str/replace x #"[-: ]" "_")]
    (print x)))

(defmethod print-sql-ast :REPEATABLE [[_ x]]
  (print "+"))

(defmethod print-sql-ast :choice [[_ _ x]]
  (if (pos? (long *sql-ast-print-nesting*))
    (do (print "| ")
        (print-sql-ast x))
    (do (println)
        (print (str sql-print-indent "| "))
        (print-sql-ast x))))

(defmethod print-sql-ast :optional [[_ _ & xs]]
  (binding [*sql-ast-print-nesting* (inc (long *sql-ast-print-nesting*))]
    (let [xs (butlast xs)
          single? (= 1 (count xs))]
      (when-not single?
        (print "("))
      (print-sql-ast-list xs)
      (if single?
        (print "?")
        (print ")?")))))

(defmethod print-sql-ast :mandatory [[_ _ & xs]]
  (binding [*sql-ast-print-nesting* (inc (long *sql-ast-print-nesting*))]
    (let [xs (butlast xs)]
      (print "(")
      (print-sql-ast-list xs)
      (print ")"))))

(defmethod print-sql-ast :syntax [[_ & xs]]
  (print-sql-ast-list xs))

(defmethod print-sql-ast :definition [[_ n _ & xs]]
  (let [fragment? (contains? fragment-set (second n))
        n (symbol (with-out-str
                    (print-sql-ast n)))]
    (println)
    (when fragment?
      (println "fragment"))
    (println n)
    (print sql-print-indent)
    (print ": ")
    (if-let [override (get rule-overrides n)]
      (do (println override)
          (print sql-print-indent)
          (println ";"))
      (binding [*sql-ast-current-name* n]
        (print-sql-ast-list xs)
        (println)
        (print sql-print-indent)
        (println ";")))))

(defn sql-spec-ast->antlr-grammar-string [grammar-name sql-ast]
  (str/replace
   (->> (with-out-str
          (println (str "grammar " grammar-name ";"))
          (println)
          (print-sql-ast sql-ast)
          (println)
          (println extra-rules))
        (str/split-lines)
        (map str/trimr)
        (str/join "\n"))
   "+?"
   "*"))

(def sql2011-grammar-file (File. (.toURI (io/resource "core2/sql/SQL2011.g"))))
(def sql2011-spec-file (File. (.toURI (io/resource "core2/sql/SQL2011.txt"))))

(defn generate-parser [grammar-name sql-spec-file antlr-grammar-file]
  (->> (parse-sql-spec (slurp sql-spec-file))
       (sql-spec-ast->antlr-grammar-string grammar-name)
       (spit antlr-grammar-file))
  (-> (Tool. (into-array ["-package" "core2.sql" "-no-listener" "-no-visitor" (str antlr-grammar-file)]))
      (.processGrammarsOnCommandLine)))

(defn -main [& args]
  (generate-parser "SQL2011" sql2011-spec-file sql2011-grammar-file))

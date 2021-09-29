(ns core2.sql.insta-generator
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [instaparse.core :as insta])
  (:import java.io.File))

(set! *unchecked-math* :warn-on-boxed)

(def sql-spec-grammar-g4 "
spec: (HEADER_COMMENT / definition)* ;
definition: NAME '::=' syntax ;
syntax_element: (optional / mandatory / NAME / SEE_THE_SYNTAX_RULES / !'::=' TOKEN) REPEATABLE? ;
syntax: syntax_element+ choice* ;
optional: '[' syntax+ ']' ;
mandatory: '{' syntax+ '}' ;
choice: '|' syntax_element+ ;
REPEATABLE: '...' ;
SEE_THE_SYNTAX_RULES: #'!!.*?\\n' ;
NAME: #'<[-_:/a-zA-Z 0-9]+?>' ;
TOKEN: #'[^ |\\n\\r\\t.!/]+' ;
HEADER_COMMENT: #'// *\\d.*?\\n' ;
        ")

(def parse-sql-spec
  (insta/parser sql-spec-grammar-g4
                :auto-whitespace (insta/parser "
whitespace: (#'\\s*//.*?\\n\\s*' | #'\\s*')+")))

(def syntax-rules-overrides
  {'space "' '"
   'quote "'\\''"
   'period "'.'"
   'solidus "'/'"
   'reverse_solidus "'\\\\'"
   'left_bracket "'['"
   'right_bracket "']'"
   'vertical_bar "'|'"
   'concatenation_operator "'||'"
   'left_brace "'{'"
   'right_brace "'}'"
   'identifier_start "simple_latin_letter"
   'identifier_extend "simple_latin_letter
    | digit
    | underscore"
   'unicode_escape_character "'\\\\'"
   'nondoublequote_character "#'[^\"]'"
   'doublequote_symbol ""
   'double_period "'..'"
   'nonquote_character "#'[^\\']'"
   'non_escaped_character "#'.'"
   'escaped_character "#'\\\\.'"})

;; NOTE: A rule must exist to be overridden and cannot be commented
;; out. This is to ensure the override ends up in the right place in
;; the grammar.
(def rule-overrides
  {'separator
   "EPSILON"
   'character_string_literal
   "quote character_representation* quote (separator quote character_representation* quote)*"
   'unicode_character_string_literal
   "'U' ampersand quote unicode_representation* quote (separator quote unicode_representation* quote)* unicode_escape_specifier"
   'character_set_name
   "(schema_name period)? identifier"
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
    | width_bucket_function"})

(def extra-rules "(* SQL:2011 needed definitions in parts not generated. *)

(* 11 Schema definition and manipulation *)

(* 11.3 <table definition> *)

application_time_period_name
    : identifier
    ;

(* 21 Embedded SQL *)

(* 21.1 <embedded SQL host program> *)

embedded_variable_name
    : colon identifier
    ;

(* SQL:2016 6.30 <numeric value function> *)

trigonometric_function
    : trigonometric_function_name left_paren numeric_value_expression right_paren
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
    : 'LOG' left_paren general_logarithm_base comma general_logarithm_argument right_paren
    ;

general_logarithm_base
    : numeric_value_expression
    ;

general_logarithm_argument
    : numeric_value_expression
    ;

common_logarithm
    : 'LOG10' left_paren numeric_value_expression right_paren
    ;")

(def ^:private ^:dynamic *sql-ast-print-nesting* 0)
(def ^:private ^:dynamic *sql-ast-current-name*)
(def ^:private sql-print-indent "    ")

(defmulti print-sql-ast first)

(defn print-sql-ast-list [xs]
  (loop [[x & [next-x :as xs]] xs]
    (when x
      (print-sql-ast x)
      (when next-x
        (print " "))
      (recur xs))))

(defmethod print-sql-ast :spec [[_ & xs]]
  (doseq [x xs]
    (print-sql-ast x)))

(defmethod print-sql-ast :HEADER_COMMENT [[_ x]]
  (println)
  (println "(*" (str/replace (str/trim x) #"^//" "") "*)"))

(defmethod print-sql-ast :COMMENT [[_ x]])

(defmethod print-sql-ast :SEE_THE_SYNTAX_RULES [[_ x]]
  (print (get syntax-rules-overrides *sql-ast-current-name*)))

(defmethod print-sql-ast :TOKEN [[_ x]]
  (print (str "'" x "'")))

(defmethod print-sql-ast :NAME [[_ x]]
  (let [x (str/lower-case x)
        x (subs x 1 (dec (count x)))
        x (str/replace x #"[-: ]" "_")]
    (print x)))

(defmethod print-sql-ast :REPEATABLE [[_ x]]
  (print "+"))

(defmethod print-sql-ast :choice [[_ _ & xs]]
  (if (pos? (long *sql-ast-print-nesting*))
    (do (print "| ")
        (print-sql-ast-list xs))
    (do (println)
        (print (str sql-print-indent "| "))
        (print-sql-ast-list xs))))

(defmethod print-sql-ast :optional [[_ _ & xs]]
  (binding [*sql-ast-print-nesting* (inc (long *sql-ast-print-nesting*))]
    (let [xs (butlast xs)]
      (print "[ ")
      (print-sql-ast-list xs)
      (print " ]"))))

(defmethod print-sql-ast :mandatory [[_ _ & xs]]
  (binding [*sql-ast-print-nesting* (inc (long *sql-ast-print-nesting*))]
    (let [xs (butlast xs)]
      (print "( ")
      (print-sql-ast-list xs)
      (print " )"))))

(defmethod print-sql-ast :syntax_element [[_ x repeatable?]]
  (print-sql-ast x)
  (when repeatable?
    (print-sql-ast repeatable?)))

(defmethod print-sql-ast :syntax [[_ & xs]]
  (print-sql-ast-list xs))

(defmethod print-sql-ast :definition [[_ n _ & xs]]
  (let [n (symbol (with-out-str
                    (print-sql-ast n)))]
    (println)
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

(defn sql-spec-ast->ebnf-grammar-string [_ sql-ast]
  (->> (with-out-str
         (print-sql-ast sql-ast)
         (println)
         (println extra-rules))
       (str/split-lines)
       (map str/trimr)
       (str/join "\n")))

(def sql2011-grammar-file (File. (.toURI (io/resource "core2/sql/SQL2011.ebnf"))))
(def sql2011-spec-file (File. (.toURI (io/resource "core2/sql/SQL2011_insta.txt"))))

(defn generate-parser [grammar-name sql-spec-file ebnf-grammar-file]
  (->> (parse-sql-spec (slurp sql-spec-file))
       (sql-spec-ast->ebnf-grammar-string grammar-name)
       (spit ebnf-grammar-file)))

(defn -main [& args]
  (generate-parser "SQL2011" sql2011-spec-file sql2011-grammar-file))

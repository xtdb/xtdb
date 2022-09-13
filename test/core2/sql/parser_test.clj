(ns core2.sql.parser-test
  (:require [clojure.test :as t]
            [core2.sql.parser :as p]))

(t/deftest test-sql-comments
  (t/is (= " -- single line comment" (re-find p/sql-single-line-comment-pattern "'1\n2' -- single line comment")))
  (t/is (= " -- single line comment" (re-find p/sql-single-line-comment-pattern " -- single line comment\n '1\n2'")))
  (t/is (= "/* multi line\ncomment */" (re-find p/sql-multi-line-comment-pattern "/* multi line\ncomment */ '1\n2'")))
  (t/is (= "/* multi line\ncomment */" (re-find p/sql-multi-line-comment-pattern "'1\n2' /* multi line\ncomment */ ")))
  (t/is (= "/* multi line\ncomment" (re-find p/sql-multi-line-comment-pattern "'1\n2' /* multi line\ncomment")))

  (let [expected [:character_string_literal "'1\n2'"]]
    (t/is (= expected (p/sql-parser "'1\n2'" :value_expression_primary)))
    (t/is (= expected (p/sql-parser "\n '1\n2'\n  " :value_expression_primary)))
    (t/is (= expected (p/sql-parser "'1\n2' -- single line comment \n" :value_expression_primary)))
    (t/is (= expected (p/sql-parser "'1\n2' -- single line comment" :value_expression_primary)))
    (t/is (= expected (p/sql-parser "-- single line comment\n '1\n2'" :value_expression_primary)))
    (t/is (= expected (p/sql-parser " '1\n2' /* multi line\ncomment */" :value_expression_primary)))
    (t/is (= expected (p/sql-parser "\n '1\n2' \n /* multi line\ncomment */" :value_expression_primary)))
    (t/is (= expected (p/sql-parser "'1\n2' \n /* multi line\ncomment" :value_expression_primary)))
    (t/is (= expected (p/sql-parser "  /* multi line\ncomment */ '1\n2'" :value_expression_primary)))))

(t/deftest test-sql-punctuation
  (t/is (= 1 (.end (doto (re-matcher p/sql-after-punctuation-pattern ") ")
                     (.find)))))
  (t/is (= 1 (.end (doto (re-matcher p/sql-word-boundary-pattern "-a")
                     (.find)))))

  (t/is (= [:factor [:minus_sign "-"] [:factor [:minus_sign "-"] [:exact_numeric_literal "2"]]]
           (p/sql-parser "- - 2" :factor)))

  (t/is (p/failure? (p/sql-parser "SELECT * FROMfoo" :directly_executable_statement)))
  (t/is (p/failure? (p/sql-parser "SELECT * FROM fooWHERE x.foo = 1 " :directly_executable_statement)))

  ;; TODO: what should really happen here?
  (t/is (not (p/failure? (p/sql-parser "SELECT * FROM fooASbar" :directly_executable_statement)))))

(t/deftest test-sql-multiple-statements
  (let [sql-statements "-- this is my file

SELECT 2 FROM x; -- with a trailing comment

/* with a multi-line

comment

*/

INSERT INTO foo (a, b) VALUES /* and a comment inline */ (1, 2);

-- SELECT 1 FROM x; -- commented out line

/* SELECT 'foo

bar'

FROM x;
*/

SELECT 'foo
bar' FROM x;

"
        actual (p/sql-parser sql-statements  :direct_sql_statements)]
    (t/is (= [:query_expression
              :insert_statement
              :query_expression]
             (for [[_ [_ [x]]] (rest actual)]
               x)))))

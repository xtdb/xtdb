(ns core2.sql.logic-test.direct-sql-test
  (:require [core2.sql.logic-test.runner :as slt]))

(defmacro def-slt-test-variations
  [& args]
  `(do
     ~@(mapcat
         (fn [file]
           [(list 'slt/def-slt-test file {:direct-sql true})
            (list 'slt/def-slt-test file {:direct-sql true :decorrelate? false} (symbol (str file "-correlated")))]) args)))

(def-slt-test-variations
  direct-sql--dml
  direct-sql--gcse-statistics
  direct-sql--numeric-value-functions-6.28
  direct-sql--period_predicates
  direct-sql--remove-names
  direct-sql--set-functions
  direct-sql--system_time
  direct-sql--period_specifications
  direct-sql--periods-and-derived-cols
  direct-sql--object-array
  direct-sql--limit
  direct-sql--arrow-table
  direct-sql--sl-a5
  direct-sql--slt-variables
  direct-sql--sl-demo
  direct-sql--qualified_joins
  direct-sql--identifier-case-sensitivity)

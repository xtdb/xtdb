(ns core2.sql.logic-test.direct-sql-test
  (:require [core2.sql.logic-test.runner :as slt]))

(slt/def-slt-test direct-sql--dml {:direct-sql true})
(slt/def-slt-test direct-sql--gcse-statistics {:direct-sql true})
(slt/def-slt-test direct-sql--numeric-value-functions-6.28 {:direct-sql true})
(slt/def-slt-test direct-sql--period_predicates {:direct-sql true})
(slt/def-slt-test direct-sql--remove-names {:direct-sql true})
(slt/def-slt-test direct-sql--set-functions {:direct-sql true})
(slt/def-slt-test direct-sql--system_time {:direct-sql true})
(slt/def-slt-test direct-sql--period_specifications {:direct-sql true})
(slt/def-slt-test direct-sql--periods-and-derived-cols {:direct-sql true})
(slt/def-slt-test direct-sql--object-array {:direct-sql true})
(slt/def-slt-test direct-sql--limit {:direct-sql true})

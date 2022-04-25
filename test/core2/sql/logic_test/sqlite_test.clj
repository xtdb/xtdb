(ns core2.sql.logic-test.sqlite-test
  (:require [clojure.test :as t]
            [core2.sql.logic-test.runner :as slt]
            [core2.test-util :as tu]))

(t/use-fixtures :each tu/with-node slt/with-xtdb)

(slt/deftest select1)

(slt/deftest select2)

(slt/deftest select3)

(slt/deftest select4)

(slt/deftest select5)

;; TODO: the entire way we specify and refer to these needs work, but
;; this makes these tests possible to run.

(slt/deftest random-aggregates-slt_good_0)

(slt/deftest random-expr-slt_good_0)

(slt/deftest random-groupby-slt_good_0)

(slt/deftest random-select-slt_good_0)

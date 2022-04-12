(ns core2.sql.logic-test.sqlite-test
  (:require [clojure.test :as t]
            [core2.sql.logic-test.runner :as slt]
            [core2.sql.logic-test.xtdb-engine]
            [core2.test-util :as tu]))

(t/use-fixtures :each tu/with-node)

(slt/deftest select1)

(slt/deftest select2)

(slt/deftest select3)

(slt/deftest select4)

(slt/deftest select5)

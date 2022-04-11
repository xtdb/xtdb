(ns core2.sql.logic-test.sqlite-test
  (:require [clojure.test :as t]
            [core2.sql.logic-test.runner :as slt]
            [core2.sql.logic-test.xtdb-engine]
            [core2.test-util :as tu]))

(t/use-fixtures :each tu/with-node)

(slt/generate-ns-slt-tests!)

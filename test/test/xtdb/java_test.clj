(ns xtdb.java-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.fixtures :as fix :refer [*api*]])
  (:import java.util.HashMap))

(t/use-fixtures :each fix/with-node)

(t/deftest test-java-types
  (t/testing "Can use Java List/Map types when using submit-tx"
    (fix/submit+await-tx (fix/vec->array-list
                          [(fix/vec->array-list
                            [::xt/put
                             (HashMap. {:xt/id :test})])]))

    (t/is (= {:xt/id :test} (xt/entity (xt/db *api*) :test)))

    (fix/submit+await-tx [[::xt/put {:xt/id :test-java
                                     :list (fix/vec->array-list [1 2 3])
                                     :map (HashMap. {:foo "bar"})}]
                          [::xt/put {:xt/id :test-clj
                                     :list [1 2 3]
                                     :map {:foo "bar"}}]])

    #_ ; FIXME #1684
    (t/is (= #{[:test-java] [:test-clj]}
             (xt/q (xt/db *api*)
                   '{:find [?e], :where [[?e :list 2]]})))

    (t/is (= #{[:test-java] [:test-clj]}
             (xt/q (xt/db *api*)
                   '{:find [?e], :where [[?e :map {:foo "bar"}]]})))))

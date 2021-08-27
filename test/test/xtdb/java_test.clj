(ns xtdb.java-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.fixtures :as fix :refer [*api*]])
  (:import java.util.HashMap))

(t/use-fixtures :each fix/with-node)

(t/deftest test-java-types
  (t/testing "Can use Java List/Map types when using submit-tx"
    (t/is (xt/submit-tx *api* [[::xt/put (HashMap. {:xt/id :test})]]))

    (t/is (xt/submit-tx *api* (fix/vec->array-list [[::xt/put {:xt/id :test}]])))

    (t/is (xt/submit-tx *api* (fix/vec->array-list
                                 [[::xt/put
                                   (HashMap. {:xt/id :test})]])))

    (t/is (xt/submit-tx *api* (fix/vec->array-list
                                 [(fix/vec->array-list
                                   [::xt/put {:xt/id :test2}])])))

    (t/is (xt/submit-tx *api* (fix/vec->array-list
                                 [(fix/vec->array-list
                                   [::xt/put
                                    (HashMap. {:xt/id :test2})])])))))

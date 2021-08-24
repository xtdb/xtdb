(ns crux.java-test
  (:require [clojure.test :as t]
            [crux.api :as crux]
            [crux.fixtures :as fix :refer [*api*]])
  (:import java.util.HashMap))

(t/use-fixtures :each fix/with-node)

(t/deftest test-java-types
  (t/testing "Can use Java List/Map types when using submit-tx"
    (t/is (crux/submit-tx *api* [[:crux.tx/put (HashMap. {:xt/id :test})]]))

    (t/is (crux/submit-tx *api* (fix/vec->array-list [[:crux.tx/put {:xt/id :test}]])))

    (t/is (crux/submit-tx *api* (fix/vec->array-list
                                 [[:crux.tx/put
                                   (HashMap. {:xt/id :test})]])))

    (t/is (crux/submit-tx *api* (fix/vec->array-list
                                 [(fix/vec->array-list
                                   [:crux.tx/put {:xt/id :test2}])])))

    (t/is (crux/submit-tx *api* (fix/vec->array-list
                                 [(fix/vec->array-list
                                   [:crux.tx/put
                                    (HashMap. {:xt/id :test2})])])))))

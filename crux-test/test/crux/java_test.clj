(ns crux.java-test
  (:require [clojure.test :as t]
            [crux.api :as crux]
            [crux.fixtures.standalone :as fs]
            [crux.fixtures.api :as apif :refer [*api*]]))


(t/use-fixtures :each fs/with-standalone-node apif/with-node)

(t/deftest test-java-types
  (t/testing "Can use Java List/Map types when using submit-tx"
    (t/is (crux/submit-tx *api* [[:crux.tx/put (java.util.HashMap. {:crux.db/id :test})]]))

    (t/is (crux/submit-tx *api* (java.util.ArrayList. [[:crux.tx/put {:crux.db/id :test}]])))

    (t/is (crux/submit-tx *api* (java.util.ArrayList. [[:crux.tx/put
                                                        (java.util.HashMap. {:crux.db/id :test})]])))

    (t/is (crux/submit-tx *api* (java.util.ArrayList. [(java.util.ArrayList.
                                                        [:crux.tx/put {:crux.db/id :test2}])])))

    (t/is (crux/submit-tx *api* (java.util.ArrayList. [(java.util.ArrayList.
                                                        [:crux.tx/put
                                                         (java.util.HashMap. {:crux.db/id :test2})])])))))

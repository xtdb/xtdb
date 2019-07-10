(ns crux.jdbc.jdbc-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [crux.fixtures.api :refer [*api*]]
            [crux.jdbc.fixtures.jdbc :as fj]))

(t/use-fixtures :each fj/with-jdbc-system)

(t/deftest test-happy-path-jdbc-event-log
  (let [doc {:crux.db/id :origin-man :name "Adam"}
        submitted-tx (.submitTx *api* [[:crux.tx/put doc]])]
    (.sync *api* (:crux.tx/tx-time submitted-tx) nil)
    (t/is (.entity (.db *api*) :origin-man))))

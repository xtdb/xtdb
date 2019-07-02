(ns crux.http-test
  (:require  [clojure.test :as t]
             [crux.fixtures.api :refer [*api*]]
             [crux.fixtures.standalone :as fs]
             [crux.fixtures.http-server :as fh]))

(t/use-fixtures :each fs/with-standalone-system fh/with-http-server)

(t/deftest test-can-write-and-retrieve-entity-via-http
  (let [id #crux/id :https://adam.com
        doc {:crux.db/id id, :name "Adam"}
        submitted-tx (.submitTx *api* [[:crux.tx/put doc]])]
    (.sync *api* (:crux.tx/tx-time submitted-tx) nil)
    (t/is (.entity (.db *api*) id))))

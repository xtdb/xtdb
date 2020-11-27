(ns crux.lucene-multi-field-test
  (:require [clojure.test :as t]
            [crux.api :as c]
            [crux.db :as db]
            [crux.fixtures :as fix :refer [*api* submit+await-tx]]
            [crux.fixtures.lucene :as lf]
            [crux.lucene :as l]))

(defn- index-docs! [document-store lucene-store docs]
  (l/index-docs! document-store lucene-store docs))

(t/use-fixtures :each (lf/with-lucene-multi-docs-module index-docs!) fix/with-node)

(t/deftest test-sanity-check
  (submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]])
  (with-open [db (c/open-db *api*)]
    (t/is (seq (c/q db {:find '[?e ?v]
                        :where '[[(text-search :name "Ivan") [[?e ?v]]]
                                 [?e :crux.db/id]]})))))

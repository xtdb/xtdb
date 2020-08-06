(ns crux.current-queries-test
  (:require [clojure.test :as t]
            [crux.api :as api]
            [crux.fixtures :as fix :refer [*api*]]
            [crux.fixtures.every-api :as every-api]))

(t/use-fixtures :once every-api/with-embedded-kafka-cluster)
(t/use-fixtures :each (partial fix/with-opts {:crux.bus/sync? true}) every-api/with-each-api-implementation)

(t/deftest test-current-queries
  (let [submitted-tx (api/submit-tx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]])]
    (t/is (= submitted-tx (api/await-tx *api* submitted-tx)))
    (t/is (true? (api/tx-committed? *api* submitted-tx)))
    (let [db (api/db *api*)]
      (t/is (api/q (api/db *api*)
                   '{:find [e]
                     :where [[e :name "Ivan"]]}))

      (t/testing "test current-query - post successful query"
        (let [current-queries (api/current-queries *api*)]
          (t/is (= :completed (:status (first current-queries))))))

      (t/is (thrown-with-msg? Exception
                              #"Find refers to unknown variable: f"
                              (api/q db '{:find [f], :where [[e :crux.db/id _]]})))

      (t/testing "test current-query - post failed query"
        (let [malformed-query (first (api/current-queries *api*))]
          (t/is (= '{:find [f], :where [[e :crux.db/id _]]} (:query malformed-query)))
          (t/is (= :failed (:status malformed-query)))))

      (t/testing "test current-query - streaming query"
        (with-open [res (api/open-q db '{:find [e]
                                         :where [[e :name "Ivan"]]})]

          (t/testing "test current-query - streaming query (not realized)"
            (let [streaming-query (first (api/current-queries *api*))]
              (= '{:find [e] :where [[e :name "Ivan"]]} (:query streaming-query))
              (= :in-progress (:status streaming-query))))

          (t/is (iterator-seq res))

          (t/testing "test current-query - streaming query (result realized)"
            (= :complete (:status (first (api/current-queries *api*))))))))))

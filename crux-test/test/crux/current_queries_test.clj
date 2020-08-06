(ns crux.current-queries-test
  (:require [clojure.test :as t]
            [crux.api :as api]
            [crux.fixtures :as fix :refer [*api*]]
            [crux.fixtures.every-api :as every-api]
            [crux.node :as n])
  (:import java.time.Duration
           java.util.Date))

(t/use-fixtures :once every-api/with-embedded-kafka-cluster)
(t/use-fixtures :each (partial fix/with-opts {:crux.bus/sync? true}) every-api/with-each-api-implementation)

(t/deftest test-cleaning-current-queries
  (let [clean-expired-queries @#'n/clean-expired-queries
        queries [{:finished-at (Date.)
                  ::query-id 1}
                 {:finished-at (Date/from (.minus (.toInstant (Date.))
                                                  (Duration/ofSeconds 5)))
                  ::query-id 2}
                 {:finished-at (Date/from (.minus (.toInstant (Date.))
                                                  (Duration/ofSeconds 10)))
                  ::query-id 3}]]
    (t/testing "test current-query - check max count expiration"
      (t/is (= [1]
               (->> (clean-expired-queries queries
                                           {::n/finished-queries-max-age (Duration/ofSeconds 8)
                                            ::n/finished-queries-max-count 1})
                    (map ::query-id))))

      (t/is (= [1 2]
               (->> (clean-expired-queries queries
                                           {::n/finished-queries-max-age (Duration/ofSeconds 8)
                                            ::n/finished-queries-max-count 2})
                    (map ::query-id)))))

    (t/testing "test current-query - check time expiration"
      (t/is (= [1]
               (->> (clean-expired-queries queries
                                           {::n/finished-queries-max-age (Duration/ofSeconds 4)
                                            ::n/finished-queries-max-count 5})
                    (map ::query-id))))
      (t/is (= [1 2]
               (->> (clean-expired-queries queries
                                           {::n/finished-queries-max-age (Duration/ofSeconds 8)
                                            ::n/finished-queries-max-count 5})
                    (map ::query-id)))))))

(t/deftest test-current-queries
  (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"}]])

  (let [db (api/db *api*)]
    (api/q db
           '{:find [e]
             :where [[e :name "Ivan"]]})

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

        (doall (iterator-seq res))

        (t/testing "test current-query - streaming query (result realized)"
          (= :complete (:status (first (api/current-queries *api*)))))))))

(ns crux.api-test
  (:require [clojure.test :as t]
            [crux.api :as api]
            [crux.fixtures :as f])
  (:import crux.index.EntityTx
           crux.tx.SubmittedTx))

(t/use-fixtures :each f/with-kv-store f/with-http-server)

(t/deftest test-can-access-api-over-http
  (with-open [api-client (api/new-api-client f/*api-url*)]
    (let [submitted-tx (api/submit-tx api-client [[:crux.tx/put :ivan {:crux.db/id :ivan :name "Ivan"}]])]
      (t/is (instance? SubmittedTx submitted-tx))
      (t/is (true? (api/submitted-tx-updated-entity? api-client submitted-tx :ivan))))

    (t/is (= #{[:ivan]} (api/q (api/db api-client) '{:find [e]
                                                     :where [[e :name "Ivan"]]})))
    (t/is (= #{} (api/q (api/db api-client #inst "1999") '{:find [e]
                                                           :where [[e :name "Ivan"]]})))

    (t/is (= {:crux.db/id :ivan :name "Ivan"} (api/doc (api/db api-client) :ivan)))

    (t/is (nil? (api/doc (api/db api-client #inst "1999") :ivan)))

    (let [entity-tx (api/entity-tx (api/db api-client) :ivan)
          content-hash (:content-hash entity-tx)]
      (t/is (instance? EntityTx entity-tx))
      (t/is (= {:crux.db/id :ivan :name "Ivan"} (api/document api-client content-hash)))
      (t/is (= [entity-tx] (api/history api-client :ivan)))

      (t/is (nil? (api/entity-tx (api/db api-client #inst "1999") :ivan))))))

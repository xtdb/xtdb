(ns crux.api-test
  (:require [clojure.test :as t]
            [crux.api :as api]
            [crux.fixtures :as f])
  (:import crux.index.EntityTx
           crux.tx.SubmittedTx
           clojure.lang.LazySeq))

(t/use-fixtures :each f/with-kv-store f/with-http-server)

(t/deftest test-can-access-api-over-http
  (with-open [api-client (api/new-api-client f/*api-url*)]
    (t/testing "status"
      (t/is (= {:crux.zk/zk-active? false
                :crux.kv-store/kv-backend "crux.rocksdb.RocksKv"
                :crux.kv-store/estimate-num-keys 0
                :crux.tx-log/tx-time nil}
               (dissoc (api/status api-client) :crux.kv-store/size))))

    (t/testing "transaction"
      (let [submitted-tx (api/submit-tx api-client [[:crux.tx/put :ivan {:crux.db/id :ivan :name "Ivan"}]])]
        (t/is (true? (api/submitted-tx-updated-entity? api-client submitted-tx :ivan)))

        (let [status-map (api/status api-client)]
          (t/is (pos? (:crux.kv-store/estimate-num-keys status-map)))
          (t/is (= (:transact-time submitted-tx) (:crux.tx-log/tx-time status-map))))))

    (t/testing "query"
      (t/is (= #{[:ivan]} (api/q (api/db api-client) '{:find [e]
                                                       :where [[e :name "Ivan"]]})))
      (t/is (= #{} (api/q (api/db api-client #inst "1999") '{:find [e]
                                                             :where [[e :name "Ivan"]]})))

      (t/testing "malformed query"
        (t/is (thrown-with-msg? Exception
                                #"HTTP status 400"
                                (api/q (api/db api-client) '{:find [e]})))))

    (t/testing "query with streaming result"
      (let [db (api/db api-client)]
        (with-open [snapshot (api/new-snapshot db)]
          (let [result (api/q db snapshot '{:find [e]
                                            :where [[e :name "Ivan"]]})]
            (t/is (instance? LazySeq result))
            (t/is (not (realized? result)))
            (t/is (= '([:ivan]) result))
            (t/is (realized? result))))))

    (t/testing "entity"
      (t/is (= {:crux.db/id :ivan :name "Ivan"} (api/entity (api/db api-client) :ivan)))
      (t/is (nil? (api/entity (api/db api-client #inst "1999") :ivan))))

    (t/testing "entity-tx, document and history"
      (let [entity-tx (api/entity-tx (api/db api-client) :ivan)]
        (t/is (= {:crux.db/id :ivan :name "Ivan"} (api/document api-client (:content-hash entity-tx))))
        (t/is (= [entity-tx] (api/history api-client :ivan)))

        (t/is (nil? (api/entity-tx (api/db api-client #inst "1999") :ivan)))))))

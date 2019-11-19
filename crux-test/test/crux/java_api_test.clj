(ns crux.java-api-test
  (:require [clojure.test :as t]
            [crux.api :as crux])
  (:import [crux.api.v2 CruxNode StandaloneTopology KafkaTopology
            Document PutOperation CasOperation CruxId Database Query
            DeleteOperation EvictOperation]))

(t/deftest test-java-api
  (t/testing "Can create node, transact to node, and query node"
    (let [standalone (-> (StandaloneTopology/standaloneTopology)
                         (.withDbDir "data/db-dir-1")
                         (.withEventLogDir "data/eventlog-1"))
          node (CruxNode/cruxNode standalone)]

      (t/testing "Can create node"
        (t/is node))

      (t/testing "Transactions"
        (let [id (CruxId/cruxId "test-id")
              doc (-> (Document/document id)
                      (.with "Key" "Value1"))
              doc2 (-> (Document/document id)
                       (.with "Key" "Value2"))]

          (t/testing "Can create Documents/id"
            (t/is id)
            (t/is doc)
            (t/is doc2))

          (let [putOp (PutOperation/putOp doc)
                casOp (CasOperation/casOp doc doc2)
                delOp (DeleteOperation/deleteOp id)
                evictOp (EvictOperation/evictOp id)]

            (t/testing "Can create Operations"
              (t/is putOp)
              (t/is casOp)
              (t/is delOp)
              (t/is evictOp))

            (t/testing "Can submit Transactions"
              (t/is (.submitTx node [putOp]))
              (Thread/sleep 300)

              (t/is (.submitTx node [casOp]))
              (Thread/sleep 300)

              (t/is (.submitTx node [delOp]))
              (Thread/sleep 300)

              (t/is (.submitTx node [evictOp]))
              (Thread/sleep 300)))))

      (t/testing "Queries"
        (let [query (-> (Query/find "[e]")
                        (.where "[[e :crux.db/id _]]"))
              db (.db node)]
          (t/testing "Can create query"
            (t/is query))
          (t/testing "Can get a database out of node"
            (t/is db))
          (t/testing "Can query database"
            (t/is (= #{} (.query db query))))))

      (t/testing "Can close node"
        (t/is (nil? (.close node))))

      (t/testing "Calling function on closed node creates an exception"
        (t/is (thrown-with-msg? IllegalStateException
                                #"Crux node is closed"
                                (.db node)))))))

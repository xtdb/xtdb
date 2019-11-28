(ns crux.java-api-test
  (:require [clojure.test :as t]
            [crux.api :as crux])
  (:import [crux.api.alpha CruxNode StandaloneTopology KafkaTopology
            Document PutOperation CasOperation CruxId Database Query
            DeleteOperation EvictOperation Util]
           java.time.Duration))

(t/deftest test-java-api
  (t/testing "Can create node, transact to node, and query node"
    (let [node (-> (StandaloneTopology/standaloneTopology)
                   (.withKvStore "crux.kv.memdb/kv")
                   (.withEventLogKvStore "crux.kv.memdb/kv")
                   (.withDbDir "data/db-dir-1")
                   (.withEventLogDir "data/eventlog-1")
                   (.startNode))]

      (t/testing "Can create node"
        (t/is node))

      (t/testing "Can create Keyword using Util class"
        (t/is (= :hello (Util/keyword "hello"))))

      (t/testing "Can create Symbol using Util class"
        (t/is (= (symbol "hello") (Util/symbol "hello"))))

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

              (let [submittedPutOp (.submitTx node [putOp])]
                (t/testing "Can extract information from TxResult"
                  (t/is (.txTime submittedPutOp))
                  (t/is (.txId submittedPutOp)))

                (t/testing "Can Sync node against transaction time of operation"
                  (t/is (.sync node (.txTime submittedPutOp) nil)))

                (t/testing "Testing putOp hasSubmittedTxUpdatedEntity & hasSubmittedTxCorrectedEntity"
                  (t/is (.hasSubmittedTxUpdatedEntity node submittedPutOp id))
                  (t/is (.hasSubmittedTxCorrectedEntity node submittedPutOp
                                                        (.txTime submittedPutOp) id))))

              ;; TODO: Submitting CasOp currently breaks tests when .TxLog is returned
              ;(t/is (.sync node (.txTime (.submitTx node [casOp])) nil))
              (t/is (.sync node (.txTime (.submitTx node [delOp])) nil))
              (t/is (.sync node (.txTime (.submitTx node [evictOp])) nil))
              (t/is (.sync node (.txTime (.submitTx node [putOp])) nil))))))

      (t/testing "Can Sync node"
        (.sync node (Duration/ofMillis 100)))

      (t/testing "Can get status & status information of the node"
        (let [status (.status node)]
          (t/is status)
          (t/is (.version status))
          (t/is (nil? (.revision status)))
          (t/is (.kvStore status))
          (t/is (.estimatedNumberOfKeys status))
          (t/is (.size status))
          (t/is (.indexVersion status))))

      (t/testing "Database"
        (let [query (-> (Query/find "[e]")
                        (.where "[[e :crux.db/id _]]"))
              db (.db node)]

          (t/testing "Can get a database out of node"
            (t/is db))

          (t/testing "Can get a database out of node (at valid time)"
            (t/is (.db node #inst "2018-05-18T09:20:27.966-00:00")))

          (t/testing "Can get a database out of node (at valid time & transaction time)"
            (t/is (.db node #inst "2018-05-18T09:20:27.966-00:00" #inst "2018-05-18T09:20:27.966-00:00")))

          (t/testing "Can use .entity to query an entity"
            (t/is (.entity db (CruxId/cruxId "test-id"))))

          (t/testing "Can use .entity to query an non-existing entity"
            (t/is (nil? (.entity db (CruxId/cruxId "test-id1")))))

          (t/testing "Can use .entityTx to query an entity, and extract fields from the EntityTx object"
            (let [entityTx (.entityTx db (CruxId/cruxId "test-id"))]
              (t/is entityTx)
              (t/is (.id entityTx))
              (t/is (.contentHash entityTx))
              (t/is (.validTime entityTx))
              (t/is (.txTime entityTx))
              (t/is (.txId entityTx))))

          (t/testing "Can use .entityTx to query an non-existing entity"
            (t/is (nil? (.entity db (CruxId/cruxId "test-id1")))))

          (t/testing "Can use txLog function and TxLog objects"
            (let [txLogIterator (.txLog node (.txLogContext node) 10000 true)]
              (t/is txLogIterator)

              (let [txLog (.next txLogIterator)]
                (t/is txLog)
                (t/is (.txId txLog))
                (t/is (.txTime txLog))
                (t/is (instance? PutOperation
                                 (first (.txOps txLog)))))))

          (t/testing "Queries"
            (t/testing "Can create query"
              (t/is query))

            (t/testing "Can query database"
              (t/is (.query db query)))

            (t/testing "Can get results from a ResultTuple (by symbol)"
              (t/is :test-id
                    (-> (.query db query)
                        (first)
                        (.get 'e))))
            (t/testing "Can get results from a ResultTuple (by index)"
              (t/is :test-id
                    (-> (.query db query)
                        (first)
                        (.get 0)))))))

      (t/testing "Can close node"
        (t/is (nil? (.close node))))

      (t/testing "Calling function on closed node creates an exception"
        (t/is (thrown-with-msg? IllegalStateException
                                #"Crux node is closed"
                                (.db node)))))))

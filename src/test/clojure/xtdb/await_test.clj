(ns xtdb.await-test
  (:require [clojure.test :as t]
            [xtdb.await :as await]
            [xtdb.serde :as serde]
            [xtdb.time :as time])
  (:import (java.util.concurrent PriorityBlockingQueue TimeUnit)))

(defn- ->tx [tx-id]
  (serde/->TxKey tx-id (time/->instant #inst "2020")))

(t/deftest test-await
  (t/is (= (->tx 3)
           @(await/await-tx-async 2
                                  (constantly (->tx 3))
                                  (PriorityBlockingQueue.)))
        "ready already")

  (t/is (= ::waiting (.getNow (await/await-tx-async 4
                                                    (constantly (->tx 3))
                                                    (PriorityBlockingQueue.))
                              ::waiting))
        "waiting")

  (let [awaiters (PriorityBlockingQueue.)
        fut5 (await/await-tx-async 5 (constantly (->tx 3)) awaiters)
        fut4 (await/await-tx-async 4 (constantly (->tx 3)) awaiters)]
    (t/is (= ::waiting (.getNow fut4 ::waiting)))
    (t/is (= ::waiting (.getNow fut5 ::waiting)))

    (let [committed-tx (serde/->tx-committed 4 (time/->instant #inst "2020"))]
      (await/notify-tx committed-tx awaiters)

      ;;avoids race condition, await-tx-async doesn't instantly react to being notified
      (t/is (= committed-tx (.get fut4 100 TimeUnit/MILLISECONDS))
            "now yields the committed tx"))

    (t/is (= ::waiting (.getNow fut5 ::waiting))
          "still waiting")

    (let [aborted-tx (serde/->tx-aborted 5 (time/->instant #inst "2020") (ex-info "oh no" {}))]
      (await/notify-tx aborted-tx awaiters)

      (t/is (= aborted-tx (.get fut5 100 TimeUnit/MILLISECONDS))
            "now yields the aborted tx")))

  (let [fut (await/await-tx-async (->tx 5) #(throw (RuntimeException.)) (PriorityBlockingQueue.))]
    (t/is (.isCompletedExceptionally fut))
    (t/is (thrown? RuntimeException (.getNow fut ::waiting))))

  (let [awaiters (PriorityBlockingQueue.)
        fut5 (await/await-tx-async 5 (constantly (->tx 3)) awaiters)
        fut4 (await/await-tx-async 4 (constantly (->tx 3)) awaiters)]
    (t/is (= ::waiting (.getNow fut4 ::waiting)))
    (t/is (= ::waiting (.getNow fut5 ::waiting)))

    (await/notify-ex (RuntimeException.) awaiters)

    (t/is (.isCompletedExceptionally fut4))
    (t/is (.isCompletedExceptionally fut5))))

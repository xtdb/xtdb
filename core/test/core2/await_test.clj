(ns core2.await-test
  (:require [clojure.test :as t]
            [core2.await :as await]
            [core2.tx :as tx])
  (:import java.util.concurrent.PriorityBlockingQueue))

(defn- ->tx [tx-id]
  (tx/->TransactionInstant tx-id nil))

(t/deftest test-await
  (t/is (= (->tx 2)
           @(await/await-tx-async (->tx 2)
                                  (constantly (->tx 3))
                                  (PriorityBlockingQueue.)))
        "ready already")

  (t/is (= ::waiting (.getNow (await/await-tx-async (->tx 4)
                                                    (constantly (->tx 3))
                                                    (PriorityBlockingQueue.))
                              ::waiting))
        "waiting")

  (let [awaiters (PriorityBlockingQueue.)
        fut5 (await/await-tx-async (->tx 5) (constantly (->tx 3)) awaiters)
        fut4 (await/await-tx-async (->tx 4) (constantly (->tx 3)) awaiters)]
    (t/is (= ::waiting (.getNow fut4 ::waiting)))
    (t/is (= ::waiting (.getNow fut5 ::waiting)))

    (await/notify-tx (->tx 4) awaiters)

    (t/is (= (->tx 4) (.getNow fut4 ::waiting))
          "now yields")

    (t/is (= ::waiting (.getNow fut5 ::waiting))
          "still waiting"))

  (let [fut (await/await-tx-async (->tx 5) #(throw (RuntimeException.)) (PriorityBlockingQueue.))]
    (t/is (.isCompletedExceptionally fut))
    (t/is (thrown? RuntimeException (.getNow fut ::waiting))))

  (let [awaiters (PriorityBlockingQueue.)
        fut5 (await/await-tx-async (->tx 5) (constantly (->tx 3)) awaiters)
        fut4 (await/await-tx-async (->tx 4) (constantly (->tx 3)) awaiters)]
    (t/is (= ::waiting (.getNow fut4 ::waiting)))
    (t/is (= ::waiting (.getNow fut5 ::waiting)))

    (await/notify-ex (RuntimeException.) awaiters)

    (t/is (.isCompletedExceptionally fut4))
    (t/is (.isCompletedExceptionally fut5))))

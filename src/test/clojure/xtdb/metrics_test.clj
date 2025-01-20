(ns xtdb.metrics-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]
            [xtdb.types])
  (:import io.micrometer.core.instrument.composite.CompositeMeterRegistry
           io.micrometer.core.instrument.Counter))

(t/use-fixtures :each tu/with-mock-clock tu/with-node tu/with-simple-registry)

(t/deftest test-error-and-warning-counter
  (let [registry ^CompositeMeterRegistry (tu/component tu/*node* :xtdb.metrics/registry)]
    (t/is (thrown-with-msg? Exception #"" (xt/q tu/*node* "SLECT 1")))
    (t/is (thrown-with-msg? Exception #"" (jdbc/execute! tu/*conn* ["SLECT 1"])))
    (t/is (thrown-with-msg? Exception #"" (xt/q tu/*node* "SELECT 1/0")))
    (t/is (thrown-with-msg? Exception #"" (jdbc/execute! tu/*conn* ["SELECT 1/0"])))

    (xt/q tu/*node* "SELECT foo FROM bar")
    (jdbc/execute! tu/*conn* ["SELECT foo FROM bar"])

    (t/is (= 4.0 (.count ^Counter (.counter (.find registry "query.error")))))

    (t/is (= 2.0 (.count ^Counter (.counter (.find registry "query.warning")))))))

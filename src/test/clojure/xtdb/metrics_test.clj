(ns xtdb.metrics-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.types])
  (:import io.micrometer.core.instrument.composite.CompositeMeterRegistry
           (io.micrometer.core.instrument.simple SimpleMeterRegistry)
           (io.micrometer.core.instrument Counter Gauge)))

(t/use-fixtures :each tu/with-mock-clock)

(t/deftest test-error-and-warning-counter
  (let [node (xtn/start-node tu/*node-opts*)
        conn (jdbc/get-connection node)
        registry ^CompositeMeterRegistry (tu/component node :xtdb.metrics/registry)]
    (.add registry (SimpleMeterRegistry.))
    (t/is (thrown-with-msg? Exception #"" (xt/q node "SLECT 1")))
    (t/is (thrown-with-msg? Exception #"" (jdbc/execute! conn ["SLECT 1"])))
    (t/is (thrown-with-msg? Exception #"" (xt/q node "SELECT 1/0")))
    (t/is (thrown-with-msg? Exception #"" (jdbc/execute! conn ["SELECT 1/0"])))

    (xt/q node "SELECT foo FROM bar")
    (jdbc/execute! conn ["SELECT foo FROM bar"])

    (t/is (= 4.0 (.count ^Counter (.counter (.find registry "query.error")))))

    (t/is (= 2.0 (.count ^Counter (.counter (.find registry "query.warning")))))))

(t/deftest test-total-and-active-connections
  (let [node (xtn/start-node tu/*node-opts*)
        conn1 (jdbc/get-connection node)
        conn2 (jdbc/get-connection node)
        registry ^CompositeMeterRegistry (tu/component node :xtdb.metrics/registry)]
    (.add registry (SimpleMeterRegistry.))
    (jdbc/execute! conn1 ["SELECT 1"])
    (.close conn1)


    ;; We have a connection open so this should be equal to 1.0
    (t/is (= 1.0 (.value ^Gauge (.gauge (.find registry "pgwire.active_connections")))))
    (t/is (= 2.0 (.count ^Counter (.counter (.find registry "pgwire.total_connections")))))

    #_
    (.increment ^Counter (.counter (.find registry "pgwire.total_connections")))
    #_
    (t/is (= 2.0 (.count ^Counter (.counter (.find registry "pgwire.total_connections")))))))

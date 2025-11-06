(ns xtdb.hikari-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.next.jdbc :as xt-jdbc]
            [xtdb.node :as xtn])
  (:import (com.zaxxer.hikari HikariConfig HikariDataSource)))

(t/deftest works-with-hikaricp-postgresql-url
  (t/testing "HikariCP with PostgreSQL URL works with copy-in"
    (with-open [node (xtn/start-node)]
      (let [port (-> node :system :xtdb.pgwire/server :read-write :port)
            jdbc-url (str "jdbc:postgresql://localhost:" port "/xtdb")
            hikari-config (doto (HikariConfig.)
                            (.setJdbcUrl jdbc-url)
                            (.setUsername "xtdb")
                            (.setMaximumPoolSize 5)
                            (.setMinimumIdle 2))
            hikari-ds (HikariDataSource. hikari-config)]

        (t/testing "single insert using copy-in"
          (with-open [conn (jdbc/get-connection hikari-ds)]
            (jdbc/execute! conn ["INSERT INTO foo RECORDS {_id: 1, name: 'Alice'}"])
            (t/is (= [{:xt/id 1 :name "Alice"}]
                     (jdbc/execute! conn ["SELECT * FROM foo WHERE _id = 1"]
                                    {:builder-fn xt-jdbc/builder-fn})))))

        (t/testing "multiple consecutive inserts without connection leaks"
          (dotimes [i 20]
            (with-open [conn (jdbc/get-connection hikari-ds)]
              (jdbc/execute! conn [(str "INSERT INTO bar RECORDS {_id: " i ", value: " i "}")])))

          (with-open [conn (jdbc/get-connection hikari-ds)]
            (let [result (jdbc/execute! conn ["SELECT COUNT(*) as cnt FROM bar"]
                                        {:builder-fn xt-jdbc/builder-fn})]
              (t/is (= 20 (:cnt (first result)))))))

        (t/testing "connection pool remains healthy"
          (let [pool-stats (.getHikariPoolMXBean hikari-ds)]
            (t/is (zero? (.getActiveConnections pool-stats))
                  "All connections should be returned to pool")
            (t/is (zero? (.getThreadsAwaitingConnection pool-stats))
                  "No threads should be waiting for connections")))

        (.close hikari-ds)))))

(t/deftest works-with-hikaricp-xtdb-driver
  (t/testing "HikariCP with jdbc:xtdb:// URL works with copy-in"
    (with-open [node (xtn/start-node)]
      (let [port (-> node :system :xtdb.pgwire/server :read-write :port)
            jdbc-url (str "jdbc:xtdb://localhost:" port "/xtdb")
            hikari-config (doto (HikariConfig.)
                            (.setJdbcUrl jdbc-url)
                            (.setDriverClassName "xtdb.jdbc.Driver")
                            (.setUsername "xtdb")
                            (.setMaximumPoolSize 5)
                            (.setMinimumIdle 2))
            hikari-ds (HikariDataSource. hikari-config)]

        (t/testing "single insert with xtdb driver"
          (with-open [conn (jdbc/get-connection hikari-ds)]
            (jdbc/execute! conn ["INSERT INTO baz RECORDS {_id: 'x1', val: 42}"])
            (t/is (= [{:xt/id "x1" :val 42}]
                     (jdbc/execute! conn ["SELECT * FROM baz WHERE _id = 'x1'"]
                                    {:builder-fn xt-jdbc/builder-fn})))))

        (t/testing "multiple consecutive operations with xtdb driver"
          (dotimes [i 15]
            (with-open [conn (jdbc/get-connection hikari-ds)]
              (jdbc/execute! conn [(str "INSERT INTO qux RECORDS {_id: 'q" i "', num: " i "}")])))

          (with-open [conn (jdbc/get-connection hikari-ds)]
            (let [result (jdbc/execute! conn ["SELECT COUNT(*) as cnt FROM qux"]
                                        {:builder-fn xt-jdbc/builder-fn})]
              (t/is (= 15 (:cnt (first result)))))))

        (t/testing "connection pool remains healthy with xtdb driver"
          (let [pool-stats (.getHikariPoolMXBean hikari-ds)]
            (t/is (zero? (.getActiveConnections pool-stats))
                  "All connections should be returned to pool")
            (t/is (zero? (.getThreadsAwaitingConnection pool-stats))
                  "No threads should be waiting for connections")))

        (.close hikari-ds)))))

(t/deftest works-with-hikaricp-setdatasource
  (t/testing "HikariCP with setDataSource works"
    (with-open [node (xtn/start-node)]
      (let [hikari-config (doto (HikariConfig.)
                            (.setDataSource node)
                            (.setMaximumPoolSize 5)
                            (.setMinimumIdle 2))
            hikari-ds (HikariDataSource. hikari-config)]

        (t/testing "single insert using node as datasource"
          (with-open [conn (jdbc/get-connection hikari-ds)]
            (jdbc/execute! conn ["INSERT INTO ds_test RECORDS {_id: 1, name: 'Bob'}"])
            (t/is (= [{:xt/id 1 :name "Bob"}]
                     (jdbc/execute! conn ["SELECT * FROM ds_test WHERE _id = 1"]
                                    {:builder-fn xt-jdbc/builder-fn})))))

        (t/testing "multiple consecutive operations via setDataSource"
          (dotimes [i 10]
            (with-open [conn (jdbc/get-connection hikari-ds)]
              (jdbc/execute! conn [(str "INSERT INTO ds_multi RECORDS {_id: " i ", val: " (* i 10) "}")])))

          (with-open [conn (jdbc/get-connection hikari-ds)]
            (let [result (jdbc/execute! conn ["SELECT COUNT(*) as cnt FROM ds_multi"]
                                        {:builder-fn xt-jdbc/builder-fn})]
              (t/is (= 10 (:cnt (first result)))))))

        (t/testing "connection pool remains healthy with setDataSource"
          (let [pool-stats (.getHikariPoolMXBean hikari-ds)]
            (t/is (zero? (.getActiveConnections pool-stats))
                  "All connections should be returned to pool")
            (t/is (zero? (.getThreadsAwaitingConnection pool-stats))
                  "No threads should be waiting for connections")))

        (.close hikari-ds)))))

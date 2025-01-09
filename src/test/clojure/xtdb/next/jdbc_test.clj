(ns xtdb.next.jdbc-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.next.jdbc :as xt-jdbc]
            [xtdb.node :as xtn]))

(t/deftest uses-xtdb-node-as-jdbc-conn
  (with-open [node (xtn/start-node {:server {:port 0}})
              conn (jdbc/get-connection node)]
    (jdbc/execute! conn ["INSERT INTO foo RECORDS {_id: 1}"])
    (t/is (= [{:xt/id 1}] (jdbc/execute! conn ["SELECT * FROM foo"]
                                         {:builder-fn xt-jdbc/builder-fn})))))

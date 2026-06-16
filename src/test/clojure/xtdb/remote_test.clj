(ns xtdb.remote-test
  (:require [clojure.test :as t]
            [xtdb.node :as xtn])
  (:import [xtdb.postgres PostgresRemote$Factory]))

(t/deftest resolves-postgres-remote-from-config
  (let [config (xtn/->config {:remotes {:pg [:postgres {:host "localhost" :database "gleif"
                                                        :username "postgres" :password "postgres"}]}})
        remotes (.getRemotes config)
        pg (get remotes "pg")]
    (t/is (instance? PostgresRemote$Factory pg))
    (t/is (= "localhost" (.getHostname pg)))
    (t/is (= 5432 (.getPort pg)) "port defaults to 5432")
    (t/is (= "gleif" (.getDatabase pg)))
    (t/is (= "postgres" (.getUsername pg)))
    (t/is (= "postgres" (.getPassword pg)))))

(t/deftest unknown-remote-type-throws
  (t/is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown remote type"
                          (xtn/->config {:remotes {:x [:nope {}]}}))))

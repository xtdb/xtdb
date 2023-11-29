(ns xtdb.jdbc-startup-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.fixtures :as fix]
            [xtdb.fixtures.jdbc :as fj])
  (:import (clojure.lang ExceptionInfo)
           (java.util.concurrent ExecutionException Executors TimeUnit)
           (java.util.concurrent.locks ReadWriteLock ReentrantReadWriteLock)))

(comment

  (require 'clojure.java.shell)
  (clojure.java.shell/sh "docker-compose" "up" "-d" :dir "modules/jdbc")
  (clojure.java.shell/sh "docker-compose" "down" :dir "modules/jdbc")
  (fj/set-test-dialects! :mysql :mssql :postgres :h2 :sqlite)

  )

(t/deftest startup-race-2776
  ;; test concurrent startups do not fail due to races in the schema setup
  (fix/with-tmp-dirs #{db-dir}
    (doseq [[dialect-kw {:keys [db-spec dialect reset]}]
            {:postgres
             {:dialect 'xtdb.jdbc.psql/->dialect
              :db-spec {:dbtype "postgresql" :dbname "xtdbtest", :user "postgres", :password "postgres"}
              :reset
              (fn reset-pg [db-spec]
                (with-open [conn (jdbc/get-connection (dissoc db-spec :dbname))]
                  (jdbc/execute! conn ["DROP DATABASE IF EXISTS xtdbtest"])
                  (jdbc/execute! conn ["CREATE DATABASE xtdbtest"]))
                db-spec)}
             :h2
             {:dialect 'xtdb.jdbc.h2/->dialect
              :db-spec {:dbtype "h2" :dbname (str (io/file db-dir "xtdbtest"))}
              :reset (fn reset-h2 [db-spec] (io/delete-file (:dbname db-spec) true))}}
            :when (fj/jdbc-dialects dialect-kw)]
      (let [exec (Executors/newFixedThreadPool 4)
            futs (atom [])

            node-cfg
            {:xtdb.jdbc/connection-pool {:db-spec db-spec, :dialect dialect}
             :xtdb/tx-log
             {:xtdb/module 'xtdb.jdbc/->tx-log
              :connection-pool :xtdb.jdbc/connection-pool}
             :xtdb/document-store
             {:xtdb/module 'xtdb.jdbc/->document-store,
              :connection-pool :xtdb.jdbc/connection-pool}}

            reset-lock (ReentrantReadWriteLock. true)

            spawn-call
            (fn [i]
              (if (= 0 (mod i 5))
                (let [lock (.writeLock reset-lock)]
                  (.lock lock)
                  (try
                    (reset db-spec)
                    (finally
                      (.unlock lock))))

                (let [lock (.readLock reset-lock)]
                  (.lock lock)
                  (try
                    (with-open [_ (xt/start-node node-cfg)])
                    (finally
                      (.unlock lock))))))

            spawn (fn [i] (swap! futs conj (.submit exec ^Runnable (partial spawn-call i))))
            n-tasks 20]
        (try
          (dotimes [i n-tasks]
            (spawn i))

          (try
            (mapv deref @futs)
            (catch ExecutionException e
              (throw (.getCause e))))

          (catch ExceptionInfo e
            (if (= "Error starting system" (.getMessage e))
              (throw (.getCause e))
              (throw e)))

          (finally
            (.shutdown exec)
            (assert (.awaitTermination exec 5 TimeUnit/SECONDS))))

        (with-open [node (xt/start-node node-cfg)]
          (xt/submit-tx node [[::xt/put {:xt/id 42}]])
          (xt/sync node)
          (t/is (= {:xt/id 42} (xt/entity (xt/db node) 42))))))))

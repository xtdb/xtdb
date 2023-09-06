(ns xtdb.jdbc.java-configuration-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t])
  (:import (clojure.lang IDeref)
           (java.util HashMap Map)
           (xtdb.api IXtdb ModuleConfiguration NodeConfiguration NodeConfiguration)))

(set! *warn-on-reflection* false)

(t/deftest java-map-permitted-as-db-spec-test
  (let [dbname (name (gensym "test-db"))]
    (-> (NodeConfiguration/builder)
        (.with "xtdb.jdbc/connection-pool"
               (-> (ModuleConfiguration/builder)
                   (.with "dialect"
                          (-> (ModuleConfiguration/builder)
                              (.with "xtdb/module" "xtdb.jdbc.h2/->dialect")
                              (.build)))
                   (.set "db-spec" (doto (HashMap.) (.put "dbname" dbname)))
                   (.build)))
        (.with "xtdb/tx-log"
               (-> (ModuleConfiguration/builder)
                   (.with "xtdb/module" "xtdb.jdbc/->tx-log")
                   (.with "connection-pool" "xtdb.jdbc/connection-pool")
                   (.build)))
        (.with "xtdb/document-store"
               (-> (ModuleConfiguration/builder)
                   (.with "xtdb/module" "xtdb.jdbc/->document-store")
                   (.with "connection-pool" "xtdb.jdbc/connection-pool")
                   (.build)))
        (.build)
        IXtdb/startNode
        .close)
    (t/is (.exists (io/file (format "%s.mv.db" dbname))))
    (io/delete-file (format "%s.mv.db" dbname) true)))

(t/deftest java-map-permitted-as-pool-opts-test
  (let [dbname (name (gensym "test-db"))]
    (with-open [node (-> (NodeConfiguration/builder)
                         (.with "xtdb.jdbc/connection-pool"
                                (-> (ModuleConfiguration/builder)
                                    (.with "dialect"
                                           (-> (ModuleConfiguration/builder)
                                               (.with "xtdb/module" "xtdb.jdbc.h2/->dialect")
                                               (.build)))
                                    (.set "db-spec" (doto (HashMap.) (.put "dbname" dbname)))
                                    (.set "pool-opts" (doto (HashMap.) (.put "maximumPoolSize" 42)))
                                    (.build)))
                         (.with "xtdb/tx-log"
                                (-> (ModuleConfiguration/builder)
                                    (.with "xtdb/module" "xtdb.jdbc/->tx-log")
                                    (.with "connection-pool" "xtdb.jdbc/connection-pool")
                                    (.build)))
                         (.with "xtdb/document-store"
                                (-> (ModuleConfiguration/builder)
                                    (.with "xtdb/module" "xtdb.jdbc/->document-store")
                                    (.with "connection-pool" "xtdb.jdbc/connection-pool")
                                    (.build)))
                         (.build)
                         IXtdb/startNode)]
      (t/is (= 42 (-> node :node :!system deref :xtdb.jdbc/connection-pool :pool .getMaximumPoolSize)))
      (io/delete-file (format "%s.mv.db" dbname) true))))

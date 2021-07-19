(ns crux.jdbc-doc-store2-test
  (:require [crux.jdbc :as jdbc]
            [clojure.test :as t]
            [crux.fixtures.document-store :as fix.ds]
            [crux.system :as sys]
            [crux.fixtures.jdbc :as fj]))

(t/use-fixtures :each fj/with-each-jdbc-dialect)

(t/deftest test-document-store2
  (with-open [sys (-> (sys/prep-system {:pool (merge {:crux/module `jdbc/->connection-pool}
                                                     fj/*jdbc-opts*)
                                        :doc-store {:crux/module `jdbc/->document-store2
                                                    :connection-pool :pool}})
                      (sys/start-system))]

    (fix.ds/test-doc-store (:doc-store sys))))

(ns xtdb.authz-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]))

;; GRANT/REVOKE tests that require a superuser principal live in xtdb.pgwire.authz-test,
;; where they run over a pgwire connection that authenticates as the `xtdb` superuser.

(t/deftest user-dml-to-membership-table-rejected
  (with-open [node (xtn/start-node)]
    (t/is (thrown-with-msg? Exception #"Cannot write to table"
                            (xt/execute-tx node [[:sql "INSERT INTO xt.role_membership (_id, \"user\", role) VALUES (1, 'eve', 'admin')"]]))
          "ordinary user DML to xt.role_membership stays blocked by the forbidden-schemas guard")))

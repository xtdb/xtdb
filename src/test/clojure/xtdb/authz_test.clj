(ns xtdb.authz-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]))

(defn- membership-rows [node]
  (xt/q node "SELECT \"user\", role FROM xt.role_membership ORDER BY \"user\", role"))

(t/deftest grant-revoke-round-trip
  (with-open [node (xtn/start-node)]
    (xt/execute-tx node [[:sql "GRANT analyst TO alice"]])
    (xt/execute-tx node [[:sql "GRANT admin TO alice"]])
    (xt/execute-tx node [[:sql "GRANT analyst TO bob"]])

    (t/is (= [{:user "alice", :role "admin"}
              {:user "alice", :role "analyst"}
              {:user "bob", :role "analyst"}]
             (membership-rows node)))

    (xt/execute-tx node [[:sql "REVOKE analyst FROM alice"]])

    (t/is (= [{:user "alice", :role "admin"}
              {:user "bob", :role "analyst"}]
             (membership-rows node))
          "REVOKE soft-closes just the one membership")

    (t/testing "re-GRANT supersedes; REVOKE of an absent membership is a no-op"
      (xt/execute-tx node [[:sql "GRANT analyst TO alice"]])
      (xt/execute-tx node [[:sql "REVOKE reporter FROM carol"]])
      (t/is (= [{:user "alice", :role "admin"}
                {:user "alice", :role "analyst"}
                {:user "bob", :role "analyst"}]
               (membership-rows node))))))

;; the acceptance test on #5683: REVOKE is a system-time soft-close, so membership
;; history stays queryable as-of any past system-time.
(t/deftest membership-queryable-as-of-system-time
  (with-open [node (xtn/start-node)]
    (let [t1 (:system-time (xt/execute-tx node [[:sql "GRANT analyst TO alice"]]))
          t2 (:system-time (xt/execute-tx node [[:sql "REVOKE analyst FROM alice"]]))]
      (t/is (= [{:user "alice", :role "analyst"}]
               (xt/q node "SELECT \"user\", role FROM xt.role_membership" {:snapshot-time t1}))
            "as-of the grant: membership in force")

      (t/is (= []
               (xt/q node "SELECT \"user\", role FROM xt.role_membership" {:snapshot-time t2}))
            "as-of the revoke: membership closed"))))

(t/deftest user-dml-to-membership-table-rejected
  (with-open [node (xtn/start-node)]
    (t/is (thrown-with-msg? Exception #"Cannot write to table"
                            (xt/execute-tx node [[:sql "INSERT INTO xt.role_membership (_id, \"user\", role) VALUES (1, 'eve', 'admin')"]]))
          "ordinary user DML to xt.role_membership stays blocked by the forbidden-schemas guard")))

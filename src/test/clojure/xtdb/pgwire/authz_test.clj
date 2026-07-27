(ns xtdb.pgwire.authz-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.pgwire-test :as pgw-test])
  (:import org.postgresql.util.PSQLException
           xtdb.api.PasswordHash))

(def ^:private membership-q
  "SELECT r.rolname AS role, u.rolname AS member
   FROM pg_auth_members m
   JOIN pg_roles r ON r.oid = m.roleid
   JOIN pg_roles u ON u.oid = m.member
   ORDER BY role, member")

(t/deftest membership-surfaced-through-pg-catalog
  (with-open [node (xtn/start-node)
              conn (jdbc/get-connection node)]
    (jdbc/execute! conn ["GRANT analyst TO alice"])
    (jdbc/execute! conn ["GRANT analyst TO bob"])
    (jdbc/execute! conn ["GRANT admin TO alice"])

    (t/is (= [{:role "admin", :member "alice"}
              {:role "analyst", :member "alice"}
              {:role "analyst", :member "bob"}]
             (pgw-test/q conn [membership-q])))

    (t/testing "granted roles and member users both appear in pg_roles"
      (t/is (= [{:rolname "admin", :rolsuper false, :rolcanlogin false}
                {:rolname "alice", :rolsuper false, :rolcanlogin true}
                {:rolname "analyst", :rolsuper false, :rolcanlogin false}
                {:rolname "bob", :rolsuper false, :rolcanlogin true}
                {:rolname "xtdb", :rolsuper true, :rolcanlogin true}]
               (pgw-test/q conn ["SELECT rolname, rolsuper, rolcanlogin FROM pg_roles ORDER BY rolname"]))))

    (jdbc/execute! conn ["REVOKE analyst FROM alice"])
    (t/is (= [{:role "admin", :member "alice"}
              {:role "analyst", :member "bob"}]
             (pgw-test/q conn [membership-q])))))

(t/deftest name-both-user-and-role-appears-once-in-pg-roles
  (with-open [node (xtn/start-node)
              conn (jdbc/get-connection node)]
    (jdbc/execute! conn ["GRANT analyst TO alice"])
    (jdbc/execute! conn ["GRANT alice TO bob"])

    (t/is (= [{:rolname "alice", :rolcanlogin true}]
             (pgw-test/q conn ["SELECT rolname, rolcanlogin FROM pg_roles WHERE rolname = 'alice'"]))
          "a name that is both a member-user and a granted role appears once, as a login user")

    (t/is (= [{:role "alice", :member "bob"}
              {:role "analyst", :member "alice"}]
             (pgw-test/q conn [membership-q]))
          "the pg_auth_members join resolves cleanly despite alice being both")))

(t/deftest superuser-only
  (let [xtdb-pw (PasswordHash/argon2id "xtdb-pw")
        alice-pw (PasswordHash/argon2id "alice-pw")]
    (with-open [node (xtn/start-node {:authn [:user-list {:users {"xtdb" xtdb-pw, "alice" alice-pw}
                                                          :rules [{:method :password}]}]})]
      (binding [pgw-test/*port* (.getServerPort node)]
        (t/testing "a configured xtdb user may manage membership"
          (with-open [conn (pgw-test/pgjdbc-conn {:user "xtdb", "password" "xtdb-pw"})]
            (jdbc/execute! conn ["GRANT analyst TO bob"])
            (t/is (= [{:user "bob", :role "analyst"}]
                     (pgw-test/q conn ["SELECT \"user\", role FROM xt.role_membership"])))))

        (t/testing "a non-superuser is rejected"
          (with-open [conn (pgw-test/pgjdbc-conn {:user "alice", "password" "alice-pw"})]
            (t/is (thrown-with-msg? PSQLException #"(?i)superuser"
                                    (jdbc/execute! conn ["GRANT reporter TO bob"])))))))))

(t/deftest role-membership-managed-on-primary-only
  (with-open [node (xtn/start-node)]
    (jdbc/execute! node ["ATTACH DATABASE new_db"])
    (binding [pgw-test/*port* (.getServerPort node)]
      (with-open [conn (pgw-test/pgjdbc-conn {:dbname "new_db"})]
        (t/is (thrown-with-msg? PSQLException #"(?i)primary"
                                (jdbc/execute! conn ["GRANT analyst TO alice"])))))))

;; Moved from xtdb.authz-test: GRANT/REVOKE requires a superuser principal, which the in-process
;; ADBC connection lacks (user = nil). pgwire authenticates as the `xtdb` superuser by default.

(def ^:private membership-q-xt
  "SELECT \"user\", role FROM xt.role_membership ORDER BY \"user\", role")

(t/deftest grant-revoke-round-trip
  (with-open [node (xtn/start-node)
              conn (jdbc/get-connection node)]
    (jdbc/execute! conn ["GRANT analyst TO alice"])
    (jdbc/execute! conn ["GRANT admin TO alice"])
    (jdbc/execute! conn ["GRANT analyst TO bob"])

    (t/is (= [{:user "alice", :role "admin"}
              {:user "alice", :role "analyst"}
              {:user "bob", :role "analyst"}]
             (pgw-test/q conn [membership-q-xt])))

    (jdbc/execute! conn ["REVOKE analyst FROM alice"])

    (t/is (= [{:user "alice", :role "admin"}
              {:user "bob", :role "analyst"}]
             (pgw-test/q conn [membership-q-xt]))
          "REVOKE soft-closes just the one membership")

    (t/testing "re-GRANT supersedes; REVOKE of an absent membership is a no-op"
      (jdbc/execute! conn ["GRANT analyst TO alice"])
      (jdbc/execute! conn ["REVOKE reporter FROM carol"])
      (t/is (= [{:user "alice", :role "admin"}
                {:user "alice", :role "analyst"}
                {:user "bob", :role "analyst"}]
               (pgw-test/q conn [membership-q-xt]))))))

;; The acceptance test on #5683: REVOKE is a system-time soft-close, so membership
;; history stays queryable as-of any past system-time.
(t/deftest membership-queryable-as-of-system-time
  (with-open [node (xtn/start-node)
              conn (jdbc/get-connection node)]
    (jdbc/execute! conn ["GRANT analyst TO alice"])
    (let [t1 (.toInstant (:system-time (first (pgw-test/q conn ["SHOW LATEST_SUBMITTED_TX"]))))]
      (jdbc/execute! conn ["REVOKE analyst FROM alice"])
      (let [t2 (.toInstant (:system-time (first (pgw-test/q conn ["SHOW LATEST_SUBMITTED_TX"]))))]

        (t/is (= [{:user "alice", :role "analyst"}]
                 (xt/q conn membership-q-xt {:snapshot-time t1}))
              "as-of the grant: membership in force")

        (t/is (= []
                 (xt/q conn membership-q-xt {:snapshot-time t2}))
              "as-of the revoke: membership closed")))))

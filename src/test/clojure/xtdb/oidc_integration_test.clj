(ns xtdb.oidc-integration-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc] 
            [xtdb.authn-test :as authn-test]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu])
  (:import [org.postgresql.util PSQLException]))

(def ^:dynamic *clients* nil)

(defn seed-container [f]
  (let [{:keys [clients]} (authn-test/seed! authn-test/container {:access-token-lifespan (int 2)})]
    (binding [*clients* clients]
      (f))))

(t/use-fixtures :once
  (fn [f]
    (tu/with-container authn-test/container
      (fn [_]
        (f)))))

(t/use-fixtures :each seed-container)

(t/deftest ^:integration test-oidc-password-flow
  (with-open [node (xtn/start-node {:authn [:openid-connect {:issuer-url (str (.getAuthServerUrl authn-test/container) "/realms/master")
                                                             :client-id "xtdb"
                                                             :client-secret "xtdb-secret"
                                                             :rules [{:user "test-user" :method :password :address "127.0.0.1"}
                                                                     {:user "oid-client" :method :client-credentials}]}]})]
    (let [user-connection-builder (-> (.createConnectionBuilder node) (.user "test-user"))]
      (t/testing "successful password authentication"
        (with-open [conn (-> user-connection-builder
                             (.password "password124")
                             (.build))]
          (let [result (jdbc/execute-one! conn ["SELECT 1 as test"])]
            (t/is (= 1 (:test result))))))

      (t/testing "failed password authentication"
        (t/is (thrown-with-msg? PSQLException #"Password authentication failed for user: test-user"
                                (-> user-connection-builder
                                    (.password "wrong-password")
                                    (.build))))))))

(t/deftest ^:integration test-oidc-client-credentials
  (with-open [node (xtn/start-node {:authn [:openid-connect {:issuer-url (str (.getAuthServerUrl authn-test/container) "/realms/master")
                                                             :client-id "xtdb"
                                                             :client-secret "xtdb-secret"
                                                             :rules [{:user "oid-client" :method :client-credentials}]}]})]
    (let [{:keys [client-id client-secret]} (:test *clients*)
          client-connection-builder (-> (.createConnectionBuilder node) (.user "oid-client"))]

      (t/testing "successful client credentials authentication"
        (with-open [conn (-> client-connection-builder
                             (.password (format "%s:%s" client-id client-secret))
                             (.build))]
          (let [result (jdbc/execute-one! conn ["SELECT 'authenticated' as status"])]
            (t/is (= "authenticated" (:status result))))))

      (t/testing "failed client credentials authentication with badly written credentials"
        (t/is (thrown-with-msg? PSQLException #"Client credentials must be provided in the format 'client-id:client-secret'"
                                (-> client-connection-builder
                                    (.password "too:many:colons")
                                    (.build)))))

      (t/testing "failed client credentials authentication with non-existent credentials"
        (t/is (thrown-with-msg? PSQLException #"Client credentials authentication failed for client: bad-client"
                                (-> client-connection-builder
                                    (.password "bad-client:bad-secret")
                                    (.build))))))))

(t/deftest ^:integration test-oidc-session-management
  (with-open [node (xtn/start-node {:authn [:openid-connect {:issuer-url (str (.getAuthServerUrl authn-test/container) "/realms/master")
                                                             :client-id "xtdb"
                                                             :client-secret "xtdb-secret"
                                                             :rules [{:user "test-user" :method :password :address "127.0.0.1"}]}]})]
    (let [user-connection-builder (-> (.createConnectionBuilder node)
                                      (.user "test-user")
                                      (.password "password124"))]

      (t/testing "session persists across multiple queries"
        (with-open [conn (.build user-connection-builder)]
          (let [result1 (jdbc/execute-one! conn ["SELECT 1 as first_query"])
                result2 (jdbc/execute-one! conn ["SELECT 2 as second_query"])]
            (t/is (= 1 (:first_query result1)))
            (t/is (= 2 (:second_query result2))))))

      (t/testing "multiple concurrent connections work"
        (with-open [conn1 (.build user-connection-builder)
                    conn2 (.build user-connection-builder)]
          (let [result1 (jdbc/execute-one! conn1 ["SELECT 'conn1' as source"])
                result2 (jdbc/execute-one! conn2 ["SELECT 'conn2' as source"])]
            (t/is (= "conn1" (:source result1)))
            (t/is (= "conn2" (:source result2)))))))))

(t/deftest ^:integration test-oidc-multiple-auth-methods
  (let [{:keys [client-id client-secret]} (:test *clients*)]
    (with-open [node (xtn/start-node {:authn [:openid-connect {:issuer-url (str (.getAuthServerUrl authn-test/container) "/realms/master")
                                                               :client-id "xtdb"
                                                               :client-secret "xtdb-secret"
                                                               :rules [{:user "test-user" :method :password :address "127.0.0.1"}
                                                                       {:user "oid-client" :method :client-credentials}]}]})]

      (t/testing "password flow works"
        (with-open [conn (-> (.createConnectionBuilder node)
                             (.user "test-user")
                             (.password "password124")
                             (.build))]
          (let [result (jdbc/execute-one! conn ["SELECT 'password-auth' as method"])]
            (t/is (= "password-auth" (:method result))))))

      (t/testing "client credentials flow works"
        (with-open [conn (-> (.createConnectionBuilder node)
                             (.user "oid-client")
                             (.password (format "%s:%s" client-id client-secret))
                             (.build))]
          (let [result (jdbc/execute-one! conn ["SELECT 'client-creds-auth' as method"])]
            (t/is (= "client-creds-auth" (:method result))))))

      (t/testing "wrong method for user fails"
        (t/is (thrown? Exception (-> (.createConnectionBuilder node)
                                     (.user "oid-client")
                                     (.password "password123")
                                     (.build))))))))

(t/deftest ^:integration test-oidc-client-credentials-token-expiry
  (let [{:keys [client-id client-secret]} (:test *clients*)
        mock-instant-source (tu/->mock-clock
                             [#inst "2020-01-01T12:00:00Z" ; initial connection time
                              #inst "2020-01-01T12:00:00Z" ; used to calculate token expiry for connection
                              #inst "2020-01-01T12:00:01Z" ; used to validate if token expired within :msg-parse (not yet expired)
                              #inst "2020-01-01T12:00:01Z" ; used to validate if token expired within :msg-bind (not yet expired)
                              #inst "2020-01-01T12:00:05Z" ; used to validate if token expired within :msg-parse (expired)
                              #inst "2020-01-01T12:00:05Z" ; used to calculate refreshed-token expiry time
                              #inst "2020-01-01T12:00:06Z" ; used to validate if token expired within :msg-bind (refreshed)
                              ])]
    (with-open [node (xtn/start-node {:authn [:openid-connect {:issuer-url (str (.getAuthServerUrl authn-test/container) "/realms/master")
                                                               :client-id "xtdb"
                                                               :client-secret "xtdb-secret"
                                                               :instant-src mock-instant-source
                                                               :rules [{:user "oid-client" :method :client-credentials}]}]})]
      (let [client-connection-builder (-> (.createConnectionBuilder node)
                                          (.user "oid-client")
                                          (.password (format "%s:%s" client-id client-secret)))]

        (t/testing "token expiry and automatic refresh works"
          (with-open [conn (.build client-connection-builder)]
            (t/testing "initial connection and first revalidate"
              (let [result (jdbc/execute-one! conn ["SELECT 'initial' as status"])]
                (t/is (= "initial" (:status result)))))

            (t/testing "after first expiry, token is refreshed"
              (let [result (jdbc/execute-one! conn ["SELECT 'after-expiry' as status"])]
                (t/is (= "after-expiry" (:status result)))))))))))

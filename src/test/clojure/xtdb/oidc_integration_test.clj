(ns xtdb.oidc-integration-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc] 
            [xtdb.authn-test :as authn-test]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu])
  (:import [org.postgresql.util PSQLException]))

(t/use-fixtures :once
  (fn [f]
    (tu/with-container authn-test/container
      (fn [_]
        (f)))))

(t/deftest ^:integration test-oidc-password-flow
  (authn-test/seed! authn-test/container)
  (let [port (tu/free-port)
        oidc-config [:openid-connect {:issuer-url (str (.getAuthServerUrl authn-test/container) "/realms/master")
                                      :client-id "xtdb"
                                      :client-secret "xtdb-secret"
                                      :rules [{:user "test-user" :method :password :address "127.0.0.1"}]}]]
    (with-open [_node (xtn/start-node {:authn oidc-config
                                       :server {:port port}})]
      (let [ds-spec {:dbtype "postgresql"
                     :host "localhost"
                     :port port
                     :dbname "xtdb"
                     :user "test-user"
                     :password "password124"}]

        (t/testing "successful password authentication"
          (with-open [conn (jdbc/get-connection ds-spec)]
            (let [result (jdbc/execute-one! conn ["SELECT 1 as test"])]
              (t/is (= 1 (:test result))))))
        
        (t/testing "failed password authentication"
          (t/is (thrown-with-msg? PSQLException #"password authentication failed for user: test-user"
                                  (jdbc/get-connection (assoc ds-spec :password "wrong-password")))))))))

(t/deftest ^:integration test-oidc-client-credentials
  (let [clients (:clients (authn-test/seed! authn-test/container))
        {:keys [client-id client-secret]} (:test clients)
        port (tu/free-port)
        oidc-config [:openid-connect {:issuer-url (str (.getAuthServerUrl authn-test/container) "/realms/master")
                                      :client-id "xtdb"
                                      :client-secret "xtdb-secret"
                                      :rules [{:user "oid-client" :method :client-credentials}]}]]
    (with-open [_node (xtn/start-node {:authn oidc-config
                                       :server {:port port}})]
      (let [ds-spec {:dbtype "postgresql"
                     :host "localhost"
                     :port port
                     :dbname "xtdb"
                     :user "oid-client"
                     :password (authn-test/encode-client-creds client-id client-secret)}]

        (t/testing "successful client credentials authentication"
          (with-open [conn (jdbc/get-connection ds-spec)]
            (let [result (jdbc/execute-one! conn ["SELECT 'authenticated' as status"])]
              (t/is (= "authenticated" (:status result))))))

        
        (t/testing "failed client credentials authentication with badly encoded credentials"
          (t/is (thrown-with-msg? PSQLException #"ERROR: client credentials error: Invalid base64 encoding: Illegal base64 character 2d"
                                  (jdbc/get-connection (assoc ds-spec :password "bad-client:bad-secret")))))

        (t/testing "failed client credentials authentication with non-existent credentials"
          (t/is (thrown-with-msg? PSQLException #"client credentials authentication failed for user: oid-client"
                                  (jdbc/get-connection (assoc ds-spec :password (authn-test/encode-client-creds "bad-client" "bad-secret"))))))))))

(t/deftest ^:integration test-oidc-session-management
  (authn-test/seed! authn-test/container)
  (let [port (tu/free-port)
        oidc-config [:openid-connect {:issuer-url (str (.getAuthServerUrl authn-test/container) "/realms/master")
                                      :client-id "xtdb"
                                      :client-secret "xtdb-secret"
                                      :rules [{:user "test-user" :method :password :address "127.0.0.1"}]}]]
    (with-open [_node (xtn/start-node {:authn oidc-config
                                       :server {:port port}})]
      (let [ds-spec {:dbtype "postgresql"
                     :host "localhost"
                     :port port  
                     :dbname "xtdb"
                     :user "test-user"
                     :password "password124"}]
        
        (t/testing "session persists across multiple queries"
          (with-open [conn (jdbc/get-connection ds-spec)]
            (let [result1 (jdbc/execute-one! conn ["SELECT 1 as first_query"])
                  result2 (jdbc/execute-one! conn ["SELECT 2 as second_query"])]
              (t/is (= 1 (:first_query result1)))
              (t/is (= 2 (:second_query result2))))))
        
        (t/testing "multiple concurrent connections work"
          (let [conn1 (jdbc/get-connection ds-spec)
                conn2 (jdbc/get-connection ds-spec)]
            (try
              (let [result1 (jdbc/execute-one! conn1 ["SELECT 'conn1' as source"])
                    result2 (jdbc/execute-one! conn2 ["SELECT 'conn2' as source"])]
                (t/is (= "conn1" (:source result1)))
                (t/is (= "conn2" (:source result2))))
              (finally
                (.close conn1)
                (.close conn2)))))))))

(t/deftest ^:integration test-oidc-multiple-auth-methods
  (let [{:keys [clients]} (authn-test/seed! authn-test/container) 
        {:keys [client-id client-secret]} (:test clients)
        port (tu/free-port)
        oidc-config [:openid-connect {:issuer-url (str (.getAuthServerUrl authn-test/container) "/realms/master")
                                      :client-id "xtdb"
                                      :client-secret "xtdb-secret"
                                      :rules [{:user "test-user" :method :password :address "127.0.0.1"}
                                              {:user "oid-client" :method :client-credentials}]}]]
    (with-open [_node (xtn/start-node {:authn oidc-config
                                       :server {:port port}})]
      
      (t/testing "password flow works"
        (let [ds-spec {:dbtype "postgresql" :host "localhost" :port port :dbname "xtdb"
                       :user "test-user" :password "password124"}]
          (with-open [conn (jdbc/get-connection ds-spec)]
            (let [result (jdbc/execute-one! conn ["SELECT 'password-auth' as method"])]
              (t/is (= "password-auth" (:method result)))))))
      
      (t/testing "client credentials flow works"
        (let [ds-spec {:dbtype "postgresql" :host "localhost" :port port :dbname "xtdb"
                       :user "oid-client" :password (authn-test/encode-client-creds client-id client-secret)}]
          (with-open [conn (jdbc/get-connection ds-spec)]
            (let [result (jdbc/execute-one! conn ["SELECT 'client-creds-auth' as method"])]
              (t/is (= "client-creds-auth" (:method result)))))))
      
      (t/testing "wrong method for user fails"
        (let [password-ds-spec {:dbtype "postgresql" :host "localhost" :port port :dbname "xtdb"
                                :user "oid-client" :password "password123"}]
          (t/is (thrown? Exception (jdbc/get-connection password-ds-spec))))))))

(t/deftest ^:integration test-oidc-client-credentials-token-expiry
  (let [clients (:clients (authn-test/seed! authn-test/container :expires-in 3))
        {:keys [client-id client-secret]} (:test clients)
        port (tu/free-port)
        oidc-config [:openid-connect {:issuer-url (str (.getAuthServerUrl authn-test/container) "/realms/master")
                                      :client-id "xtdb"
                                      :client-secret "xtdb-secret"
                                      :rules [{:user "oid-client" :method :client-credentials}]}]]
    (with-open [_node (xtn/start-node {:authn oidc-config
                                       :server {:port port}})]
      (let [ds-spec {:dbtype "postgresql"
                     :host "localhost"
                     :port port
                     :dbname "xtdb"
                     :user "oid-client"
                     :password (authn-test/encode-client-creds client-id client-secret)}]
        
        (t/testing "token expiry and automatic refresh works"
          ;; Initial connection should work
          (with-open [conn (jdbc/get-connection ds-spec)]
            (let [result (jdbc/execute-one! conn ["SELECT 'initial' as status"])]
              (t/is (= "initial" (:status result)))))
          
          ;; Wait for token to expire
          (Thread/sleep 5000)
          
          ;; Connection should still work due to automatic token refresh
          (with-open [conn (jdbc/get-connection ds-spec)]
            (let [result (jdbc/execute-one! conn ["SELECT 'after-expiry' as status"])]
              (t/is (= "after-expiry" (:status result))))))))))

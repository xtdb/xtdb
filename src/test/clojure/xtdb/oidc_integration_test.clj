(ns xtdb.oidc-integration-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc] 
            [xtdb.authn-test :as authn-test]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu])
  (:import [java.util Base64]
           [org.apache.arrow.adbc.core AdbcConnection AdbcDatabase AdbcException AdbcStatusCode]
           [org.apache.arrow.adbc.driver.flightsql FlightSqlDriver]
           [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.postgresql.util PSQLException]
           [xtdb.api Xtdb]))

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
  ;; access-token-lifespan is 2s (see seed-container), so a token minted at T expires at T+2s.
  ;; We drive logical time through a settable clock so the test asserts on time, not on the exact
  ;; number of clock reads the pgwire auth path happens to make.
  (let [{:keys [client-id client-secret]} (:test *clients*)
        !now (atom #inst "2020-01-01T12:00:00Z")
        mock-instant-source (tu/->settable-clock !now)]
    (with-open [node (xtn/start-node {:authn [:openid-connect {:issuer-url (str (.getAuthServerUrl authn-test/container) "/realms/master")
                                                               :client-id "xtdb"
                                                               :client-secret "xtdb-secret"
                                                               :instant-src mock-instant-source
                                                               :rules [{:user "oid-client" :method :client-credentials}]}]})]
      (let [client-connection-builder (-> (.createConnectionBuilder node)
                                          (.user "oid-client")
                                          (.password (format "%s:%s" client-id client-secret)))]

        (t/testing "token expiry and automatic refresh works"
          ;; connection mints a token expiring at 12:00:02
          (with-open [conn (.build client-connection-builder)]
            (t/testing "initial connection and first revalidate"
              (reset! !now #inst "2020-01-01T12:00:01Z") ; before expiry
              (let [result (jdbc/execute-one! conn ["SELECT 'initial' as status"])]
                (t/is (= "initial" (:status result)))))

            (t/testing "after first expiry, token is refreshed"
              (reset! !now #inst "2020-01-01T12:00:05Z") ; past expiry -> revalidate refreshes
              (let [result (jdbc/execute-one! conn ["SELECT 'after-expiry' as status"])]
                (t/is (= "after-expiry" (:status result)))))))))))

(defn- adbc-with-client-creds ^AdbcDatabase [^BufferAllocator al ^Xtdb node client-id client-secret]
  (-> (FlightSqlDriver. al)
      (.open {"uri" (str "grpc+tcp://127.0.0.1:" (.getFlightSqlPort node))
              "adbc.flight.sql.rpc.call_header.authorization"
              (str "Client " (.encodeToString (Base64/getEncoder) (.getBytes (str client-id ":" client-secret) "UTF-8")))
              "adbc.flight.sql.rpc.with_cookie_middleware" true})))

(defn- adbc-row-count [^AdbcConnection conn sql]
  (with-open [stmt (.createStatement conn)]
    (.setSqlQuery stmt sql)
    (with-open [res (.executeQuery stmt)]
      (let [rdr (.getReader res)]
        (.loadNextBatch rdr)
        (.getRowCount (.getVectorSchemaRoot rdr))))))

(defn- flight-sql-oidc-node ^Xtdb [rules]
  (xtn/start-node {:flight-sql {:port 0}
                   :authn [:openid-connect {:issuer-url (str (.getAuthServerUrl authn-test/container) "/realms/master")
                                            :client-id "xtdb"
                                            :client-secret "xtdb-secret"
                                            :rules rules}]}))

(defn- client-creds-rejection
  "nil if a Flight SQL client with these credentials can query `node`, else the ADBC status it's rejected with."
  [al node client-id client-secret]
  (with-open [db (adbc-with-client-creds al node client-id client-secret)]
    (try
      (with-open [conn (.connect db)]
        (adbc-row-count conn "SELECT 1 AS x")
        nil)
      (catch AdbcException e (.getStatus e)))))

(t/deftest ^:integration test-flight-sql-client-credentials
  (let [{:keys [client-id client-secret service-account-user-id]} (:test *clients*)]
    (with-open [al (RootAllocator.)]
      (with-open [node (flight-sql-oidc-node [{:user service-account-user-id :method :client-credentials}
                                              {:method :password}])]
        (with-open [db (adbc-with-client-creds al node client-id client-secret)
                    conn (.connect db)]
          (t/is (= 1 (adbc-row-count conn "SELECT 1 AS x")))
          (t/is (= 1 (adbc-row-count conn "SELECT 2 AS x")) "a later call on the same session"))

        (t/is (= AdbcStatusCode/UNAUTHENTICATED (client-creds-rejection al node client-id "wrong-secret"))))

      (with-open [node (flight-sql-oidc-node [{:user "oid-client" :method :client-credentials}])]
        (t/is (= AdbcStatusCode/UNAUTHENTICATED (client-creds-rejection al node client-id client-secret))
              "a rule naming another user doesn't admit the service account"))

      (with-open [node (flight-sql-oidc-node [{:address "127.0.0.1" :method :password}
                                              {:user service-account-user-id :method :client-credentials}])]
        (t/is (= AdbcStatusCode/UNAUTHENTICATED (client-creds-rejection al node client-id client-secret))
              "an earlier rule for the caller's address decides, as for any other caller")))))

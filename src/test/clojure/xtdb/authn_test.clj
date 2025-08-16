(ns xtdb.authn-test
  (:require [clojure.test :as t]
            [xtdb.authn :as authn] 
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu])
  (:import [dasniko.testcontainers.keycloak KeycloakContainer]
           [java.net URI] 
           [java.time Instant]
           [org.keycloak.representations.idm ClientRepresentation CredentialRepresentation UserRepresentation RealmRepresentation]
           [xtdb.api Authenticator SimpleResult OAuthPasswordResult OAuthClientCredentialsResult OAuthResult]))
 
(defonce ^KeycloakContainer container
  (KeycloakContainer. "quay.io/keycloak/keycloak:26.0"))

(defn seed! 
  ([^KeycloakContainer c] (seed! c {}))
  ([^KeycloakContainer c {:keys [access-token-lifespan]}]
   (with-open [admin-client (.getKeycloakAdminClient c)]
     (when access-token-lifespan
       (let [realm-resource (-> admin-client (.realm "master"))] 
         (.update realm-resource (doto (RealmRepresentation.)
                                   (.setAccessTokenLifespan access-token-lifespan)))))
     {:users (let [users (-> admin-client
                             (.realm "master")
                             (.users))]

               (.create users
                        (doto (UserRepresentation.)
                          (.setEnabled true)
                          (.setUsername "test-user")
                          (.setEmail "test@example.com")))

               (let [user-id (some-> ^UserRepresentation (first (.search users "test-user"))
                                     (.getId))]
                 (-> (.get users user-id)
                     (.resetPassword (doto (CredentialRepresentation.)
                                       (.setType "password")
                                       (.setValue "password124"))))
                 {:test-user user-id}))

      :clients (let [clients (-> admin-client
                                 (.realm "master")
                                 (.clients))]
                 (.create clients (doto (ClientRepresentation.)
                                    (.setName "xtdb")
                                    (.setClientId "xtdb")
                                    (.setSecret "xtdb-secret")
                                    (.setDirectAccessGrantsEnabled true)))
                 (.create clients (doto (ClientRepresentation.)
                                    (.setName "test-client")
                                    (.setId "test-client")
                                    (.setClientId "test-client")
                                    (.setSecret "test-secret")
                                    (.setServiceAccountsEnabled true)))
                 {:xtdb {:client-id "xtdb", :client-secret "xtdb-secret"}
                  :test {:client-id "test-client",
                         :client-secret "test-secret",
                         :service-account-user-id (-> (.get clients "test-client")
                                                      (.getServiceAccountUser)
                                                      (.getId))}})})))

(t/use-fixtures :once
  (fn [f]
    (tu/with-container container
      (fn [_]
        (f)))))

(t/deftest test-token-expiry
  (t/testing "calculates expiry correctly"
    (let [time (Instant/parse "2020-01-01T12:00:00Z")
          token-response {:expires_in 3600}
          expires-at (authn/calculate-expires-at token-response time)]
      (t/is (= (Instant/parse "2020-01-01T13:00:00Z") expires-at))))

  (t/testing "expires at handles missing expires_in"
    (t/is (nil? (authn/calculate-expires-at {} (Instant/parse "2020-01-01T12:00:00Z")))))

  (t/testing "token-expired?"
    (let [oauth-result (OAuthPasswordResult. "user-id" (Instant/parse "2020-01-01T12:00:00Z") "access-token" "refresh-token")]
      (t/is (authn/token-expired? oauth-result (Instant/parse "2020-01-01T13:00:00Z")))
      (t/is (not (authn/token-expired? oauth-result (Instant/parse "2020-01-01T11:00:00Z")))))))

(t/deftest test-invalid-issuer-url
  (t/testing "error caught when issuer URL is invalid"
    (let [invalid-url (.toURL (URI. "http://invalid-url"))]
      (t/is (anomalous? [:incorrect :xtdb/oidc-config-discovery-error
                         #"Error thrown when fetching OIDC configuration from http://invalid-url"]
                        (authn/discover-oidc-config invalid-url)))))
  
  (t/testing "discovery fails with unknown realm"
    (let [unknown-realm-url (.toURL (URI. (str (.getAuthServerUrl container) "/realms/other-realm")))]
      (t/is (anomalous? [:incorrect :xtdb/oidc-config-discovery-error
                         #"Failed to discover OIDC configuration from"]
                        (authn/discover-oidc-config unknown-realm-url))))))

(t/deftest test-refresh-token-errors
  (let [issuer-url (.toURL (URI. (str (.getAuthServerUrl container) "/realms/master")))
        oidc-config (authn/discover-oidc-config issuer-url)
        authn (authn/->OpenIdConnect oidc-config "xtdb" "xtdb-secret"
                                     [{:method #xt.authn/method :password}])]

    (t/is (anomalous? [:incorrect :xtdb/authn-failed "No valid token or refresh token available for refresh"]
                      (authn/refresh-token authn nil)))))

(t/deftest test-oidc-password-flow
  (let [test-user-id (-> (seed! container) 
                         (get-in [:users :test-user]))
        issuer-url (.toURL (URI. (str (.getAuthServerUrl container) "/realms/master")))
        oidc-config (authn/discover-oidc-config issuer-url)
        ^Authenticator authn (authn/->OpenIdConnect oidc-config "xtdb" "xtdb-secret"
                                                    [{:method #xt.authn/method :password}])]
    
    (t/testing "method routing"
      (t/is (= #xt.authn/method :password
               (.methodFor authn "test-user" "127.0.0.1"))))

    (t/testing "successful password authentication"
      (let [^OAuthPasswordResult auth-result (.verifyPassword authn "test-user" "password124")]
        (t/is (= test-user-id (.getUserId auth-result)))))

    (t/testing "authentication failures"
      (t/is (anomalous? [:incorrect :xtdb/authn-failed "Password authentication failed for user: test-user"] 
                        (.verifyPassword authn "test-user" "password123"))))))

(t/deftest test-oidc-client-credential-flow
  (let [clients (:clients (seed! container))
        {:keys [^String client-id ^String client-secret service-account-user-id]} (:test clients)
        issuer-url (.toURL (URI. (str (.getAuthServerUrl container) "/realms/master")))
        oidc-config (authn/discover-oidc-config issuer-url)
        ^Authenticator authn (authn/->OpenIdConnect oidc-config "xtdb" "xtdb-secret"
                                                    [{:method #xt.authn/method :client-credentials}])]
    
    (t/testing "method routing"
      (t/is (= #xt.authn/method :client-credentials (.methodFor authn "oid-client" "127.0.0.1"))))

    (t/testing "successful client credentials authentication"
      (let [^OAuthClientCredentialsResult auth-result (.verifyClientCredentials authn client-id client-secret)]
        (t/is (= service-account-user-id (.getUserId auth-result)))
        (t/is (= client-id (.getClientId auth-result)))
        (t/is (= client-secret (.getClientSecret auth-result)))))

    (t/testing "authentication failures" 
      (t/is (anomalous? [:incorrect :xtdb/authn-failed "Client credentials authentication failed for client: bad-client"]
                        (.verifyClientCredentials authn "bad-client" "client-secret"))))))

(t/deftest test-oidc-node-integration
  (seed! container {:access-token-lifespan (int 3)})  ;; Setup short-lived tokens
  (t/testing "OIDC authentication through XTDB node with token expiry"
    (with-open [node (xtn/start-node {:authn [:openid-connect {:issuer-url (str (.getAuthServerUrl container) "/realms/master")
                                                               :client-id "xtdb"
                                                               :client-secret "xtdb-secret"
                                                               :rules [{:user "test-user" :method :password :address "127.0.0.1"}
                                                                       {:user "oidc-client" :method :client-credentials}]}]})]
      (let [authn (authn/<-node node)
            clients (:clients (seed! container))
            {:keys [client-id client-secret]} (:test clients)]
        
        (t/testing "client credentials flow with real node"
          (let [^OAuthClientCredentialsResult auth-result (.verifyClientCredentials authn client-id client-secret)] 
            (t/is (= client-id (.getClientId auth-result)))
            (t/is (not (authn/token-expired? auth-result (Instant/now))))
            
            ;; Wait for token to expire
            (Thread/sleep 5000)
            
            ;; Token should now be expired
            (t/is (authn/token-expired? auth-result (Instant/now)))
            
            ;; Refresh should work
            (let [^OAuthClientCredentialsResult refreshed-result (authn/refresh-token authn auth-result)] 
              (t/is (= client-id (.getClientId refreshed-result)))
              (t/is (not (authn/token-expired? refreshed-result (Instant/now))))
              (t/is (.isAfter (.getExpiresAt refreshed-result) (.getExpiresAt auth-result))))))))))

(comment
  ;; start these once when you're developing,
  ;; save the time of starting the container for each run
  (.start container)

  (seed! container)

  (.getAuthServerUrl container)

  (.stop container))

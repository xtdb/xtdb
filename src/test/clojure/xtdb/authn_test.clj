(ns xtdb.authn-test
  (:require [clojure.test :as t]
            [xtdb.authn :as authn] 
            [xtdb.test-util :as tu])
  (:import [dasniko.testcontainers.keycloak KeycloakContainer]
           [java.net URI] 
           [java.time Instant InstantSource]
           [org.keycloak.representations.idm ClientRepresentation CredentialRepresentation UserRepresentation RealmRepresentation]
           [xtdb.api Authenticator]))
 
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
    (let [expires-at (Instant/parse "2020-01-01T12:00:00Z")]
      (t/is (authn/token-expired? expires-at (Instant/parse "2020-01-01T13:00:00Z")))
      (t/is (not (authn/token-expired? expires-at (Instant/parse "2020-01-01T11:00:00Z")))))))

(t/deftest test-invalid-issuer-url
  (t/testing "error caught when issuer URL is invalid"
    (let [invalid-url (.toURL (URI. "http://invalid-url"))]
      (t/is (anomalous? [:incorrect :xtdb.authn/oidc-discovery-failed
                         #"Error thrown when fetching OIDC configuration from http://invalid-url"]
                        (authn/discover-oidc-config invalid-url)))))
  
  (t/testing "discovery fails with unknown realm"
    (let [unknown-realm-url (.toURL (URI. (str (.getAuthServerUrl container) "/realms/other-realm")))]
      (t/is (anomalous? [:incorrect :xtdb.authn/oidc-discovery-failed
                         #"Failed to discover OIDC configuration from"]
                        (authn/discover-oidc-config unknown-realm-url))))))

(t/deftest test-revalidate
  (let [issuer-url (.toURL (URI. (str (.getAuthServerUrl container) "/realms/master")))
        oidc-config (authn/discover-oidc-config issuer-url)
        base-time (Instant/parse "2020-01-01T12:00:00Z")
        after-expiry (.plusSeconds base-time 7200)
        mock-instant-source (tu/->mock-clock [base-time base-time base-time after-expiry after-expiry]) 
        ^Authenticator authn (authn/->OpenIdConnect oidc-config "xtdb" "xtdb-secret"
                                                    (authn/->rules-cfg [{:method :client-credentials}])
                                                    mock-instant-source)
        clients (:clients (seed! container {:access-token-lifespan (int 3600)})) 
        {:keys [client-id client-secret]} (:test clients)]
    
    (t/testing "revalidate returns nil for valid token"
      (let [principal (.verifyClientCredentials authn client-id client-secret)] ; uses base-time
        (t/is (nil? (.revalidate principal))))) ; uses base-time - token still valid
    
    (t/testing "revalidate returns refreshed token for expired token"
      (let [principal (.verifyClientCredentials authn client-id client-secret) ; uses base-time
            refreshed (.revalidate principal)] ; uses after-expiry - token expired, fetched a new token at after-expiry
        (t/is (some? refreshed))
        (t/is (= client-id (:client-id refreshed)))
        (t/is (.isAfter ^Instant (:expires-at refreshed) (:expires-at principal)))))))

(t/deftest test-oidc-password-flow
  (let [test-user-id (-> (seed! container) 
                         (get-in [:users :test-user]))
        issuer-url (.toURL (URI. (str (.getAuthServerUrl container) "/realms/master")))
        oidc-config (authn/discover-oidc-config issuer-url)
        ^Authenticator authn (authn/->OpenIdConnect oidc-config "xtdb" "xtdb-secret"
                                                    (authn/->rules-cfg [{:method :password}])
                                                    (InstantSource/system))]
    
    (t/testing "method routing"
      (t/is (= #xt.authn/method :password
               (.methodFor authn "test-user" "127.0.0.1"))))

    (t/testing "successful password authentication"
      (let [principal (.verifyPassword authn "test-user" "password124")]
        (t/is (= test-user-id (.getUserId principal)))))

    (t/testing "authentication failures"
      (t/is (anomalous? [:forbidden :xtdb.authn/failed "Password authentication failed for user: test-user"] 
                        (.verifyPassword authn "test-user" "password123"))))))

(t/deftest test-oidc-client-credential-flow
  (let [clients (:clients (seed! container))
        {:keys [^String client-id ^String client-secret service-account-user-id]} (:test clients)
        issuer-url (.toURL (URI. (str (.getAuthServerUrl container) "/realms/master")))
        oidc-config (authn/discover-oidc-config issuer-url)
        ^Authenticator authn (authn/->OpenIdConnect oidc-config "xtdb" "xtdb-secret"
                                                    (authn/->rules-cfg [{:method :client-credentials}])
                                                    (InstantSource/system))]
    
    (t/testing "method routing"
      (t/is (= #xt.authn/method :client-credentials (.methodFor authn "oid-client" "127.0.0.1"))))

    (t/testing "successful client credentials authentication"
      (let [principal (.verifyClientCredentials authn client-id client-secret)]
        (t/is (= service-account-user-id (.getUserId principal)))
        (t/is (= client-id (:client-id principal)))
        (t/is (= client-secret (:client-secret principal)))))

    (t/testing "authentication failures" 
      (t/is (anomalous? [:forbidden :xtdb.authn/failed "Client credentials authentication failed for client: bad-client"]
                        (.verifyClientCredentials authn "bad-client" "client-secret"))))))

(comment
  ;; start these once when you're developing,
  ;; save the time of starting the container for each run
  (.start container)

  (seed! container)

  (.getAuthServerUrl container)

  (.stop container))

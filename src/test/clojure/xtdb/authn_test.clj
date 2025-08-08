(ns xtdb.authn-test
  (:require [clojure.test :as t]
            [xtdb.authn :as authn] 
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu])
  (:import [dasniko.testcontainers.keycloak KeycloakContainer]
           [java.net URI]
           [java.util Base64]
           [org.keycloak.representations.idm ClientRepresentation CredentialRepresentation UserRepresentation RealmRepresentation]
           [xtdb.api Authenticator]))

(defonce ^KeycloakContainer container
  (KeycloakContainer. "quay.io/keycloak/keycloak:26.0"))

(defn seed! [^KeycloakContainer c & {:keys [access-token-lifespan]}]
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
                                                     (.getId))}})}))

(t/use-fixtures :once
  (fn [f]
    (tu/with-container container
      (fn [_]
        (f)))))

(defn encode-client-creds [^String client-id ^String client-secret]
  (let [client-cred-str (format "%s:%s" client-id client-secret)]
    (.encodeToString (Base64/getEncoder) (.getBytes client-cred-str "UTF-8"))))

(t/deftest test-parse-client-cred
  (t/testing "test correct encoding"
    (let [encoded (encode-client-creds "test-client" "test-secret")
          result (authn/parse-client-creds encoded)]
      (t/is (= {:client-id "test-client" :client-secret "test-secret"} result))))

  (t/testing "test invalid inputs"
    (t/is (= {:error "Missing client credentials"} 
             (authn/parse-client-creds nil)))
    (t/is (= {:error "Empty client credentials"} 
             (authn/parse-client-creds "")))
    (t/is (= {:error "Invalid base64 encoding: Illegal base64 character 2d"} 
             (authn/parse-client-creds "not-base64!")))
    
    (let [no-colon (.encodeToString (Base64/getEncoder) (.getBytes "no-colon" "UTF-8"))]
      (t/is (= {:error "Credentials must contain exactly 2 parts separated by ':'"} 
               (authn/parse-client-creds no-colon))))
    
    (let [too-many-parts (.encodeToString (Base64/getEncoder) (.getBytes "client:secret:extra" "UTF-8"))]
      (t/is (= {:error "Credentials must contain exactly 2 parts separated by ':'"} 
               (authn/parse-client-creds too-many-parts))))))

(t/deftest test-token-expiry
  (t/testing "calculates expiry correctly"
    (let [current-time (System/currentTimeMillis)
          token-response {:expires_in 3600}
          expires-at (authn/calulcate-expires-at token-response)]
      (t/is (> expires-at current-time))
      (t/is (not (authn/token-expired? {:expires-at expires-at})))))
  
  (t/testing "expires at handles missing expires_in"
    (t/is (nil? (authn/calulcate-expires-at {}))))

  (t/testing "detects expired tokens"
    (let [expired-time (- (System/currentTimeMillis) 1000)
          future-time (+ (System/currentTimeMillis) 3600000)]
      (t/is (authn/token-expired? {:expires-at expired-time}))
      (t/is (not (authn/token-expired? {:expires-at future-time})))
      (t/is (not (authn/token-expired? {}))))))

(t/deftest test-invalid-issuer-url
  (t/testing "error caught when issuer URL is invalid"
    (let [invalid-url (.toURL (URI. "http://invalid-url"))]
      (t/is (thrown-with-msg? IllegalArgumentException #"Failed to discover OIDC configuration from" (authn/discover-oidc-config invalid-url)))))
  
  (t/testing "discovery fails with unknown realm"
    (let [unknown-realm-url (.toURL (URI. (str (.getAuthServerUrl container) "/realms/other-realm")))]
      (t/is (nil? (authn/discover-oidc-config unknown-realm-url))))))

(t/deftest test-refresh-token-errors
  (let [issuer-url (.toURL (URI. (str (.getAuthServerUrl container) "/realms/master")))
        oidc-config (authn/discover-oidc-config issuer-url)
        authn (authn/->OpenIdConnect oidc-config "xtdb" "xtdb-secret"
                                     [{:method #xt.authn/method :password}])]

    (t/is (thrown-with-msg? IllegalArgumentException
                            #"No valid token or refresh token available"
                            (authn/refresh-token authn {:refresh-token nil})))))

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
      (let [auth-result (.verifyPassword authn "test-user" "password124")
            oauth-session (:oauth-session auth-result)]
        (t/is (= test-user-id (:user-id auth-result)))
        (t/is (some? oauth-session))
        (t/is (some? (:user-info oauth-session)))
        (t/is (some? (:expires-at oauth-session)))
        (t/is (some? (:refresh-token oauth-session)))))

    (t/testing "authentication failures"
      (t/is (nil? (.verifyPassword authn "test-user" "password123")))
      (t/is (nil? (.verifyPassword authn "testy-mcgee" "password123"))))))

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
      (let [auth-result (.verifyClientCredentials authn (encode-client-creds client-id client-secret))
            oauth-session (:oauth-session auth-result)]
        (t/is (= service-account-user-id (:user-id auth-result)))
        (t/is (some? oauth-session))
        (t/is (some? (:user-info oauth-session)))
        (t/is (some? (:expires-at oauth-session)))
        (t/is (= client-id (:client-id oauth-session)))
        (t/is (= client-secret (:client-secret oauth-session)))))

    (t/testing "authentication failures"
      (t/is (thrown-with-msg? IllegalArgumentException #"Invalid base64 encoding"
                              (.verifyClientCredentials authn (format "%s:%s" client-id client-secret))))
      (t/is (nil? (.verifyClientCredentials authn (encode-client-creds "bad-client" "client-secret")))))))

(t/deftest test-oidc-node-integration
  (seed! container :access-token-lifespan (int 2))  ;; Setup short-lived tokens
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
          (let [auth-result (.verifyClientCredentials authn (encode-client-creds client-id client-secret))
                oauth-session (:oauth-session auth-result)]
            (t/is (some? auth-result))
            (t/is (some? oauth-session))
            (t/is (= client-id (:client-id oauth-session)))
            (t/is (not (authn/token-expired? oauth-session)))
            
            ;; Wait for token to expire
            (Thread/sleep 5000)
            
            ;; Token should now be expired
            (t/is (authn/token-expired? oauth-session))
            
            ;; Refresh should work
            (let [refreshed-session (authn/refresh-token authn oauth-session)]
              (t/is (some? refreshed-session))
              (t/is (not (authn/token-expired? refreshed-session)))
              (t/is (= client-id (:client-id refreshed-session)))
              (t/is (> (:expires-at refreshed-session) (:expires-at oauth-session))))))))))

(comment
  ;; start these once when you're developing,
  ;; save the time of starting the container for each run
  (.start container)

  (seed! container)

  (.getAuthServerUrl container)

  (.stop container))

(ns xtdb.authn
  (:require [buddy.hashers :as hashers]
            [integrant.core :as ig]
            [hato.client :as http]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.query :as q]
            [xtdb.error :as err])
  (:import [java.io Writer]
           (xtdb.api Authenticator Authenticator$DeviceAuthResponse Authenticator$Factory Authenticator$Factory$OpenIdConnect
                     Authenticator$Factory$UserTable Authenticator$Method Authenticator$MethodRule Xtdb$Config
                     SimpleResult OAuthPasswordResult OAuthClientCredentialsResult OAuthResult)
           (xtdb.query IQuerySource)
           [java.net URI]
           [java.time Duration Instant InstantSource]))

(defn verify-pw
  [^IQuerySource q-src db-cat user password]
  (when-not password
    (throw (err/incorrect :xtdb/authn-failed (format "password authentication failed for user: %s" user))))

  (with-open [res (-> (.prepareQuery q-src
                                     "SELECT passwd AS encrypted FROM pg_user WHERE username = ?"
                                     db-cat {:default-db "xtdb"})
                      (.openQuery {:args [user]}))]
    (let [{:keys [encrypted]} (first (.toList (q/cursor->stream res {:key-fn #xt/key-fn :kebab-case-keyword})))]
      (if (and encrypted (:valid (hashers/verify password encrypted)))
        user
        (throw (err/incorrect :xtdb/authn-failed (format "password authentication failed for user: %s" user)))))))

(defn- method-for [rules {:keys [remote-addr user]}]
  (some (fn [{rule-user :user, rule-address :address, :keys [method]}]
          (when (and (or (nil? rule-user) (= user rule-user))
                     (or (nil? rule-address) (= remote-addr rule-address)))
            method))
        rules))

(defn read-authn-method [method]
  (case method
    :trust Authenticator$Method/TRUST
    :password Authenticator$Method/PASSWORD
    :device-auth Authenticator$Method/DEVICE_AUTH
    :client-credentials Authenticator$Method/CLIENT_CREDENTIALS))

(defmethod print-dup Authenticator$Method [^Authenticator$Method m, ^Writer w]
  (.write w "#xt.authn/method ")
  (print-method (case (str m) "TRUST" :trust, "PASSWORD" :password, "DEVICE_AUTH" :device-auth, "CLIENT_CREDENTIALS" :client-credentials) w))

(defmethod print-method Authenticator$Method [^Authenticator$Method m, ^Writer w]
  (print-dup m w))

(defn ->rules-cfg [rules]
  (vec
   (for [{:keys [method user remote-addr]} rules]
     (Authenticator$MethodRule. (read-authn-method method)
                                user remote-addr))))

(defn <-rules-cfg [rules-cfg]
  (vec
   (for [^Authenticator$MethodRule auth-rule rules-cfg]
     {:method (.getMethod auth-rule)
      :user (.getUser auth-rule)
      :remote-addr (.getRemoteAddress auth-rule)})))

(defmethod xtn/apply-config! :xtdb/authn [^Xtdb$Config config, _, [tag opts]]
  (xtn/apply-config! config
                     (case tag
                       :user-table ::user-table-authn
                       :openid-connect ::openid-connect-authn
                       tag)
                     opts))

(defmethod xtn/apply-config! ::user-table-authn [^Xtdb$Config config, _, {:keys [rules]}]
  (.authn config (Authenticator$Factory$UserTable. (->rules-cfg rules))))

(defrecord UserTableAuthn [rules q-src db-cat]
  Authenticator
  (methodFor [_ user remote-addr]
    (method-for rules {:user user, :remote-addr remote-addr}))

  (verifyPassword [_ user password]
    (let [user-id (verify-pw q-src db-cat user password)]
      (SimpleResult. user-id))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn ->user-table-authn [^Authenticator$Factory$UserTable cfg, q-src, db-cat]
  (->UserTableAuthn (<-rules-cfg (.getRules cfg)) q-src db-cat))

(defmethod ig/expand-key :xtdb/authn [k opts]
  {k (into {:q-src (ig/ref :xtdb.query/query-source)
            :db-cat (ig/ref :xtdb/db-catalog)}
           opts)})

(defn- validate-oidc-config [config discovery-url]
  (let [{:keys [token_endpoint userinfo_endpoint]} config]
    (cond
      (not token_endpoint)
      (throw (err/incorrect :xtdb/oidc-config-discovery-error
                            (format "OIDC configuration missing required token_endpoint from %s" discovery-url)
                            {:body config}))

      (not userinfo_endpoint)
      (throw (err/incorrect :xtdb/oidc-config-discovery-error
                            (format "OIDC configuration missing required userinfo_endpoint from %s" discovery-url)
                            {:body config}))

      :else config)))

(defn discover-oidc-config [issuer-url]
  (let [discovery-url (str issuer-url "/.well-known/openid-configuration")]
    (try
      (let [{:keys [status body]} (http/get discovery-url
                                            {:throw-exceptions false
                                             :as :json})]
        (if (= 200 status)
          (validate-oidc-config body discovery-url)
          (throw (err/incorrect :xtdb/oidc-config-discovery-error
                                (format "Failed to discover OIDC configuration from %s: %s" discovery-url (:error body))
                                {:status status, :body body}))))
      (catch Exception e
        (throw (err/incorrect :xtdb/oidc-config-discovery-error
                              (format "Error thrown when fetching OIDC configuration from %s: %s" discovery-url (.getMessage e))
                              {:exception e}))))))

(defn calculate-expires-at [{:keys [expires_in]} ^Instant now]
  (some->> expires_in (.plusSeconds now)))

(defn oauth-token [{:keys [oidc-config client-id client-secret ^InstantSource instant-src]} opts]
  (let [token-endpoint (:token_endpoint oidc-config)
        {:keys [status body]} (http/post token-endpoint
                                         {:form-params (into {:client_id client-id
                                                              :client_secret client-secret}
                                                             opts)
                                          :throw-exceptions false
                                          :coerce :always
                                          :as :json})]
    {:status status
     :body {:access-token (:access_token body)
            :refresh-token (:refresh_token body)
            :expires-at (calculate-expires-at body (.instant instant-src))}}))

(defn oauth-userinfo [{:keys [oidc-config]} token]
  (let [userinfo-endpoint (:userinfo_endpoint oidc-config)
        {:keys [status body]} (http/get userinfo-endpoint
                                        {:oauth-token token
                                         :throw-exceptions false
                                         :as :json})]
    (if (= 200 status)
      {:user-id (:sub body)}
      (throw (err/incorrect :xtdb/authn-failed
                            (format "Failed to obtain user info from %s: %s" userinfo-endpoint (:error body))
                            {:status status, :body body})))))

(defn token-expired? [^OAuthResult auth-result ^Instant now]
  (when-let [expires-at (.getExpiresAt ^OAuthResult auth-result)]
    (.isAfter now expires-at)))

;; TODO: Could live in Kotlin
(defn refresh-token [authn ^OAuthResult auth-result]
  (cond
    ;; For password/device auth flows - use refresh token from OAuthPasswordResult
    (instance? OAuthPasswordResult auth-result)
    (let [oauth-result ^OAuthPasswordResult auth-result
          refresh-token (.getRefreshToken oauth-result)
          {:keys [status body]} (oauth-token authn {:grant_type "refresh_token"
                                                    :refresh_token refresh-token})]
      (if (= 200 status)
        (.withExpiry oauth-result ^Instant (:expires-at body))
        (throw (err/incorrect :xtdb/authn-failed
                              (format "Failed to refresh OAuth token: %s" (:error body))
                              {:status status, :body body}))))

    ;; For client credentials flow - get a fresh token using stored client credentials
    (instance? OAuthClientCredentialsResult auth-result)
    (let [creds-result ^OAuthClientCredentialsResult auth-result
          client-id (.getClientId creds-result)
          client-secret (.getClientSecret creds-result)
          {:keys [status body]} (oauth-token authn {:grant_type "client_credentials"
                                                    :scope "openid"
                                                    :client_id client-id
                                                    :client_secret client-secret})]
      (if (= 200 status)
        (.withExpiry creds-result ^Instant (:expires-at body))
        (throw (err/incorrect :xtdb/authn-failed
                              (format "Failed to refresh client credentials token: %s" (:error body))
                              {:status status, :body body}))))

    :else
    (throw (err/incorrect :xtdb/authn-failed "No valid token or refresh token available for refresh"))))

(defn oauth-device-info [{:keys [oidc-config client-id client-secret]}]
  (let [device-endpoint (:device_authorization_endpoint oidc-config)]
    (when-not device-endpoint
      (throw (err/incorrect :xtdb/authn-failed
                            "OIDC provider does not support device authorization flow - missing device_authorization_endpoint")))
    (let [{:keys [status body]} (http/post device-endpoint
                                           {:form-params {:client_id client-id
                                                          :client_secret client-secret
                                                          :scope "openid"}
                                            :throw-exceptions false
                                            :as :json})]
      (if (= 200 status)
        {:device-code (:device_code body)
         :verification-uri-complete (:verification_uri_complete body)
         :interval (:interval body)
         :expires-at (calculate-expires-at body (.instant (InstantSource/system)))}
        (throw (err/incorrect :xtdb/authn-failed
                              (format "Failed to obtain device info from %s: %s" device-endpoint (:error body))
                              {:status status, :body body}))))))


(defrecord DeviceAuthResponse [authn url device-code ^Duration interval ^Instant expires-at]
  Authenticator$DeviceAuthResponse
  (getUrl [_] url)

  (await [_]
    (let [{:keys [oidc-config ^InstantSource instant-src]} authn]
      (loop []
        (when (.isAfter (.instant instant-src) expires-at) (throw (err/incorrect :xtdb/authn-failed "Device authorization has expired")))

        (let [token-endpoint (:token_endpoint oidc-config)
              {:keys [status body]} (http/post token-endpoint
                                               {:form-params {:client_id (:client-id authn)
                                                              :client_secret (:client-secret authn)
                                                              :grant_type "urn:ietf:params:oauth:grant-type:device_code"
                                                              :device_code device-code}
                                                :throw-exceptions false
                                                :coerce :always
                                                :as :json})]
          (case (long status)
            200 (let [access-token (:access_token body)
                      refresh-token (:refresh_token body)
                      expires-at (calculate-expires-at body (.instant instant-src))
                      {:keys [user-id]} (oauth-userinfo authn access-token)]
                  (OAuthPasswordResult. user-id expires-at access-token refresh-token))
            400 (if (= "authorization_pending" (:error body))
                  (do (Thread/sleep (.toMillis interval)) (recur))
                  (throw (err/incorrect :xtdb/authn-failed
                                        (format "Device authentication failed: %s" (:error body))
                                        {:status status, :body body})))
            (throw (err/incorrect :xtdb/authn-failed
                                  (format "Device authentication failed with status %d" status)
                                  {:status status, :body body}))))))))

(defrecord OpenIdConnect [oidc-config client-id client-secret rules ^InstantSource instant-src]
  Authenticator
  (methodFor [_ user remote-addr]
    (method-for rules {:user user, :remote-addr remote-addr}))

  (verifyPassword [this user password]
    (let [{:keys [status body]} (oauth-token this {:grant_type "password", :scope "openid"
                                                   :username user, :password password})]
      (if (= 200 status)
        (let [{:keys [access-token refresh-token expires-at]} body
              {:keys [user-id]} (oauth-userinfo this access-token)]
          (OAuthPasswordResult. user-id expires-at access-token refresh-token))
        (throw (err/incorrect :xtdb/authn-failed
                              (format "Password authentication failed for user: %s" user)
                              {:status status, :body body})))))

  (startDeviceAuth [this _user]
    (let [{:keys [device-code verification-uri-complete interval expires-at]} (oauth-device-info this)]
      (->DeviceAuthResponse this (.toURL (URI. verification-uri-complete)) device-code (Duration/ofSeconds interval) expires-at)))

  (verifyClientCredentials [this client-id client-secret]
    (let [{:keys [status body]} (oauth-token this {:grant_type "client_credentials",
                                                   :scope "openid",
                                                   :client_id client-id
                                                   :client_secret client-secret})]
      (if (= 200 status)
        (let [{:keys [access-token expires-at]} body
              {:keys [user-id]} (oauth-userinfo this access-token)]
          (OAuthClientCredentialsResult. user-id expires-at access-token client-id client-secret))
        (throw (err/incorrect :xtdb/authn-failed
                              (format "Client credentials authentication failed for client: %s" client-id)
                              {:status status, :body body})))))

  (revalidate [this auth-result]
    (when (token-expired? auth-result (.instant instant-src))
      (refresh-token this auth-result))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn ->oidc-authn [^Authenticator$Factory$OpenIdConnect cfg]
  (let [oidc-config (discover-oidc-config (.getIssuerUrl cfg))]
    (->OpenIdConnect oidc-config
                     (.getClientId cfg)
                     (.getClientSecret cfg)
                     (<-rules-cfg (.getRules cfg))
                     (.getInstantSource cfg))))

(defmethod xtn/apply-config! ::openid-connect-authn [^Xtdb$Config config, _, {:keys [issuer-url client-id client-secret rules ^InstantSource instant-src]}]
  (.authn config
          (cond-> (Authenticator$Factory$OpenIdConnect. (.toURL (URI. issuer-url))
                                                        client-id
                                                        client-secret
                                                        (->rules-cfg rules))
            instant-src (.instantSource instant-src))))

(defmethod ig/init-key :xtdb/authn [_ {:keys [^Authenticator$Factory authn-factory, q-src, db-cat]}]
  (.open authn-factory q-src db-cat))

(defn <-node ^xtdb.api.Authenticator [node] (:authn node))

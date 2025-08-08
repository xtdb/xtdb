(ns xtdb.authn
  (:require [buddy.hashers :as hashers]
            [clojure.string :as string]
            [integrant.core :as ig]
            [hato.client :as http]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.query :as q])
  (:import [java.io Writer]
           (xtdb.api Authenticator Authenticator$Factory Authenticator$Factory$UserTable Authenticator$Method Authenticator$MethodRule Xtdb$Config)
           (xtdb.query IQuerySource)
           [java.net URI]
           [java.time Duration]
           [java.util Base64]
           (xtdb.api Authenticator Authenticator$DeviceAuthResponse Authenticator$Factory Authenticator$Factory$OpenIdConnect Authenticator$Factory$UserTable Authenticator$Method Authenticator$MethodRule Xtdb$Config)))

(defn verify-pw [^IQuerySource q-src, db-cat, user password]
  (when password
    (with-open [res (-> (.prepareQuery q-src "SELECT passwd AS encrypted FROM pg_user WHERE username = ?"
                                       db-cat {:default-db "xtdb"})
                        (.openQuery {:args [user]}))]

      (when-let [{:keys [encrypted]} (first (.toList (q/cursor->stream res {:key-fn #xt/key-fn :kebab-case-keyword})))]
        (when (:valid (hashers/verify password encrypted))
          user)))))

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
    (when-let [user-id (verify-pw q-src db-cat user password)]
      {:user-id user-id})))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn ->user-table-authn [^Authenticator$Factory$UserTable cfg, q-src, db-cat]
  (->UserTableAuthn (<-rules-cfg (.getRules cfg)) q-src db-cat))

(defmethod ig/prep-key :xtdb/authn [_ opts]
  (into {:q-src (ig/ref :xtdb.query/query-source)
         :db-cat (ig/ref :xtdb/db-catalog)}
        opts))

(defn discover-oidc-config [issuer-url]
  (try
    (let [discovery-url (str issuer-url "/.well-known/openid-configuration")
          {:keys [status body]} (http/get discovery-url
                                          {:throw-exceptions false
                                           :as :json})]
      (when (= 200 status)
        body))
    (catch Exception e
      (throw (IllegalArgumentException. (format "Failed to discover OIDC configuration from %s: %s" issuer-url (.getMessage e)))))))

(defn oauth-token [{:keys [oidc-config client-id client-secret]} opts] 
  (let [token-endpoint (:token_endpoint oidc-config)]
    (when token-endpoint
      (http/post token-endpoint
                 {:form-params (into {:client_id client-id
                                      :client_secret client-secret}
                                     opts)
                  :throw-exceptions false
                  :coerce :always
                  :as :json}))))

(defn calulcate-expires-at [oauth-token]
  (when-let [expires-in (:expires_in oauth-token)]
    (+ (System/currentTimeMillis) (* expires-in 1000))))

(defn oauth-userinfo [{:keys [oidc-config]} token]
  (let [userinfo-endpoint (:userinfo_endpoint oidc-config)]
    (when userinfo-endpoint
      (let [{:keys [status body]} (http/get userinfo-endpoint
                                            {:oauth-token token
                                             :throw-exceptions false
                                             :as :json})]
        (when (= 200 status)
          body)))))

(defn token-expired? [token-info]
  (when-let [expires-at (:expires-at token-info)]
    (< expires-at (System/currentTimeMillis))))

(defn refresh-token [authn {:keys [refresh-token client-id client-secret] :as oauth}] 
  (cond
    ;; For password/device auth flows - use refresh token
    refresh-token
    (let [{:keys [status body]} (oauth-token authn {:grant_type "refresh_token"
                                                    :refresh_token (:refresh_token oauth)})]
      (when (= 200 status)
        {:expires-at (calulcate-expires-at body)
         :refresh_token (:refresh_token body)}))
    
    ;; For client credentials flow - get a fresh token using stored client credentials
    ;; (Client credentials flow does not use refresh tokens)
    (and client-id client-secret)
    (let [{:keys [status body]} (oauth-token authn {:grant_type "client_credentials"
                                                    :scope "openid"
                                                    :client_id client-id
                                                    :client_secret client-secret})]
      (when (= 200 status)
        {:expires-at (calulcate-expires-at body)
         :client-id client-id
         :client-secret client-secret}))
    
    :else 
    (throw (IllegalArgumentException. "No valid token or refresh token available for refresh"))))

(defn oauth-device-info [{:keys [oidc-config client-id client-secret]}]
  (let [device-endpoint (:device_authorization_endpoint oidc-config)]
    (when device-endpoint
      (let [{:keys [status body]} (http/post device-endpoint
                                             {:form-params {:client_id client-id
                                                            :client_secret client-secret
                                                            :scope "openid"}
                                              :throw-exceptions false
                                              :as :json})]
        (when (= 200 status)
          body)))))

(defn parse-client-creds [^String credentials]
  (cond
    (nil? credentials)
    {:error "Missing client credentials"}
    
    (string/blank? credentials)
    {:error "Empty client credentials"}
    
    :else
    (try
      (let [decoded (String. (.decode (Base64/getDecoder) credentials) "UTF-8")
            parts (string/split decoded #":")]
        (if (= 2 (count parts))
          (let [[client-id client-secret] parts]
            {:client-id client-id :client-secret client-secret})
          {:error "Credentials must contain exactly 2 parts separated by ':'"}))
      (catch Exception e
        {:error (format "Invalid base64 encoding: %s" (.getMessage e))}))))

(defrecord DeviceAuthResponse [authn url device-code ^Duration interval]
  Authenticator$DeviceAuthResponse
  (getUrl [_] url)

  (await [_]
    (loop []
      (let [{:keys [status body] :as token-response} (oauth-token authn
                                                                 {:grant_type "urn:ietf:params:oauth:grant-type:device_code"
                                                                  :device_code device-code})]
        (case (long status)
          200 (let [user-info (oauth-userinfo authn (:access_token body))]
                {:user-id (:sub user-info)
                 :oauth-session {:user-info user-info
                                :expires-at (calulcate-expires-at token-response)
                                :refresh-token (:refresh_token body)}})
          400 (when (= "authorization_pending" (:error body))
                (Thread/sleep interval)
                (recur)))))))

(defrecord OpenIdConnect [oidc-config client-id client-secret rules]
  Authenticator
  (methodFor [_ user remote-addr]
    (method-for rules {:user user, :remote-addr remote-addr}))

  (verifyPassword [this user password]
    (let [{:keys [status body]} (oauth-token this {:grant_type "password", :scope "openid"
                                                   :username user, :password password})] 
      (when (= status 200)
        (let [user-info (oauth-userinfo this (:access_token body))]
          {:user-id (:sub user-info)
           :oauth-session {:user-info user-info
                          :expires-at (calulcate-expires-at body)
                          :refresh-token (:refresh_token body)}}))))

  (startDeviceAuth [this _user]
    (let [{:keys [device_code verification_uri_complete interval]} (oauth-device-info this)]
      (->DeviceAuthResponse this (.toURL (URI. verification_uri_complete))
                            device_code (Duration/ofSeconds interval))))

  (verifyClientCredentials [this client-credentials]
    (let [parsed (parse-client-creds client-credentials)] 
      (if (:error parsed)
        (throw (IllegalArgumentException. (:error parsed)))
        (let [{:keys [client-id client-secret]} parsed
              {:keys [status body]} (oauth-token this {:grant_type "client_credentials",
                                                       :scope "openid",
                                                       :client_id client-id
                                                       :client_secret client-secret})] 
          (when (= status 200)
            (let [user-info (oauth-userinfo this (:access_token body))] 
              {:user-id (:sub user-info)
               :oauth-session {:user-info user-info
                               :expires-at (calulcate-expires-at body)
                               :client-id client-id
                               :client-secret client-secret}})))))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn ->oidc-authn [^Authenticator$Factory$OpenIdConnect cfg]
  (let [oidc-config (discover-oidc-config (.getIssuerUrl cfg))]
    (when-not oidc-config
      (throw (IllegalArgumentException. (str "Failed to fetch OIDC configuration from: " (.getIssuerUrl cfg)))))
    (->OpenIdConnect oidc-config (.getClientId cfg) (.getClientSecret cfg) (<-rules-cfg (.getRules cfg)))))

(defmethod xtn/apply-config! ::openid-connect-authn [^Xtdb$Config config, _, {:keys [issuer-url client-id client-secret rules]}]
  (let [oidc-config (discover-oidc-config issuer-url)]
    (when-not oidc-config
      (throw (IllegalArgumentException. (str "Failed to fetch OIDC configuration from: " issuer-url))))
    (.authn config (Authenticator$Factory$OpenIdConnect. (.toURL (URI. issuer-url)) client-id client-secret (->rules-cfg rules)))))

(defmethod ig/init-key :xtdb/authn [_ {:keys [^Authenticator$Factory authn-factory, q-src, db-cat]}]
  (.open authn-factory q-src db-cat))

(defn <-node ^xtdb.api.Authenticator [node] (:authn node))

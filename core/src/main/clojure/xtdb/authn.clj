(ns xtdb.authn
  (:require [buddy.hashers :as hashers]
            [integrant.core :as ig]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import [java.io Writer]
           (xtdb.api Authenticator Authenticator$Factory Authenticator$Factory$UserTable Authenticator$Method Authenticator$MethodRule Xtdb$Config)))

(defn encrypt-pw [pw]
  (hashers/derive pw {:alg :argon2id}))

(defn verify-pw [node user password]
  (when password
    (when-let [{:keys [encrypted]} (first (xt/q node ["SELECT passwd AS encrypted FROM pg_user WHERE username = ?" user]))]
      (when (:valid (hashers/verify password encrypted))
        user))))

(defn- method-for [rules {:keys [remote-addr user]}]
  (some (fn [{rule-user :user, rule-address :address, :keys [method]}]
          (when (and (or (nil? rule-user) (= user rule-user))
                     (or (nil? rule-address) (= remote-addr rule-address)))
            method))
        rules))

(defn read-authn-method [method]
  (case method
    :trust Authenticator$Method/TRUST
    :password Authenticator$Method/PASSWORD))

(defmethod print-dup Authenticator$Method [^Authenticator$Method m, ^Writer w]
  (.write w "#xt.authn/method ")
  (print-method (case (str m) "TRUST" :trust, "PASSWORD" :password) w))

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
                       :user-table ::user-table-authn)
                     opts))

(defmethod xtn/apply-config! ::user-table-authn [^Xtdb$Config config, _, {:keys [rules]}]
  (.authn config (Authenticator$Factory$UserTable. (->rules-cfg rules))))

(defrecord UserTableAuthn [rules]
  Authenticator
  (methodFor [_ user remote-addr]
    (method-for rules {:user user, :remote-addr remote-addr}))

  (verifyPassword [_ node user password]
    (verify-pw node user password)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn ->user-table-authn [^Authenticator$Factory$UserTable cfg]
  (->UserTableAuthn (<-rules-cfg (.getRules cfg))))

(defmethod ig/init-key :xtdb/authn [_ ^Authenticator$Factory authn-factory]
  (.open authn-factory))

(def default-authn
  (ig/init-key :xtdb/authn (Authenticator$Factory$UserTable.)))

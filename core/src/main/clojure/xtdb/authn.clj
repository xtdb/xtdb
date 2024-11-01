(ns xtdb.authn
  (:require [buddy.hashers :as hashers]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.api :as xt]
            [xtdb.node :as xtn])
  (:import (xtdb.api AuthnConfig AuthnConfig$Method AuthnConfig$Rule Xtdb$Config)))

(defn encrypt-pw [pw]
  (hashers/derive pw {:alg :argon2id}))

(defn verify-pw [node user password]
  (when-let [{:keys [encrypted]} (first (xt/q node ["SELECT passwd AS encrypted FROM pg_user WHERE username = ?" user]))]
    (when (:valid (hashers/verify password encrypted))
      user)))

(defn first-matching-rule [{:keys [rules]} address user]
  (some (fn [{rule-user :user, rule-address :address, :keys [method]}]
          (when (and (or (= user rule-user) (nil? rule-user))
                     (or (= address rule-address) (nil? rule-address)))
            method))
        rules))

(defn ->authn-config [authn-rules]
  (AuthnConfig. (for [{:keys [user method address]} authn-rules]
                  (AuthnConfig$Rule. user address
                                     (case method
                                       :trust AuthnConfig$Method/TRUST
                                       :password AuthnConfig$Method/PASSWORD)))))

(defn <-authn-config [^AuthnConfig authn-config]
  {:rules (for [^AuthnConfig$Rule auth-rule (.getRules authn-config)]
            {:user (.getUser auth-rule)
             :method (condp = (.getMethod auth-rule)
                       AuthnConfig$Method/TRUST :trust
                       AuthnConfig$Method/PASSWORD :password)
             :address (.getAddress auth-rule)})})

(defmethod xtn/apply-config! :xtdb/authn [^Xtdb$Config config, _, {:keys [rules]}]
  (cond-> config
    (some? rules) (.authn (->authn-config rules))))

(defmethod ig/prep-key :xtdb/authn [_ ^AuthnConfig config]
  (<-authn-config config))

(defmethod ig/init-key :xtdb/authn [_ opts]
  opts)

(def default-authn
  (<-authn-config (AuthnConfig.)))

(ns xtdb.authn
  (:require [buddy.hashers :as hashers]
            [integrant.core :as ig]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.query :as q])
  (:import [java.io Writer]
           (xtdb.api Authenticator Authenticator$Factory Authenticator$Factory$UserTable Authenticator$Method Authenticator$MethodRule Xtdb$Config)
           xtdb.database.Database
           (xtdb.indexer Watermark$Source)
           (xtdb.query IQuerySource)))

(defn verify-pw [^IQuerySource q-src, db, ^Watermark$Source wm-src, user password]
  (when password
    (with-open [res (-> (.prepareQuery q-src "SELECT passwd AS encrypted FROM pg_user WHERE username = ?" db wm-src {})
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

(defrecord UserTableAuthn [rules q-src db wm-src]
  Authenticator
  (methodFor [_ user remote-addr]
    (method-for rules {:user user, :remote-addr remote-addr}))

  (verifyPassword [_ user password]
    (verify-pw q-src db wm-src user password)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn ->user-table-authn [^Authenticator$Factory$UserTable cfg, q-src, db, wm-src]
  (->UserTableAuthn (<-rules-cfg (.getRules cfg)) q-src db wm-src))

(defmethod ig/prep-key :xtdb/authn [_ opts]
  (into {:q-src (ig/ref :xtdb.query/query-source)
         :db (ig/ref :xtdb/database)}
        opts))

(defmethod ig/init-key :xtdb/authn [_ {:keys [^Authenticator$Factory authn-factory, q-src, ^Database db]}]
  (.open authn-factory q-src db (.getLiveIndex db)))

(defn <-node ^xtdb.api.Authenticator [node] (:authn node))

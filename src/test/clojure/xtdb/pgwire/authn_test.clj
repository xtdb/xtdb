(ns xtdb.pgwire.authn-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.authn :as authn]
            [xtdb.authn-test :as authn-test]
            [xtdb.pgwire :as pgwire]
            [xtdb.pgwire-protocol-test :refer [->recording-frontend]]
            [xtdb.test-util :as tu])
  (:import [java.time Clock InstantSource]))

(def ^:dynamic ^:private *port* nil)

(defn with-port [f]
  (let [server (-> tu/*node* :system :xtdb.pgwire/server)]
    (binding [*port* (:port server)]
      (f))))

(t/use-fixtures :each tu/with-allocator tu/with-mock-clock tu/with-node with-port)

(t/use-fixtures :once (fn [f]
                        (tu/with-container authn-test/container
                          (fn [_]
                            (f)))))

(defn ->oidc-conn ^java.lang.AutoCloseable [frontend startup-opts]
  (let [oidc-config (authn/discover-oidc-config (str (.getAuthServerUrl authn-test/container) "/realms/master"))
        authn (authn/->OpenIdConnect oidc-config
                                     "xtdb"
                                     "xtdb-secret"
                                     [{:user "test-user" :method #xt.authn/method :password :address "127.0.0.1"}
                                      {:user "oid-client" :method #xt.authn/method :client-credentials}]
                                     (InstantSource/system))
        conn (pgwire/map->Connection {:server {:server-state (atom {:parameters {"server_encoding" "UTF8"
                                                                                 "client_encoding" "UTF8"
                                                                                 "DateStyle" "ISO"
                                                                                 "IntervalStyle" "ISO_8601"}})
                                               :node tu/*node*
                                               :authn authn}
                                      :allocator tu/*allocator*
                                      :frontend frontend
                                      :cid -1
                                      :!closing? (atom false)
                                      :conn-state (atom {:session {:clock (Clock/systemUTC)}})
                                      :default-db "xtdb"})]
    (try
      (pgwire/cmd-startup-pg30 conn startup-opts)
      (catch Exception e
        (if (#{:xtdb/authn-failed :xtdb/invalid-client-credentials} (:xtdb.error/code (ex-data e)))
          (doto conn
            (pgwire/send-ex e)
            (pgwire/handle-msg* {:msg-name :msg-terminate}))
          (throw e))))))

(deftest ^:integration test-oidc-password-auth-successs
  (let [{{user-id :test-user} :users} (authn-test/seed! authn-test/container)
        {:keys [!in-msgs] :as frontend} (->recording-frontend [{:msg-name :msg-password :password "password124"}])]
    (with-open [_ (->oidc-conn frontend {"user" "test-user", "database" "xtdb"})]
      (t/is (= [[:msg-auth {:result 3}]
                [:msg-auth {:result 0}]
                [:msg-parameter-status {:parameter "server_encoding", :value "UTF8"}]
                [:msg-parameter-status {:parameter "client_encoding", :value "UTF8"}]
                [:msg-parameter-status {:parameter "DateStyle", :value "ISO"}]
                [:msg-parameter-status {:parameter "IntervalStyle", :value "ISO_8601"}]
                [:msg-parameter-status {:parameter "user", :value user-id}]
                [:msg-parameter-status {:parameter "database", :value "xtdb"}]
                [:msg-backend-key-data {:process-id -1, :secret-key 0}]
                [:msg-ready {:status :idle}]]
               @!in-msgs)))))

(deftest ^:integration test-oidc-password-auth-failure
  (let [_ (authn-test/seed! authn-test/container)
        {:keys [!in-msgs] :as frontend} (->recording-frontend [{:msg-name :msg-password :password "wrong-password"}])]
    (with-open [_ (->oidc-conn frontend {"user" "test-user", "database" "xtdb"})]
      (t/is (= [[:msg-auth {:result 3}]
                [:msg-error-response
                 {:error-fields
                  {:severity "ERROR",
                   :localized-severity "ERROR",
                   :sql-state "28P01",
                   :message "Password authentication failed for user: test-user"
                   :detail {:category "cognitect.anomalies/incorrect",
                            :status 401,
                            :code "xtdb/authn-failed",
                            :body {:refresh-token nil, :expires-at nil, :access-token nil}
                            :message "Password authentication failed for user: test-user"}}}]]
               @!in-msgs)))))

(deftest ^:integration test-oidc-client-credentials-auth-success
  (let [{:keys [clients]} (authn-test/seed! authn-test/container)
        {:keys [client-id client-secret service-account-user-id]} (:test clients)
        client-password (format "%s:%s" client-id client-secret)
        {:keys [!in-msgs] :as frontend} (->recording-frontend [{:msg-name :msg-password :password client-password}])]
    (with-open [_ (->oidc-conn frontend {"user" "oid-client", "database" "xtdb"})]
      (t/is (= [[:msg-auth {:result 3}]
                [:msg-auth {:result 0}]
                [:msg-parameter-status {:parameter "server_encoding", :value "UTF8"}]
                [:msg-parameter-status {:parameter "client_encoding", :value "UTF8"}]
                [:msg-parameter-status {:parameter "DateStyle", :value "ISO"}]
                [:msg-parameter-status {:parameter "IntervalStyle", :value "ISO_8601"}]
                [:msg-parameter-status {:parameter "user", :value service-account-user-id}]
                [:msg-parameter-status {:parameter "database", :value "xtdb"}]
                [:msg-backend-key-data {:process-id -1, :secret-key 0}]
                [:msg-ready {:status :idle}]]
               @!in-msgs)))))

(deftest ^:integration test-oidc-client-credentials-auth-failure
  (let [_ (authn-test/seed! authn-test/container)
        {:keys [!in-msgs] :as frontend} (->recording-frontend [{:msg-name :msg-password :password "too:many:colons"}])]
    (with-open [_ (->oidc-conn frontend {"user" "oid-client", "database" "xtdb"})]
      (t/is (= [[:msg-auth {:result 3}]
                [:msg-error-response
                 {:error-fields
                  {:severity "ERROR",
                   :localized-severity "ERROR",
                   :sql-state "28P01",
                   :message "Client credentials must be provided in the format 'client-id:client-secret'",
                   :detail {:category "cognitect.anomalies/incorrect",
                            :code "xtdb/invalid-client-credentials",
                            :message "Client credentials must be provided in the format 'client-id:client-secret'"}}}]]
               @!in-msgs)))))

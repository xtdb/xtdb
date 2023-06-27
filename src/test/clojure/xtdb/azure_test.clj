(ns xtdb.azure-test
  (:require [clojure.java.shell :as sh]
            [clojure.test :as t]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.api :as xt]
            [xtdb.azure :as azure]
            [xtdb.object-store-test :as os-test]
            [xtdb.node :as node]
            [clojure.tools.logging :as log])
  (:import java.util.UUID
           (java.io Closeable)))

(def resource-group-name "azure-modules-test")
(def storage-account "xtdbazureobjectstoretest")
(def container "xtdb-test")
(def eventhub-namespace "xtdbeventhublogtest")
(def config-present? (some? (and (System/getenv "AZURE_CLIENT_ID")
                                 (System/getenv "AZURE_CLIENT_SECRET")
                                 (System/getenv "AZURE_TENANT_ID")
                                 (System/getenv "AZURE_SUBSCRIPTION_ID"))))

(defn cli-available? []
  (= 0 (:exit (sh/sh "az" "--help"))))

(defn logged-in? []
  (= 0 (:exit (sh/sh "az" "account" "show"))))

(defn run-if-auth-available [f]
  (if (cli-available?)
    (if (logged-in?)
      (f)
      (log/warn "azure cli appears to be available but you are not logged in, run `az login` before running the tests"))
    (if config-present?
      (f)
      (log/warn "azure cli is unavailable and the auth env vars are not set: AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID, AZURE_SUBSCRIPTION_ID"))))

(t/use-fixtures :once run-if-auth-available)

(defn object-store ^Closeable [prefix]
  (->> (ig/prep-key ::azure/blob-object-store {:storage-account storage-account
                                               :container container
                                               :prefix (str "xtdb.azure-test." prefix)})
       (ig/init-key ::azure/blob-object-store)))

(t/deftest ^:azure put-delete-test
  (let [os (object-store (random-uuid))]
    (os-test/test-put-delete os)))

(t/deftest ^:azure range-test
  (let [os (object-store (random-uuid))]
    (os-test/test-range os)))

(t/deftest ^:azure list-test
  (let [os (object-store (random-uuid))]
    (os-test/test-list-objects os)))

(t/deftest ^:azure test-eventhub-log
  (with-open [node (node/start-node {::azure/event-hub-log {:namespace eventhub-namespace
                                                            :resource-group-name resource-group-name
                                                            :event-hub-name (str "xtdb.azure-test-hub." (UUID/randomUUID))
                                                            :create-event-hub? true
                                                            :retention-period-in-days 1}})]
    (xt/submit-tx node [[:put :xt_docs {:xt/id :foo}]])
    (t/is (= [{:id :foo}]
             (xt/q node '{:find [id]
                          :where [($ :xt_docs [{:xt/id id}])]})))))

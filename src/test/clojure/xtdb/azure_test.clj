(ns xtdb.azure-test
  (:require [clojure.test :as t]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.api :as xt]
            [xtdb.azure :as azure]
            [xtdb.object-store-test :as os-test]
            [xtdb.node :as node]
            [xtdb.test-util :as tu]
            [clojure.tools.logging :as log])
  (:import java.util.UUID))

(def resource-group-name "azure-modules-test")
(def storage-account "xtdbazureobjectstoretest")
(def container "xtdb-test")
(def eventhub-namespace "xtdbeventhublogtest")
(def config-present? (some? (and (System/getenv "AZURE_CLIENT_ID")
                                 (System/getenv "AZURE_CLIENT_SECRET")
                                 (System/getenv "AZURE_TENANT_ID")
                                 (System/getenv "AZURE_SUBSCRIPTION_ID"))))

(def ^:dynamic *obj-store*)

(os-test/def-obj-store-tests ^:azure azure [f]
  (log/info "Azure config present (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_SUBCRIPTION_ID & AZURE_TENANT_ID set)? - " config-present?)
  (when config-present?
    (let [sys (-> {::azure/blob-object-store {:storage-account storage-account
                                              :container container
                                              :prefix (str "xtdb.azure-test." (UUID/randomUUID))}}
                  ig/prep
                  ig/init)]
      (try
        (binding [*obj-store* (::azure/blob-object-store sys)]
          (f *obj-store*))
        (finally
          (ig/halt! sys))))))

(t/deftest ^:azure test-eventhub-log
  (log/info "Azure config present (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_SUBCRIPTION_ID & AZURE_TENANT_ID set)? - " config-present?)
  (when config-present?
    (with-open [node (node/start-node {::azure/event-hub-log {:namespace eventhub-namespace
                                                              :resource-group-name resource-group-name
                                                              :event-hub-name (str "xtdb.azure-test-hub." (UUID/randomUUID))
                                                              :create-event-hub? true
                                                              :retention-period-in-days 1}})]
      (xt/submit-tx node [[:put :xt_docs {:xt/id :foo}]])
      (t/is (= [{:id :foo}]
               (xt/q node '{:find [id]
                            :where [($ :xt_docs [{:xt/id id}])]}))))))

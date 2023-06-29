(ns xtdb.azure.blobs-test
  (:require [xtdb.azure.blobs :as azb]
            [clojure.test :as t]
            [clojure.tools.logging :as log]
            [xtdb.fixtures.document-store :as fix.ds]
            [xtdb.system :as sys]))

(def storage-account "xtdbazureobjectstoretest")
(def container "xtdb-test")
(def config-present? (some? (and (System/getenv "AZURE_CLIENT_ID")
                                 (System/getenv "AZURE_CLIENT_SECRET")
                                 (System/getenv "AZURE_TENANT_ID")
                                 (System/getenv "AZURE_SUBSCRIPTION_ID"))))

(t/deftest test-blobs-doc-store
  (log/info "Azure config present (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_SUBSCRIPTION_ID & AZURE_TENANT_ID set)? - " config-present?)
  (when config-present?
    (with-open [sys (-> (sys/prep-system {::azb/document-store
                                          {:storage-account storage-account
                                           :container container}})
                        (sys/start-system))]

      (fix.ds/test-doc-store (::azb/document-store sys)))))

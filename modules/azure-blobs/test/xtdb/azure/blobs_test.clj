(ns xtdb.azure.blobs-test
  (:require [clojure.test :as t]
            [clojure.tools.logging :as log]
            [xtdb.azure.blobs :as azb]
            [xtdb.codec :as c]
            [xtdb.db :as db]
            [xtdb.fixtures.document-store :as fix.ds]
            [xtdb.system :as sys])
   (:import java.util.UUID))

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

(t/deftest test-blobs-doc-store-with-prefix
  (log/info "Azure config present (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_SUBSCRIPTION_ID & AZURE_TENANT_ID set)? - " config-present?)
  (when config-present?
    (with-open [sys (-> (sys/prep-system {::azb/document-store
                                          {:storage-account storage-account
                                           :container container
                                           :prefix (str "test-prefix-" (UUID/randomUUID))}})
                        (sys/start-system))]

      (fix.ds/test-doc-store (::azb/document-store sys)))))

(t/deftest test-blobs-issue-2602
  (log/info "Azure config present (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_SUBSCRIPTION_ID & AZURE_TENANT_ID set)? - " config-present?)
  (when config-present?
    (with-open [sys (-> (sys/prep-system {::azb/document-store
                                          {:storage-account storage-account
                                           :container container}})
                        (sys/start-system))]
      (let [doc-store (::azb/document-store sys)
            keyword-document {:xt/id :test-entity
                              :name "test entity with keyword"}
            keyword-key (c/new-id keyword-document)
            uuid-document {:xt/id (UUID/randomUUID)
                           :name "test entity with UUID"}
            uuid-key (c/new-id uuid-document)]

        (t/testing "can insert and fetch keyword id document"
          (db/submit-docs doc-store {keyword-key keyword-document})

          (t/is (= {keyword-key keyword-document}
                   (db/fetch-docs doc-store [keyword-key]))))

        (t/testing "can insert and fetch UUID document"
          (db/submit-docs doc-store {uuid-key uuid-document})

          (t/is (= {uuid-key uuid-document}
                   (db/fetch-docs doc-store [uuid-key]))))))))

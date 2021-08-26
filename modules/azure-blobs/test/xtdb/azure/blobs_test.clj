(ns xtdb.azure.blobs-test
  (:require [xtdb.azure.blobs :as azb]
            [clojure.test :as t]
            [crux.fixtures.document-store :as fix.ds]
            [crux.system :as sys]))

(def test-azure-blobs-sas-token
  (System/getenv "XTDB_AZURE_BLOBS_TEST_SAS_TOKEN"))

(def test-azure-blobs-storage-account
  (or (System/getProperty "xtdb.azure.blobs.test-storage-account")
      (System/getenv "XTDB_AZURE_BLOBS_TEST_STORAGE_ACCOUNT")))

(def test-azure-blobs-container
  (or (System/getProperty "xtdb.azure.blobs.test-container")
      (System/getenv "XTDB_AZURE_BLOBS_TEST_CONTAINER")))

(t/deftest test-blobs-doc-store
  (when (and test-azure-blobs-sas-token
             test-azure-blobs-storage-account
             test-azure-blobs-container)
    (with-open [sys (-> (sys/prep-system {::azb/document-store
                                          {:sas-token test-azure-blobs-sas-token
                                           :storage-account test-azure-blobs-storage-account
                                           :container test-azure-blobs-container}})
                        (sys/start-system))]

      (fix.ds/test-doc-store (::azb/document-store sys)))))

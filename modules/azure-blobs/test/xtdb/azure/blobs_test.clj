(ns xtdb.azure.blobs-test
  (:require [xtdb.azure.blobs :as azb]
            [clojure.test :as t]
            [xtdb.codec :as c]
            [xtdb.db :as db]
            [xtdb.fixtures.document-store :as fix.ds]
            [xtdb.system :as sys])
  (:import java.util.UUID))

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

(t/deftest test-blobs-issue-2602
  (when (and test-azure-blobs-sas-token
             test-azure-blobs-storage-account
             test-azure-blobs-container)
    (with-open [sys (-> (sys/prep-system {::azb/document-store
                                          {:sas-token test-azure-blobs-sas-token
                                           :storage-account test-azure-blobs-storage-account
                                           :container test-azure-blobs-container}})
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

(ns xtdb.azure.blobs-test
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.test :as t]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.azure.blobs :as azb]
            [xtdb.checkpoint :as cp]
            [xtdb.codec :as c]
            [xtdb.db :as db]
            [xtdb.fixtures :as fix]
            [xtdb.fixtures.checkpoint-store :as fix.cp-store]
            [xtdb.fixtures.document-store :as fix.ds]
            [xtdb.system :as sys])
   (:import java.util.UUID
            java.util.Date))

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

(t/deftest test-checkpoint-store
  (log/info "Azure config present (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_SUBSCRIPTION_ID & AZURE_TENANT_ID set)? - " config-present?)
  (when config-present?
    (with-open [sys (-> (sys/prep-system {:store {:xtdb/module `azb/->cp-store
                                                  :storage-account storage-account
                                                  :container container
                                                  :prefix (str "test-checkpoint-" (UUID/randomUUID))}})
                        (sys/start-system))]
      (fix.cp-store/test-checkpoint-store (:store sys)))))

(t/deftest test-checkpoint-store-cleanup
  (with-open [sys (-> (sys/prep-system {:store {:xtdb/module `azb/->cp-store
                                                :storage-account storage-account
                                                :container container
                                                :prefix (str "test-checkpoint-" (UUID/randomUUID))}})
                      (sys/start-system))]
    (fix/with-tmp-dirs #{dir}
      (let [cp-at (Date.)
            cp-store (:store sys)
            ;; create file for upload
            _ (spit (io/file dir "hello.txt") "Hello world")
            {:keys [::azb/azure-dir]} (cp/upload-checkpoint cp-store dir {::cp/cp-format ::foo-cp-format
                                                                          :tx {::xt/tx-id 1}
                                                                          :cp-at cp-at})
            metadata-file (string/replace (str "metadata-" azure-dir) #"/" ".edn")]

        (t/testing "call to upload-checkpoint creates expected folder & checkpoint metadata file for the checkpoint"
          (let [blob-set (set (azb/list-blobs cp-store nil))]
            (t/is (= 2 (count blob-set)))
            (t/is (contains? blob-set metadata-file))
            (t/is (contains? blob-set (str azure-dir "hello.txt")))))

        (t/testing "call to `cleanup-checkpoints` entirely removes an uploaded checkpoint and metadata"
          (cp/cleanup-checkpoint cp-store {:tx {::xt/tx-id 1}
                                           :cp-at cp-at})
          (t/is (empty? (azb/list-blobs cp-store nil))))))))

(t/deftest test-checkpoint-store-failed-cleanup
  (with-open [sys (-> (sys/prep-system {:store {:xtdb/module `azb/->cp-store
                                                :storage-account storage-account
                                                :container container
                                                :prefix (str "test-checkpoint-" (UUID/randomUUID))}})
                      (sys/start-system))]
    (fix/with-tmp-dirs #{dir}
      (let [cp-at (Date.)
            cp-store (:store sys)
            ;; create file for upload
            _ (spit (io/file dir "hello.txt") "Hello world")
            {:keys [::azb/azure-dir]} (cp/upload-checkpoint cp-store dir {::cp/cp-format ::foo-cp-format
                                                                          :tx {::xt/tx-id 1}
                                                                          :cp-at cp-at})
            metadata-file (string/replace (str "metadata-" azure-dir) #"/" ".edn")]

        (t/testing "call to upload-checkpoint creates expected folder & checkpoint metadata file for the checkpoint"
          (let [blob-set (set (azb/list-blobs cp-store nil))]
            (t/is (= 2 (count blob-set)))
            (t/is (contains? blob-set metadata-file))
            (t/is (contains? blob-set (str azure-dir "hello.txt")))))

        (t/testing "error in `cleanup-checkpoints` after deleting checkpoint metadata file still leads to checkpoint not being available"
          (with-redefs [azb/list-blobs (fn [_ _] (throw (Exception. "Test Exception")))]
            (t/is (thrown-with-msg? Exception
                                    #"Test Exception"
                                    (cp/cleanup-checkpoint cp-store {:tx {::xt/tx-id 1}
                                                                     :cp-at cp-at}))))
          ;; Only directory should be available - checkpoint metadata file should have been deleted
          (t/is (= [(str azure-dir "hello.txt")]
                   (azb/list-blobs cp-store nil)))
          ;; Should not be able to fetch checkpoint as checkpoint metadata file is gone
          (t/is (empty? (cp/available-checkpoints cp-store {::cp/cp-format ::foo-cp-format}))))))))

(t/deftest test-checkpoint-store-cleanup-no-edn-file
  (with-open [sys (-> (sys/prep-system {:store {:xtdb/module `azb/->cp-store
                                                :storage-account storage-account
                                                :container container
                                                :prefix (str "test-checkpoint-" (UUID/randomUUID))}})
                      (sys/start-system))]
    (fix/with-tmp-dirs #{dir}
      (let [cp-at (Date.)
            cp-store (:store sys)
            ;; create file for upload
            _ (spit (io/file dir "hello.txt") "Hello world")
            {:keys [::azb/azure-dir]} (cp/upload-checkpoint cp-store dir {::cp/cp-format ::foo-cp-format
                                                                          :tx {::xt/tx-id 1}
                                                                          :cp-at cp-at})
            metadata-file (string/replace (str "metadata-" azure-dir) #"/" ".edn")]

        ;; delete the checkpoint file
        (azb/delete-blob cp-store metadata-file)

        (t/testing "checkpoint folder present, edn file should be deleted"
          (let [blob-set (set (azb/list-blobs cp-store nil))]
            (t/is (= 1 (count blob-set)))
            (t/is (not (contains? blob-set metadata-file)))
            (t/is (contains? blob-set (str azure-dir "hello.txt")))))

        (t/testing "call to `cleanup-checkpoints` with no edn file should still remove an uploaded checkpoint and metadata"
          (cp/cleanup-checkpoint cp-store {:tx {::xt/tx-id 1}
                                           :cp-at cp-at})
          (t/is (empty? (azb/list-blobs cp-store nil))))))))

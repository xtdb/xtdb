(ns xtdb.google.cloud-storage-test
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.checkpoint :as cp]
            [xtdb.fixtures :as fix]
            [xtdb.fixtures.document-store :as fix.ds]
            [xtdb.fixtures.checkpoint-store :as fix.cp]
            [xtdb.google.cloud-storage :as gcs]
            [xtdb.system :as sys])
  (:import java.util.UUID
           java.util.Date
           [com.google.cloud.storage StorageOptions Storage$BucketGetOption Bucket$BucketSourceOption StorageException]))

(def project-id "xtdb-scratch")
(def test-bucket "xtdb-cloud-storage-test-bucket")

;; Check google auth & that bucket exists 
(t/use-fixtures :once
  (fn [f]
    (try
      (let [storage (-> (StorageOptions/newBuilder)
                        (.setProjectId project-id)
                        (.build)
                        (.getService))
            bucket (.get storage test-bucket (into-array Storage$BucketGetOption []))]
        (when (.exists bucket (into-array Bucket$BucketSourceOption []))
          (f)))
      (catch StorageException e
        (if (= 401(.getCode e))
          nil
          (throw e))))))

(t/deftest test-doc-store
  (with-open [sys (-> (sys/prep-system {::gcs/document-store {:project-id project-id
                                                              :bucket test-bucket}})
                      (sys/start-system))]

    (fix.ds/test-doc-store (::gcs/document-store sys))))

(t/deftest test-doc-store-with-prefix
  (with-open [sys (-> (sys/prep-system {::gcs/document-store {:project-id project-id
                                                              :bucket test-bucket
                                                              :prefix (format "test-doc-store-%s" (UUID/randomUUID))}})
                      (sys/start-system))]

    (fix.ds/test-doc-store (::gcs/document-store sys))))

(t/deftest test-cp-store
  (with-open [sys (-> (sys/prep-system {::gcs/checkpoint-store {:project-id project-id
                                                                :bucket test-bucket
                                                                :prefix (format "test-checkpoint-store-%s" (UUID/randomUUID))}})
                      (sys/start-system))]

    (fix.cp/test-checkpoint-store (::gcs/checkpoint-store sys))))

(t/deftest test-checkpoint-store-cleanup
  (with-open [sys (-> (sys/prep-system {::gcs/checkpoint-store {:project-id project-id
                                                                :bucket test-bucket
                                                                :prefix (format "test-checkpoint-store-%s" (UUID/randomUUID))}})
                      (sys/start-system))]
    (fix/with-tmp-dirs #{dir}
      (let [cp-at (Date.)
            cp-store (::gcs/checkpoint-store sys)
            ;; create file for upload
            _ (spit (io/file dir "hello.txt") "Hello world")
            {:keys [::gcs/gcs-dir]} (cp/upload-checkpoint cp-store dir {::cp/cp-format ::foo-cp-format
                                                                        :tx {::xt/tx-id 1}
                                                                        :cp-at cp-at})
            metadata-file (string/replace (str "metadata-" gcs-dir) #"/" ".edn")]

        (t/testing "call to upload-checkpoint creates expected folder & checkpoint metadata file for the checkpoint"
          (let [blob-set (set (gcs/list-blobs cp-store nil))]
            (t/is (= 2 (count blob-set)))
            (t/is (contains? blob-set metadata-file))
            (t/is (contains? blob-set (str gcs-dir "hello.txt")))))

        (t/testing "call to `cleanup-checkpoints` entirely removes an uploaded checkpoint and metadata"
          (cp/cleanup-checkpoint cp-store {:tx {::xt/tx-id 1}
                                           :cp-at cp-at})
          (t/is (empty? (gcs/list-blobs cp-store nil))))))))


(t/deftest test-checkpoint-store-failed-cleanup
  (with-open [sys (-> (sys/prep-system {::gcs/checkpoint-store {:project-id project-id
                                                                :bucket test-bucket
                                                                :prefix (format "test-checkpoint-store-%s" (UUID/randomUUID))}})
                      (sys/start-system))]
    (fix/with-tmp-dirs #{dir}
      (let [cp-at (Date.)
            cp-store (::gcs/checkpoint-store sys)
            ;; create file for upload
            _ (spit (io/file dir "hello.txt") "Hello world")
            {:keys [::gcs/gcs-dir]} (cp/upload-checkpoint cp-store dir {::cp/cp-format ::foo-cp-format
                                                                        :tx {::xt/tx-id 1}
                                                                        :cp-at cp-at})
            metadata-file (string/replace (str "metadata-" gcs-dir) #"/" ".edn")]

        (t/testing "call to upload-checkpoint creates expected folder & checkpoint metadata file for the checkpoint"
          (let [blob-set (set (gcs/list-blobs cp-store nil))]
            (t/is (= 2 (count blob-set)))
            (t/is (contains? blob-set metadata-file))
            (t/is (contains? blob-set (str gcs-dir "hello.txt")))))

        (t/testing "error in `cleanup-checkpoints` after deleting checkpoint metadata file still leads to checkpoint not being available"
          (with-redefs [gcs/list-blobs (fn [_ _] (throw (Exception. "Test Exception")))]
            (t/is (thrown-with-msg? Exception
                                    #"Test Exception"
                                    (cp/cleanup-checkpoint cp-store {:tx {::xt/tx-id 1}
                                                                     :cp-at cp-at}))))
          ;; Only directory should be available - checkpoint metadata file should have been deleted
          (t/is (= [(str gcs-dir "hello.txt")]
                   (gcs/list-blobs cp-store nil)))
          ;; Should not be able to fetch checkpoint as checkpoint metadata file is gone
          (t/is (empty? (cp/available-checkpoints cp-store {::cp/cp-format ::foo-cp-format}))))))))

(t/deftest test-checkpoint-store-cleanup-no-edn-file
  (with-open [sys (-> (sys/prep-system {::gcs/checkpoint-store {:project-id project-id
                                                                :bucket test-bucket
                                                                :prefix (format "test-checkpoint-store-%s" (UUID/randomUUID))}})
                      (sys/start-system))]
    (fix/with-tmp-dirs #{dir}
      (let [cp-at (Date.)
            cp-store (::gcs/checkpoint-store sys)
            ;; create file for upload
            _ (spit (io/file dir "hello.txt") "Hello world")
            {:keys [::gcs/gcs-dir]} (cp/upload-checkpoint cp-store dir {::cp/cp-format ::foo-cp-format
                                                                        :tx {::xt/tx-id 1}
                                                                        :cp-at cp-at})
            metadata-file (string/replace (str "metadata-" gcs-dir) #"/" ".edn")]

        ;; delete the checkpoint file
        (gcs/delete-blob cp-store metadata-file)

        (t/testing "checkpoint folder present, edn file should be deleted"
          (let [blob-set (set (gcs/list-blobs cp-store nil))]
            (t/is (= 1 (count blob-set)))
            (t/is (not (contains? blob-set metadata-file)))
            (t/is (contains? blob-set (str gcs-dir "hello.txt")))))

        (t/testing "call to `cleanup-checkpoints` with no edn file should still remove an uploaded checkpoint and metadata"
          (cp/cleanup-checkpoint cp-store {:tx {::xt/tx-id 1}
                                           :cp-at cp-at})
          (t/is (empty? (gcs/list-blobs cp-store nil))))))))

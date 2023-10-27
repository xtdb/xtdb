(ns xtdb.azure-test
  (:require [clojure.java.shell :as sh]
            [clojure.test :as t]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.api :as xt]
            [xtdb.azure :as azure]
            [xtdb.object-store-test :as os-test]
            [xtdb.node :as node]
            [clojure.tools.logging :as log]
            [clojure.set :as set])
  (:import [java.util UUID]
           [java.io Closeable]
           [java.nio ByteBuffer]
           [com.azure.storage.blob.models ListBlobsOptions BlobListDetails BlobItem]
           [com.azure.storage.blob BlobContainerClient]
           [xtdb.object_store ObjectStore SupportsMultipart IMultipartUpload]))

(def resource-group-name "azure-modules-test")
(def storage-account "xtdbteststorageaccount")
(def container "xtdb-test-object-store")
(def eventhub-namespace "xtdb-test-storage-account-eventbus")
(def servicebus-namespace "xtdb-test-storage-account-eventbus")
(def servicebus-topic-name "xtdb-test-storage-bus-topic")
(def config-present? (some? (and (System/getenv "AZURE_CLIENT_ID")
                                 (System/getenv "AZURE_CLIENT_SECRET")
                                 (System/getenv "AZURE_TENANT_ID")
                                 (System/getenv "AZURE_SUBSCRIPTION_ID"))))

(defn cli-available? []
  (= 0 (:exit (sh/sh "az" "--help"))))

(defn logged-in? []
  (= 0 (:exit (sh/sh "az" "account" "show"))))

(defn run-if-auth-available [f]
  (cond
    config-present? (f)

    (not (cli-available?))
    (log/warn "azure cli is unavailable and the auth env vars are not set: AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID, AZURE_SUBSCRIPTION_ID")

    (not (logged-in?))
    (log/warn "azure cli appears to be available but you are not logged in, run `az login` before running the tests")

    :else (f)))

(t/use-fixtures :once run-if-auth-available)

(defn object-store ^Closeable [prefix]
  (->> (ig/prep-key ::azure/blob-object-store {:storage-account storage-account
                                               :container container
                                               :servicebus-namespace servicebus-namespace
                                               :servicebus-topic-name servicebus-topic-name
                                               :prefix (str "xtdb.azure-test." prefix)})
       (ig/init-key ::azure/blob-object-store)))

(t/deftest ^:azure put-delete-test
  (with-open [os (object-store (random-uuid))]
    (os-test/test-put-delete os)))

(t/deftest ^:azure range-test
  (with-open [os (object-store (random-uuid))]
    (os-test/test-range os)))

(t/deftest ^:azure list-test
  (with-open [os (object-store (random-uuid))]
    (os-test/test-list-objects os)))

(t/deftest ^:azure list-test-with-prior-objects
  (let [prefix (random-uuid)]
    (with-open [os (object-store prefix)]
      (os-test/put-edn os "alice" :alice)
      (os-test/put-edn os "alan" :alan)
      (t/is (= ["alan" "alice"] (.listObjects ^ObjectStore os))))

    (with-open [os (object-store prefix)]
      (t/testing "prior objects will still be there, should be available on a list request"
        (t/is (= ["alan" "alice"] (.listObjects ^ObjectStore os))))
      (t/testing "should be able to delete prior objects and have that reflected in list objects output"
        @(.deleteObject ^ObjectStore os "alice")
        (t/is (= ["alan"] (.listObjects ^ObjectStore os)))))))

(t/deftest ^:azure multiple-object-store-list-test
  (let [prefix (random-uuid)
        wait-time-ms 5000]
    (with-open [os-1 (object-store prefix)
                os-2 (object-store prefix)]
      (os-test/put-edn os-1 "alice" :alice)
      (os-test/put-edn os-2 "alan" :alan)
      (Thread/sleep wait-time-ms)
      (t/is (= ["alan" "alice"] (.listObjects ^ObjectStore os-1)))
      (t/is (= ["alan" "alice"] (.listObjects ^ObjectStore os-2))))))

;; Currently not testing this - will need to setup the event hub namespace and config to run
;; (t/deftest ^:azure test-eventhub-log
;;   (with-open [node (node/start-node {::azure/event-hub-log {:namespace eventhub-namespace
;;                                                             :resource-group-name resource-group-name
;;                                                             :event-hub-name (str "xtdb.azure-test-hub." (UUID/randomUUID))
;;                                                             :create-event-hub? true
;;                                                             :retention-period-in-days 1}})]
;;     (xt/submit-tx node [[:put :xt_docs {:xt/id :foo}]])
;;     (t/is (= [{:id :foo}]
;;              (xt/q node '{:find [id]
;;                           :where [($ :xt_docs [{:xt/id id}])]})))))

(defn list-filenames [^BlobContainerClient blob-container-client prefix ^ListBlobsOptions list-opts]
  (->> (.listBlobs blob-container-client list-opts nil)
       (.iterator)
       (iterator-seq)
       (mapv (fn [^BlobItem blob-item]
               (subs (.getName blob-item) (count prefix))))
       (set)))

(defn fetch-uncomitted-blobs [^BlobContainerClient blob-container-client prefix]
  (let [base-opts (-> (ListBlobsOptions.)
                      (.setPrefix prefix))
        comitted-blobs (list-filenames blob-container-client prefix base-opts)
        all-blobs (list-filenames blob-container-client
                                  prefix
                                  (.setDetails base-opts
                                               (-> (BlobListDetails.)
                                                   (.setRetrieveUncommittedBlobs true))))]
    (set/difference all-blobs comitted-blobs)))

(t/deftest ^:azure multipart-start-and-cancel
  (with-open [os (object-store (random-uuid))]
    (let [blob-container-client (:blob-container-client os)
          prefix (:prefix os)]
      (t/testing "Call to start multipart should work/return an object"
        (let [multipart-upload ^IMultipartUpload  @(.startMultipart ^SupportsMultipart os "test-multi-created")]
          (t/is multipart-upload)

          (t/testing "Uploading a part should create an uncomitted blob"
            (let [uncomitted-blobs (fetch-uncomitted-blobs blob-container-client prefix)]
              (= #{"test-multi-created"} uncomitted-blobs)))

          (t/testing "Call to abort a multipart upload should work - uncomitted blob removed"
            @(.abort multipart-upload)
            (let [uncomitted-blobs (fetch-uncomitted-blobs blob-container-client prefix)]
              (= #{} uncomitted-blobs))))))))

(t/deftest ^:azure multipart-put-test
  (with-open [os (object-store (random-uuid))]
    (let [blob-container-client (:blob-container-client os)
          prefix (:prefix os)
          multipart-upload ^IMultipartUpload @(.startMultipart ^SupportsMultipart os "test-multi-put")
          part-size 500
          file-part-1 ^ByteBuffer (os-test/generate-random-byte-buffer part-size)
          file-part-2 ^ByteBuffer (os-test/generate-random-byte-buffer part-size)]

      ;; Uploading parts to multipart upload
      @(.uploadPart multipart-upload (.flip file-part-1))
      @(.uploadPart multipart-upload (.flip file-part-2))

      (t/testing "Call to complete a multipart upload should work - should be removed from the uncomitted list"
        @(.complete multipart-upload)
        (let [uncomitted-blobs (fetch-uncomitted-blobs blob-container-client prefix)]
          (t/is (= #{} uncomitted-blobs))))

      (t/testing "Multipart upload works correctly - file present and contents correct"
        (t/is (= ["test-multi-put"] (.listObjects ^ObjectStore os)))

        (let [^ByteBuffer uploaded-buffer @(.getObject ^ObjectStore os "test-multi-put")]
          (t/testing "capacity should be equal to total of 2 parts"
            (t/is (= (* 2 part-size) (.capacity uploaded-buffer)))))))))

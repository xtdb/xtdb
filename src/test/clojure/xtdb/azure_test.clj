(ns xtdb.azure-test
  (:require [clojure.java.shell :as sh]
            [clojure.set :as set]
            [clojure.test :as t]
            [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig] 
            [xtdb.api :as xt]
            [xtdb.azure :as azure]
            [xtdb.datasets.tpch :as tpch]
            [xtdb.node :as xtn]
            [xtdb.object-store-test :as os-test]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [com.azure.storage.blob BlobContainerClient]
           [com.azure.storage.blob.models BlobItem BlobListDetails ListBlobsOptions]
           [java.io Closeable]
           [java.nio ByteBuffer]
           [java.nio.file Path]
           [java.time Duration]
           xtdb.IObjectStore
           [xtdb.multipart IMultipartUpload SupportsMultipart]))

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

(defn wait-between-tests [f]
  (log/info "Waiting 10 seconds between tests... (Allowing AQMP conneciton time to clear up)")
  (Thread/sleep 10000)
  (f))

(t/use-fixtures :once run-if-auth-available)
(t/use-fixtures :each wait-between-tests)

(defn object-store ^Closeable [prefix]
  (->> (ig/prep-key ::azure/blob-object-store {:storage-account storage-account
                                               :container container
                                               :servicebus-namespace servicebus-namespace
                                               :servicebus-topic-name servicebus-topic-name
                                               :prefix (util/->path (str "xtdb.azure-test." prefix))})
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
      (os-test/put-edn os (util/->path "alice") :alice)
      (os-test/put-edn os (util/->path "alan") :alan)
      (t/is (= (mapv util/->path ["alan" "alice"])
               (.listObjects ^IObjectStore os))))

    (with-open [os (object-store prefix)]
      (t/testing "prior objects will still be there, should be available on a list request"
        (t/is (= (mapv util/->path ["alan" "alice"])
                 (.listObjects ^IObjectStore os))))
      
      (t/testing "should be able to delete prior objects and have that reflected in list objects output"
        @(.deleteObject ^IObjectStore os (util/->path "alice"))
        (t/is (= (mapv util/->path ["alan"]) 
                 (.listObjects ^IObjectStore os)))))))

(t/deftest ^:azure multiple-object-store-list-test 
  (let [prefix (random-uuid)
        wait-time-ms 5000]
    (with-open [os-1 (object-store prefix)
                os-2 (object-store prefix)]
      (os-test/put-edn os-1 (util/->path "alice") :alice)
      (os-test/put-edn os-2 (util/->path "alan") :alan)
      (Thread/sleep wait-time-ms)
      
      (t/is (= (mapv util/->path ["alan" "alice"]) 
               (.listObjects ^IObjectStore os-1)))
      
      (t/is (= (mapv util/->path ["alan" "alice"]) 
               (.listObjects ^IObjectStore os-2))))))

;; Currently not testing this - will need to setup the event hub namespace and config to run
;; (t/deftest ^:azure test-eventhub-log
;;   (with-open [node (xtn/start-node {::azure/event-hub-log {:namespace eventhub-namespace
;;                                                             :resource-group-name resource-group-name
;;                                                             :event-hub-name (str "xtdb.azure-test-hub." (UUID/randomUUID))
;;                                                             :create-event-hub? true
;;                                                             :retention-period-in-days 1}})]
;;     (xt/submit-tx node [(xt/put :xt_docs {:xt/id :foo}])])
;;     (t/is (= [{:id :foo}]
;;              (xt/q node '(from :xt_docs [{:xt/id id}])))))

(defn list-filenames [^BlobContainerClient blob-container-client ^Path prefix ^ListBlobsOptions list-opts]
  (->> (.listBlobs blob-container-client list-opts nil)
       (.iterator)
       (iterator-seq)
       (mapv (fn [^BlobItem blob-item]
               (.relativize prefix (util/->path (.getName blob-item)))))
       (set)))

(defn fetch-uncomitted-blobs [^BlobContainerClient blob-container-client ^Path prefix]
  (let [base-opts (-> (ListBlobsOptions.)
                      (.setPrefix (str prefix)))
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
        (let [multipart-upload ^IMultipartUpload @(.startMultipart ^SupportsMultipart os (util/->path "test-multi-created"))]
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
          multipart-upload ^IMultipartUpload @(.startMultipart ^SupportsMultipart os (util/->path "test-multi-put"))
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
        (t/is (= (mapv util/->path ["test-multi-put"]) 
                 (.listObjects ^IObjectStore os)))

        (let [^ByteBuffer uploaded-buffer @(.getObject ^IObjectStore os (util/->path "test-multi-put"))]
          (t/testing "capacity should be equal to total of 2 parts"
            (t/is (= (* 2 part-size) (.capacity uploaded-buffer)))))))))


(t/deftest ^:azure node-level-test
  (util/with-tmp-dirs #{disk-store}
    (util/with-open [node (xtn/start-node {:xtdb.buffer-pool/remote {:object-store (ig/ref ::azure/blob-object-store)
                                                                     :disk-store disk-store}
                                           ::azure/blob-object-store {:storage-account storage-account
                                                                      :container container
                                                                      :servicebus-namespace servicebus-namespace
                                                                      :servicebus-topic-name servicebus-topic-name
                                                                      :prefix (util/->path (str "xtdb.azure-test." (random-uuid)))}})]
      ;; Submit some documents to the node
      (t/is (xt/submit-tx node [(xt/put :bar {:xt/id "bar1"})
                                (xt/put :bar {:xt/id "bar2"})
                                (xt/put :bar {:xt/id "bar3"})]))

      ;; Ensure finish-chunk! works
      (t/is (nil? (tu/finish-chunk! node)))

      ;; Ensure can query back out results
      (t/is (= [{:e "bar2"} {:e "bar1"} {:e "bar3"}]
               (xtdb.api/q node '(from :bar [{:xt/id e}]))))

      (let [object-store (get-in node [:system ::azure/blob-object-store])]
      ;; Ensure some files are written
        (t/is (not-empty (.listObjects ^IObjectStore object-store)))))))

;; Using large enough TPCH ensures multiparts get properly used within the bufferpool
(t/deftest ^:azure tpch-test-node
  (util/with-tmp-dirs #{disk-store}
    (util/with-open [node (xtn/start-node {:xtdb.buffer-pool/remote {:object-store (ig/ref ::azure/blob-object-store)
                                                                     :disk-store disk-store}
                                           ::azure/blob-object-store {:storage-account storage-account
                                                                      :container container
                                                                      :servicebus-namespace servicebus-namespace
                                                                      :servicebus-topic-name servicebus-topic-name
                                                                      :prefix (util/->path (str "xtdb.azure-test." (random-uuid)))}})]
      ;; Submit tpch docs 
      (-> (tpch/submit-docs! node 0.05)
          (tu/then-await-tx node (Duration/ofHours 1)))

      ;; Ensure finish-chunk! works
      (t/is (nil? (tu/finish-chunk! node)))

      (let [object-store (get-in node [:system ::azure/blob-object-store])]
      ;; Ensure files have been written
        (t/is (not-empty (.listObjects ^IObjectStore object-store)))))))

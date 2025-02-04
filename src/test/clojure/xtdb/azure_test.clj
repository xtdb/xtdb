(ns xtdb.azure-test
  (:require [clojure.java.shell :as sh]
            [clojure.test :as t]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.buffer-pool-test :as bp-test]
            [xtdb.datasets.tpch :as tpch]
            [xtdb.node :as xtn]
            [xtdb.object-store :as os]
            [xtdb.object-store-test :as os-test]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.nio ByteBuffer)
           (java.nio.file Files)
           (java.time Duration)
           (xtdb.api.storage ObjectStore)
           (xtdb.azure BlobStorage)
           (xtdb.buffer_pool RemoteBufferPool)
           (xtdb.multipart IMultipartUpload SupportsMultipart)))

(def storage-account "xtdbteststorageaccount")
(def container "xtdb-test-object-store")

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

(defn object-store ^xtdb.azure.BlobStorage [prefix]
  (-> (BlobStorage/azureBlobStorage storage-account container)
      (.prefix (util/->path (str prefix)))
      (.openObjectStore)))

(t/deftest ^:azure put-delete-test
  (with-open [os (object-store (random-uuid))]
    (os-test/test-put-delete os)))

(t/deftest ^:azure get-object-to-file-twice-shouldnt-fail
  (util/with-tmp-dirs #{local-disk-cache}
    (with-open [^ObjectStore os (object-store (random-uuid))]
      (let [alice {:xt/id :alice, :name "Alice"}
            alice-key (util/->path "alice")]
        (os-test/put-edn os alice-key alice)

        (let [out-path (.resolve local-disk-cache "alice.edn")]
          (t/testing "first get successful"
            (t/is (= out-path @(.getObject os alice-key out-path)))
            (t/is (= alice (read-string (Files/readString out-path)))))

          (t/testing "second get successful & doesn't throw"
            (t/is (= out-path @(.getObject os alice-key out-path)))
            (t/is (= alice (read-string (Files/readString out-path))))))))))

(defn start-kafka-node [local-disk-cache prefix]
  (xtn/start-node
   {:storage [:remote
              {:object-store [:azure {:storage-account storage-account
                                      :container container
                                      :prefix (util/->path (str "xtdb.azure-test." prefix))}]
               :local-disk-cache local-disk-cache}]
    :log [:kafka {:topic (str "xtdb.kafka-test." prefix)
                  :bootstrap-servers "localhost:9092"}]
    :compactor {:threads 0}}))

(t/deftest ^:azure list-test
  (util/with-tmp-dirs #{local-disk-cache}
    (util/with-open [node (start-kafka-node local-disk-cache (random-uuid))]
      (let [buffer-pool (bp-test/fetch-buffer-pool-from-node node)]
        (bp-test/test-list-objects buffer-pool)))))

(t/deftest ^:azure list-test-with-prior-objects
  (util/with-tmp-dirs #{local-disk-cache}
    (let [prefix (random-uuid)]
      (util/with-open [node (start-kafka-node local-disk-cache prefix)]
        (let [^RemoteBufferPool buffer-pool (bp-test/fetch-buffer-pool-from-node node)]
          (bp-test/put-edn buffer-pool (util/->path "alice") :alice)
          (bp-test/put-edn buffer-pool (util/->path "alan") :alan)
          (Thread/sleep 1000)
          (t/is (= [(os/->StoredObject "alan" 5) (os/->StoredObject "alice" 6)]
                   (vec (.listAllObjects buffer-pool))))))
      
      (util/with-open [node (start-kafka-node local-disk-cache prefix)]
        (let [^RemoteBufferPool buffer-pool (bp-test/fetch-buffer-pool-from-node node)]
          (t/testing "prior objects will still be there, should be available on a list request"
            (t/is (= [(os/->StoredObject "alan" 5) (os/->StoredObject "alice" 6)]
                     (vec (.listAllObjects buffer-pool)))))

          (t/testing "should be able to add new objects and have that reflected in list objects output"
            (bp-test/put-edn buffer-pool (util/->path "alex") :alex)
            (Thread/sleep 1000)
            (t/is (= [(os/->StoredObject "alan" 5) (os/->StoredObject "alex" 5) (os/->StoredObject "alice" 6)]
                     (vec (.listAllObjects buffer-pool))))))))))

(t/deftest ^:azure multiple-node-list-test
  (util/with-tmp-dirs #{local-disk-cache}
    (let [prefix (random-uuid)]
      (util/with-open [node-1 (start-kafka-node local-disk-cache prefix)
                       node-2 (start-kafka-node local-disk-cache prefix)]
        (let [^RemoteBufferPool buffer-pool-1 (bp-test/fetch-buffer-pool-from-node node-1)
              ^RemoteBufferPool buffer-pool-2 (bp-test/fetch-buffer-pool-from-node node-2)]
          (bp-test/put-edn buffer-pool-1 (util/->path "alice") :alice)
          (bp-test/put-edn buffer-pool-2 (util/->path "alan") :alan)
          (Thread/sleep 1000)
          (t/is (= [(os/->StoredObject "alan" 5) (os/->StoredObject "alice" 6)]
                   (vec (.listAllObjects buffer-pool-1))))

          (t/is (= [(os/->StoredObject "alan" 5) (os/->StoredObject "alice" 6)]
                   (vec (.listAllObjects buffer-pool-2)))))))))

(t/deftest ^:azure multipart-start-and-cancel
  (with-open [os (object-store (random-uuid))]
    (t/testing "Call to start multipart should work/return an object"
      (let [multipart-upload ^IMultipartUpload @(.startMultipart ^SupportsMultipart os (util/->path "test-multi-created"))]
        (t/is multipart-upload)

        (t/testing "Uploading a part should create an uncomitted blob"
          (= #{"test-multi-created"} (set (.listUncommittedBlobs os))))

        (t/testing "Call to abort a multipart upload should work - no file comitted"
          @(.abort multipart-upload)
          (t/is (= #{} (set (.listAllObjects os)))))))))

(t/deftest ^:azure multipart-put-test
  (with-open [os (object-store (random-uuid))]
    (let [multipart-upload ^IMultipartUpload @(.startMultipart ^SupportsMultipart os (util/->path "test-multi-put"))
          part-size 500
          file-part-1 ^ByteBuffer (os-test/generate-random-byte-buffer part-size)
          file-part-2 ^ByteBuffer (os-test/generate-random-byte-buffer part-size)

          parts [;; Uploading parts to multipart upload
                 @(.uploadPart multipart-upload file-part-1)
                 @(.uploadPart multipart-upload file-part-2)]]

      (t/testing "Call to complete a multipart upload should work - should be removed from the uncomitted list"
        @(.complete multipart-upload parts)
        (t/is (empty? (.listUncommittedBlobs os))))

      (t/testing "Multipart upload works correctly - file present and contents correct"
        (t/is (= [(os/->StoredObject (util/->path "test-multi-put") (* 2 part-size))]
                 (vec (.listAllObjects ^ObjectStore os))))

        (let [^ByteBuffer uploaded-buffer @(.getObject ^ObjectStore os (util/->path "test-multi-put"))]
          (t/testing "capacity should be equal to total of 2 parts"
            (t/is (= (* 2 part-size) (.capacity uploaded-buffer)))))))))

(t/deftest ^:azure node-level-test
  (util/with-tmp-dirs #{local-disk-cache}
    (util/with-open [node (start-kafka-node local-disk-cache (random-uuid))]
      (let [^RemoteBufferPool buffer-pool (bp-test/fetch-buffer-pool-from-node node)]
        (t/is (true? (:committed? (xt/execute-tx node [[:put-docs :bar {:xt/id "bar1"}]
                                                       [:put-docs :bar {:xt/id "bar2"}]
                                                       [:put-docs :bar {:xt/id "bar3"}]]))))

        (tu/finish-block! node)

        (t/is (= [{:e "bar2"} {:e "bar1"} {:e "bar3"}]
                 (xt/q node '(from :bar [{:xt/id e}]))))

        ;; Ensure some files written to buffer-pool
        (t/is (seq (.listAllObjects buffer-pool)))))))

;; Using large enough TPCH ensures multiparts get properly used within the bufferpool
(t/deftest ^:azure tpch-test-node
  (util/with-tmp-dirs #{local-disk-cache}
    (util/with-open [node (start-kafka-node local-disk-cache (random-uuid))]
      ;; Submit tpch docs
      (tpch/submit-docs! node 0.05)
      (tu/then-await-tx (:latest-submitted-tx-id (xt/status node)) node (Duration/ofHours 1))

      ;; Ensure finish-block! works
      (t/is (nil? (tu/finish-block! node)))

      ;; Ensure some files written to buffer-pool 
      (let [^RemoteBufferPool buffer-pool (bp-test/fetch-buffer-pool-from-node node)]
        (t/is (seq (.listAllObjects buffer-pool)))))))

(t/deftest ^:azure multipart-uploads-with-more-parts-work-correctly
  (with-open [os (object-store (random-uuid))]
    (let [multipart-upload ^IMultipartUpload @(.startMultipart ^SupportsMultipart os (util/->path "test-larger-multi-put"))
          part-size 500
          parts (doall
                 (for [_ (range 20)]
                   (let [buf ^ByteBuffer (os-test/generate-random-byte-buffer part-size)]
                     {:buf buf
                      :!part (.uploadPart multipart-upload buf)})))]

      (t/testing "Call to complete a multipart upload should work - should be removed from the uncomitted list"
        @(.complete multipart-upload (mapv (comp deref :!part) parts))
        (t/is (empty? (.listUncommittedBlobs os))))

      (t/testing "Multipart upload works correctly - file present and contents correct"
        (t/is (= [(os/->StoredObject (util/->path "test-larger-multi-put") (* 20 part-size))]
                 (vec (.listAllObjects ^ObjectStore os))))

        (let [^ByteBuffer uploaded-buffer @(.getObject ^ObjectStore os (util/->path "test-larger-multi-put"))]
          (t/testing "capacity should be equal to total of 20 parts"
            (t/is (= (* 20 part-size) (.capacity uploaded-buffer)))))))))

(t/deftest ^:azure multipart-object-already-exists
  (with-open [os (object-store (random-uuid))]
    (let [part-size 500]

      (t/testing "Initial multipart works correctly"
        (let [initial-multipart-upload ^IMultipartUpload @(.startMultipart os (util/->path "test-multipart"))
              parts (doall
                     (for [_ (range 2)]
                       (let [file-part ^ByteBuffer (os-test/generate-random-byte-buffer part-size)]
                         (.uploadPart initial-multipart-upload file-part))))]

          @(.complete initial-multipart-upload (mapv deref parts))

          (t/is (= [(os/->StoredObject (util/->path "test-multipart") (* 2 part-size))]
                   (vec (.listAllObjects ^ObjectStore os))))

          (t/is (empty? (.listUncommittedBlobs os)))))

      (t/testing "Attempt to multipart upload to an existing object shouldn't throw, should abort and remove uncomitted blobs"
        (let [second-multipart-upload ^IMultipartUpload @(.startMultipart os (util/->path "test-multipart"))
              parts (doall
                     (for [_ (range 3)]
                       (let [file-part ^ByteBuffer (os-test/generate-random-byte-buffer part-size)]
                         (.uploadPart second-multipart-upload file-part))))]

          @(.complete second-multipart-upload (mapv deref parts))

          (t/is (empty? (.listUncommittedBlobs os)))))

      (t/testing "still has the original object"
        (t/is (= [(os/->StoredObject (util/->path "test-multipart") (* 2 part-size))]
                 (vec (.listAllObjects ^ObjectStore os))))

        (let [^ByteBuffer uploaded-buffer @(.getObject ^ObjectStore os (util/->path "test-multipart"))]
          (t/testing "capacity should be equal to total of 2 parts (ie, initial upload)"
            (t/is (= (* 2 part-size) (.capacity uploaded-buffer)))))))))

(t/deftest ^:azure interrupt-multipart-upload
  (with-open [os (object-store (random-uuid))]
    (let [parts (repeatedly 5 #(.flip (os-test/generate-random-byte-buffer 10000000)))
          upload-thread (Thread.
                         (fn []
                           (try
                             ;; Start the multipart upload
                             (RemoteBufferPool/uploadMultipartBuffers os (util/->path "multipart-interrupted") parts)
                             (catch InterruptedException _
                               (log/warn "Upload was interrupted")))))]
      ;; Start the upload thread
      (.start upload-thread)

      ;; Give it some time to start uploading
      (Thread/sleep 500)

      (.interrupt upload-thread)

      (t/testing "no committed blobs should be present"
        (t/is (empty? (.listAllObjects os)))))))

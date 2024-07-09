(ns xtdb.aws.s3-test
  (:require [clojure.test :as t]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.api :as xt]
            [xtdb.datasets.tpch :as tpch]
            [xtdb.node :as xtn]
            [xtdb.object-store-test :as os-test]
            [xtdb.aws.s3 :as s3]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.io Closeable]
           [java.nio ByteBuffer]
           [java.nio.file Path]
           [java.time Duration]
           [software.amazon.awssdk.services.s3 S3AsyncClient]
           [software.amazon.awssdk.services.s3.model ListMultipartUploadsRequest ListMultipartUploadsResponse MultipartUpload]
           [xtdb.api.storage ObjectStore]
           [xtdb.aws S3 S3$Factory]
           [xtdb.buffer_pool RemoteBufferPool]
           [xtdb.multipart IMultipartUpload SupportsMultipart]))

;; Setup the stack via cloudformation - see modules/s3/cloudformation/s3-stack.yml
;; Ensure region is set locally to wherever cloudformation stack is created (ie, eu-west-1 if stack on there)

(def bucket
  (or (System/getProperty "xtdb.aws.s3-test.bucket")
      "xtdb-object-store-iam-test"))

(def sns-topic-arn
  (or (System/getProperty "xtdb.aws.s3-test.sns-topic-arn")
      "arn:aws:sns:eu-west-1:199686536682:xtdb-object-store-iam-test-bucket-events"))

(defn object-store ^Closeable [prefix]
  (let [factory (-> (S3/s3 bucket sns-topic-arn)
                    (.prefix (util/->path (str prefix))))]
    (s3/open-object-store factory)))

(t/deftest ^:s3 put-delete-test
  (with-open [os (object-store (random-uuid))]
    (os-test/test-put-delete os)))

(t/deftest ^:s3 range-test
  (with-open [os (object-store (random-uuid))]
    (os-test/test-range os)))

(t/deftest ^:s3 list-test
  (with-open [os (object-store (random-uuid))]
    (os-test/test-list-objects os)))

(t/deftest ^:s3 list-test-with-prior-objects
  (let [prefix (random-uuid)]
    (with-open [os (object-store prefix)]
      (os-test/put-edn os (util/->path "alice") :alice)
      (os-test/put-edn os (util/->path "alan") :alan)
      (t/is (= (mapv util/->path ["alan" "alice"])
               (.listAllObjects ^ObjectStore os))))

    (with-open [os (object-store prefix)]
      (t/testing "prior objects will still be there, should be available on a list request"
        (t/is (= (mapv util/->path ["alan" "alice"])
                 (.listAllObjects ^ObjectStore os))))

      (t/testing "should be able to delete prior objects and have that reflected in list objects output"
        @(.deleteObject ^ObjectStore os (util/->path "alice"))
        (t/is (= (mapv util/->path ["alan"])
                 (.listAllObjects ^ObjectStore os)))))))

(t/deftest ^:s3 multiple-object-store-list-test
  (let [prefix (random-uuid)
        wait-time-ms 20000]
    (with-open [os-1 (object-store prefix)
                os-2 (object-store prefix)]
      (os-test/put-edn os-1 (util/->path "alice") :alice)
      (os-test/put-edn os-2 (util/->path "alan") :alan)
      (Thread/sleep wait-time-ms)

      (t/is (= (mapv util/->path ["alan" "alice"])
               (.listAllObjects ^ObjectStore os-1)))

      (t/is (= (mapv util/->path ["alan" "alice"])
               (.listAllObjects ^ObjectStore os-2))))))

(t/deftest ^:s3 multipart-start-and-cancel
  (with-open [os (object-store (random-uuid))]
    (let [prefix (:prefix os)
          multipart-key (util/->path "test-multi-created")
          multipart-upload ^IMultipartUpload  @(.startMultipart ^SupportsMultipart os multipart-key)
          prefixed-key (str (.resolve ^Path prefix multipart-key))]
      (t/testing "Call to start a multipart upload should work and be visible in multipart upload list"
        (let [list-multipart-uploads-response @(.listMultipartUploads ^S3AsyncClient (:client os)
                                                                      (-> (ListMultipartUploadsRequest/builder)
                                                                          (.bucket bucket)
                                                                          (.prefix (str prefix))
                                                                          ^ListMultipartUploadsRequest (.build)))
              [^MultipartUpload upload] (.uploads ^ListMultipartUploadsResponse list-multipart-uploads-response)]
          (t/is (= (.uploadId upload) (:upload-id multipart-upload)) "upload id should be present")
          (t/is (= (.key upload) prefixed-key) "should be under the prefixed key")))

      (t/testing "Call to abort a multipart upload should work - should be removed from the upload list"
        @(.abort multipart-upload)
        (let [list-multipart-uploads-response @(.listMultipartUploads ^S3AsyncClient (:client os)
                                                                      (-> (ListMultipartUploadsRequest/builder)
                                                                          (.bucket bucket)
                                                                          (.prefix (str prefix))
                                                                          ^ListMultipartUploadsRequest (.build)))
              uploads (.uploads ^ListMultipartUploadsResponse list-multipart-uploads-response)]
          (t/is (= [] uploads) "uploads should be empty"))))))

(t/deftest ^:s3 multipart-put-test
  (with-open [os (object-store (random-uuid))]
    (let [prefix (:prefix os)
          multipart-upload ^IMultipartUpload @(.startMultipart ^SupportsMultipart os (util/->path "test-multi-put"))
          part-size (* 5 1024 1024)
          file-part-1 (os-test/generate-random-byte-buffer part-size)
          file-part-2 (os-test/generate-random-byte-buffer part-size)]

      ;; Uploading parts to multipart upload
      @(.uploadPart multipart-upload file-part-1)
      @(.uploadPart multipart-upload file-part-2)

      (t/testing "Call to complete a multipart upload should work - should be removed from the upload list"
        @(.complete multipart-upload)
        (let [list-multipart-uploads-response @(.listMultipartUploads ^S3AsyncClient (:client os)
                                                                      (-> (ListMultipartUploadsRequest/builder)
                                                                          (.bucket bucket)
                                                                          (.prefix (str prefix))
                                                                          ^ListMultipartUploadsRequest (.build)))
              uploads (.uploads ^ListMultipartUploadsResponse list-multipart-uploads-response)]
          (t/is (= [] uploads) "uploads should be empty")))

      (t/testing "Multipart upload works correctly - file present and contents correct"
        (t/is (= (mapv util/->path ["test-multi-put"])
                 (.listAllObjects ^ObjectStore os)))

        (let [^ByteBuffer uploaded-buffer @(.getObject ^ObjectStore os (util/->path "test-multi-put"))]
          (t/testing "capacity should be equal to total of 2 parts"
            (t/is (= (* 2 part-size) (.capacity uploaded-buffer)))))))))

(t/deftest ^:s3 node-level-test
  (util/with-tmp-dirs #{local-disk-cache}
    (util/with-open [node (xtn/start-node
                           {:storage [:remote
                                      {:object-store [:s3 {:bucket bucket
                                                           :prefix (util/->path (str (random-uuid)))
                                                           :sns-topic-arn sns-topic-arn}]
                                       :local-disk-cache local-disk-cache}]})]

      ;; Submit some documents to the node
      (t/is (= true
               (:committed? (xt/execute-tx node [[:put-docs :bar {:xt/id "bar1"}]
                                                 [:put-docs :bar {:xt/id "bar2"}]
                                                 [:put-docs :bar {:xt/id "bar3"}]]))))

      ;; Ensure finish-chunk! works
      (t/is (nil? (tu/finish-chunk! node)))

      ;; Ensure can query back out results
      (t/is (= [{:e "bar2"} {:e "bar1"} {:e "bar3"}]
               (xt/q node '(from :bar [{:xt/id e}])))) 
      
      (let [{:keys [^ObjectStore object-store] :as buffer-pool} (val (first (ig/find-derived (:system node) :xtdb/buffer-pool)))]
        (t/is (instance? RemoteBufferPool buffer-pool))
        (t/is (instance? ObjectStore object-store))
        ;; Ensure some files are written
        (t/is (seq (.listAllObjects object-store)))))))

;; Using large enough TPCH ensures multiparts get properly used within the bufferpool
#_(t/deftest ^:s3 tpch-test-node
  (util/with-tmp-dirs #{local-disk-cache}
    (util/with-open [node (xtn/start-node
                           {:storage [:remote
                                      {:object-store [:s3 {:bucket bucket
                                                           :prefix (util/->path (str (random-uuid)))
                                                           :sns-topic-arn sns-topic-arn}]
                                       :local-disk-cache local-disk-cache}]})]
      ;; Submit tpch docs
      (-> (tpch/submit-docs! node 0.05)
          (tu/then-await-tx node (Duration/ofHours 1)))

      ;; Ensure finish-chunk! works
      (t/is (nil? (tu/finish-chunk! node)))

      (let [{:keys [^ObjectStore object-store] :as buffer-pool} (val (first (ig/find-derived (:system node) :xtdb/buffer-pool)))]
        (t/is (instance? RemoteBufferPool buffer-pool))
        (t/is (instance? ObjectStore object-store))
        ;; Ensure some files are written
        (t/is (seq (.listAllObjects object-store)))))))

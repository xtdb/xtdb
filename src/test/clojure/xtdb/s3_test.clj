(ns xtdb.s3-test
  (:require [clojure.test :as t]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.object-store-test :as os-test]
            [xtdb.s3 :as s3])
  (:import [java.io Closeable]
           [java.nio ByteBuffer]
           [java.util.concurrent CompletableFuture]
           [software.amazon.awssdk.services.s3 S3AsyncClient]
           [software.amazon.awssdk.services.s3.model ListMultipartUploadsRequest ListMultipartUploadsResponse MultipartUpload]
           [xtdb.object_store ObjectStore IMultipartUpload]))

;; Setup the stack via cloudformation - see modules/s3/cloudformation/s3-stack.yml
;; Ensure region is set locally to wherever cloudformation stack is created (ie, eu-west-1 if stack on there)

(def bucket
  (or (System/getProperty "xtdb.s3-test.bucket")
      "xtdb-object-store-iam-test"))

(def sns-topic-arn
  (or (System/getProperty "xtdb.s3-test.sns-topic-arn")
      "arn:aws:sns:eu-west-1:199686536682:xtdb-object-store-iam-test-bucket-events"))

(defn object-store ^Closeable [prefix]
  (->> (ig/prep-key ::s3/object-store {:bucket bucket,
                                       :prefix (str prefix)
                                       :sns-topic-arn sns-topic-arn})
       (ig/init-key ::s3/object-store)))

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
      (os-test/put-edn os "alice" :alice)
      (os-test/put-edn os "alan" :alan)
      (t/is (= ["alan" "alice"] (.listObjects ^ObjectStore os))))

    (with-open [os (object-store prefix)]
      (t/testing "prior objects will still be there, should be available on a list request"
        (t/is (= ["alan" "alice"] (.listObjects ^ObjectStore os))))

      (t/testing "should be able to delete prior objects and have that reflected in list objects output"
        @(.deleteObject ^ObjectStore os "alice") 
        (t/is (= ["alan"] (.listObjects ^ObjectStore os)))))))

(t/deftest ^:s3 multiple-object-store-list-test
  (let [prefix (random-uuid)
        wait-time-ms 10000]
    (with-open [os-1 (object-store prefix)
                os-2 (object-store prefix)]
      (os-test/put-edn os-1 "alice" :alice)
      (os-test/put-edn os-2 "alan" :alan)
      (Thread/sleep wait-time-ms)
      (t/is (= ["alan" "alice"] (.listObjects ^ObjectStore os-1)))
      (t/is (= ["alan" "alice"] (.listObjects ^ObjectStore os-2))))))

(t/deftest ^:s3 multipart-start-and-cancel
  (with-open [os (object-store (random-uuid))]
    (let [multipart-upload ^IMultipartUpload  @(.startMultipart os "test-multi-created")
          prefixed-key (str (:prefix os) "test-multi-created")]
      (t/testing "Call to start a multipart upload should work and be visible in multipart upload list"
        (let [list-multipart-uploads-response @(.listMultipartUploads ^S3AsyncClient (:client os)
                                                                      (-> (ListMultipartUploadsRequest/builder)
                                                                          (.bucket bucket)
                                                                          ^ListMultipartUploadsRequest (.build)))
              [^MultipartUpload upload] (.uploads ^ListMultipartUploadsResponse list-multipart-uploads-response)]
          (t/is (= (.uploadId upload) (:upload-id multipart-upload)) "upload id should be present")
          (t/is (= (.key upload) prefixed-key) "should be under the prefixed key")))
      
      (t/testing "Call to abort a multipart upload should work - should be removed from the upload list"
        @(.abort multipart-upload)
        (let [list-multipart-uploads-response @(.listMultipartUploads ^S3AsyncClient (:client os)
                                                                      (-> (ListMultipartUploadsRequest/builder)
                                                                          (.bucket bucket)
                                                                          ^ListMultipartUploadsRequest (.build)))
              uploads (.uploads ^ListMultipartUploadsResponse list-multipart-uploads-response)]
          (t/is (= [] uploads) "uploads should be empty"))))))

;; Generates a byte buffer of random characters
(defn generate-random-byte-buffer [buffer-size]
  (let [random         (java.util.Random.)
        byte-buffer    (ByteBuffer/allocate buffer-size)]
    (loop [i 0]
      (if (< i buffer-size)
        (do
          (.put byte-buffer (byte (.nextInt random 128)))
          (recur (inc i)))
        byte-buffer))))

(t/deftest ^:s3 multipart-put-test
  (with-open [os (object-store (random-uuid))]
    (let [multipart-upload ^IMultipartUpload @(.startMultipart os "test-multi-put")
          part-size (* 5 1024 1024)
          file-part-1 (generate-random-byte-buffer part-size)
          file-part-2 (generate-random-byte-buffer part-size)]

      ;; Uploading parts to multipart upload
      @(.uploadPart multipart-upload file-part-1)
      @(.uploadPart multipart-upload file-part-2)

      (t/testing "Call to complete a multipart upload should work - should be removed from the upload list"
        @(.complete multipart-upload)
        (let [list-multipart-uploads-response @(.listMultipartUploads ^S3AsyncClient (:client os)
                                                                      (-> (ListMultipartUploadsRequest/builder)
                                                                          (.bucket bucket)
                                                                          ^ListMultipartUploadsRequest (.build)))
              uploads (.uploads ^ListMultipartUploadsResponse list-multipart-uploads-response)]
          (t/is (= [] uploads) "uploads should be empty")))

      (t/testing "Multipart upload works correctly - file present and contents correct"
        (t/is (= ["test-multi-put"] (.listObjects ^ObjectStore os)))

        (let [uploaded-buffer @(.getObject os "test-multi-put")]
          (t/testing "capacity should be equal to total of 2 parts"
            (t/is (= (* 2 part-size) (.capacity uploaded-buffer)))))))))

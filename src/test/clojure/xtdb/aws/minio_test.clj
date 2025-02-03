(ns xtdb.aws.minio-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.buffer-pool-test :as bp-test]
            [xtdb.node :as xtn]
            [xtdb.object-store :as os]
            [xtdb.object-store-test :as os-test]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.nio ByteBuffer]
           [java.nio.file Path]
           [software.amazon.awssdk.services.s3 S3AsyncClient]
           [software.amazon.awssdk.services.s3.model ListMultipartUploadsRequest ListMultipartUploadsResponse]
           [xtdb.api.storage ObjectStore]
           [xtdb.aws S3]
           [xtdb.buffer_pool RemoteBufferPool]
           [xtdb.multipart IMultipartUpload SupportsMultipart]))

;; To run these, run the MinIO and Kafka containers in the docker-compose file
;; http://localhost:9001, minioadmin/minioadmin
;; Identity -> Users -> Create: xtdb/test-password, policy: readwrite
;; Bucket: xtdb, policy: readwrite

(def ^:const bucket "xtdb")

(def test-creds
  {:access-key "xtdb"
   :secret-key "test-password"})

(defn object-store ^xtdb.api.storage.ObjectStore [prefix]
  (let [{:keys [access-key secret-key]} test-creds]
    (-> (S3/s3 bucket)
        (.prefix (util/->path (str prefix)))
        (.credentials access-key secret-key)
        (.endpoint "http://127.0.0.1:9000")
        (.openObjectStore))))

(t/deftest ^:minio put-delete-test
  (with-open [os (object-store (random-uuid))]
    (os-test/test-put-delete os)))

(defn start-node [^Path node-dir prefix]
  (xtn/start-node
   {:storage [:remote {:object-store [:s3 {:bucket bucket
                                           :prefix (str prefix)
                                           :credentials test-creds
                                           :endpoint "http://127.0.0.1:9000"}]
                       :local-disk-cache (.resolve node-dir "local-cache")}]
    :log [:kafka {:topic (str "xtdb.kafka-test." prefix)
                  :bootstrap-servers "localhost:9092"}]}))

(t/deftest ^:minio list-test
  (util/with-tmp-dirs #{local-disk-cache}
    (util/with-open [node (start-node local-disk-cache (random-uuid))]
      (let [buffer-pool (bp-test/fetch-buffer-pool-from-node node)]
        (bp-test/test-list-objects buffer-pool)))))

(t/deftest ^:minio list-test-with-prior-objects
  (util/with-tmp-dirs #{local-disk-cache}
    (let [prefix (random-uuid)]
      (util/with-open [node (start-node local-disk-cache prefix)]
        (let [^RemoteBufferPool buffer-pool (bp-test/fetch-buffer-pool-from-node node)]
          (bp-test/put-edn buffer-pool (util/->path "alice") :alice)
          (bp-test/put-edn buffer-pool (util/->path "alan") :alan)
          (Thread/sleep 100)
          (t/is (= [(os/->StoredObject "alan" 5) (os/->StoredObject "alice" 6)]
                   (.listAllObjects buffer-pool)))))

      (util/with-open [node (start-node local-disk-cache prefix)]
        (let [^RemoteBufferPool buffer-pool (bp-test/fetch-buffer-pool-from-node node)]
          (t/testing "prior objects will still be there, should be available on a list request"
          (Thread/sleep 100)
            (t/is (= [(os/->StoredObject "alan" 5) (os/->StoredObject "alice" 6)]
                     (.listAllObjects buffer-pool))))

          (t/testing "should be able to add new objects and have that reflected in list objects output"
            (bp-test/put-edn buffer-pool (util/->path "alex") :alex)
            (Thread/sleep 100)
            (t/is (= [(os/->StoredObject "alan" 5) (os/->StoredObject "alex" 5) (os/->StoredObject "alice" 6)]
                     (.listAllObjects buffer-pool)))))))))

(t/deftest ^:minio multiple-node-list-test
  (util/with-tmp-dirs #{local-disk-cache}
    (let [prefix (random-uuid)]
      (util/with-open [node-1 (start-node local-disk-cache prefix)
                       node-2 (start-node local-disk-cache prefix)]
        (let [^RemoteBufferPool buffer-pool-1 (bp-test/fetch-buffer-pool-from-node node-1)
              ^RemoteBufferPool buffer-pool-2 (bp-test/fetch-buffer-pool-from-node node-2)]
          (bp-test/put-edn buffer-pool-1 (util/->path "alice") :alice)
          (bp-test/put-edn buffer-pool-2 (util/->path "alan") :alan)
          (Thread/sleep 1000)
          (t/is (= [(os/->StoredObject "alan" 5) (os/->StoredObject "alice" 6)]
                   (.listAllObjects buffer-pool-1)))

          (t/is (= [(os/->StoredObject "alan" 5) (os/->StoredObject "alice" 6)]
                   (.listAllObjects buffer-pool-2))))))))

(t/deftest ^:minio multipart-start-and-cancel
  (with-open [os (object-store (random-uuid))]
    (let [multipart-key (util/->path "test-multi-created")
          multipart-upload ^IMultipartUpload  @(.startMultipart ^SupportsMultipart os multipart-key)]

      ;; MinIO doesn't seem to support listing incomplete uploads

      (t/testing "Call to abort a multipart upload should work"
        (t/is @(.abort multipart-upload))))))

(t/deftest ^:minio multipart-put-test
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
        (t/is (= [(os/->StoredObject (util/->path "test-multi-put")
                                     (* 2 part-size))]
                 (.listAllObjects ^ObjectStore os)))

        (let [^ByteBuffer uploaded-buffer @(.getObject ^ObjectStore os (util/->path "test-multi-put"))]
          (t/testing "capacity should be equal to total of 2 parts"
            (t/is (= (* 2 part-size) (.capacity uploaded-buffer)))))))))

(t/deftest ^:minio node-level-test
  (util/with-tmp-dirs #{local-disk-cache}
    (util/with-open [node (start-node local-disk-cache (random-uuid))]
      (let [^RemoteBufferPool buffer-pool (bp-test/fetch-buffer-pool-from-node node)]
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
  
        ;; Ensure some files written to buffer-pool
        (t/is (seq (.listAllObjects buffer-pool)))))))

;; Using large enough TPCH ensures multiparts get properly used within the bufferpool
(comment
  (import [java.time Duration])
  (require '[xtdb.datasets.tpch :as tpch]
           '[integrant.core :as ig])

  (t/deftest ^:minio tpch-test-node
    (util/with-tmp-dirs #{local-disk-cache}
                        (util/with-open [node (start-node local-disk-cache (str (random-uuid)))]
                                        ;; Submit tpch docs
                                        (-> (tpch/submit-docs! node 0.05)
            (tu/then-await-tx node (Duration/ofHours 1)))

                                        ;; Ensure finish-chunk! works
                                        (t/is (nil? (tu/finish-chunk! node)))

                                        (let [{:keys [^ObjectStore object-store] :as buffer-pool} (val (first (ig/find-derived (:system node) :xtdb/storage)))]
                                          (t/is (instance? RemoteBufferPool buffer-pool))
                                          (t/is (instance? ObjectStore object-store))
                                          ;; Ensure some files are written
                                          (t/is (seq (.listAllObjects object-store))))))))

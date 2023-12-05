(ns xtdb.google-cloud-test
  (:require [clojure.java.shell :as sh]
            [clojure.tools.logging :as log]
            [clojure.test :as t]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.api :as xt]
            [xtdb.datasets.tpch :as tpch]
            [xtdb.node :as xtn]
            [xtdb.google-cloud :as google-cloud]
            [xtdb.object-store-test :as os-test]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [com.google.cloud.storage Bucket Storage StorageOptions StorageOptions$Builder Storage$BucketGetOption Bucket$BucketSourceOption StorageException]
           [java.io Closeable]
           [xtdb IObjectStore]
           [java.time Duration]))

;; Ensure you are authenticated with google cloud before running these tests - there are two options to do this:
;; - gcloud auth Login onto an account which belongs to the `xtdb-devs@gmail.com` group
;; - assume the role of the service account created for these tests (this allows us to verify the role works as intended)
;; ---> gcloud auth activate-service-account --key-file=<KEYFILE>
;; ---> Where <KEYFILE> is the filepath to a key file for 'xtdb-test-service-account' 

(def project-id "xtdb-scratch")
(def pubsub-topic "gcp-test-xtdb-object-store-notif-topic")
(def test-bucket "gcp-test-xtdb-object-store")

(defn config-present? []
  (try
    (let [^Storage storage (-> (StorageOptions/newBuilder)
                               ^StorageOptions$Builder (.setProjectId project-id)
                               ^StorageOptions (.build)
                               (.getService))
          ^Bucket bucket (.get storage ^String test-bucket ^"[Lcom.google.cloud.storage.Storage$BucketGetOption;" (into-array Storage$BucketGetOption []))]
      (.exists bucket (into-array Bucket$BucketSourceOption [])))
    (catch StorageException e
      (when-not (= 401 (.getCode e))
        (throw e)))))

(defn cli-available? []
  (= 0 (:exit (sh/sh "gcloud" "--help"))))

(defn run-if-auth-available [f]
  (cond
    (config-present?) (f)

    (not (cli-available?))
    (log/warn "gcloud cli is unavailable, and application default credentials are not set")

    :else
    (log/warn "gcloud cli appears to be available but you are not authenticated, run `gcloud auth application-default login` before running the tests")))

(t/use-fixtures :once run-if-auth-available)

(defn object-store ^Closeable [prefix]
  (->> (ig/prep-key ::google-cloud/blob-object-store {:project-id project-id
                                                      :bucket test-bucket
                                                      :pubsub-topic pubsub-topic
                                                      :prefix (str "xtdb.google-cloud-test." prefix)})
       (ig/init-key ::google-cloud/blob-object-store)))

(t/deftest ^:google-cloud put-delete-test
  (let [os (object-store (random-uuid))]
    
    (os-test/test-put-delete os)))

(t/deftest ^:google-cloud range-test
  (let [os (object-store (random-uuid))]
    (os-test/test-range os)))

(t/deftest ^:google-cloud list-test
  (with-open [os (object-store (random-uuid))]
    (os-test/test-list-objects os)))

(t/deftest ^:google-cloud list-test-with-prior-objects
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

(t/deftest ^:google-cloud multiple-object-store-list-test
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

(t/deftest ^:azure node-level-test
  (util/with-tmp-dirs #{disk-store}
    (util/with-open [node (xtn/start-node {:xtdb.buffer-pool/remote {:object-store (ig/ref ::google-cloud/blob-object-store)
                                                                     :disk-store disk-store}
                                           ::google-cloud/blob-object-store {:project-id project-id
                                                                             :bucket test-bucket
                                                                             :pubsub-topic pubsub-topic
                                                                             :prefix (str "xtdb.google-cloud-test." (random-uuid))}})]
      ;; Submit some documents to the node
      (t/is (xt/submit-tx node [(xt/put :bar {:xt/id "bar1"})
                                (xt/put :bar {:xt/id "bar2"})
                                (xt/put :bar {:xt/id "bar3"})]))

      ;; Ensure finish-chunk! works
      (t/is (nil? (tu/finish-chunk! node)))

      ;; Ensure can query back out results
      (t/is (= [{:e "bar2"} {:e "bar1"} {:e "bar3"}]
               (xtdb.api/q node '(from :bar [{:xt/id e}]))))

      (let [object-store (get-in node [:system ::google-cloud/blob-object-store])]
      ;; Ensure some files are written
        (t/is (not-empty (.listObjects ^IObjectStore object-store)))))))

;; Using large enough TPCH ensures multiparts get properly used within the bufferpool
(t/deftest ^:azure tpch-test-node
  (util/with-tmp-dirs #{disk-store}
    (util/with-open [node (xtn/start-node {:xtdb.buffer-pool/remote {:object-store (ig/ref ::google-cloud/blob-object-store)
                                                                     :disk-store disk-store}
                                           ::google-cloud/blob-object-store {:project-id project-id
                                                                             :bucket test-bucket
                                                                             :pubsub-topic pubsub-topic
                                                                             :prefix (str "xtdb.google-cloud-test." (random-uuid))}})]
      ;; Submit tpch docs 
      (-> (tpch/submit-docs! node 0.05)
          (tu/then-await-tx node (Duration/ofHours 1)))

      ;; Ensure finish-chunk! works
      (t/is (nil? (tu/finish-chunk! node)))

      (let [object-store (get-in node [:system ::google-cloud/blob-object-store])]
      ;; Ensure files have been written
        (t/is (not-empty (.listObjects ^IObjectStore object-store)))))))

(ns xtdb.google-cloud-test
  (:require [clojure.java.shell :as sh]
            [clojure.tools.logging :as log]
            [clojure.test :as t]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.google-cloud :as google-cloud]
            [xtdb.object-store-test :as os-test])
  (:import [com.google.cloud.storage Bucket Storage StorageOptions StorageOptions$Builder Storage$BucketGetOption Bucket$BucketSourceOption StorageException]
           [java.io Closeable]
           [xtdb.object_store ObjectStore]))

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

(def wait-time-ms 5000)

(t/deftest ^:azure list-test
  (with-open [os (object-store (random-uuid))]
    (os-test/test-list-objects os wait-time-ms)))

(t/deftest ^:azure list-test-with-prior-objects
  (let [prefix (random-uuid)]
    (with-open [os (object-store prefix)]
      (os-test/put-edn os "alice" :alice)
      (os-test/put-edn os "alan" :alan)
      (Thread/sleep wait-time-ms)
      (t/is (= ["alan" "alice"] (.listObjects ^ObjectStore os))))

    (with-open [os (object-store prefix)]
      (t/testing "prior objects will still be there, should be available on a list request"
        (t/is (= ["alan" "alice"] (.listObjects ^ObjectStore os))))
      (t/testing "should be able to delete prior objects and have that reflected in list objects output"
        @(.deleteObject ^ObjectStore os "alice")
        (Thread/sleep wait-time-ms)
        (t/is (= ["alan"] (.listObjects ^ObjectStore os)))))))

(t/deftest ^:azure multiple-object-store-list-test
  (let [prefix (random-uuid)]
    (with-open [os-1 (object-store prefix)
                os-2 (object-store prefix)]
      (os-test/put-edn os-1 "alice" :alice)
      (os-test/put-edn os-2 "alan" :alan)
      (Thread/sleep wait-time-ms)
      (t/is (= ["alan" "alice"] (.listObjects ^ObjectStore os-2))))))

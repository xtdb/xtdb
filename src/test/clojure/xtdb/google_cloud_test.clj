(ns xtdb.google-cloud-test
  (:require [clojure.java.shell :as sh]
            [clojure.tools.logging :as log]
            [clojure.test :as t]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.google-cloud :as google-cloud]
            [xtdb.object-store-test :as os-test])
  (:import [java.io Closeable]
           [com.google.cloud.storage StorageOptions Storage$BucketGetOption Bucket$BucketSourceOption StorageException]))

(def project-id "xtdb-scratch")
(def test-bucket "xtdb-cloud-storage-test-bucket")

(defn config-present? []
  (try
    (let [storage (-> (StorageOptions/newBuilder)
                      (.setProjectId project-id)
                      (.build)
                      (.getService))
          bucket (.get storage test-bucket (into-array Storage$BucketGetOption []))]
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
                                                      :prefix (str "xtdb.google-cloud-test." prefix)})
       (ig/init-key ::google-cloud/blob-object-store)))

(t/deftest ^:google-cloud put-delete-test
  (let [os (object-store (random-uuid))]
    
    (os-test/test-put-delete os)))

;; Current fails since it DOESNT error for "OOB for last index"
(t/deftest ^:google-cloud range-test
  (let [os (object-store (random-uuid))]
    (os-test/test-range os)))

(t/deftest ^:google-cloud list-test
  (let [os (object-store (random-uuid))]
    (os-test/test-list-objects os)))

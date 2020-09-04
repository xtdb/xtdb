(ns crux.blobs-test
  (:require [crux.blobs :as b]
            [clojure.test :as t]
            [crux.doc-store-test :as dst]
            [crux.topology :as topo]))

(def test-blobs-sas-token
  (System/getenv "CRUX_BLOBS_TEST_SAS_TOKEN"))

(def test-blobs-storage-account
  (or (System/getProperty "crux.blobs.test-storage-account")
      (System/getenv "CRUX_BLOBS_TEST_STORAGE_ACCOUNT")))

(def test-blobs-container
  (or (System/getProperty "crux.blobs.test-container")
      (System/getenv "CRUX_BLOBS_TEST_CONTAINER")))

(t/use-fixtures :each
  (fn [f]
    (when (and test-blobs-sas-token
               test-blobs-storage-account
               test-blobs-container)
      (let [[topo close] (topo/start-topology {:crux.node/topology b/blobs-doc-store
                                               ::b/sas-token test-blobs-sas-token
                                               ::b/storage-account test-blobs-storage-account
                                               ::b/container test-blobs-container})]
        (try
          (binding [dst/*doc-store* (:crux.node/document-store topo)]
            (f))
          (finally
            (close)))))))

(defn test-ns-hook []
  (dst/test-doc-store (find-ns 'crux.blobs-test)))

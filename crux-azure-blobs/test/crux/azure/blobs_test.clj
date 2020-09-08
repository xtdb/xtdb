(ns crux.azure.blobs-test
  (:require [crux.azure.blobs :as b]
            [clojure.test :as t]
            [crux.doc-store-test :as dst]
            [crux.topology :as topo]))

(def test-azure-blobs-sas-token
  (System/getenv "CRUX_BLOBS_TEST_SAS_TOKEN"))

(def test-azure-blobs-storage-account
  (or (System/getProperty "crux.blobs.test-storage-account")
      (System/getenv "CRUX_BLOBS_TEST_STORAGE_ACCOUNT")))

(def test-azure-blobs-container
  (or (System/getProperty "crux.blobs.test-container")
      (System/getenv "CRUX_BLOBS_TEST_CONTAINER")))

(t/use-fixtures :each
  (fn [f]
    (when (and test-azure-blobs-sas-token
               test-azure-blobs-storage-account
               test-azure-blobs-container)
      (let [[topo close] (topo/start-topology {:crux.node/topology b/doc-store
                                               ::b/sas-token test-azure-blobs-sas-token
                                               ::b/storage-account test-azure-blobs-storage-account
                                               ::b/container test-azure-blobs-container})]
        (try
          (binding [dst/*doc-store* (:crux.node/document-store topo)]
            (f))
          (finally
            (close)))))))

(defn test-ns-hook []
  (dst/test-doc-store (find-ns 'crux.azure.blobs-test)))

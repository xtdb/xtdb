(ns crux.blobs-test
  (:require [crux.blobs :as blobs]
            [clojure.test :as t]
            [crux.doc-store-test :as dst]
            [crux.topology :as topo]))

(def test-blobs-storage-account
  (or (System/getProperty "crux.blobs.test-storage-account")
      (System/getenv "CRUX_BLOBS_TEST_STORAGE_ACCOUNT")))

(def test-blobs-container
  (or (System/getProperty "crux.blobs.test-container")
      (System/getenv "CRUX_BLOBS_TEST_CONTAINER")))

(t/use-fixtures :each
  (fn [f]
    (let [[topo close] (topo/start-topology {:crux.node/topology blobs/blobs-doc-store
                                             ::blobs/storage-account test-blobs-storage-account
                                             ::blobs/container test-blobs-container})]
      (try
        (binding [dst/*doc-store* (:crux.node/document-store topo)]
          (f))
        (finally
          (close))))))

(defn test-ns-hook []
  (dst/test-doc-store (find-ns 'crux.blobs-test)))

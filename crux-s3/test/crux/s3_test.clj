(ns crux.s3-test
  (:require [crux.s3 :as s3]
            [clojure.test :as t]
            [crux.doc-store-test :as dst]
            [crux.topology :as topo])
  (:import (java.util UUID)
           (software.amazon.awssdk.services.s3 S3Client)
           (software.amazon.awssdk.services.s3.model GetObjectRequest PutObjectRequest)))

(def test-s3-bucket
  (or (System/getProperty "crux.s3.test-bucket")
      (System/getenv "CRUX_S3_TEST_BUCKET")))

(def ^:dynamic ^S3Client *client*)

(t/use-fixtures :once
  (fn [f]
    (when test-s3-bucket
      (binding [*client* (S3Client/create)]
        (f)))))

(t/use-fixtures :each
  (fn [f]
    (let [test-s3-prefix (str "crux-s3-test-" (UUID/randomUUID))
          [topo close] (topo/start-topology {:crux.node/topology s3/s3-doc-store
                                             ::s3/bucket test-s3-bucket
                                             ::s3/prefix test-s3-prefix})]

      (try
        (binding [dst/*doc-store* (:crux.node/document-store topo)]
          (f))
        (finally
          (close))))))

(defn test-ns-hook []
  (dst/test-doc-store (find-ns 'crux.s3-test)))

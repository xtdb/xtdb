(ns crux.s3-test
  (:require [crux.s3 :as s3]
            [clojure.test :as t]
            [crux.doc-store-test :as dst]
            [crux.system :as sys])
  (:import (java.util UUID)
           (crux.s3 S3Configurator)
           (software.amazon.awssdk.regions Region)
           (software.amazon.awssdk.services.s3 S3AsyncClient)
           (software.amazon.awssdk.services.s3.model GetObjectRequest PutObjectRequest)))

(def test-s3-bucket
  (or (System/getProperty "crux.s3.test-bucket")
      (System/getenv "CRUX_S3_TEST_BUCKET")))

(def ^:dynamic ^S3AsyncClient *client*)

(t/use-fixtures :once
  (fn [f]
    (when test-s3-bucket
      (binding [*client* (-> (S3AsyncClient/builder)
                             (.region (Region/EU_WEST_2))
                             (.build))]
        (f)))))

(defn ->configurator [_]
  (reify S3Configurator
    (makeClient [_] *client*)))

(t/use-fixtures :each
  (fn [f]
    (with-open [sys (-> (sys/prep-system {::s3/document-store
                                          {:bucket test-s3-bucket
                                           :prefix (str "crux-s3-test-" (UUID/randomUUID))
                                           :configurator `->configurator}})
                        (sys/start-system))]

      (binding [dst/*doc-store* (::s3/document-store sys)]
        (f)))))

(defn test-ns-hook []
  (dst/test-doc-store (find-ns 'crux.s3-test)))

(ns xtdb.s3-test
  (:require [clojure.test :as t]
            [crux.fixtures.document-store :as fix.ds]
            [xtdb.s3 :as s3]
            [crux.system :as sys])
  (:import xtdb.s3.S3Configurator
           java.util.UUID
           software.amazon.awssdk.regions.Region
           software.amazon.awssdk.services.s3.S3AsyncClient))

(def test-s3-bucket
  (or (System/getProperty "xtdb.s3.test-bucket")
      (System/getenv "CRUX_S3_TEST_BUCKET")))

(def test-s3-region
  (or (System/getProperty "xtdb.s3.test-region")
      (System/getenv "CRUX_S3_TEST_REGION")))

(def ^:dynamic ^S3AsyncClient *client*)

(defn with-s3-client [f]
  (when test-s3-bucket
    (let [builder (S3AsyncClient/builder)
          _ (when test-s3-region
              (.region builder (Region/of test-s3-region)))]
      (binding [*client* (.build builder)]
        (f)))))

(t/use-fixtures :once with-s3-client)

(defn ->configurator [_]
  (reify S3Configurator
    (makeClient [_] *client*)))

(t/deftest test-s3-doc-store
  (with-open [sys (-> (sys/prep-system {::s3/document-store
                                        {:bucket test-s3-bucket
                                         :prefix (str "crux-s3-test-" (UUID/randomUUID))
                                         :configurator `->configurator}})
                      (sys/start-system))]

    (fix.ds/test-doc-store (::s3/document-store sys))))

(ns crux.s3-test
  (:require [clojure.test :as t]
            [crux.doc-store-test :as dst]
            [crux.s3 :as s3]
            [crux.system :as sys])
  (:import crux.s3.S3Configurator
           java.util.UUID
           software.amazon.awssdk.regions.Region
           software.amazon.awssdk.services.s3.S3AsyncClient))

(def test-s3-bucket
  (or (System/getProperty "crux.s3.test-bucket")
      (System/getenv "CRUX_S3_TEST_BUCKET")))

(def test-s3-region
  (or (System/getProperty "crux.s3.test-region")
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

(t/use-fixtures :each
  (fn [f]
    (with-open [sys (-> (sys/prep-system {::s3/document-store
                                          {:bucket test-s3-bucket
                                           :prefix (str "crux-s3-test-" (UUID/randomUUID))
                                           :configurator `->configurator}})
                        (sys/start-system))]

      (binding [dst/*doc-store* (::s3/document-store sys)]
        (f)))))

;; required for CIDER to run the tests in this namespace
;; see https://github.com/clojure-emacs/cider-nrepl/issues/680
(t/deftest _)

(defn test-ns-hook []
  (dst/test-doc-store (find-ns 'crux.s3-test)))

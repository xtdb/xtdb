(ns xtdb.google.cloud-storage-test
  (:require [xtdb.google.cloud-storage :as gcs]
            [clojure.test :as t]
            [crux.fixtures.document-store :as fix.ds]
            [crux.fixtures.checkpoint-store :as fix.cp]
            [crux.system :as sys])
  (:import java.util.UUID))

(def test-bucket
  (System/getProperty "xtdb.google.cloud-storage-test.bucket"))

(t/use-fixtures :once
  (fn [f]
    (when test-bucket
      (f))))

(t/deftest test-doc-store
  (with-open [sys (-> (sys/prep-system {::gcs/document-store {:root-path (format "gs://%s/test-%s" test-bucket (UUID/randomUUID))}})
                      (sys/start-system))]

    (fix.ds/test-doc-store (::gcs/document-store sys))))

(t/deftest test-cp-store
  (with-open [sys (-> (sys/prep-system {::gcs/checkpoint-store {:path (format "gs://%s/test-%s" test-bucket (UUID/randomUUID))}})
                      (sys/start-system))]

    (fix.cp/test-checkpoint-store (::gcs/checkpoint-store sys))))

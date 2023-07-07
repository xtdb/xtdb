(ns xtdb.google.cloud-storage-test
  (:require [xtdb.google.cloud-storage :as gcs]
            [clojure.test :as t]
            [xtdb.fixtures.document-store :as fix.ds]
            [xtdb.fixtures.checkpoint-store :as fix.cp]
            [xtdb.system :as sys])
  (:import java.util.UUID))

(def project-id "xtdb-scratch")
(def test-bucket "xtdb-cloud-storage-test-bucket")

(t/use-fixtures :once
  (fn [f]
    (when test-bucket
      (f))))

(t/deftest test-doc-store
  (with-open [sys (-> (sys/prep-system {::gcs/document-store {:project-id project-id
                                                              :bucket test-bucket}})
                      (sys/start-system))]

    (fix.ds/test-doc-store (::gcs/document-store sys))))

(t/deftest test-doc-store-with-prefix
  (with-open [sys (-> (sys/prep-system {::gcs/document-store {:project-id project-id
                                                              :bucket test-bucket
                                                              :prefix (format "test-doc-store-%s" (UUID/randomUUID))}})
                      (sys/start-system))]

    (fix.ds/test-doc-store (::gcs/document-store sys))))

(t/deftest test-cp-store
  (with-open [sys (-> (sys/prep-system {::gcs/checkpoint-store {:project-id project-id
                                                                :bucket test-bucket
                                                                :prefix (format "test-checkpoint-store-%s" (UUID/randomUUID))}})
                      (sys/start-system))]

    (fix.cp/test-checkpoint-store (::gcs/checkpoint-store sys))))

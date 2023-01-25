(ns xtdb.s3.checkpoint-test
  (:require [clojure.test :as t]
            [xtdb.fixtures.checkpoint-store :as fix.cp-store]
            [xtdb.s3-test :as s3t]
            [xtdb.s3.checkpoint :as s3c]
            [xtdb.system :as sys])
  (:import xtdb.s3.S3Configurator
           java.util.UUID
           software.amazon.awssdk.regions.Region
           software.amazon.awssdk.services.s3.S3AsyncClient))

(def ^:dynamic ^S3AsyncClient *crt-client*)

(defn with-s3-crt-client [f]
  (when s3t/test-s3-bucket
    (let [builder (S3AsyncClient/builder)
          _ (when s3t/test-s3-region
              (.region builder (Region/of s3t/test-s3-region)))]
      (binding [*crt-client* (.build builder)]
        (f)))))

(t/use-fixtures :once with-s3-crt-client)
(t/use-fixtures :once s3t/with-s3-client)

(defn ->crt-configurator [_]
  (reify S3Configurator
    (makeClient [_] *crt-client*)))

(t/deftest test-checkpoint-store
  (with-open [sys (-> (sys/prep-system {:store {:xtdb/module `s3c/->cp-store
                                                :configurator `s3t/->configurator
                                                :bucket s3t/test-s3-bucket
                                                :prefix (str "s3-cp-" (UUID/randomUUID))}})
                      (sys/start-system))]
    (fix.cp-store/test-checkpoint-store (:store sys))))


(t/deftest test-checkpoint-store-transfer-manager
  (with-open [sys (-> (sys/prep-system {:store {:xtdb/module `s3c/->cp-store
                                                :configurator `->crt-configurator
                                                :bucket s3t/test-s3-bucket
                                                :transfer-manager? true
                                                :prefix (str "s3-cp-" (UUID/randomUUID))}})
                      (sys/start-system))]
    (fix.cp-store/test-checkpoint-store (:store sys))))

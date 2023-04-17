(ns xtdb.s3.checkpoint-transfer-manager-test
  (:require [clojure.test :as t]
            [xtdb.fixtures.checkpoint-store :as fix.cp-store]
            [xtdb.s3-test :as s3t]
            [xtdb.s3.checkpoint :as s3c]
            [xtdb.s3.checkpoint-transfer-manager :as s3ctm]
            [xtdb.system :as sys])
  (:import xtdb.s3.S3Configurator
           java.util.UUID
           software.amazon.awssdk.regions.Region
           software.amazon.awssdk.services.s3.S3AsyncClient))

(def ^:dynamic ^S3AsyncClient *crt-client*)

(defn with-s3-crt-client [f]
  (when s3t/test-s3-bucket
    (let [builder (S3AsyncClient/crtBuilder)
          _ (when s3t/test-s3-region
              (.region builder (Region/of s3t/test-s3-region)))]
      (binding [*crt-client* (.build builder)]
        (f)))))

(t/use-fixtures :once with-s3-crt-client)

(defn ->crt-configurator [_]
  (reify S3Configurator
    (makeClient [_] *crt-client*)))

(t/deftest test-checkpoint-store-transfer-manager
  (with-open [sys (-> (sys/prep-system {:store {:xtdb/module `s3ctm/->cp-store
                                                :configurator `->crt-configurator
                                                :bucket s3t/test-s3-bucket
                                                :transfer-manager? true
                                                :prefix (str "s3-cp-" (UUID/randomUUID))}})
                      (sys/start-system))]
    (fix.cp-store/test-checkpoint-store (:store sys))))

(ns crux.s3.checkpoint-test
  (:require [clojure.test :as t]
            [crux.fixtures.checkpoint-store :as fix.cp-store]
            [crux.s3-test :as s3t]
            [crux.s3.checkpoint :as s3c]
            [crux.system :as sys])
  (:import java.util.UUID))

(t/use-fixtures :once s3t/with-s3-client)

(t/deftest test-checkpoint-store
  (with-open [sys (-> (sys/prep-system {:store {:xt/module `s3c/->cp-store
                                                :configurator `s3t/->configurator
                                                :bucket s3t/test-s3-bucket
                                                :prefix (str "s3-cp-" (UUID/randomUUID))}})
                      (sys/start-system))]
    (fix.cp-store/test-checkpoint-store (:store sys))))

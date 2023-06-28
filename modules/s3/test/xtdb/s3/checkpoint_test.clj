(ns xtdb.s3.checkpoint-test
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.checkpoint :as cp]
            [xtdb.fixtures :as fix]
            [xtdb.fixtures.checkpoint-store :as fix.cp-store]
            [xtdb.s3 :as s3]
            [xtdb.s3-test :as s3t]
            [xtdb.s3.checkpoint :as s3c]
            [xtdb.system :as sys])
  (:import xtdb.s3.S3Configurator
           java.util.UUID
           java.util.Date
           software.amazon.awssdk.regions.Region
           software.amazon.awssdk.services.s3.S3AsyncClient))

(t/use-fixtures :each s3t/with-s3-client)

(t/deftest test-checkpoint-store
  (with-open [sys (-> (sys/prep-system {:store {:xtdb/module `s3c/->cp-store
                                                :configurator `s3t/->configurator
                                                :bucket s3t/test-s3-bucket
                                                :prefix (str "s3-cp-" (UUID/randomUUID))}})
                      (sys/start-system))]
    (fix.cp-store/test-checkpoint-store (:store sys))))

(t/deftest test-checkpoint-store-cleanup
  (with-open [sys (-> (sys/prep-system {:store {:xtdb/module `s3c/->cp-store
                                                :configurator `s3t/->configurator
                                                :bucket s3t/test-s3-bucket
                                                :prefix (str "s3-cp-" (UUID/randomUUID))}})
                      (sys/start-system))]
    (fix/with-tmp-dirs #{dir}
      (let [cp-at (Date.)
            cp-store (:store sys)
            ;; create file for upload
            _ (spit (io/file dir "hello.txt") "Hello world")
            {:keys [::s3c/s3-dir] :as res} (cp/upload-checkpoint cp-store dir {::cp/cp-format ::foo-cp-format
                                                                               :tx {::xt/tx-id 1}
                                                                               :cp-at cp-at})]

        (t/testing "call to upload-checkpoint creates expected folder & checkpoint metadata file for the checkpoint"
          (let [object-info (into {} (s3/list-objects cp-store {}))]
            (t/is (= s3-dir (:common-prefix object-info)))
            (t/is (= (string/replace s3-dir #"/" ".edn")
                     (:object object-info)))))

        (t/testing "call to `cleanup-checkpoints` entirely removes an uploaded checkpoint and metadata"
          (cp/cleanup-checkpoint cp-store {:tx {::xt/tx-id 1}
                                           :cp-at cp-at})
          (t/is (empty? (s3/list-objects cp-store {}))))))))

(t/deftest test-checkpoint-store-failed-cleanup
  (with-open [sys (-> (sys/prep-system {:store {:xtdb/module `s3c/->cp-store
                                                :configurator `s3t/->configurator
                                                :bucket s3t/test-s3-bucket
                                                :prefix (str "s3-cp-" (UUID/randomUUID))}})
                      (sys/start-system))]
    (fix/with-tmp-dirs #{dir}
      (let [cp-at (Date.)
            cp-store (:store sys)
            ;; create file for upload
            _ (spit (io/file dir "hello.txt") "Hello world")
            {:keys [::s3c/s3-dir] :as res} (cp/upload-checkpoint cp-store dir {::cp/cp-format ::foo-cp-format
                                                                               :tx {::xt/tx-id 1}
                                                                               :cp-at cp-at})]

        (t/testing "call to upload-checkpoint creates expected folder & checkpoint metadata file for the checkpoint"
          (let [object-info (into {} (s3/list-objects cp-store {}))]
            (t/is (= s3-dir (:common-prefix object-info)))
            (t/is (= (string/replace s3-dir #"/" ".edn")
                     (:object object-info)))))

        (t/testing "error in `cleanup-checkpoints` after deleting checkpoint metadata file still leads to checkpoint not being available"
          (with-redefs [s3/list-objects (fn [_ _] (throw (Exception. "Test Exception")))]
            (t/is (thrown-with-msg? Exception
                                    #"Test Exception"
                                    (cp/cleanup-checkpoint cp-store {:tx {::xt/tx-id 1}
                                                                     :cp-at cp-at}))))
          ;; Only directory should be available - checkpoint metadata file should have been deleted
          (t/is (= [[:common-prefix s3-dir]]
                   (vec (s3/list-objects cp-store {}))))
          ;; Should not be able to fetch checkpoint as checkpoint metadata file is gone
          (t/is (empty? (cp/available-checkpoints cp-store ::foo-cp-format))))))))

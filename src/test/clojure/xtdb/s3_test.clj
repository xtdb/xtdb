(ns xtdb.s3-test
  (:require [clojure.test :as t]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.object-store-test :as os-test]
            [xtdb.s3 :as s3])
  (:import (java.io Closeable)
           xtdb.object_store.ObjectStore))

;; Setup the stack via cloudformation - see modules/s3/cloudformation/s3-stack.yml
;; Will need to create and set an access token for the S3TestUser
;; Ensure region is set locally to wherever cloudformation stack is created (ie, eu-west-1 if stack on there)

(def bucket
  (or (System/getProperty "xtdb.s3-test.bucket")
      "xtdb-object-store-iam-test"))

(def sns-topic-arn
  (or (System/getProperty "xtdb.s3-test.sns-topic-arn")
      "arn:aws:sns:eu-west-1:199686536682:xtdb-object-store-iam-test-bucket-events"))

(defn object-store ^Closeable [prefix]
  (->> (ig/prep-key ::s3/object-store {:bucket bucket,
                                       :prefix (str prefix)
                                       :sns-topic-arn sns-topic-arn})
       (ig/init-key ::s3/object-store)))

(t/deftest ^:s3 put-delete-test
  (with-open [os (object-store (random-uuid))]
    (os-test/test-put-delete os)))

(t/deftest ^:s3 range-test
  (with-open [os (object-store (random-uuid))]
    (os-test/test-range os)))

(def wait-time-ms 10000)

(t/deftest ^:s3 list-test
  (with-open [os (object-store (random-uuid))]
    (os-test/test-list-objects os wait-time-ms)))

(t/deftest ^:s3 list-test-with-prior-objects
  (let [prefix (random-uuid)]
    (with-open [os (object-store prefix)]
      (os-test/put-edn os "alice" :alice)
      (os-test/put-edn os "alan" :alan)
      (Thread/sleep wait-time-ms)
      (t/is (= ["alan" "alice"] (.listObjects ^ObjectStore os))))

    (with-open [os (object-store prefix)]
      (t/testing "prior objects will still be there, should be available on a list request"
        (t/is (= ["alan" "alice"] (.listObjects ^ObjectStore os))))

      (t/testing "should be able to delete prior objects and have that reflected in list objects output"
        @(.deleteObject ^ObjectStore os "alice")
        (Thread/sleep wait-time-ms)
        (t/is (= ["alan"] (.listObjects ^ObjectStore os)))))))

(t/deftest ^:s3 multiple-object-store-list-test
  (let [prefix (random-uuid)]
    (with-open [os-1 (object-store prefix)
                os-2 (object-store prefix)]
      (os-test/put-edn os-1 "alice" :alice)
      (os-test/put-edn os-2 "alan" :alan)
      (Thread/sleep wait-time-ms)
      (t/is (= ["alan" "alice"] (.listObjects ^ObjectStore os-2))))))

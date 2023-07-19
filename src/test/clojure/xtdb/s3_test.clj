(ns xtdb.s3-test
  (:require [clojure.test :as t]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.object-store-test :as os-test]
            [xtdb.s3 :as s3])
  (:import (java.io Closeable)))

(def bucket
  (or (System/getProperty "xtdb.s3-test.bucket")
      "xtdb-s3-test-object-store"))

(def sns-topic-arn
  (or (System/getProperty "xtdb.s3-test.sns-topic-arn")
      "arn:aws:sns:eu-west-1:199686536682:xtdb-object-store-s3-test-bucket-events"))

(defn object-store ^Closeable [prefix]
  (->> (ig/prep-key ::s3/object-store {:bucket bucket, :prefix (str prefix)})
       (ig/init-key ::s3/object-store)))

(t/deftest ^:s3 put-delete-test
  (with-open [os (object-store (random-uuid))]
    (os-test/test-put-delete os)))

(t/deftest ^:s3 range-test
  (with-open [os (object-store (random-uuid))]
    (os-test/test-range os)))

(t/deftest ^:s3 list-test
  (with-open [os (object-store (random-uuid))]
    (os-test/test-list-objects os)))

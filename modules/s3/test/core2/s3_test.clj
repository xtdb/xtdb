(ns core2.s3-test
  (:require [clojure.test :as t]
            [core2.object-store-test :as os-test]
            [core2.s3 :as s3]
            [core2.system :as sys])
  (:import java.util.UUID))

(def bucket (System/getProperty "core2.s3-test.bucket"))

(def ^:dynamic *obj-store*)

(t/use-fixtures :each
  (fn [f]
    (when bucket
      (with-open [sys (-> (sys/prep-system {::obj-store {:core2/module `s3/->object-store
                                                         :bucket bucket
                                                         :prefix (str "core2.s3-test." (UUID/randomUUID))}})
                          (sys/start-system))]
        (binding [*obj-store* (::obj-store sys)]
          (f))))))

(os-test/def-obj-store-tests s3 [f]
  (f *obj-store*))

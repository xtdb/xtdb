(ns xtdb.s3-test
  (:require [clojure.test :as t]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.object-store-test :as os-test]
            [xtdb.s3 :as s3])
  (:import java.util.UUID))

(def bucket
  (or (System/getProperty "xtdb.s3-test.bucket")
      "xtdb-s3-test"))

(def ^:dynamic *obj-store*)

(t/use-fixtures :each
  (fn [f]
    (when bucket
      (let [sys (-> {::s3/object-store {:bucket bucket
                                        :prefix (str "xtdb.s3-test." (UUID/randomUUID))}}
                    ig/prep
                    ig/init)]
        (try
          (binding [*obj-store* (::s3/object-store sys)]
            (f))
          (finally
            (ig/halt! sys)))))))

(os-test/def-obj-store-tests ^:s3 s3 [f]
  (f *obj-store*))

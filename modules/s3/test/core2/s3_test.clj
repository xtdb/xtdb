(ns core2.s3-test
  (:require [clojure.test :as t]
            [core2.object-store-test :as os-test]
            [core2.s3 :as s3]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import java.util.UUID))

(def bucket (System/getProperty "core2.s3-test.bucket"))

(def ^:dynamic *obj-store*)

(t/use-fixtures :each
  (fn [f]
    (when bucket
      (let [sys (-> {::s3/object-store {:bucket bucket
                                        :prefix (str "core2.s3-test." (UUID/randomUUID))}}
                    ig/prep
                    ig/init)]
        (try
          (binding [*obj-store* (::s3/object-store sys)]
            (f))
          (finally
            (ig/halt! sys)))))))

(os-test/def-obj-store-tests s3 [f]
  (f *obj-store*))

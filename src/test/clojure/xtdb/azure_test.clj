(ns xtdb.azure-test
  (:require [clojure.test :as t]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.object-store-test :as os-test]
            [xtdb.azure :as azure]
            [clojure.tools.logging :as log])
  (:import java.util.UUID))

(def storage-account "xtdbazureobjectstoretest")
(def container "xtdb-test")

(def ^:dynamic *obj-store*)

(t/use-fixtures :each
  (fn [f]
    (let [sys (-> {::azure/blob-object-store {:storage-account storage-account
                                              :container container
                                              :prefix (str "xtdb.azure-test." (UUID/randomUUID))}}
                  ig/prep
                  ig/init)]
      (try
        (binding [*obj-store* (::azure/blob-object-store sys)]
          (f))
        (finally
          (ig/halt! sys))))))

;; TODO: Throws an error when trying to return an IllegalStateException - classcast issue?
(os-test/def-obj-store-tests ^:azure azure [f]
  (f *obj-store*))

(ns xtdb.azure-test
  (:require [clojure.test :as t]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.object-store-test :as os-test]
            [xtdb.azure :as azure]
            [clojure.tools.logging :as log])
  (:import java.util.UUID))

(def storage-account "xtdbazureobjectstoretest")
(def container "xtdb-test")
(def config-present? (some? (and (System/getenv "AZURE_CLIENT_ID")
                                 (System/getenv "AZURE_CLIENT_SECRET")
                                 (System/getenv "AZURE_TENANT_ID"))))

(def ^:dynamic *obj-store*)

(t/use-fixtures :each
  (fn [f]
    (log/info "Azure config present (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET & AZURE_TENANT_ID set)? - " config-present?)
    (when config-present?
      (let [sys (-> {::azure/blob-object-store {:storage-account storage-account
                                                :container container
                                                :prefix (str "xtdb.azure-test." (UUID/randomUUID))}}
                    ig/prep
                    ig/init)]
        (try
          (binding [*obj-store* (::azure/blob-object-store sys)]
            (f))
          (finally
            (ig/halt! sys)))))))

;; TODO: Throws an error when trying to return an IllegalStateException - classcast issue?
(os-test/def-obj-store-tests ^:azure azure [f]
  (f *obj-store*))

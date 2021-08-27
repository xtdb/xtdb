(ns xtdb.document-store-test
  (:require [clojure.test :as t]
            [xtdb.document-store :as ds]
            [xtdb.fixtures :as fix]
            [xtdb.fixtures.document-store :as fix.ds]
            [xtdb.system :as sys])
  (:import java.io.File))

(t/deftest test-document-store
  (fix/with-tmp-dirs #{doc-store-dir}
    (with-open [sys (-> (sys/prep-system {:doc-store {:xt/module `ds/->nio-document-store
                                                      :root-path (.toPath ^File doc-store-dir)}})
                        (sys/start-system))]

      (fix.ds/test-doc-store (:doc-store sys)))))

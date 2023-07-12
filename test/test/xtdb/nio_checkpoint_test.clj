(ns xtdb.nio-checkpoint-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.checkpoint :as cp]
            [xtdb.fixtures :as fix]
            [xtdb.fixtures.checkpoint-store :as fix.cp-store]
            [xtdb.io :as xio]
            [xtdb.system :as sys])
  (:import [java.util Date UUID]
           [java.nio.file Paths]
           java.net.URI))

;; can be ran using
;; lein with-profiles +with-nio-storage-test test xtdb.nio-checkpoint-test
;; will need to be authenticated with Google Cloud (currently what we're testing against)

(def nio-path
  (System/getProperty "xtdb.nio-checkpoint-test.nio-path"))

(defn ->path [filename]
  (let [uri (URI. (str nio-path filename))]
    (Paths/get uri)))

(t/use-fixtures :once
  (fn [f]
    (when nio-path
      (f))))

(t/deftest test-nio-fs-checkpoint-store
  (with-open [sys (-> (sys/prep-system {::cp/filesystem-checkpoint-store {:path (format "%s/test-%s" nio-path (UUID/randomUUID))}})
                      (sys/start-system))]
    (fix.cp-store/test-checkpoint-store (::cp/filesystem-checkpoint-store sys))))

(t/deftest test-nio-fs-checkpoint-store-failed-download
  (with-open [sys (-> (sys/prep-system {::cp/filesystem-checkpoint-store {:path (format "%s/test-%s" nio-path (UUID/randomUUID))}})
                      (sys/start-system))]
    (fix.cp-store/test-checkpoint-broken-store-failed-download (::cp/filesystem-checkpoint-store sys))))

(t/deftest test-nio-fs-checkpoint-store-cleanup
  (with-open [sys (-> (sys/prep-system {::cp/filesystem-checkpoint-store {:path (format "%s/test-%s" nio-path (UUID/randomUUID))}})
                      (sys/start-system))]
    (fix/with-tmp-dirs #{dir}
      (let [cp-store (::cp/filesystem-checkpoint-store sys)
            cp-at (Date.)]

      ;; create file for upload
        (spit (io/file dir "hello.txt") "Hello world")

        (let [{:keys [::cp/cp-uri]} (cp/upload-checkpoint cp-store dir {::cp/cp-format ::foo-cp-format
                                                                        :tx {::xt/tx-id 1}
                                                                        :cp-at cp-at})]
          (t/testing "call to upload-checkpoint creates expected folder & checkpoint metadata file for the checkpoint"
            (t/is (xio/path-exists? (->path cp-uri)))
            (t/is (xio/path-exists? (->path (str cp-uri ".edn")))))

          (t/testing "call to `cleanup-checkpoints` entirely removes an uploaded checkpoint and metadata"
            (cp/cleanup-checkpoint cp-store {:tx {::xt/tx-id 1}
                                             :cp-at cp-at})
            (t/is (= false (xio/path-exists? (->path cp-uri))))
            (t/is (= false (xio/path-exists? (->path (str cp-uri ".edn")))))))))))


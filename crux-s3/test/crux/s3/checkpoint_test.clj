(ns crux.s3.checkpoint-test
  (:require [crux.s3.checkpoint :as s3c]
            [crux.s3-test :as s3t]
            [clojure.test :as t]
            [crux.fixtures :as fix]
            [crux.system :as sys]
            [clojure.java.io :as io]
            [crux.checkpoint :as cp]
            [crux.tx :as tx])
  (:import (java.util UUID)))

(t/use-fixtures :once s3t/with-s3-client)

(t/deftest test-checkpoint-store
  (with-open [sys (-> (sys/prep-system {:store {:crux/module `s3c/->cp-store
                                                :configurator `s3t/->configurator
                                                :bucket s3t/test-s3-bucket
                                                :prefix (str "s3-cp-" (UUID/randomUUID))}})
                      (sys/start-system))]
    (fix/with-tmp-dir "s3-cp" [cp-dir]
      (let [{:keys [store]} sys
            src-dir (doto (io/file cp-dir "src")
                      (.mkdirs))
            cp-1 {::cp/cp-format ::foo-cp-format
                  :tx {::tx/tx-id 1}}
            cp-2 {::cp/cp-format ::foo-cp-format
                  :tx {::tx/tx-id 2}}]

        (t/testing "first checkpoint"
          (spit (io/file src-dir "hello.txt") "Hello world")

          (t/is (= cp-1
                   (-> (cp/upload-checkpoint store src-dir cp-1)
                       (select-keys #{::cp/cp-format :tx}))))

          (t/is (empty? (cp/available-checkpoints store {::cp/cp-format ::bar-cp-format})))

          (let [dest-dir (io/file cp-dir "dest")
                cps (cp/available-checkpoints store {::cp/cp-format ::foo-cp-format})]
            (t/is (= [cp-1]
                     (->> (cp/available-checkpoints store {::cp/cp-format ::foo-cp-format})
                          (map #(select-keys % #{::cp/cp-format :tx})))))
            (cp/download-checkpoint store (first cps) dest-dir)
            (t/is (= "Hello world"
                     (slurp (io/file dest-dir "hello.txt"))))))

        (t/testing "second checkpoint"
          (spit (io/file src-dir "ivan.txt") "Hey Ivan!")

          (t/is (= cp-2
                   (-> (cp/upload-checkpoint store src-dir cp-2)
                       (select-keys #{::cp/cp-format :tx}))))

          (t/is (empty? (cp/available-checkpoints store {::cp/cp-format ::bar-cp-format})))

          (let [dest-dir (io/file cp-dir "dest-2")
                cps (cp/available-checkpoints store {::cp/cp-format ::foo-cp-format})]
            (t/is (= [cp-2 cp-1]
                     (->> (cp/available-checkpoints store {::cp/cp-format ::foo-cp-format})
                          (map #(select-keys % #{::cp/cp-format :tx})))))
            (cp/download-checkpoint store (first cps) dest-dir)
            (t/is (= "Hello world"
                     (slurp (io/file dest-dir "hello.txt"))))

            (t/is (= "Hey Ivan!"
                     (slurp (io/file dest-dir "ivan.txt"))))))))))

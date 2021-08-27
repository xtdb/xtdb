(ns xtdb.fixtures.checkpoint-store
  (:require [xtdb.fixtures :as fix]
            [clojure.java.io :as io]
            [xtdb.checkpoint :as cp]
            [xtdb.tx :as tx]
            [clojure.test :as t]))

(defn test-checkpoint-store [cp-store]
  (fix/with-tmp-dirs #{local-dir}
    (let [src-dir (doto (io/file local-dir "src")
                    (.mkdirs))
          cp-1 {::cp/cp-format ::foo-cp-format
                :tx {:xt/tx-id 1}}
          cp-2 {::cp/cp-format ::foo-cp-format
                :tx {:xt/tx-id 2}}]

      (t/testing "first checkpoint"
        (spit (io/file src-dir "hello.txt") "Hello world")

        (t/is (= cp-1
                 (-> (cp/upload-checkpoint cp-store src-dir cp-1)
                     (select-keys #{::cp/cp-format :tx}))))

        (t/is (empty? (cp/available-checkpoints cp-store {::cp/cp-format ::bar-cp-format})))

        (let [dest-dir (io/file local-dir "dest")
              cps (cp/available-checkpoints cp-store {::cp/cp-format ::foo-cp-format})]
          (t/is (= [cp-1]
                   (->> (cp/available-checkpoints cp-store {::cp/cp-format ::foo-cp-format})
                        (map #(select-keys % #{::cp/cp-format :tx})))))
          (cp/download-checkpoint cp-store (first cps) dest-dir)
          (t/is (= "Hello world"
                   (slurp (io/file dest-dir "hello.txt"))))))

      (t/testing "second checkpoint"
        (spit (io/file src-dir "ivan.txt") "Hey Ivan!")

        (t/is (= cp-2
                 (-> (cp/upload-checkpoint cp-store src-dir cp-2)
                     (select-keys #{::cp/cp-format :tx}))))

        (t/is (empty? (cp/available-checkpoints cp-store {::cp/cp-format ::bar-cp-format})))

        (let [dest-dir (io/file local-dir "dest-2")
              cps (cp/available-checkpoints cp-store {::cp/cp-format ::foo-cp-format})]
          (t/is (= [cp-2 cp-1]
                   (->> (cp/available-checkpoints cp-store {::cp/cp-format ::foo-cp-format})
                        (map #(select-keys % #{::cp/cp-format :tx})))))
          (cp/download-checkpoint cp-store (first cps) dest-dir)
          (t/is (= "Hello world"
                   (slurp (io/file dest-dir "hello.txt"))))

          (t/is (= "Hey Ivan!"
                   (slurp (io/file dest-dir "ivan.txt")))))))))

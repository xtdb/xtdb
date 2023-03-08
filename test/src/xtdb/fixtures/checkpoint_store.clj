(ns xtdb.fixtures.checkpoint-store
  (:require [xtdb.api :as xt]
            [xtdb.fixtures :as fix]
            [xtdb.checkpoint :as cp]
            [clojure.java.io :as io]
            [clojure.test :as t])
  (:import  [java.nio.file NoSuchFileException CopyOption Files FileVisitOption LinkOption Path]
            java.nio.file.attribute.FileAttribute))

(defn test-checkpoint-store [cp-store]
  (fix/with-tmp-dirs #{local-dir}
    (let [src-dir (doto (io/file local-dir "src")
                    (.mkdirs))
          cp-1 {::cp/cp-format ::foo-cp-format
                :tx {::xt/tx-id 1}}
          cp-2 {::cp/cp-format ::foo-cp-format
                :tx {::xt/tx-id 2}}]

      (t/testing "destination dir exists and contains a file"
        (let [dest-dir (io/file local-dir "dest")
              rogue-file (io/file dest-dir "hello.txt")
              cps (cp/available-checkpoints cp-store {::cp/cp-format ::foo-cp-format})]
          (try
            (.mkdirs dest-dir)
            (spit rogue-file "I should not be present")
            (t/is (thrown? IllegalArgumentException  (cp/download-checkpoint cp-store (first cps) dest-dir)))
            (finally
                  (io/delete-file rogue-file)))))

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

(defn- sync-path-throw [^Path from-root-path ^Path to-root-path]
  (doseq [^Path from-path (-> (Files/walk from-root-path Integer/MAX_VALUE (make-array FileVisitOption 0))
                              .iterator
                              iterator-seq)
          :let [to-path (.resolve to-root-path (str (.relativize from-root-path from-path)))]]
    (cond
      (Files/isDirectory from-path (make-array LinkOption 0))
      (Files/createDirectories to-path (make-array FileAttribute 0))

      (Files/isRegularFile from-path (make-array LinkOption 0))
      (Files/copy from-path to-path ^"[Ljava.nio.file.CopyOption;" (make-array CopyOption 0))))
  (throw (Exception. "broken!")))

(defn test-checkpoint-broken-store-failed-download
  [cp-store]
  (fix/with-tmp-dirs #{local-dir}
    (let [src-dir (doto (io/file local-dir "src")
                    (.mkdirs))
          cp-1 {::cp/cp-format ::foo-cp-format
                :tx {::xt/tx-id 1}}]

      (t/testing "no incomplete index dir after failed checkpoint download"
        (spit (io/file src-dir "hello.txt") "Hello world")

        (t/is (= cp-1
                 (-> (cp/upload-checkpoint cp-store src-dir cp-1)
                     (select-keys #{::cp/cp-format :tx}))))

        (let [dest-dir (io/file local-dir "dest")
              cps (cp/available-checkpoints cp-store {::cp/cp-format ::foo-cp-format})]
          (t/is (= [cp-1]
                   (->> (cp/available-checkpoints cp-store {::cp/cp-format ::foo-cp-format})
                        (map #(select-keys % #{::cp/cp-format :tx})))))
          (with-redefs [xtdb.checkpoint/sync-path sync-path-throw]
            (t/is (thrown? Exception (cp/download-checkpoint cp-store (first cps) dest-dir))))

          (t/is (thrown? NoSuchFileException (Files/list (.toPath dest-dir)))))))))

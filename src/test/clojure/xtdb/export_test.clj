(ns xtdb.export-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.export :as export]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.nio.file Path]
           [xtdb.api Xtdb$Config]
           xtdb.api.log.Log
           xtdb.api.storage.Storage))

(t/deftest test-export-snapshot
  (let [node-dir (util/->path "target/export-test")]
    (util/delete-dir node-dir)
    (with-open [node (tu/->local-node {:node-dir node-dir
                                       :rows-per-block 10})]
      (xt/execute-tx node [[:put-docs :foo {:xt/id 1, :name "Alice", :age 30}]
                           [:put-docs :foo {:xt/id 2, :name "Bob", :age 25}]
                           [:put-docs :foo {:xt/id 3, :name "Charlie", :age 35}]
                           [:put-docs :bar {:xt/id 1, :category "A"}]
                           [:put-docs :bar {:xt/id 2, :category "B"}]])

      (t/is (= 3 (count (xt/q node "SELECT * FROM foo"))))
      (t/is (= 2 (count (xt/q node "SELECT * FROM bar"))))

      (tu/flush-block! node))

    ;; compacted files won't be copied because they're not in the block
    ;; this is to inject some chaos :) 
    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (c/compact-all! node #xt/duration "PT1M"))

    (let [{:keys [^Path export-dir tables file-count]} (export/export-snapshot! (doto (Xtdb$Config.)
                                                                                  (.log (Log/localLog (.resolve node-dir "log")))
                                                                                  (.storage (Storage/local (.resolve node-dir "objects"))))
                                                                                "xtdb")]

      (t/is (= 3 tables))
      (t/is (= 10 file-count))

      (let [export-dir (-> node-dir
                           (.resolve "objects")
                           (.resolve (Storage/storageRoot Storage/VERSION 0))
                           (.resolve export-dir))
            export-root (-> export-dir
                            (.resolve (Storage/storageRoot Storage/VERSION 0)))]

        (t/is (.exists (.toFile export-root)))
        (t/is (.exists (.toFile (.resolve export-root "tables"))))
        (t/is (.exists (.toFile (.resolve export-root "blocks"))))

        (with-open [export-node (xtn/start-node {:log [:in-memory {:epoch 1}]
                                                 :storage [:local {:path export-dir}]})]

          (let [foo-results (xt/q export-node "SELECT _id, name, age FROM foo ORDER BY _id")
                bar-results (xt/q export-node "SELECT _id, category FROM bar ORDER BY _id")]

            (t/is (= [{:xt/id 1, :name "Alice", :age 30}
                      {:xt/id 2, :name "Bob", :age 25}
                      {:xt/id 3, :name "Charlie", :age 35}]
                     foo-results))

            (t/is (= [{:xt/id 1, :category "A"}
                      {:xt/id 2, :category "B"}]
                     bar-results))

            (t/is (= [{:xt/id 1, :name "Alice", :category "A"}
                      {:xt/id 2, :name "Bob", :category "B"}]
                     (xt/q export-node "SELECT f._id, f.name, b.category
                                        FROM foo f
                                        JOIN bar b ON f._id = b._id
                                        ORDER BY f._id")))))))))



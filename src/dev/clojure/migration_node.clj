(ns migration-node
  (:require [xtdb.api :as xt]
            [xtdb.util :as util]
            [xtdb.test-util :as tu]
            [xtdb.trie-catalog :as trie-cat]
            [xtdb.compactor :as c]))

(def migration-node-dir
  (util/->path "src/test/resources/xtdb/trie-catalog-test/all-files-node") )

(def doc {:foo (apply str (repeat 10 "bar"))})

(defn create-test-node-dir
  "This function creates a local directory at the above location with all files up
   to L3 historical and L2 current (which are the first levels we are partitioning at).
   File size is artifically bound by a kilobyte."
  [path]
  (util/delete-dir path)
  (binding [trie-cat/*file-size-target* 1024]
    (with-open [node (tu/->local-node {:node-dir path, :gc? false})]
      (let [total-size 4000]
        (doseq [batch (partition 1000 (range total-size))]
          (xt/execute-tx node [(into [:put-docs :docs] (for [i batch]
                                                         (assoc doc :xt/id i)))])

          (xt/execute-tx node [(into [:put-docs :docs] (for [i batch]
                                                         (-> doc
                                                             (assoc :xt/id (+ i total-size)
                                                                    :xt/valid-from #inst "2020-01-01"
                                                                    :xt/valid-to #inst "2020-02-01"))))])
          (tu/flush-block! node)))
      (c/compact-all! node nil)
      ;; this adds the new tries to the log
      (tu/flush-block! node))))

(comment
  (create-test-node-dir migration-node-dir)
  )

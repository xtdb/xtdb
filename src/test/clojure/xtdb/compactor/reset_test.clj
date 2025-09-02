(ns xtdb.compactor.reset-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.compactor.reset :as cr]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [xtdb.api Xtdb$Config]
           xtdb.api.log.Log
           xtdb.api.storage.Storage))

(t/deftest test-compactor-reset
  (let [node-root (util/->path "src/test/resources/xtdb/compactor-reset-node")
        tmp-root (util/->path "src/test/resources/xtdb/compactor-reset-node-tmp")]
    (util/delete-dir tmp-root)
    (try
      (util/copy-dir node-root tmp-root)

      (cr/reset-compactor! (doto (Xtdb$Config.)
                             (.log (Log/localLog (.resolve tmp-root "log")))
                             (.storage (Storage/local (.resolve tmp-root "objects"))))
                           "xtdb"
                           {:dry-run? false})

      (with-open [node (tu/->local-node {:node-dir tmp-root, :compactor-threads 0})]
        (t/is (= [{:xt/id 1, :name "foo", :bar-name "baz"}]
                 (xt/q node "SELECT f._id, f.name, b.name AS bar_name
                       FROM foo f
                       JOIN bar b ON f._id = b._id"))))

      (with-open [node (tu/->local-node {:node-dir tmp-root})]
        (c/compact-all! node #xt/duration "PT5M")
        (t/is (= [{:xt/id 1, :name "foo", :bar-name "baz"}]
                 (xt/q node "SELECT f._id, f.name, b.name AS bar_name
                       FROM foo f
                       JOIN bar b ON f._id = b._id"))))

      (finally
        (util/delete-dir tmp-root)))))

(ns xtdb.empty-struct-l0s-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.compactor.reset :as cr]
            [xtdb.empty-struct-l0s :as l0s]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [xtdb.api Xtdb$Config]
           xtdb.api.log.Log
           xtdb.api.storage.Storage
           xtdb.database.Database$Config))

(t/deftest test-l0s-migration
  (let [node-root (util/->path "src/test/resources/xtdb/l0s-mig-node")
        tmp-root (util/->path "src/test/resources/xtdb/l0s-mig-node-tmp")]
    (util/delete-dir tmp-root)
    (try
      (util/copy-dir node-root tmp-root)

      (let [conf (doto (Xtdb$Config.)
                   (.database "xtdb" (Database$Config. (Log/localLog (.resolve tmp-root "log"))
                                                       (Storage/localStorage (.resolve tmp-root "objects")))))]
        (cr/reset-compactor! conf "xtdb" {:dry-run? false})
        (l0s/migrate-l0s! conf "xtdb" "foo" {:dry-run? false}))

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

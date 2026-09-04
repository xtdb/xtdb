(ns xtdb.stats-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.db-catalog :as db]
            [xtdb.logical-plan :as lp]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import (xtdb.query IQuerySource$QueryCatalog)))

(t/use-fixtures :each tu/with-allocator)

(deftest test-scan
  (with-open [node (xtn/start-node (assoc tu/*node-opts* :indexer {:rows-per-block 2}))]
    (let [scan-emitter (:scan-emitter (.getQuerySource (util/node-base node)))
          ^IQuerySource$QueryCatalog db-cat (db/<-node node)]
      (xt/submit-tx node [[:put-docs :foo {:xt/id "foo1"}]
                          [:put-docs :bar {:xt/id "bar1"}]])

      (xt/submit-tx node [[:put-docs :foo {:xt/id "foo2"}]
                          [:put-docs :baz {:xt/id "baz1"}]])

      (xt/execute-tx node [[:put-docs :foo {:xt/id "foo3"}]
                           [:put-docs :bar {:xt/id "bar2"}]])

      ;; these stats come from the table catalog, which only counts rows in *finished* blocks — and
      ;; nothing finishes the block holding the last tx's rows, since that's triggered by later indexing
      (tu/flush-block! node)

      (let [emit-opts {:scan-emitter scan-emitter
                       :db-cat db-cat
                       :dbs (into {} (.resolveDbs db-cat))}]

        (t/is (= {:row-count 3}
                 (:stats (lp/emit-expr '{:op :scan, :opts {:db-name "xtdb", :table #xt/table foo, :columns [[:column id]]}}
                                       emit-opts))))

        (t/is (= {:row-count 2}
                 (:stats (lp/emit-expr '{:op :scan, :opts {:db-name "xtdb", :table #xt/table bar, :columns [[:column id]]}}
                                       emit-opts))))))))

(deftest test-project
  (t/is (= {:row-count 5}
           (:stats
             (lp/emit-expr
               '{:op :project,
                 :projections [[:column foo]],
                 :relation
                 {:op ::tu/pages
                  :stats {:row-count 5}
                  :pages [[{:foo 1}]]}}
               {})))))

(deftest test-rename
  (t/is (= {:row-count 12}
           (:stats
             (lp/emit-expr
               '{:op :rename,
                 :columns {foo bar}
                 :relation
                 {:op ::tu/pages,
                  :stats {:row-count 12}
                  :pages [[{:foo 1}]]}}
               {})))))

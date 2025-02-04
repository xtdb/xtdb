(ns xtdb.stats-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.logical-plan :as lp]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]
            [xtdb.util :as util]))

(t/use-fixtures :each tu/with-allocator)

(deftest test-scan
  (with-open [node (xtn/start-node (merge tu/*node-opts* {:indexer {:rows-per-block 2}}))]
    (let [scan-emitter (util/component node :xtdb.operator.scan/scan-emitter)]
      (xt/submit-tx node [[:put-docs :foo {:xt/id "foo1"}]
                          [:put-docs :bar {:xt/id "bar1"}]])

      (xt/submit-tx node [[:put-docs :foo {:xt/id "foo2"}]
                          [:put-docs :baz {:xt/id "baz1"}]])

      (-> (xt/submit-tx node [[:put-docs :foo {:xt/id "foo3"}]
                              [:put-docs :bar {:xt/id "bar2"}]])
          (tu/then-await-tx node))

      (t/is (= {:row-count 3}
               (:stats (lp/emit-expr '{:op :scan, :scan-opts {:table public/foo}, :columns [[:column id]]}
                                     {:scan-fields {['foo 'id] (types/col-type->field :utf8)},
                                      :scan-emitter scan-emitter}))))

      (t/is (= {:row-count 2}
               (:stats (lp/emit-expr '{:op :scan, :scan-opts {:table public/bar}, :columns [[:column id]]}
                                     {:scan-fields {['bar 'id] (types/col-type->field :utf8)},
                                      :scan-emitter scan-emitter})))))))

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

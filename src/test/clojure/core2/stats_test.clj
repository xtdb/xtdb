(ns core2.stats-test
  (:require [clojure.test :as t :refer [deftest]]
            [core2.datalog :as c2]
            [core2.logical-plan :as lp]
            [core2.node :as node]
            [core2.test-util :as tu]
            [core2.util :as util]))

(t/use-fixtures :each tu/with-allocator)

(deftest test-scan
  (with-open [node (node/start-node {:core2/live-chunk {:rows-per-block 2 , :rows-per-chunk 2}})]
    (let [scan-emitter (util/component node :core2.operator.scan/scan-emitter)]
      (-> (c2/submit-tx node [[:put {:id "foo1" :_table "foo"}]
                              [:put {:id "bar1" :_table "bar"}]])
          (tu/then-await-tx node))

      (-> (c2/submit-tx node [[:put {:id "foo2" :_table "foo"}]
                              [:put {:id "baz1" :_table "baz"}]])
          (tu/then-await-tx node))

      (-> (c2/submit-tx node [[:put {:id "foo3" :_table "foo"}]
                              [:put {:id "bar2" :_table "bar"}]])
          (tu/then-await-tx node))

      (t/is (= {:row-count 3}
               (:stats (lp/emit-expr '{:op :scan, :table foo, :columns [[:column id]]}
                                     {:scan-col-types {['$ 'id] :utf8},
                                      :scan-emitter scan-emitter}))))

      (t/is (= {:row-count 2}
               (:stats (lp/emit-expr '{:op :scan, :table bar, :columns [[:column id]]}
                                     {:scan-col-types {['$ 'id] :utf8},
                                      :scan-emitter scan-emitter})))))))

(deftest test-project
  (t/is (= {:row-count 5}
           (:stats
             (lp/emit-expr
               '{:op :project,
                 :projections [[:column foo]],
                 :relation
                 {:op :core2.test-util/blocks,
                  :stats {:row-count 5}
                  :blocks [[{:foo 1}]]}}
               {})))))

(deftest test-rename
  (t/is (= {:row-count 12}
           (:stats
             (lp/emit-expr
               '{:op :rename,
                 :columns {foo bar}
                 :relation
                 {:op :core2.test-util/blocks,
                  :stats {:row-count 12}
                  :blocks [[{:foo 1}]]}}
               {})))))

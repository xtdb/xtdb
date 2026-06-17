(ns xtdb.duv-promotion-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]))

(t/use-fixtures :each tu/with-allocator tu/with-mock-clock tu/with-node)

(t/deftest type-named-struct-key-promotes-within-tx-5714
  (xt/execute-tx tu/*node* [[:put-docs :t
                             {:xt/id 0, :data {:utf8 1}}
                             {:xt/id 1, :data {:utf8 "x"}}]])
  (t/is (= #{{:xt/id 0, :data {:utf8 1}} {:xt/id 1, :data {:utf8 "x"}}}
           (set (xt/q tu/*node* "SELECT _id, data FROM t ORDER BY _id")))))

(t/deftest cross-tx-promotion-doesnt-wedge-ingestion-5714
  (let [node-dir (util/->path "target/duv-promotion/cross-tx")]
    (util/delete-dir node-dir)
    (binding [c/*ignore-signal-block?* true]
      (util/with-open [node (tu/->local-node {:node-dir node-dir})]
        (xt/execute-tx node [[:put-docs :t {:xt/id 1, :data {:utf8 1}}]])
        (xt/execute-tx node [[:put-docs :t {:xt/id 2, :data {:utf8 "x"}}]])
        (t/is (= #{{:xt/id 1, :data {:utf8 1}} {:xt/id 2, :data {:utf8 "x"}}}
                 (set (xt/q node "SELECT _id, data FROM t ORDER BY _id")))))

      (util/with-open [node (tu/->local-node {:node-dir node-dir})]
        (t/is (= #{{:xt/id 1, :data {:utf8 1}} {:xt/id 2, :data {:utf8 "x"}}}
                 (set (xt/q node "SELECT _id, data FROM t ORDER BY _id"))))))))

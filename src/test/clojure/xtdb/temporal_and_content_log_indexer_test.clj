(ns xtdb.temporal-and-content-log-indexer-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.object-store :as os]
            [xtdb.test-json :as tj]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.time Duration)
           (xtdb.object_store ObjectStore)))

(t/use-fixtures :once tu/with-allocator)

(def txs
  [[[:put :hello {:xt/id #uuid "cb8815ee-85f7-4c61-a803-2ea1c949cf8d" :a 1}]
    [:put :world {:xt/id #uuid "424f5622-c826-4ded-a5db-e2144d665c38" :b 2}]]
   [[:delete :hello #uuid "cb8815ee-85f7-4c61-a803-2ea1c949cf8d"]
    [:put :world {:xt/id #uuid "424f5622-c826-4ded-a5db-e2144d665c38" :b 3}]]
   [[:evict :world #uuid "424f5622-c826-4ded-a5db-e2144d665c38"]]
   ;; sql
   [[:sql "INSERT INTO foo (xt$id, bar, toto) VALUES (1, 1, 'toto')"]
    [:sql "UPDATE foo SET bar = 2 WHERE foo.xt$id = 1"]
    [:sql "DELETE FROM foo WHERE foo.bar = 2"]
    [:sql "INSERT INTO foo (xt$id, bar) VALUES (2, 2)"]]
   ;; sql evict
   [[:sql "ERASE FROM foo WHERE foo.xt$id = 2"]]
   ;; abort
   [[:sql "INSERT INTO foo (xt$id, xt$valid_from, xt$valid_to) VALUES (1, DATE '2020-01-01', DATE '2019-01-01')"]]])

(t/deftest can-build-temporal-and-content-log
  (let [node-dir (util/->path "target/can-build-temporal-and-content-log")]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (let [^ObjectStore os  (tu/component node ::os/file-system-object-store)]

        (let [last-tx-key (last (for [tx-ops txs] (xt/submit-tx node tx-ops)))]
          (tu/then-await-tx last-tx-key node (Duration/ofSeconds 2)))

        (tu/finish-chunk! node)

        (let [objects-list (->> (.listObjects os "chunk-00/") (filter #(str/ends-with? % "/temporal-log.arrow")))]
          (t/is (= 1 (count objects-list)))
          (t/is (= ["chunk-00/temporal-log.arrow"] objects-list)))

        (let [objects-list (->> (.listObjects os "chunk-00/") (filter #(str/ends-with? % "/content-log.arrow")))]
          (t/is (= 1 (count objects-list)))
          (t/is (= ["chunk-00/content-log.arrow"] objects-list))))

      (tj/check-json (.toPath (io/as-file (io/resource "xtdb/indexer-test/can-build-temporal-and-content-log") ) )
                     (.resolve node-dir "objects")
                     #".*-log.arrow"))))


(comment
  (do (util/delete-dir (.toPath (io/file "src/test/resources/xtdb/indexer-test/can-build-temporal-and-content-log/")))
      (.mkdirs (io/file "src/test/resources/xtdb/indexer-test/can-build-temporal-and-content-log/")))

  (require 'dev)

  (->> (dev/read-arrow-file (.toPath (io/file "target/can-build-temporal-and-content-log/objects/chunk-00/temporal-log.arrow")))
       (into [] cat))

  (->> (dev/read-arrow-file (.toPath (io/file "target/can-build-temporal-and-content-log/objects/chunk-00/content-log.arrow")))
       (into [] cat)))
